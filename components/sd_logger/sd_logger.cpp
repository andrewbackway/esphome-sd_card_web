#include "sd_logger.h"

#include "esphome/core/log.h"
#include "esphome/core/application.h"
#include "esphome/core/util.h"
#include "esphome/core/hal.h"

#include <sys/stat.h>
#include <dirent.h>

extern "C" {
// miniz (bundled) – single-file
#include "miniz.c"
}

namespace esphome {
namespace sdlog {

static const char *TAG = "sd_logger";

// ---------- Component lifecycle ----------

void SDLogger::setup() {
  ESP_LOGI(TAG, "Setup start");
  this->ensure_dir_();

  // subscribe to sensor state updates
  for (auto *s : this->sensors_) {
    if (s == nullptr) continue;
    s->add_on_state_callback([this, s](float state) {
      this->on_sensor_state(s, state);
    });
  }

  // start uploader task
  xTaskCreatePinnedToCore(&SDLogger::upload_task_trampoline_, "sdlog_upl",
                          6144, this, 5, &this->upload_task_handle_,
                          APP_CPU_NUM);

  ESP_LOGI(TAG, "Setup done: url=%s gzip=%s path=%s upload_interval=%u ms",
           this->upload_url_.c_str(), this->gzip_ ? "true" : "false",
           this->log_path_.c_str(), this->upload_interval_ms_);
}

void SDLogger::loop() {
  // schedule background upload on cadence (non-blocking)
  const uint64_t now = this->now_ms_();
  if (!this->background_busy_ &&
      (now - this->last_background_upload_ms_) >= this->upload_interval_ms_) {
    this->last_background_upload_ms_ = now;
    // nudge the task by setting backoff to 0
    this->backoff_ms_ = 0;
  }
}

void SDLogger::dump_config() {
  ESP_LOGCONFIG(TAG, "SD Logger:");
  ESP_LOGCONFIG(TAG, "  Upload URL: %s", this->upload_url_.c_str());
  ESP_LOGCONFIG(TAG, "  Log path: %s", this->log_path_.c_str());
  ESP_LOGCONFIG(TAG, "  Upload interval: %u ms", this->upload_interval_ms_);
  ESP_LOGCONFIG(TAG, "  Backoff initial/max: %u/%u ms", this->backoff_initial_ms_, this->backoff_max_ms_);
  ESP_LOGCONFIG(TAG, "  Gzip: %s", this->gzip_ ? "ENABLED (miniz bundled)" : "DISABLED");
  ESP_LOGCONFIG(TAG, "  Tracked sensors: %u", (unsigned) this->sensors_.size());
}

// ---------- Event path ----------

void SDLogger::on_sensor_state(sensor::Sensor *s, float state) {
  const uint64_t now = this->now_ms_();

  // throttle per sensor 30s
  auto it = last_send_ms_.find(s);
  if (it != last_send_ms_.end() && (now - it->second) < SENSOR_MIN_PERIOD_MS) {
    ESP_LOGV(TAG, "Throttle %s: %u ms left", s->get_name().c_str(),
             (unsigned) (SENSOR_MIN_PERIOD_MS - (now - it->second)));
    return;
  }
  last_send_ms_[s] = now;

  // require time sync to avoid garbage timestamps
  if (!this->time_synced_()) {
    ESP_LOGW(TAG, "Skipping log, time not yet synchronized");
    return;
  }

  // CSV line: epoch, name, value\n
  const uint32_t epoch = this->time_->now().timestamp;
  char buf[96];
  int n = snprintf(buf, sizeof(buf), "%u,%s,%.6f\n", epoch, s->get_name().c_str(), state);
  if (n <= 0) return;
  std::string line(buf, n);

  if (this->is_online_()) {
    // enqueue for live upload (non-blocking)
    std::lock_guard<std::mutex> lk(this->live_q_mtx_);
    if (this->live_q_.size() >= SDLOG_MAX_LIVE_QUEUE)
      this->live_q_.pop_front();
    this->live_q_.push_back(LiveLine{line, now});
  } else {
    // offline: append to rotating file
    this->rotate_file_if_needed_(now);
    if (!this->append_line_to_file_(line)) {
      ESP_LOGE(TAG, "Failed to append to file");
    }
  }
}

// ---------- Upload task ----------

void SDLogger::upload_task_trampoline_(void *self) {
  static_cast<SDLogger*>(self)->upload_task_();
}

void SDLogger::upload_task_() {
  ESP_LOGI(TAG, "Upload task started");
  uint32_t sleep_ms = 250;

  while (true) {
    bool did_work = false;

    // 1) Prioritize live queue when online
    if (this->is_online_()) {
      LiveLine ll;
      {
        std::lock_guard<std::mutex> lk(this->live_q_mtx_);
        if (!this->live_q_.empty()) {
          ll = this->live_q_.front();
          this->live_q_.pop_front();
        }
      }
      if (!ll.csv.empty()) {
        bool ok = false;
        if (this->gzip_) {
          std::string gz;
          if (this->gzip_buffer_(reinterpret_cast<const uint8_t*>(ll.csv.data()), ll.csv.size(), gz)) {
            ok = this->http_post_lines_(gz, /*gzipped=*/true);
          }
        } else {
          ok = this->http_post_lines_(ll.csv, /*gzipped=*/false);
        }
        if (!ok) {
          // push back to offline file to avoid loss
          this->rotate_file_if_needed_(this->now_ms_());
          this->append_line_to_file_(ll.csv);
          // trigger backoff for HTTP errors
          if (this->backoff_ms_ == 0) this->backoff_ms_ = this->backoff_initial_ms_;
        } else {
          this->backoff_ms_ = 0; // success resets backoff
        }
        did_work = true;
      }
    }

    // 2) Background: upload one file when interval elapsed or backoff=0 and online
    const uint64_t now = this->now_ms_();
    if (this->is_online_()) {
      bool interval_due = (now - this->last_background_upload_ms_) >= this->upload_interval_ms_;
      bool unlocked = (this->backoff_ms_ == 0);
      if (interval_due || unlocked) {
        this->background_busy_ = true;
        std::string oldest;
        this->scan_and_queue_oldest_file_(oldest);
        if (!oldest.empty()) {
          // upload file
          FILE *fp = fopen(oldest.c_str(), "rb");
          if (!fp) {
            ESP_LOGE(TAG, "Failed to open %s", oldest.c_str());
          } else {
            fseek(fp, 0, SEEK_END);
            const long sz = ftell(fp);
            fseek(fp, 0, SEEK_SET);
            bool ok = this->http_post_file_(fp, (size_t) sz, this->gzip_);
            fclose(fp);
            if (ok) {
              remove(oldest.c_str());
              ESP_LOGI(TAG, "Uploaded and removed %s", oldest.c_str());
              this->backoff_ms_ = 0;
              this->last_background_upload_ms_ = now;
            } else {
              ESP_LOGW(TAG, "Upload failed for %s", oldest.c_str());
              // backoff
              this->backoff_ms_ = this->backoff_ms_ == 0 ? this->backoff_initial_ms_
                                                         : std::min(this->backoff_ms_ * 2, this->backoff_max_ms_);
            }
          }
          did_work = true;
        }
        this->background_busy_ = false;
      }
    }

    // 3) Sleep/backoff
    if (!did_work) {
      // apply gentle backoff if set
      uint32_t delay_ms = sleep_ms;
      if (this->backoff_ms_ > 0)
        delay_ms = std::max(delay_ms, this->backoff_ms_);
      vTaskDelay(pdMS_TO_TICKS(delay_ms));
    } else {
      // tiny yield to keep system responsive
      vTaskDelay(pdMS_TO_TICKS(5));
    }
  }
}

// ---------- Files ----------

bool SDLogger::ensure_dir_() {
  struct stat st{};
  if (stat(this->log_path_.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) return true;
  int rc = mkdir(this->log_path_.c_str(), 0755);
  if (rc != 0) {
    ESP_LOGE(TAG, "mkdir %s failed", this->log_path_.c_str());
    return false;
  }
  return true;
}

std::string SDLogger::current_file_path_() {
  return this->current_file_path_cache_;
}

void SDLogger::rotate_file_if_needed_(uint64_t now_ms) {
  const uint64_t now_s = now_ms / 1000ULL;
  const uint64_t window = (now_s / SDLOG_ROTATE_SECONDS) * SDLOG_ROTATE_SECONDS;
  if (window == this->current_window_start_s_ && !this->current_file_path_cache_.empty()) return;

  // Name: logs/YYYYMMDD_HHMMSS.csv (start time of 30s window)
  auto tm = this->time_->now();
  // adjust to window start
  uint32_t t0 = (uint32_t) window;
  char name[64];
  // We don't have strftime; format manually using RTC
  // If RTC doesn't match 'window' precisely, we fallback to tm of now.
  snprintf(name, sizeof(name), "%04d%02d%02d_%02d%02d%02d.csv",
           tm.year, tm.month, tm.day_of_month, tm.hour, tm.minute, tm.second);
  // Store full path
  this->current_file_path_cache_ = this->log_path_ + "/" + name;
  this->current_window_start_s_ = window;
}

bool SDLogger::append_line_to_file_(const std::string &line) {
  const std::string path = this->current_file_path_();
  FILE *fp = fopen(path.c_str(), "ab");
  if (!fp) {
    ESP_LOGE(TAG, "fopen(%s) failed", path.c_str());
    return false;
  }
  size_t w = fwrite(line.data(), 1, line.size(), fp);
  fclose(fp);
  return w == line.size();
}

void SDLogger::scan_and_queue_oldest_file_(std::string &out_path) {
  out_path.clear();
  DIR *dir = opendir(this->log_path_.c_str());
  if (!dir) return;

  time_t best_time = 0;
  std::string best_path;
  struct dirent *ent;
  while ((ent = readdir(dir)) != nullptr) {
    if (ent->d_name[0] == '.') continue;
    std::string p = this->log_path_ + "/" + ent->d_name;
    struct stat st{};
    if (stat(p.c_str(), &st) != 0) continue;
    if (!S_ISREG(st.st_mode)) continue;
    if (best_path.empty() || st.st_mtime < best_time) {
      best_time = st.st_mtime;
      best_path = p;
    }
  }
  closedir(dir);
  out_path = best_path;
}

// ---------- HTTP ----------

static esp_err_t _http_evt(esp_http_client_event_t *evt) {
  // we could add verbose logging here if needed
  return ESP_OK;
}

bool SDLogger::http_post_lines_(const std::string &payload, bool gzipped) {
  if (this->upload_url_.empty()) return false;

  esp_http_client_config_t cfg = {};
  cfg.url = this->upload_url_.c_str();
  cfg.method = HTTP_METHOD_POST;
  cfg.timeout_ms = 10000;
  cfg.event_handler = _http_evt;

  // NOTE: HTTPS without validation (best-effort). Some IDF builds still want CA;
  // leaving cert_pem/crt_bundle_attach null typically disables validation.
  cfg.skip_cert_common_name_check = true;

  esp_http_client_handle_t client = esp_http_client_init(&cfg);
  if (!client) return false;

  esp_http_client_set_header(client, "Content-Type", "text/csv");
  if (gzipped) esp_http_client_set_header(client, "Content-Encoding", "gzip");
  if (!this->bearer_token_.empty())
    esp_http_client_set_header(client, "Authorization", this->bearer_token_.c_str());

  esp_err_t err = esp_http_client_open(client, payload.size());
  if (err != ESP_OK) {
    ESP_LOGW(TAG, "http open err=%d", (int) err);
    esp_http_client_cleanup(client);
    return false;
  }

  int w = esp_http_client_write(client, payload.data(), payload.size());
  if (w < 0 || (size_t) w != payload.size()) {
    ESP_LOGW(TAG, "http write failed");
    esp_http_client_close(client);
    esp_http_client_cleanup(client);
    return false;
  }

  int code = esp_http_client_fetch_headers(client);
  if (code < 0) {
    ESP_LOGW(TAG, "fetch headers failed");
  }
  int status = esp_http_client_get_status_code(client);
  esp_http_client_close(client);
  esp_http_client_cleanup(client);

  bool ok = status >= 200 && status < 300;
  if (!ok) ESP_LOGW(TAG, "http status %d", status);
  return ok;
}

bool SDLogger::http_post_file_(FILE *fp, size_t size, bool gzipped) {
  if (this->upload_url_.empty()) return false;

  esp_http_client_config_t cfg = {};
  cfg.url = this->upload_url_.c_str();
  cfg.method = HTTP_METHOD_POST;
  cfg.timeout_ms = 15000;
  cfg.event_handler = _http_evt;
  cfg.skip_cert_common_name_check = true;

  esp_http_client_handle_t client = esp_http_client_init(&cfg);
  if (!client) return false;

  esp_http_client_set_header(client, "Content-Type", "text/csv");
  if (gzipped) esp_http_client_set_header(client, "Content-Encoding", "gzip");
  if (!this->bearer_token_.empty())
    esp_http_client_set_header(client, "Authorization", this->bearer_token_.c_str());

  // stream file in chunks (avoid loading whole file)
  const size_t CHUNK = 2048;
  std::string out_buf;
  bool use_gz_stream = false;

  // If gzip is enabled, we’ll naïvely read full file into memory and gzip once
  // to keep code simple/robust. For large files you may want a streaming gzip.
  if (gzipped) {
    std::string file_buf;
    file_buf.resize(size);
    if (fread(file_buf.data(), 1, size, fp) != size) {
      ESP_LOGW(TAG, "read file failed");
      esp_http_client_cleanup(client);
      return false;
    }
    std::string gz;
    if (!this->gzip_buffer_(reinterpret_cast<const uint8_t*>(file_buf.data()), file_buf.size(), gz)) {
      ESP_LOGW(TAG, "gzip fail");
      esp_http_client_cleanup(client);
      return false;
    }
    if (esp_http_client_open(client, gz.size()) != ESP_OK) {
      esp_http_client_cleanup(client);
      return false;
    }
    if (esp_http_client_write(client, gz.data(), gz.size()) != (int) gz.size()) {
      esp_http_client_close(client);
      esp_http_client_cleanup(client);
      return false;
    }
  } else {
    if (esp_http_client_open(client, size) != ESP_OK) {
      esp_http_client_cleanup(client);
      return false;
    }
    size_t left = size;
    std::vector<char> buf;
    buf.resize(CHUNK);
    while (left > 0) {
      size_t rd = fread(buf.data(), 1, std::min(left, CHUNK), fp);
      if (rd == 0) break;
      int w = esp_http_client_write(client, buf.data(), rd);
      if (w < 0 || (size_t) w != rd) {
        ESP_LOGW(TAG, "write chunk failed");
        esp_http_client_close(client);
        esp_http_client_cleanup(client);
        return false;
      }
      left -= rd;
      // yield a bit
      vTaskDelay(pdMS_TO_TICKS(1));
    }
  }

  int code = esp_http_client_fetch_headers(client);
  if (code < 0) {
    ESP_LOGW(TAG, "fetch headers failed");
  }
  int status = esp_http_client_get_status_code(client);
  esp_http_client_close(client);
  esp_http_client_cleanup(client);

  bool ok = status >= 200 && status < 300;
  if (!ok) ESP_LOGW(TAG, "http status %d", status);
  return ok;
}

// ---------- gzip (miniz) ----------

bool SDLogger::gzip_buffer_(const uint8_t *in, size_t in_len, std::string &out) {
  mz_ulong bound = mz_compressBound(in_len);
  out.resize(bound);
  mz_ulong out_len = bound;
  int r = mz_compress2(reinterpret_cast<unsigned char*>(&out[0]), &out_len,
                       reinterpret_cast<const unsigned char*>(in), in_len, MZ_BEST_SPEED);
  if (r != MZ_OK) {
    ESP_LOGW(TAG, "miniz compress err=%d", r);
    return false;
  }
  out.resize(out_len);
  return true;
}

// ---------- time ----------

bool SDLogger::time_synced_() const {
  if (!this->time_) return false;
  auto t = this->time_->now();
  return t.is_valid();
}

}  // namespace sdlog
}  // namespace esphome
