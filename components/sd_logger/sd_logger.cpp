#include "sd_logger.h"

#include "esphome/core/application.h"
#include "esphome/core/helpers.h"
#include "esphome/core/version.h"
#include "esphome/components/wifi/wifi_component.h"

#include "esp_http_client.h"
#include "esp_timer.h"
#include "esp_task_wdt.h"
#include "esp_system.h"
#include "esp_log.h"
#include "sys/unistd.h"
#include "sys/stat.h"
#include <dirent.h>

// miniz (bundled C) — assumed added in component sources
extern "C" {
#include "miniz.c"
}

namespace esphome {
namespace sdlog {

// ===== Helpers =====

int64_t SDLogger::now_ms_() {
  // Requires SNTP; use time() for wall clock if set, fallback to esp_timer
  struct timeval tv{};
  if (gettimeofday(&tv, nullptr) == 0 && tv.tv_sec > 1500000000) { // ~2017+
    return int64_t(tv.tv_sec) * 1000 + (tv.tv_usec / 1000);
  }
  // Fallback monotonic (not used for filenames)
  return esp_timer_get_time() / 1000;
}

bool SDLogger::time_synced_() {
  struct timeval tv{};
  if (gettimeofday(&tv, nullptr) != 0) return false;
  return tv.tv_sec > 1500000000;
}

bool SDLogger::is_online_() const {
  if (wifi::global_wifi_component == nullptr) return false;
  if (!wifi::global_wifi_component->is_connected()) return false;
  // We assume DNS/route is fine; HTTP failures will promote fallback.
  return true;
}

bool SDLogger::network_ready_() const {
  return is_online_();
}

// ===== Component lifecycle =====

void SDLogger::setup() {
  ESP_LOGI(TAG, "setup() starting");
  ESP_LOGI(TAG, "ESPHome %s", ESPHOME_VERSION);
  log_dir_ok_ = ensure_log_dir_();
  if (!log_dir_ok_) {
    ESP_LOGE(TAG, "Failed to ensure log directory: %s", log_path_.c_str());
  }

  // Live queue for non-blocking uploads
  live_queue_ = xQueueCreate(32, sizeof(Sample));
  if (!live_queue_) {
    ESP_LOGE(TAG, "Failed to create live queue");
  }

  // Start tasks
  start_live_task_if_needed_();
  start_backlog_task_if_needed_();

  ESP_LOGI(TAG, "setup() done");
}

void SDLogger::dump_config() {
  ESP_LOGCONFIG(TAG, "SD Logger:");
  ESP_LOGCONFIG(TAG, "  Upload URL: %s", upload_url_.c_str());
  ESP_LOGCONFIG(TAG, "  Log path: %s", log_path_.c_str());
  ESP_LOGCONFIG(TAG, "  Upload interval: %u ms", upload_interval_ms_);
  ESP_LOGCONFIG(TAG, "  Backoff initial/max: %u/%u ms", backoff_.initial_ms, backoff_.max_ms);
  ESP_LOGCONFIG(TAG, "  Gzip: %s", gzip_enabled_ ? "ENABLED (miniz bundled)" : "DISABLED");
}

void SDLogger::loop() {
  // Periodically kick backlog uploader even if no events
  const int64_t now = now_ms_();
  if (now - last_upload_kick_ms_ > upload_interval_ms_) {
    last_upload_kick_ms_ = now;
    if (network_ready_()) {
      start_backlog_task_if_needed_();
    }
  }

  // If we currently have an open file, enforce 30s window rotation.
  if (cur_file_) {
    maybe_rotate_file_(now);
  }
}

// ===== Sensor ingress =====

void SDLogger::on_sensor_update(const std::string &name, float value) {
  const int64_t now = now_ms_();

  // enforce live throttle per sensor
  auto it = last_live_sent_ms_.find(name);
  if (it != last_live_sent_ms_.end()) {
    if (now - it->second < (int64_t) live_throttle_ms_) {
      ESP_LOGD(TAG, "Throttle live upload: %s (%.3f), skipped", name.c_str(), value);
      return;
    }
  }

  Sample s{ name, value, now };
  if (network_ready_()) {
    // try live path
    if (live_queue_) {
      if (xQueueSend(live_queue_, &s, 0) == pdTRUE) {
        last_live_sent_ms_[name] = now;
        ESP_LOGD(TAG, "Enqueued LIVE sample: %s=%.3f @%lld", name.c_str(), value, (long long)now);
        return;
      } else {
        ESP_LOGW(TAG, "Live queue full, falling back to file");
      }
    }
  }

  // offline/file fallback
  fallback_to_file_(s, now);
}

// ===== File management =====

bool SDLogger::ensure_log_dir_() {
  struct stat st{};
  if (stat(log_path_.c_str(), &st) == 0) {
    if (S_ISDIR(st.st_mode)) return true;
    ESP_LOGE(TAG, "Log path exists but is not a directory");
    return false;
  }
  if (mkdir(log_path_.c_str(), 0775) == 0) {
    ESP_LOGI(TAG, "Created log directory: %s", log_path_.c_str());
    return true;
  }
  ESP_LOGE(TAG, "mkdir failed for %s", log_path_.c_str());
  return false;
}

std::string SDLogger::make_filename_(int64_t window_start_ms) const {
  time_t secs = window_start_ms / 1000;
  struct tm tm{};
  localtime_r(&secs, &tm);
  char buf[64];
  strftime(buf, sizeof(buf), "%Y-%m-%d_%H-%M-%S", &tm);
  std::string base = log_path_ + "/" + std::string(buf) + ".csv";
  return base;
}

bool SDLogger::open_file_for_window_(int64_t window_start_ms) {
  if (!log_dir_ok_) return false;
  if (!time_synced_()) {
    if (!warned_no_time_) {
      ESP_LOGW(TAG, "Time not synchronized; deferring file creation");
      warned_no_time_ = true;
    }
    return false;
  }
  warned_no_time_ = false;

  cur_file_path_ = make_filename_(window_start_ms);
  cur_file_ = fopen(cur_file_path_.c_str(), "a");
  if (!cur_file_) {
    ESP_LOGE(TAG, "Failed to open %s", cur_file_path_.c_str());
    return false;
  }
  cur_window_start_ms_ = window_start_ms;
  ESP_LOGD(TAG, "Opened log file: %s", cur_file_path_.c_str());
  return true;
}

void SDLogger::close_current_file_() {
  if (cur_file_) {
    fclose(cur_file_);
    ESP_LOGD(TAG, "Closed log file: %s", cur_file_path_.c_str().c_str());
  }
  cur_file_ = nullptr;
  cur_file_path_.clear();
  cur_window_start_ms_ = 0;
}

void SDLogger::maybe_rotate_file_(int64_t now_ms) {
  const int64_t window_start = (now_ms / 30000) * 30000;
  if (cur_window_start_ms_ == 0) return;
  if (window_start != cur_window_start_ms_) {
    ESP_LOGD(TAG, "30s window elapsed; rotating file");
    close_current_file_();
  }
}

bool SDLogger::append_csv_line_(const Sample &s) {
  if (!cur_file_) {
    // align window to 30s
    const int64_t window_start = (s.epoch_ms / 30000) * 30000;
    if (!open_file_for_window_(window_start)) {
      return false;
    }
  }
  // Format: ISO8601-ish time, sensor, value
  time_t secs = s.epoch_ms / 1000;
  struct tm tm{};
  localtime_r(&secs, &tm);
  char tbuf[32];
  strftime(tbuf, sizeof(tbuf), "%Y-%m-%d %H:%M:%S", &tm);
  // write line
  int n = fprintf(cur_file_, "%s,%s,%.6f\n", tbuf, s.sensor.c_str(), (double) s.value);
  if (n <= 0) {
    ESP_LOGE(TAG, "fprintf failed for %s", cur_file_path_.c_str());
    return false;
  }
  fflush(cur_file_);
  return true;
}

void SDLogger::fallback_to_file_(const Sample &s, int64_t now_ms) {
  // rotate if needed
  maybe_rotate_file_(now_ms);
  if (!append_csv_line_(s)) {
    ESP_LOGE(TAG, "Failed to append CSV; sample dropped: %s", s.sensor.c_str());
  } else {
    ESP_LOGD(TAG, "Buffered to file: %s", cur_file_path_.c_str());
  }
}

// ===== Tasks =====

void SDLogger::start_live_task_if_needed_() {
  if (task_live_running_.load()) return;
  task_live_running_.store(true);
  ESP_LOGD(TAG, "Starting live upload task");
  xTaskCreatePinnedToCore(&SDLogger::live_task_trampoline_, "sdlog_live", 8192, this, 5, &live_task_, APP_CPU_NUM);
}

void SDLogger::start_backlog_task_if_needed_() {
  if (task_backlog_running_.load()) {
    // already running; nothing to do
    return;
  }
  task_backlog_running_.store(true);
  ESP_LOGD(TAG, "Starting backlog upload task");
  xTaskCreatePinnedToCore(&SDLogger::backlog_task_trampoline_, "sdlog_backlog", 10240, this, 4, &backlog_task_, APP_CPU_NUM);
}

void SDLogger::live_task_trampoline_(void *param) {
  // Remove ONLY this task from WDT supervision (Option 2)
  esp_task_wdt_delete(NULL);
  auto *self = static_cast<SDLogger *>(param);
  self->run_live_task_();
  self->task_live_running_.store(false);
  vTaskDelete(NULL);
}

void SDLogger::backlog_task_trampoline_(void *param) {
  // Remove ONLY this task from WDT supervision (Option 2)
  esp_task_wdt_delete(NULL);
  auto *self = static_cast<SDLogger *>(param);
  self->run_backlog_task_();
  self->task_backlog_running_.store(false);
  vTaskDelete(NULL);
}

void SDLogger::run_live_task_() {
  ESP_LOGD(TAG, "Live task started");
  Sample s{};
  while (true) {
    if (!live_queue_) {
      vTaskDelay(pdMS_TO_TICKS(200));
      continue;
    }
    if (xQueueReceive(live_queue_, &s, pdMS_TO_TICKS(500)) == pdTRUE) {
      if (!network_ready_()) {
        ESP_LOGD(TAG, "Network lost during live; buffering to file");
        fallback_to_file_(s, now_ms_());
        continue;
      }
      if (!upload_sample_http_(s)) {
        ESP_LOGW(TAG, "Live upload failed; buffering to file");
        fallback_to_file_(s, now_ms_());
      } else {
        ESP_LOGD(TAG, "Live upload OK: %s=%.3f", s.sensor.c_str(), s.value);
      }
    } else {
      // idle
      vTaskDelay(pdMS_TO_TICKS(50));
    }
  }
}

void SDLogger::run_backlog_task_() {
  ESP_LOGD(TAG, "Backlog task started");
  std::string path;
  while (network_ready_()) {
    if (!find_oldest_log_file_(path)) {
      // nothing to do
      vTaskDelay(pdMS_TO_TICKS(1000));
      continue;
    }
    ESP_LOGD(TAG, "Uploading backlog file: %s", path.c_str());
    bool ok = upload_file_http_(path);
    if (ok) {
      // delete file
      unlink(path.c_str());
      ESP_LOGD(TAG, "Deleted uploaded file: %s", path.c_str());
      backoff_.reset();
    } else {
      uint32_t wait_ms = backoff_.next();
      ESP_LOGW(TAG, "Backlog upload failed; backing off %u ms", wait_ms);
      vTaskDelay(pdMS_TO_TICKS(wait_ms));
    }
  }
  ESP_LOGD(TAG, "Backlog task exiting (network lost)");
}

// ===== Directory scan =====

static bool has_suffix(const std::string &s, const char *suf) {
  size_t ls = s.size(), lf = strlen(suf);
  if (lf > ls) return false;
  return s.compare(ls - lf, lf, suf) == 0;
}

bool SDLogger::find_oldest_log_file_(std::string &out_path) {
  out_path.clear();
  DIR *dir = opendir(log_path_.c_str());
  if (!dir) return false;
  std::string best;
  time_t best_time = LONG_MAX;

  for (struct dirent *de = readdir(dir); de != nullptr; de = readdir(dir)) {
    if (de->d_name[0] == '.') continue;
    std::string fn = log_path_ + "/" + de->d_name;
    if (!(has_suffix(fn, ".csv") || has_suffix(fn, ".csv.gz"))) continue;

    struct stat st{};
    if (stat(fn.c_str(), &st) != 0) continue;
    if (st.st_mtime < best_time) {
      best_time = st.st_mtime;
      best = fn;
    }
  }
  closedir(dir);
  if (best.empty()) return false;
  out_path = best;
  return true;
}

// ===== HTTP uploaders =====

static esp_err_t _http_event_handler(esp_http_client_event_t *evt) {
  return ESP_OK;
}

bool SDLogger::upload_sample_http_(const Sample &s) {
  // Prepare small JSON or CSV; you wanted CSV ok — we’ll send a single-line CSV body.
  // Format identical to file: "YYYY-MM-DD HH:MM:SS,sensor,value\n"
  char when[32];
  time_t secs = s.epoch_ms / 1000;
  struct tm tm{};
  localtime_r(&secs, &tm);
  strftime(when, sizeof(when), "%Y-%m-%d %H:%M:%S", &tm);

  char line[256];
  int len = snprintf(line, sizeof(line), "%s,%s,%.6f\n", when, s.sensor.c_str(), (double) s.value);
  if (len <= 0) return false;

  esp_http_client_config_t cfg{};
  cfg.url = upload_url_.c_str();
  cfg.event_handler = _http_event_handler;
  cfg.timeout_ms = 15000;
  cfg.transport_type = HTTP_TRANSPORT_OVER_SSL;
  cfg.skip_cert_common_name_check = true;   // DON'T validate CN
  cfg.crt_bundle_attach = NULL;             // DON'T validate certificate
  cfg.user_data = nullptr;

  esp_http_client_handle_t client = esp_http_client_init(&cfg);
  if (!client) return false;

  esp_http_client_set_method(client, HTTP_METHOD_POST);
  esp_http_client_set_header(client, "Content-Type", "text/csv");
  if (!bearer_token_.empty()) {
    std::string auth = "Bearer " + bearer_token_;
    esp_http_client_set_header(client, "Authorization", auth.c_str());
  }

  esp_err_t err = esp_http_client_open(client, len);
  if (err != ESP_OK) {
    ESP_LOGW(TAG, "http open failed: %s", esp_err_to_name(err));
    esp_http_client_cleanup(client);
    return false;
  }

  int w = esp_http_client_write(client, line, len);
  if (w != len) {
    ESP_LOGW(TAG, "http write short: %d/%d", w, len);
    esp_http_client_close(client);
    esp_http_client_cleanup(client);
    return false;
  }

  int code = esp_http_client_fetch_headers(client);
  int status = esp_http_client_get_status_code(client);
  esp_http_client_close(client);
  esp_http_client_cleanup(client);

  if (status >= 200 && status < 300) return true;
  ESP_LOGW(TAG, "http status for live: %d", status);
  return false;
}

bool SDLogger::gzip_file_to_temp_(const std::string &src, std::string &out_tmp_path, size_t &out_size) {
  out_tmp_path.clear();
  out_size = 0;

  // Read whole file to memory (files should be small, 30s slices)
  FILE *f = fopen(src.c_str(), "rb");
  if (!f) return false;
  fseek(f, 0, SEEK_END);
  long sz = ftell(f);
  fseek(f, 0, SEEK_SET);
  if (sz <= 0) { fclose(f); return false; }

  std::vector<uint8_t> in;
  in.resize(sz);
  if (fread(in.data(), 1, sz, f) != (size_t) sz) { fclose(f); return false; }
  fclose(f);

  // Compress with miniz
  mz_ulong bound = compressBound((mz_ulong) sz);
  std::vector<uint8_t> out;
  out.resize(bound);

  mz_ulong outlen = bound;
  int rc = compress2(out.data(), &outlen, in.data(), sz, MZ_BEST_COMPRESSION);
  if (rc != Z_OK) {
    ESP_LOGW(TAG, "miniz compress2 rc=%d", rc);
    return false;
  }

  // write temp .gz
  out_tmp_path = src + ".gz.tmp";
  FILE *g = fopen(out_tmp_path.c_str(), "wb");
  if (!g) return false;
  if (fwrite(out.data(), 1, outlen, g) != outlen) {
    fclose(g); unlink(out_tmp_path.c_str());
    return false;
  }
  fclose(g);
  out_size = (size_t) outlen;
  return true;
}

bool SDLogger::upload_file_http_(const std::string &path) {
  std::string send_path = path;
  std::string temp_path;
  size_t gz_size = 0;

  if (gzip_enabled_ && !has_suffix(path, ".gz")) {
    if (!gzip_file_to_temp_(path, temp_path, gz_size)) {
      ESP_LOGW(TAG, "gzip failed, falling back to plain file");
    } else {
      send_path = temp_path;
    }
  }

  FILE *f = fopen(send_path.c_str(), "rb");
  if (!f) {
    if (!temp_path.empty()) unlink(temp_path.c_str());
    return false;
  }
  fseek(f, 0, SEEK_END);
  long len = ftell(f);
  fseek(f, 0, SEEK_SET);
  if (len <= 0) { fclose(f); if (!temp_path.empty()) unlink(temp_path.c_str()); return false; }

  esp_http_client_config_t cfg{};
  cfg.url = upload_url_.c_str();
  cfg.event_handler = _http_event_handler;
  cfg.timeout_ms = 30000;
  cfg.transport_type = HTTP_TRANSPORT_OVER_SSL;
  cfg.skip_cert_common_name_check = true;   // DON'T validate CN
  cfg.crt_bundle_attach = NULL;             // DON'T validate certificate

  esp_http_client_handle_t client = esp_http_client_init(&cfg);
  if (!client) { fclose(f); if (!temp_path.empty()) unlink(temp_path.c_str()); return false; }

  esp_http_client_set_method(client, HTTP_METHOD_POST);
  esp_http_client_set_header(client, "Content-Type",
                             (gzip_enabled_ && !temp_path.empty()) ? "application/gzip" : "text/csv");
  if (!bearer_token_.empty()) {
    std::string auth = "Bearer " + bearer_token_;
    esp_http_client_set_header(client, "Authorization", auth.c_str());
  }

  if (esp_http_client_open(client, len) != ESP_OK) {
    ESP_LOGW(TAG, "http open failed for file");
    esp_http_client_cleanup(client);
    fclose(f);
    if (!temp_path.empty()) unlink(temp_path.c_str());
    return false;
  }

  const size_t CHUNK = 4096;
  std::vector<uint8_t> buf;
  buf.resize(CHUNK);
  long remain = len;
  while (remain > 0) {
    size_t rd = fread(buf.data(), 1, std::min<long>(CHUNK, remain), f);
    if (rd == 0) { ESP_LOGW(TAG, "fread short"); break; }
    int w = esp_http_client_write(client, (const char *) buf.data(), rd);
    if (w != (int) rd) { ESP_LOGW(TAG, "http write short"); break; }
    remain -= rd;
    vTaskDelay(pdMS_TO_TICKS(1));  // yield
  }

  fclose(f);
  int code = esp_http_client_fetch_headers(client);
  int status = esp_http_client_get_status_code(client);
  esp_http_client_close(client);
  esp_http_client_cleanup(client);

  if (!temp_path.empty()) {
    // commit tmp to .gz (replace original) only on success
    if (status >= 200 && status < 300) {
      // success: nothing to persist; original file will be deleted by caller
      unlink(temp_path.c_str());
    } else {
      // failed: keep original; remove tmp
      unlink(temp_path.c_str());
    }
  }

  if (status >= 200 && status < 300) {
    ESP_LOGD(TAG, "Upload OK: %s", path.c_str());
    return true;
  }
  ESP_LOGW(TAG, "Upload failed status=%d for %s", status, path.c_str());
  return false;
}

}  // namespace sdlog
}  // namespace esphome
