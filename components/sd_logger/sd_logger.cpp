#include "sd_logger.h"
#include <ctime>
#include "esphome/components/wifi/wifi_component.h"
#include "esp_timer.h"

namespace esphome {
namespace sdlog {

static const char *const TAG = "sd_logger";

void SDLogger::dump_config() {
  ESP_LOGCONFIG(TAG, "SD Logger:");
  ESP_LOGCONFIG(TAG, "  Upload URL: %s", this->upload_url_.c_str());
  ESP_LOGCONFIG(TAG, "  Log path: %s", this->log_path_.c_str());
  ESP_LOGCONFIG(TAG, "  Upload interval: %u ms", this->upload_interval_ms_);
  ESP_LOGCONFIG(TAG, "  Backoff initial/max: %u/%u ms", this->backoff_initial_ms_, this->backoff_max_ms_);
  ESP_LOGCONFIG(TAG, "  Gzip: %s (miniz included)", this->gzip_enabled_ ? "ENABLED" : "disabled");
  ESP_LOGCONFIG(TAG, "  Tracked sensors: %u", (unsigned) this->sensors_.size());
}

void SDLogger::setup() {
  this->ensure_log_dir_();

  // attach sensor callbacks
  for (auto *s : this->sensors_) {
    s->add_on_state_callback([this, s](float value) {
      this->write_csv_line_(s->get_object_id(), value);
    });
  }
}

void SDLogger::loop() {
  const uint32_t now = esp_timer_get_time();
  if (this->task_in_progress_) return;

  if ((now - this->last_attempt_ms_) < this->current_backoff_ms_) return;

  if (!wifi::global_wifi_component->is_connected()) {
    this->schedule_next_attempt_(false);
    return;
  }

  this->task_in_progress_ = true;
  BaseType_t ok = xTaskCreatePinnedToCore(
      &SDLogger::upload_task_trampoline_, "sdlog_upload", 8192,
      this, tskIDLE_PRIORITY + 1, &this->upload_task_, 1);
  if (ok != pdPASS) {
    ESP_LOGE(TAG, "Failed to create upload task");
    this->task_in_progress_ = false;
    this->schedule_next_attempt_(false);
  }
}

void SDLogger::add_tracked_sensor(sensor::Sensor *s) {
  if (!s) return;
  this->sensors_.push_back(s);
}

bool SDLogger::ensure_log_dir_() {
  ESP_LOGD(TAG, "Ensuring log dir at: %s", this->log_path_.c_str());
  struct stat st {};
  if (stat(this->log_path_.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) return true;
  int res = mkdir(this->log_path_.c_str(), 0775);
  if (res == 0) return true;
  ESP_LOGW(TAG, "Cannot ensure log dir at %s (errno=%d)", this->log_path_.c_str(), errno);
  return false;
}

bool SDLogger::list_files_(std::vector<std::string> &out) {
  ESP_LOGD(TAG, "Listing files in: %s", this->log_path_.c_str());
  DIR *dir = opendir(this->log_path_.c_str());
  if (!dir) return false;
  struct dirent *ent;
  while ((ent = readdir(dir)) != nullptr) {
    if (ent->d_name[0] == '.') continue;
    out.emplace_back(this->log_path_ + "/" + ent->d_name);
  }
  closedir(dir);
  std::sort(out.begin(), out.end());
  return true;
}

bool SDLogger::read_file_(const std::string &path, std::string &out) {
  ESP_LOGD(TAG, "Reading file: %s", path.c_str());
  FILE *fp = fopen(path.c_str(), "rb");
  if (!fp) return false;
  char buf[2048];
  size_t n;
  while ((n = fread(buf, 1, sizeof(buf), fp)) > 0) {
    out.append(buf, buf + n);
  }
  fclose(fp);
  return true;
}

bool SDLogger::delete_file_(const std::string &path) {
  ESP_LOGD(TAG, "Deleting file: %s", path.c_str());
  int r = remove(path.c_str());
  if (r != 0) {
    ESP_LOGW(TAG, "Failed to delete %s (errno=%d)", path.c_str(), errno);
    return false;
  }
  return true;
}

bool SDLogger::gzip_compress_(const std::string &in, std::string &out) {
  // Build gzip stream: header + deflate(raw) + trailer(CRC32, ISIZE)
  // GZIP header (10 bytes): ID1,ID2,CM,FLG,MTIME(4),XFL,OS
  // ID1=0x1f, ID2=0x8b, CM=8(deflate), FLG=0
  // MTIME=0 (unknown), XFL=0, OS=255 (unknown)
  const uint8_t gz_header[10] = {0x1f, 0x8b, 0x08, 0x00, 0, 0, 0, 0, 0x00, 0xff};

  // Reserve some space (header + input + overhead)
  out.clear();
  out.reserve( in.size() + 64 );

  // Write header
  out.insert(out.end(), (const char*)gz_header, (const char*)gz_header + sizeof(gz_header));

  // ---- Raw DEFLATE using miniz tdefl (no zlib/gzip wrapper) ----
  tdefl_compressor comp{};
  // Use default level; write raw deflate (TDEFL_WRITE_ZLIB_HEADER off)
  const int flags = TDEFL_DEFAULT_MAX_PROBES;

  if (tdefl_init(&comp, nullptr, nullptr, flags) != TDEFL_STATUS_OKAY) {
    ESP_LOGE(TAG, "tdefl_init failed");
    return false;
  }

  const size_t in_size = in.size();
  size_t in_ofs = 0;

  // Compress in chunks to avoid big temporary buffers
  const size_t CHUNK = 2048;
  uint8_t out_chunk[CHUNK];

  for (;;) {
    size_t in_avail = in_size - in_ofs;
    const void* in_ptr = (in_avail > 0) ? (in.data() + in_ofs) : nullptr;

    size_t out_avail = CHUNK;
    tdefl_status st = tdefl_compress(&comp,
                                     in_ptr, &in_avail,
                                     out_chunk, &out_avail,
                                     (in_avail > 0) ? TDEFL_NO_FLUSH : TDEFL_FINISH);

    // Append produced bytes
    if (out_avail > 0) {
      out.insert(out.end(), (const char*)out_chunk, (const char*)out_chunk + out_avail);
    }

    in_ofs += in_avail;
    if (st == TDEFL_STATUS_DONE) break;
    if (st < 0) {
      ESP_LOGE(TAG, "tdefl_compress error: %d", (int) st);
      return false;
    }
  }

  // ---- Trailer: CRC32 (input) and ISIZE (input size mod 2^32), little-endian ----
  uint32_t crc = mz_crc32(0, (const unsigned char*)in.data(), (mz_uint)in.size());
  uint32_t isize = (uint32_t)(in.size() & 0xFFFFFFFFu);

  // Append CRC32 LE
  out.push_back((char)(crc & 0xFF));
  out.push_back((char)((crc >> 8) & 0xFF));
  out.push_back((char)((crc >> 16) & 0xFF));
  out.push_back((char)((crc >> 24) & 0xFF));
  // Append ISIZE LE
  out.push_back((char)(isize & 0xFF));
  out.push_back((char)((isize >> 8) & 0xFF));
  out.push_back((char)((isize >> 16) & 0xFF));
  out.push_back((char)((isize >> 24) & 0xFF));

  return true;
}


bool SDLogger::upload_buffer_(const uint8_t *data, size_t len, bool is_gzip, int *http_status) {
  esp_http_client_config_t cfg{};
  cfg.url = this->upload_url_.c_str();
  cfg.method = HTTP_METHOD_POST;
  cfg.timeout_ms = 15000;
  // HTTPS without cert validation
  cfg.skip_cert_common_name_check = true;

  esp_http_client_handle_t client = esp_http_client_init(&cfg);
  if (!client) {
    ESP_LOGE(TAG, "esp_http_client_init failed");
    return false;
  }

  esp_http_client_set_header(client, "Content-Type", "text/csv");
  if (is_gzip) esp_http_client_set_header(client, "Content-Encoding", "gzip");
  if (!this->bearer_token_.empty())
    esp_http_client_set_header(client, "Authorization", this->bearer_token_.c_str());

  esp_http_client_set_post_field(client, (const char *) data, len);
  esp_err_t err = esp_http_client_perform(client);
  if (err != ESP_OK) {
    ESP_LOGW(TAG, "HTTP perform failed: %s", esp_err_to_name(err));
    esp_http_client_cleanup(client);
    return false;
  }

  int status = esp_http_client_get_status_code(client);
  if (http_status) *http_status = status;
  esp_http_client_cleanup(client);

  return status == 200 || status == 201;
}

void SDLogger::schedule_next_attempt_(bool success) {
  this->last_attempt_ms_ = esp_timer_get_time();
  if (success) {
    this->current_backoff_ms_ = this->upload_interval_ms_;
  } else {
    uint32_t next = this->current_backoff_ms_ * 2;
    if (next < this->current_backoff_ms_) next = this->backoff_max_ms_;  // overflow guard
    this->current_backoff_ms_ = std::min(next, this->backoff_max_ms_);
  }
}

void SDLogger::upload_task_trampoline_(void *param) {
  auto *self = static_cast<SDLogger *>(param);
  self->run_upload_task_();
  self->task_in_progress_ = false;
  self->upload_task_ = nullptr;
  vTaskDelete(nullptr);
}

void SDLogger::run_upload_task_() {
  ESP_LOGD(TAG, "Upload task started");
  
  std::vector<std::string> files;
  bool listed = this->list_files_(files);
  if (!listed || files.empty()) {
    this->schedule_next_attempt_(true);
    return;
  }

  bool all_ok = true;
  for (const auto &path : files) {
    std::string raw;
    if (!this->read_file_(path, raw)) {
      ESP_LOGW(TAG, "Read failed: %s", path.c_str());
      all_ok = false;
      break;
    }

    const uint8_t *body = reinterpret_cast<const uint8_t *>(raw.data());
    size_t body_len = raw.size();
    bool is_gzip = false;

    std::string gz;
    if (this->gzip_enabled_) {
      if (this->gzip_compress_(raw, gz)) {
        body = reinterpret_cast<const uint8_t *>(gz.data());
        body_len = gz.size();
        is_gzip = true;
      } else {
        ESP_LOGW(TAG, "Gzip requested but compression failed; sending plain CSV.");
      }
    }

    int status = 0;
    bool ok = this->upload_buffer_(body, body_len, is_gzip, &status);
    if (ok) {
      this->delete_file_(path);
      ESP_LOGI(TAG, "Uploaded (%d) and deleted: %s", status, path.c_str());
    } else {
      ESP_LOGW(TAG, "Upload failed: %s", path.c_str());
      all_ok = false;
      break;
    }
    vTaskDelay(pdMS_TO_TICKS(50));
  }

  this->schedule_next_attempt_(all_ok);
}

void SDLogger::write_csv_line_(const std::string &sensor_object_id, float value) {
  ESP_LOGD(TAG, "Logging: %s = %.6f", sensor_object_id.c_str(), value);

  time_t now_ts = 0;
  if (this->time_ && this->time_->now().is_valid()) {
    now_ts = this->time_->now().timestamp;
  } else {
    now_ts = esp_timer_get_time() / 1000;
  }

  struct tm tm_now;
  localtime_r(&now_ts, &tm_now);

  char fname[64];
  strftime(fname, sizeof(fname), "%Y%m%d.csv", &tm_now);
  std::string path = this->log_path_ + "/" + fname;

  char line[160];
  int n = snprintf(line, sizeof(line), "%ld,%s,%.6f\n",
                   static_cast<long>(now_ts), sensor_object_id.c_str(), value);

  FILE *fp = fopen(path.c_str(), "ab");
  if (!fp) {
    ESP_LOGW(TAG, "Open failed: %s", path.c_str());
    return;
  }
  fwrite(line, 1, n, fp);
  fflush(fp);
  fclose(fp);
}

}  // namespace sdlog
}  // namespace esphome
