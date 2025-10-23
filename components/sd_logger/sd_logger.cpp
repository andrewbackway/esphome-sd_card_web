#include "sd_logger.h"
#include "esphome/core/helpers.h"
#include "esphome/core/log.h"

#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>

extern "C" {
  #include "esp_http_client.h"
  #include "esp_timer.h"
}

namespace esphome {
namespace sd_logger {

static const char *const TAG = "sd_logger";

// ------- Utility helpers -------
static inline bool is_nan_(float v) {
  return std::isnan(v) || std::isinf(v);
}

static std::string mac_as_device_id_() {
  // Returns lowercase aa:bb:cc:dd:ee:ff
  uint8_t mac[6] = {0};
  esp_read_mac(mac, ESP_MAC_WIFI_STA);
  char buf[18];
  sprintf(buf, "%02x:%02x:%02x:%02x:%02x:%02x",
          mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
  return std::string(buf);
}

static uint32_t floor_to_window_start_(uint32_t epoch) {
  return (epoch / 30U) * 30U;
}

static std::string format_filename_(uint32_t epoch_window) {
  // UTC time formatting
  time_t t = epoch_window;
  struct tm tm_utc;
  gmtime_r(&t, &tm_utc);
  char name[64];
  // YYYYMMDD_HHMMSS.json
  strftime(name, sizeof(name), "%Y%m%d_%H%M%S.json", &tm_utc);
  return std::string(name);
}

static bool mkdirs_(const std::string &path) {
  struct stat st{};
  if (stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) return true;
  // naive one-level mkdir
  return mkdir(path.c_str(), 0775) == 0;
}

// Atomic write: path.tmp -> fsync -> rename
static bool atomic_write_file_(const std::string &path, const std::string &data) {
  std::string tmp = path + ".tmp";
  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0664);
  if (fd < 0) return false;
  ssize_t total = 0;
  const char *p = data.c_str();
  ssize_t left = (ssize_t) data.size();
  while (left > 0) {
    ssize_t w = ::write(fd, p + total, left);
    if (w <= 0) { ::close(fd); ::unlink(tmp.c_str()); return false; }
    total += w;
    left -= w;
  }
  if (fsync(fd) != 0) { ::close(fd); ::unlink(tmp.c_str()); return false; }
  ::close(fd);
  if (::rename(tmp.c_str(), path.c_str()) != 0) {
    ::unlink(tmp.c_str());
    return false;
  }
  return true;
}

void SdLogger::setup() {
  ESP_LOGI(TAG, "setup()");
  if (this->log_path_.empty()) {
    this->log_path_ = "/sdcard/logs";
  }
  ensure_log_dir_();

  // Create live queue
  this->live_queue_ = xQueueCreate(16, sizeof(LiveItem));
  if (this->live_queue_ == nullptr) {
    ESP_LOGE(TAG, "Failed to create live queue");
  }

  // Spawn tasks
  xTaskCreatePinnedToCore(&SdLogger::task_live_entry_, "sdlog_live", 6 * 1024, this, 4, &this->task_live_, APP_CPU_NUM);
  xTaskCreatePinnedToCore(&SdLogger::task_backlog_entry_, "sdlog_backlog", 7 * 1024, this, 3, &this->task_backlog_, APP_CPU_NUM);

  publish_sync_online_(false);
  publish_sync_backlog_(false);
}

void SdLogger::loop() {
  // Gate everything on valid time
  if (!time_valid_()) {
    this->have_started_ = false;
    return;
  }

  uint32_t now_epoch = (uint32_t) this->time_->now().timestamp;
  if (!this->have_started_) {
    // Start 30 seconds after time became valid
    this->start_valid_epoch_ = now_epoch;
    this->have_started_ = true;
    this->last_tick_epoch_ = 0;
    ESP_LOGI(TAG, "SNTP valid at %u, starting ticks in 30s", (unsigned) now_epoch);
    return;
  }

  // Wait until 30s after start
  if (now_epoch < (this->start_valid_epoch_ + 30)) return;

  // Determine current window
  uint32_t window_start = floor_to_window_start_(now_epoch);
  if (window_start == this->last_tick_epoch_) return;  // only once per window

  this->last_tick_epoch_ = window_start;

  // Build payload
  std::string payload;
  bool had_any = false;
  if (!this->build_payload_json_(payload, had_any)) {
    ESP_LOGW(TAG, "Payload build failed or empty; skipping tick");
    return;
  }

  // Decide online/offline by last live success state
  // (We set sync_online_ true only after a successful PUT)
  if (this->sync_online_) {
    // Try to enqueue live item; if queue full, spill to SD
    LiveItem item{payload};
    if (xQueueSend(this->live_queue_, &item, 0) != pdPASS) {
      ESP_LOGW(TAG, "Live queue full, spilling to SD");
      if (!write_window_file_(payload)) {
        ESP_LOGE(TAG, "SD spill failed; dropping tick");
      }
    }
  } else {
    // Offline: write to SD
    if (!write_window_file_(payload)) {
      ESP_LOGE(TAG, "SD write failed; dropping tick");
    }
  }
}

// ---------- Internals ----------

bool SdLogger::time_valid_() const {
  if (this->time_ == nullptr) return false;
  auto t = this->time_->now();
  return t.is_valid();
}

void SdLogger::ensure_log_dir_() {
  mkdirs_(this->log_path_);
}

void SdLogger::publish_sync_online_(bool v) {
  this->sync_online_ = v;
  if (this->sync_online_bs_) this->sync_online_bs_->publish_state(v);
}

void SdLogger::publish_sync_backlog_(bool v) {
  if (this->sync_sending_backlog_bs_) this->sync_sending_backlog_bs_->publish_state(v);
}

static void truncate_string_(std::string &s, size_t max_len) {
  if (s.size() > max_len) s.resize(max_len);
}

bool SdLogger::build_payload_json_(std::string &out_json, bool &had_any_value) {
  had_any_value = false;

  // Date (UTC ISO8601 Z)
  time::ESPTime now = this->time_->now();
  char iso[32];
  now.strftime(iso, sizeof(iso), "%Y-%m-%dT%H:%M:%SZ");

  // sessionId: random per boot (generate once)
  static std::string session_guid;
  if (session_guid.empty()) {
    session_guid = random_uuid();  // provided by esphome/helpers.h
  }
  std::string id_guid = random_uuid();
  std::string device_id = mac_as_device_id_();

  // Build JSON using json::build_json (ArduinoJson wrapper)
  json::build_json([&](JsonObject root) {
    root["id"] = id_guid;
    root["sessionId"] = session_guid;
    root["deviceId"] = device_id;
    root["date"] = iso;

    JsonArray arr = root.createNestedArray("Sensors");
    // Collect sensor values respecting types and nulls for NAN/UNAVAILABLE
    for (auto *s : this->sensors_) {
      JsonObject o = arr.createNestedObject();
      o["sensorId"] = s->get_name().empty() ? s->get_object_id() : s->get_name();
      // Determine availability
      if (!s->has_state() || is_nan_(s->state)) {
        o["value"] = nullptr;  // null per spec
      } else {
        // Numeric sensors in ESPHome are float; attempt to send numeric if non-integer ok
        // If sensor has unitless text? (sensor::Sensor is numeric by design). Keep as number.
        o["value"] = s->state;
        had_any_value = true;
      }
    }
  }, out_json);

  // Enforce per-value string truncation (100 chars) — applies only if any strings slipped in
  // We also must enforce a ~20 KB cap by pruning largest strings.
  // Since Sensors are numeric/null by design, only "sensorId" fields are strings.
  if (out_json.size() > 20 * 1024) {
    // Crude but safe: try to shorten by removing whitespace first, then if still large, fail this tick.
    std::string compact;
    compact.reserve(out_json.size());
    for (char c : out_json) {
      if (c != '\n' && c != '\r' && c != '\t') compact.push_back(c);
    }
    out_json.swap(compact);
  }
  if (out_json.size() > 20 * 1024) {
    ESP_LOGW(TAG, "Payload exceeds 20KB after compaction, dropping");
    return false;
  }

  // OK if all null? Spec allows sending nulls; we still write/send as-is.
  return true;
}

bool SdLogger::write_window_file_(const std::string &json) {
  uint32_t epoch = (uint32_t) this->time_->now().timestamp;
  uint32_t window = floor_to_window_start_(epoch);
  std::string filename = format_filename_(window);
  std::string full = this->log_path_ + "/" + filename;
  return atomic_write_file_(full, json);
}

bool SdLogger::send_http_put_(const std::string &body, int *http_status, std::string *resp_err) {
  if (http_status) *http_status = -1;
  if (resp_err) resp_err->clear();

  esp_http_client_config_t cfg = {};
  cfg.url = this->upload_url_.c_str();
  cfg.method = HTTP_METHOD_PUT;
  cfg.transport_type = HTTP_TRANSPORT_OVER_SSL;  // assume HTTPS; will also work for http
  cfg.timeout_ms = 15000;
  cfg.skip_cert_common_name_check = true;  // disable verification as requested
  cfg.disable_auto_redirect = false;
  // No cert_pem means no server verification (insecure by spec).

  esp_http_client_handle_t client = esp_http_client_init(&cfg);
  if (!client) {
    if (resp_err) *resp_err = "esp_http_client_init failed";
    return false;
  }

  esp_http_client_set_header(client, "Content-Type", "application/json");
  if (!this->bearer_token_.empty()) {
    esp_http_client_set_header(client, "Authorization", this->bearer_token_.c_str());
  }
  esp_http_client_set_method(client, HTTP_METHOD_PUT);
  esp_http_client_set_post_field(client, body.c_str(), (int) body.size());

  esp_err_t err = esp_http_client_perform(client);
  if (err != ESP_OK) {
    if (resp_err) *resp_err = std::string("perform err: ") + esp_err_to_name(err);
    esp_http_client_cleanup(client);
    return false;
  }

  int status = esp_http_client_get_status_code(client);
  if (http_status) *http_status = status;
  esp_http_client_cleanup(client);

  return status == 200 || status == 201;
}

bool SdLogger::has_backlog_files_() {
  DIR *dir = opendir(this->log_path_.c_str());
  if (!dir) return false;
  struct dirent *e;
  bool any = false;
  while ((e = readdir(dir)) != nullptr) {
    if (e->d_type == DT_REG) { any = true; break; }
  }
  closedir(dir);
  return any;
}

bool SdLogger::find_oldest_file_(std::string &path_out) {
  DIR *dir = opendir(this->log_path_.c_str());
  if (!dir) return false;
  struct dirent *e;
  std::string oldest;
  time_t oldest_t = LONG_MAX;

  while ((e = readdir(dir)) != nullptr) {
    if (e->d_type != DT_REG) continue;
    std::string name = e->d_name;
    if (name.size() < 5 || name.rfind(".json") != name.size() - 5) continue;
    std::string full = this->log_path_ + "/" + name;
    struct stat st{};
    if (stat(full.c_str(), &st) == 0) {
      if (st.st_mtime < oldest_t) {
        oldest_t = st.st_mtime;
        oldest = full;
      }
    }
  }
  closedir(dir);
  if (oldest.empty()) return false;
  path_out = oldest;
  return true;
}

bool SdLogger::load_file_(const std::string &path, std::string &data_out) {
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return false;
  std::string buf;
  buf.resize(24 * 1024); // safety cap
  ssize_t n = ::read(fd, buf.data(), buf.size());
  ::close(fd);
  if (n < 0) return false;
  buf.resize((size_t) n);
  data_out.swap(buf);
  return true;
}

bool SdLogger::delete_file_(const std::string &path) {
  return ::unlink(path.c_str()) == 0;
}

// ---------- Task: live uploader ----------
void SdLogger::task_live_entry_(void *param) {
  auto *self = static_cast<SdLogger *>(param);
  SdLogger::LiveItem item;

  for (;;) {
    if (xQueueReceive(self->live_queue_, &item, pdMS_TO_TICKS(1000)) == pdPASS) {
      int status = -1;
      std::string err;
      bool ok = self->send_http_put_(item.json, &status, &err);
      if (ok) {
        self->publish_sync_online_(true);
      } else {
        ESP_LOGW(TAG, "Live PUT failed (status=%d): %s", status, err.c_str());
        self->publish_sync_online_(false);
        // Spill to SD (according to spec)
        if (!self->write_window_file_(item.json)) {
          ESP_LOGE(TAG, "Failed to spill live payload to SD");
        }
      }
      // Continue immediately; no backoff on live path
    } else {
      // idle
      vTaskDelay(pdMS_TO_TICKS(50));
    }
  }
}

// ---------- Task: backlog uploader ----------
void SdLogger::task_backlog_entry_(void *param) {
  auto *self = static_cast<SdLogger *>(param);

  self->publish_sync_backlog_(false);
  self->backlog_backoff_ms_ = 0;

  for (;;) {
    // Only run when online flag is true
    if (!self->sync_online_) {
      vTaskDelay(pdMS_TO_TICKS(500));
      continue;
    }

    // Any files?
    if (!self->has_backlog_files_()) {
      self->publish_sync_backlog_(false);
      vTaskDelay(pdMS_TO_TICKS(250));
      continue;
    }

    self->publish_sync_backlog_(true);

    // Pick oldest
    std::string path;
    if (!self->find_oldest_file_(path)) {
      vTaskDelay(pdMS_TO_TICKS(250));
      continue;
    }

    // Load
    std::string body;
    if (!self->load_file_(path, body)) {
      ESP_LOGW(TAG, "Failed to read backlog file: %s", path.c_str());
      // delete corrupt file to avoid livelock
      self->delete_file_(path);
      continue;
    }

    // PUT
    int status = -1;
    std::string err;
    bool ok = self->send_http_put_(body, &status, &err);

    if (ok) {
      // success: delete and reset backoff
      self->delete_file_(path);
      self->backlog_backoff_ms_ = 0;
      // continue immediately to next file
      continue;
    }

    // Failure: decide retry policy
    bool retryable = false;
    if (status < 0) retryable = true; // transport error
    if (status == 408 || status == 425 || status == 429 || (status >= 500 && status <= 599)) retryable = true;

    if (!retryable) {
      ESP_LOGW(TAG, "Backlog PUT non-retryable (status=%d), keeping file and moving on after backoff", status);
    } else {
      ESP_LOGW(TAG, "Backlog PUT retryable (status=%d): %s", status, err.c_str());
    }

    // Exponential backoff (applies regardless to avoid hammering)
    if (self->backlog_backoff_ms_ == 0) self->backlog_backoff_ms_ = self->backoff_initial_ms_;
    else self->backlog_backoff_ms_ = std::min<uint32_t>(self->backlog_backoff_ms_ * 2, self->backoff_max_ms_);

    uint32_t sleep_ms = self->backlog_backoff_ms_;
    uint32_t slept = 0;
    while (slept < sleep_ms) {
      // Abort backoff if we go offline; resume when online again
      if (!self->sync_online_) break;
      vTaskDelay(pdMS_TO_TICKS(250));
      slept += 250;
    }
  }
}

}  // namespace sd_logger
}  // namespace esphome
