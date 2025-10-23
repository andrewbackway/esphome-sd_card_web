#include "sd_logger.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cmath>

#include "esphome/core/helpers.h"
#include "esphome/core/log.h"

extern "C" {
#include "esp_timer.h"
}

namespace esphome {
namespace sd_logger {

static const char* const TAG = "sd_logger";

// ===== Component =====
void SdLogger::setup() {
  ESP_LOGI(TAG, "setup()");
  if (this->log_path_.empty()) this->log_path_ = "/sdcard/logs";
  ensure_dir_(this->log_path_);

  this->live_queue_ = xQueueCreate(16, sizeof(LiveItem));
  if (!this->live_queue_) ESP_LOGE(TAG, "create live queue failed");

  xTaskCreatePinnedToCore(&SdLogger::task_live_entry_, "sdlog_live", 6 * 1024,
                          this, 4, &this->task_live_, APP_CPU_NUM);
  xTaskCreatePinnedToCore(&SdLogger::task_backlog_entry_, "sdlog_backlog",
                          7 * 1024, this, 3, &this->task_backlog_, APP_CPU_NUM);

  publish_sync_online_(false);
  publish_sync_backlog_(false);
}

void SdLogger::loop() {
  if (!time_valid_()) {
    this->have_started_ = false;
    return;
  }

  uint32_t now = (uint32_t)this->time_->now().timestamp;
  if (!this->have_started_) {
    this->start_valid_epoch_ = now;
    this->have_started_ = true;
    this->last_tick_window_ = 0;
    ESP_LOGI(TAG, "SNTP valid at %u, starting in 30s", (unsigned)now);
    return;
  }
  if (now < (this->start_valid_epoch_ + 30)) return;

  uint32_t win = window_start_(now);
  if (win == this->last_tick_window_) return;
  this->last_tick_window_ = win;

  std::string payload;
  if (!this->build_payload_json_(payload)) {
    ESP_LOGW(TAG, "build_payload_json failed; skip tick");
    return;
  }

  if (this->sync_online_) {
    LiveItem it{payload};
    if (xQueueSend(this->live_queue_, &it, 0) != pdPASS) {
      ESP_LOGW(TAG, "live queue full; spilling to SD");
      if (!write_window_file_(payload))
        ESP_LOGE(TAG, "spill-to-SD failed; dropping");
    }
  } else {
    if (!write_window_file_(payload)) {
      ESP_LOGE(TAG, "SD write failed; dropping tick");
    }
  }
}

// ===== Utilities =====
static std::string mac_as_device_id_() {
  uint8_t mac[6];
  // Use ESPHome helper instead of esp_read_mac()
  get_mac_address_raw(mac);  // pulls WiFi STA MAC or fallback
  char buf[18];
  sprintf(buf, "%02x:%02x:%02x:%02x:%02x:%02x", mac[0], mac[1], mac[2], mac[3],
          mac[4], mac[5]);
  return std::string(buf);
}

// basic UUIDv4 from random_uint32()
static std::string uuid_v4_() {
  uint32_t a = random_uint32();
  uint32_t b = random_uint32();
  uint32_t c = random_uint32();
  uint32_t d = random_uint32();
  // set version (4) and variant (10xx)
  uint16_t mid = (uint16_t)((b >> 16) & 0x0fff) | 0x4000;
  uint16_t var = (uint16_t)((c >> 16) & 0x3fff) | 0x8000;
  char out[37];
  snprintf(out, sizeof(out), "%08x-%04x-%04x-%04x-%04x%08x", a,
           (uint16_t)(a >> 16), mid, var, (uint16_t)c, d);
  return std::string(out);
}

static uint32_t window_start_(uint32_t epoch) { return (epoch / 30U) * 30U; }

static std::string filename_for_(uint32_t window_epoch) {
  time_t t = window_epoch;
  struct tm tm_utc;
  gmtime_r(&t, &tm_utc);
  char name[64];
  strftime(name, sizeof(name), "%Y%m%d_%H%M%S.json", &tm_utc);
  return std::string(name);
}

static bool ensure_dir_(const std::string& path) {
  struct stat st{};
  if (stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) return true;
  return mkdir(path.c_str(), 0775) == 0;
}

static bool atomic_write_(const std::string& path, const std::string& data) {
  std::string tmp = path + ".tmp";

  // 1. Attempt to open file
  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0664);
  if (fd < 0) {
    ESP_LOGE(TAG, "atomic_write: open(%s) failed (errno=%d: %s)", tmp.c_str(),
             errno, strerror(errno));
    return false;
  }

  // 2. Write full contents
  ssize_t left = (ssize_t)data.size();
  const char* p = data.c_str();
  while (left > 0) {
    ssize_t w = ::write(fd, p, left);
    if (w <= 0) {
      ESP_LOGE(TAG, "atomic_write: write(%s) failed (errno=%d: %s)",
               tmp.c_str(), errno, strerror(errno));
      ::close(fd);
      ::unlink(tmp.c_str());
      return false;
    }
    p += w;
    left -= w;
  }

  // 3. Sync to disk
  if (fsync(fd) != 0) {
    ESP_LOGE(TAG, "atomic_write: fsync(%s) failed (errno=%d: %s)", tmp.c_str(),
             errno, strerror(errno));
    ::close(fd);
    ::unlink(tmp.c_str());
    return false;
  }

  // 4. Close file
  if (::close(fd) != 0) {
    ESP_LOGE(TAG, "atomic_write: close(%s) failed (errno=%d: %s)", tmp.c_str(),
             errno, strerror(errno));
    ::unlink(tmp.c_str());
    return false;
  }

  // 5. Rename tmp → final
  if (::rename(tmp.c_str(), path.c_str()) != 0) {
    ESP_LOGE(TAG, "atomic_write: rename(%s -> %s) failed (errno=%d: %s)",
             tmp.c_str(), path.c_str(), errno, strerror(errno));
    ::unlink(tmp.c_str());
    return false;
  }

  return true;
}

// ===== internals =====
bool SdLogger::time_valid_() const {
  if (!this->time_) return false;
  auto t = this->time_->now();
  return t.is_valid();
}

void SdLogger::ensure_log_dir_() { (void)this->ensure_dir_(this->log_path_); }

void SdLogger::publish_sync_online_(bool v) {
  this->sync_online_ = v;
  if (this->sync_online_bs_) this->sync_online_bs_->publish_state(v);
}
void SdLogger::publish_sync_backlog_(bool v) {
  if (this->sync_sending_backlog_bs_)
    this->sync_sending_backlog_bs_->publish_state(v);
}

bool SdLogger::build_payload_json_(std::string& out_json) {
  // Timestamp
  ESPTime t = this->time_->now();
  char iso[32];
  t.strftime(iso, sizeof(iso), "%Y-%m-%dT%H:%M:%SZ");

  static std::string session_id;
  if (session_id.empty()) session_id = uuid_v4_();
  std::string id_guid = uuid_v4_();
  std::string device_id = mac_as_device_id_();

  out_json = json::build_json([&](JsonObject root) {
    root["id"] = id_guid;
    root["sessionId"] = session_id;
    root["deviceId"] = device_id;
    root["date"] = iso;

    JsonArray arr = root["sensors"].to<JsonArray>();
    for (auto* s : this->sensors_) {
      JsonObject o = arr.add<JsonObject>();
      // Prefer object_id if name empty
      std::string sid = s->get_object_id();
      if (sid.size() > 100)
        sid.resize(100);  // guard, though spec applies to values
      o["sensorId"] = sid.c_str();

      if (!s->has_state() || std::isnan(s->state) || std::isinf(s->state)) {
        o["value"] = nullptr;  // null per spec
      } else {
        // numeric (ESPHome sensors are floats)
        o["value"] = s->state;
      }
    }
  });

  // Hard cap ~20KB
  if (out_json.size() > 20 * 1024) {
    // Try removing whitespace (compact)
    std::string compact;
    compact.reserve(out_json.size());
    for (char c : out_json) {
      if (c != '\n' && c != '\r' && c != '\t') compact.push_back(c);
    }
    out_json.swap(compact);
  }
  if (out_json.size() > 20 * 1024) {
    ESP_LOGW(TAG, "payload > 20KB; dropping tick");
    return false;
  }
  return true;
}

bool SdLogger::write_window_file_(const std::string& json) {
  // Make sure directory exists (but DON'T recreate code — call the existing
  // helper).
  this->ensure_log_dir_();

  uint32_t epoch = (uint32_t)this->time_->now().timestamp;
  uint32_t win = this->window_start_(epoch);
  std::string full = this->log_path_ + "/" + filename_for_(win);

  if (!atomic_write_(full, json)) {
    ESP_LOGE(TAG, "Failed to write %s (errno=%d)", full.c_str(), errno);
    return false;
  }
  return true;
}

bool SdLogger::send_http_put_(const std::string& body, int* http_status,
                              std::string* resp_err) {
  if (http_status) *http_status = -1;
  if (resp_err) resp_err->clear();

  esp_http_client_config_t cfg{};
  cfg.url = this->upload_url_.c_str();
  cfg.method = HTTP_METHOD_PUT;
  cfg.timeout_ms = 15000;
  cfg.transport_type = HTTP_TRANSPORT_OVER_SSL;  // works for http/https
  cfg.skip_cert_common_name_check = true;  // disable verification per spec

  esp_http_client_handle_t client = esp_http_client_init(&cfg);
  if (!client) {
    if (resp_err) *resp_err = "http_client_init failed";
    return false;
  }

  esp_http_client_set_header(client, "Content-Type", "application/json");
  if (!this->bearer_token_.empty())
    esp_http_client_set_header(client, "Authorization",
                               this->bearer_token_.c_str());

  esp_http_client_set_post_field(client, body.c_str(), (int)body.size());

  esp_err_t err = esp_http_client_perform(client);
  if (err != ESP_OK) {
    if (resp_err) *resp_err = esp_err_to_name(err);
    esp_http_client_cleanup(client);
    return false;
  }

  int status = esp_http_client_get_status_code(client);
  if (http_status) *http_status = status;
  esp_http_client_cleanup(client);
  return status == 200 || status == 201;
}

bool SdLogger::send_http_ping_(int* http_status, std::string* resp_err) {
  if (http_status) *http_status = -1;
  if (resp_err) resp_err->clear();

  // Pick ping URL; fall back to upload_url_ if not provided.
  const std::string& url =
      this->ping_url_.empty() ? this->upload_url_ : this->ping_url_;
  if (url.empty()) {
    if (resp_err) *resp_err = "no ping_url/upload_url configured";
    return false;
  }

  esp_http_client_config_t cfg{};
  cfg.url = url.c_str();
  cfg.method = HTTP_METHOD_GET;  // GET is broadly allowed; HEAD often blocked
  cfg.timeout_ms = (int)this->ping_timeout_ms_;
  cfg.transport_type = HTTP_TRANSPORT_OVER_SSL;  // works for http/https
  cfg.skip_cert_common_name_check = true;        // per your spec+
  esp_http_client_handle_t client = esp_http_client_init(&cfg);
  if (!client) {
    if (resp_err) *resp_err = "http_client_init failed";
    return false;
  }
  // Keep it cheap.
  esp_http_client_set_header(client, "Accept", "*/*");
  if (!this->bearer_token_.empty())
    esp_http_client_set_header(client, "Authorization",
                               this->bearer_token_.c_str());
  esp_http_client_set_header(client, "Connection", "close");
  +esp_err_t err = esp_http_client_perform(client);
  bool ok = false;
  if (err == ESP_OK) {
    int code = esp_http_client_get_status_code(client);
    if (http_status) *http_status = code;
    // Consider 2xx and 3xx as "reachable"
    ok = (code >= 200 && code < 400);
  } else {
    if (resp_err) *resp_err = esp_err_to_name(err);
  }
  esp_http_client_cleanup(client);
  return ok;
}

bool SdLogger::has_backlog_files_() {
  DIR* dir = opendir(this->log_path_.c_str());
  if (!dir) return false;
  struct dirent* e;
  bool any = false;
  while ((e = readdir(dir)) != nullptr) {
    std::string name = e->d_name;
    if (name.size() >= 5 && name.rfind(".json") == name.size() - 5) {
      any = true;
      break;
    }
  }
  closedir(dir);
  return any;
}

bool SdLogger::find_oldest_file_(std::string& path_out) {
  DIR* dir = opendir(this->log_path_.c_str());
  if (!dir) return false;
  struct dirent* e;
  std::string oldest;
  time_t oldest_mtime = LONG_MAX;

  while ((e = readdir(dir)) != nullptr) {
    std::string name = e->d_name;
    if (name.size() < 5 || name.rfind(".json") != name.size() - 5) continue;
    std::string full = this->log_path_ + "/" + name;
    struct stat st{};
    if (stat(full.c_str(), &st) == 0) {
      if (st.st_mtime < oldest_mtime) {
        oldest_mtime = st.st_mtime;
        oldest = full;
      }
    }
  }
  closedir(dir);
  if (oldest.empty()) return false;
  path_out = oldest;
  return true;
}

bool SdLogger::load_file_(const std::string& path, std::string& data_out) {
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return false;
  std::string buf;
  buf.resize(24 * 1024);
  ssize_t n = ::read(fd, buf.data(), buf.size());
  ::close(fd);
  if (n < 0) return false;
  buf.resize((size_t)n);
  data_out.swap(buf);
  return true;
}

bool SdLogger::delete_file_(const std::string& path) {
  return ::unlink(path.c_str()) == 0;
}

// ===== Tasks =====
void SdLogger::task_live_entry_(void* param) {
  auto* self = static_cast<SdLogger*>(param);
  LiveItem item;
  for (;;) {
    if (xQueueReceive(self->live_queue_, &item, pdMS_TO_TICKS(1000)) ==
        pdPASS) {
      int status = -1;
      std::string err;
      bool ok = self->send_http_put_(item.json, &status, &err);
      if (ok)
        self->publish_sync_online_(true);
      else {
        ESP_LOGW(TAG, "Live PUT failed (status=%d): %s", status, err.c_str());
        self->publish_sync_online_(false);
        if (!self->write_window_file_(item.json))
          ESP_LOGE(TAG, "spill live->SD failed");
      }
    } else {
      vTaskDelay(pdMS_TO_TICKS(50));
    }
  }
}

void SdLogger::task_backlog_entry_(void* param) {
  auto* self = static_cast<SdLogger*>(param);
  self->publish_sync_backlog_(false);
  self->backlog_backoff_ms_ = 0;

  for (;;) {
    if (!self->sync_online_) {
      int status = -1;
      std::string err;
      bool pong = self->send_http_ping_(&status, &err);
      if (pong) {
        self->publish_sync_online_(true);
        // Clear any backlog backoff so we start promptly.
        self->backlog_backoff_ms_ = 0;
      } else {
        self->publish_sync_online_(false);
        // Sleep until next ping attempt.
        vTaskDelay(pdMS_TO_TICKS(self->ping_interval_ms_));
        continue;
      }
    }

    if (!self->has_backlog_files_()) {
      self->publish_sync_backlog_(false);
      vTaskDelay(pdMS_TO_TICKS(250));
      continue;
    }
    self->publish_sync_backlog_(true);

    std::string path;
    if (!self->find_oldest_file_(path)) {
      vTaskDelay(pdMS_TO_TICKS(250));
      continue;
    }

    std::string body;
    if (!self->load_file_(path, body)) {
      ESP_LOGW(TAG, "read backlog failed: %s (deleting)", path.c_str());
      self->delete_file_(path);
      continue;
    }

    int status = -1;
    std::string err;
    bool ok = self->send_http_put_(body, &status, &err);

    if (ok) {
      self->delete_file_(path);
      self->backlog_backoff_ms_ = 0;
      continue;
    }

    bool retryable = false;
    if (status < 0) retryable = true;
    if (status == 408 || status == 425 || status == 429 ||
        (status >= 500 && status <= 599))
      retryable = true;

    if (!retryable) {
      ESP_LOGW(TAG, "Backlog PUT non-retryable status=%d; keeping file",
               status);
    } else {
      ESP_LOGW(TAG, "Backlog PUT retryable status=%d: %s", status, err.c_str());
    }

    if (self->backlog_backoff_ms_ == 0)
      self->backlog_backoff_ms_ = self->backoff_initial_ms_;
    else
      self->backlog_backoff_ms_ = std::min<uint32_t>(
          self->backlog_backoff_ms_ * 2, self->backoff_max_ms_);

    uint32_t wait = self->backlog_backoff_ms_;
    uint32_t slept = 0;
    while (slept < wait) {
      if (!self->sync_online_) break;
      vTaskDelay(pdMS_TO_TICKS(250));
      slept += 250;
    }
  }
}

}  // namespace sd_logger
}  // namespace esphome
