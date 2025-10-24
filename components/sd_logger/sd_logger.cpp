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

static uint32_t window_start_(uint32_t epoch);
static std::string filename_for_(uint32_t window_epoch);
static bool ensure_dir_(const std::string& path);
static bool atomic_write_(const std::string& path, const std::string& data);

// ===== Component =====
void SdLogger::setup() {
  ESP_LOGI(TAG, "setup()");
  if (this->log_path_.empty()) this->log_path_ = "/sdcard/logs";
  this->ensure_log_dir_();

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
    ::