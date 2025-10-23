#pragma once

#include "esphome/core/component.h"
#include "esphome/components/sensor/sensor.h"
#include "esphome/components/time/real_time_clock.h"
#include "esphome/components/wifi/wifi_component.h"

#include <vector>
#include <string>
#include <queue>
#include <mutex>
#include <cstdio>
#include <cstdint>

extern "C" {
#include "esp_http_client.h"
#include "esp_timer.h"
#include "esp_system.h"
#include "esp_log.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
}

namespace esphome {
namespace sdlog {

// compile-time toggles
#ifndef SDLOG_MAX_LIVE_QUEUE
#define SDLOG_MAX_LIVE_QUEUE 128
#endif

#ifndef SDLOG_ROTATE_SECONDS
#define SDLOG_ROTATE_SECONDS 30
#endif

struct LiveLine {
  std::string csv;     // formatted single CSV line with '\n'
  uint64_t ts_ms;      // when created
};

class SDLogger : public Component {
 public:
  void set_time(time::RealTimeClock *t) { this->time_ = t; }
  void set_upload_url(const std::string &u) { this->upload_url_ = u; }
  void set_bearer_token(const std::string &t) { this->bearer_token_ = t; }
  void set_log_path(const std::string &p) { this->log_path_ = p; }
  void set_upload_interval_ms(uint32_t ms) { this->upload_interval_ms_ = ms; }
  void set_backoff(uint32_t initial_ms, uint32_t max_ms) {
    this->backoff_initial_ms_ = initial_ms;
    this->backoff_max_ms_ = max_ms;
  }
  void set_gzip(bool g) { this->gzip_ = g; }

  void add_tracked_sensor(sensor::Sensor *s) { this->sensors_.push_back(s); }

  // called by YAML-free hooks from __init__.py wiring; we internally subscribe
  void on_sensor_state(sensor::Sensor *s, float state);

  // esphome lifecycle
  void setup() override;
  void loop() override;
  void dump_config() override;

 protected:
  // internal
  static void upload_task_trampoline_(void *self);
  void upload_task_();

  bool is_online_() const {
    return wifi::global_wifi_component != nullptr &&
           wifi::global_wifi_component->is_connected();
  }

  // file helpers
  bool ensure_dir_();
  std::string current_file_path_();
  void rotate_file_if_needed_(uint64_t now_ms);
  bool append_line_to_file_(const std::string &line);
  bool upload_file_(const std::string &path);
  void scan_and_queue_oldest_file_(std::string &out_path); // returns empty if none

  // http helpers
  bool http_post_lines_(const std::string &payload, bool gzipped);
  bool http_post_file_(FILE *fp, size_t size, bool gzipped);

  // gzip helpers (miniz single-file, compiled in sd_logger/miniz.c)
  bool gzip_buffer_(const uint8_t *in, size_t in_len, std::string &out);

  // time helpers
  bool time_synced_() const;
  uint64_t now_ms_() const { return (uint64_t) (esp_timer_get_time() / 1000ULL); }
  time::RealTimeClock *time_{nullptr};

  // config
  std::string upload_url_;
  std::string bearer_token_;
  std::string log_path_ = "/sdcard/logs";
  uint32_t upload_interval_ms_{300000}; // 5min default
  uint32_t backoff_initial_ms_{30000};
  uint32_t backoff_max_ms_{900000};
  bool gzip_{true};

  // sensors + throttle
  std::vector<sensor::Sensor*> sensors_;
  // last send timestamp per-sensor (ms)
  std::unordered_map<sensor::Sensor*, uint64_t> last_send_ms_;
  static constexpr uint32_t SENSOR_MIN_PERIOD_MS = 30000; // once per sensor every 30s

  // live queue (non-blocking)
  std::deque<LiveLine> live_q_;
  std::mutex live_q_mtx_;

  // rotation state
  uint64_t current_window_start_s_{0};
  std::string current_file_path_cache_;

  // upload task
  TaskHandle_t upload_task_handle_{nullptr};
  uint64_t last_background_upload_ms_{0};
  uint32_t backoff_ms_{0};
  bool background_busy_{false};

  // debug tag
  static constexpr const char *TAG = "sd_logger";
};

}  // namespace sdlog
}  // namespace esphome
