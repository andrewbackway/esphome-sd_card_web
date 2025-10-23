#pragma once

#include "esphome/core/component.h"
#include "esphome/components/sensor/sensor.h"
#include "esphome/components/time/real_time_clock.h"
#include "esphome/core/log.h"

#include <vector>
#include <string>
#include <sys/stat.h>
#include <dirent.h>
#include <stdio.h>
#include <errno.h>
#include <algorithm>

#include "esp_http_client.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#include "miniz.h"

namespace esphome {
namespace sdlog {

class SDLogger : public Component {
 public:
  void setup() override;
  void loop() override;
  void dump_config() override;

  // Setters from codegen
  void set_upload_url(const std::string &url) { upload_url_ = url; }
  void set_bearer_token(const std::string &tok) { bearer_token_ = tok; }
  void set_log_path(const std::string &p) { log_path_ = p; }
  void set_upload_interval_ms(uint32_t ms) { upload_interval_ms_ = ms; }
  void set_backoff_initial_ms(uint32_t ms) { backoff_initial_ms_ = ms; current_backoff_ms_ = ms; }
  void set_backoff_max_ms(uint32_t ms) { backoff_max_ms_ = ms; }
  void set_gzip_enabled(bool en) { gzip_enabled_ = en; }
  void set_time(time::RealTimeClock *t) { time_ = t; }
  void add_tracked_sensor(sensor::Sensor *s);

 protected:
  static void upload_task_trampoline_(void *param);
  void run_upload_task_();

  bool ensure_log_dir_();
  bool list_files_(std::vector<std::string> &out);
  bool read_file_(const std::string &path, std::string &out);
  bool delete_file_(const std::string &path);

  bool upload_buffer_(const uint8_t *data, size_t len, bool is_gzip, int *http_status);
  bool gzip_compress_(const std::string &in, std::string &out);
  void schedule_next_attempt_(bool success);
  void write_csv_line_(const std::string &sensor_object_id, float value);

  // Config/state
  std::string upload_url_;
  std::string bearer_token_;
  std::string log_path_ = "/logs";

  uint32_t upload_interval_ms_{300000};  // 5 min
  uint32_t backoff_initial_ms_{30000};   // 30s
  uint32_t backoff_max_ms_{3600000};     // 1h
  uint32_t current_backoff_ms_{30000};
  uint32_t last_attempt_ms_{0};

  bool gzip_enabled_{false};

  time::RealTimeClock *time_{nullptr};

  TaskHandle_t upload_task_{nullptr};
  bool task_in_progress_{false};

  std::vector<sensor::Sensor *> sensors_;
};

}  // namespace sdlog
}  // namespace esphome
