#pragma once

#include "esphome/core/component.h"
#include "esphome/components/time/real_time_clock.h"
#include "esphome/components/sensor/sensor.h"
#include "esphome/components/binary_sensor/binary_sensor.h"
#include "esphome/components/json/json_util.h"

#include <vector>
#include <string>

extern "C" {
  #include "freertos/FreeRTOS.h"
  #include "freertos/task.h"
  #include "freertos/queue.h"
}

namespace esphome {
namespace sd_logger {

class SdLogger : public Component {
 public:
  void set_time(time::RealTimeClock *t) { this->time_ = t; }
  void set_upload_url(const std::string &u) { this->upload_url_ = u; }
  void set_bearer_token(const std::string &t) { this->bearer_token_ = t; }
  void set_log_path(const std::string &p) { this->log_path_ = p; }
  void set_backoff_initial_ms(uint32_t ms) { this->backoff_initial_ms_ = ms; }
  void set_backoff_max_ms(uint32_t ms) { this->backoff_max_ms_ = ms; }
  void set_sensors(const std::vector<sensor::Sensor *> &s) { this->sensors_ = s; }

  void set_sync_online_binary_sensor(binary_sensor::BinarySensor *b) { this->sync_online_bs_ = b; }
  void set_sync_sending_backlog_binary_sensor(binary_sensor::BinarySensor *b) { this->sync_sending_backlog_bs_ = b; }

  // Component overrides
  void setup() override;
  void loop() override;
  float get_setup_priority() const override { return setup_priority::AFTER_WIFI; }

 protected:
  // Internal helpers
  bool time_valid_() const;
  void ensure_log_dir_();
  bool build_payload_json_(std::string &out_json, bool &had_any_value);
  bool write_window_file_(const std::string &json);
  bool send_http_put_(const std::string &body, int *http_status, std::string *resp_err);
  void publish_sync_online_(bool v);
  void publish_sync_backlog_(bool v);

  // Task entry points
  static void task_live_entry_(void *param);
  static void task_backlog_entry_(void *param);

  // Backlog helpers
  bool has_backlog_files_();
  bool find_oldest_file_(std::string &path_out);
  bool load_file_(const std::string &path, std::string &data_out);
  bool delete_file_(const std::string &path);

  // Queue payload struct
  struct LiveItem {
    std::string json;
  };

  // Members
  time::RealTimeClock *time_{nullptr};
  std::vector<sensor::Sensor *> sensors_;
  binary_sensor::BinarySensor *sync_online_bs_{nullptr};
  binary_sensor::BinarySensor *sync_sending_backlog_bs_{nullptr};

  std::string upload_url_;
  std::string bearer_token_;
  std::string log_path_;
  uint32_t backoff_initial_ms_{30000};
  uint32_t backoff_max_ms_{15 * 60 * 1000};

  // State
  bool have_started_{false};
  uint32_t start_valid_epoch_{0};  // once time becomes valid
  uint32_t last_tick_epoch_{0};    // last 30s window we processed
  bool sync_online_{false};

  // FreeRTOS
  TaskHandle_t task_live_{nullptr};
  TaskHandle_t task_backlog_{nullptr};
  QueueHandle_t live_queue_{nullptr};  // queue of LiveItem

  // Backlog backoff
  uint32_t backlog_backoff_ms_{0};
};

}  // namespace sd_logger
}  // namespace esphome
