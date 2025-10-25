#pragma once

#include "esphome/core/component.h"
#include "esphome/core/time.h"
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

  #include "esp_system.h"      // esp_read_mac, ESP_MAC_WIFI_STA
}

extern "C" {
  #include "esp_http_client.h"
  #include "esp_tls.h"
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

  void set_ping_url(const std::string &u) { this->ping_url_ = u; }
  void set_ping_interval_ms(uint32_t ms) { this->ping_interval_ms_ = ms; }
  void set_ping_timeout_ms(uint32_t ms) { this->ping_timeout_ms_ = ms; }
  bool send_http_ping_(int *http_status, std::string *resp_err);

  void setup() override;
  void loop() override;
  float get_setup_priority() const override { return setup_priority::AFTER_WIFI; }

 protected:
  // helpers
  bool time_valid_() const;
  void ensure_log_dir_();
  bool build_payload_json_(std::string &out_json);
  bool write_window_file_(const std::string &json);
  bool send_http_put_(const std::string &body, int *http_status, std::string *resp_err);
  void publish_sync_online_(bool v);
  void publish_sync_backlog_(bool v);

  static void task_live_entry_(void *param);
  static void task_backlog_entry_(void *param);

  bool has_backlog_files_();
  bool find_oldest_file_(std::string &path_out);
  bool load_file_(const std::string &path, std::string &data_out);
  bool delete_file_(const std::string &path);

  bool http_request_(
      const char *url,
      esp_http_client_method_t method,
      const char *content_type,
      const uint8_t *body, size_t body_len,
      uint32_t timeout_ms,
      int *http_status,
      std::string *resp_err);

  bool http_ping_(const char *url, uint32_t timeout_ms, int *http_status, std::string *resp_err);

  struct LiveItem { std::string json; };

  time::RealTimeClock *time_{nullptr};
  std::vector<sensor::Sensor *> sensors_;
  binary_sensor::BinarySensor *sync_online_bs_{nullptr};
  binary_sensor::BinarySensor *sync_sending_backlog_bs_{nullptr};

  std::string upload_url_;
  std::string bearer_token_;
  std::string log_path_;
  uint32_t backoff_initial_ms_{30000};
  uint32_t backoff_max_ms_{15 * 60 * 1000};
  std::string ping_url_;               
  uint32_t    ping_interval_ms_{10000};
  uint32_t    ping_timeout_ms_{3000};  

  bool have_started_{false};
  uint32_t start_valid_epoch_{0};
  uint32_t last_tick_window_{0};
  bool sync_online_{false};

  TaskHandle_t task_live_{nullptr};
  TaskHandle_t task_backlog_{nullptr};
  QueueHandle_t live_queue_{nullptr};

  uint32_t backlog_backoff_ms_{0};
};

}  // namespace sd_logger
}  // namespace esphome