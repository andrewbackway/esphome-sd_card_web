#pragma once

#include "esphome/core/component.h"
#include "esphome/core/log.h"
#include "esphome/components/wifi/wifi_component.h"

#include <string>
#include <vector>
#include <map>
#include <atomic>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"

namespace esphome {
namespace sdlog {

static const char *const TAG = "sd_logger";

// One sample row destined for either live upload or file CSV.
struct Sample {
  std::string sensor;
  float value;
  int64_t epoch_ms;  // wall clock in ms
};

// Simple ring-scoped backoff tracker
struct Backoff {
  uint32_t initial_ms{30000};
  uint32_t max_ms{900000};
  uint32_t current_ms{0};
  void reset() { current_ms = 0; }
  uint32_t next() {
    if (current_ms == 0) current_ms = initial_ms;
    else current_ms = std::min<uint32_t>(max_ms, current_ms * 2);
    return current_ms;
  }
};

class SDLogger : public Component {
 public:
  // --- Configurable (via YAML) ---
  void set_upload_url(const std::string &url) { upload_url_ = url; }
  void set_bearer_token(const std::string &token) { bearer_token_ = token; }
  void set_log_path(const std::string &path) { log_path_ = path; }
  void set_gzip(bool enabled) { gzip_enabled_ = enabled; }
  void set_upload_interval_ms(uint32_t ms) { upload_interval_ms_ = ms; }  // for periodic kicks (kept)
  void set_backoff(uint32_t initial_ms, uint32_t max_ms) {
    backoff_.initial_ms = initial_ms; backoff_.max_ms = max_ms;
  }
  void set_live_throttle_ms(uint32_t ms) { live_throttle_ms_ = ms; } // default 30000

  // Call when a tracked sensor publishes a value.
  // (You already wire this up in your existing integration—no lambdas here.)
  void on_sensor_update(const std::string &name, float value);

  // Core
  void setup() override;
  void loop() override;
  void dump_config() override;

 protected:
  // --- connectivity ---
  bool is_online_() const;
  bool network_ready_() const;

  // --- time helpers ---
  static int64_t now_ms_();              // wallclock in ms (requires SNTP)
  static bool time_synced_();            // true once system time is set

  // --- file rotation & csv ---
  bool ensure_log_dir_();
  void maybe_rotate_file_(int64_t now_ms);
  bool open_file_for_window_(int64_t window_start_ms);
  void close_current_file_();
  std::string make_filename_(int64_t window_start_ms) const;  // /sdcard/logs/YYYY-MM-DD_HH-MM-SS.csv
  bool append_csv_line_(const Sample &s);

  // --- upload paths ---
  void start_live_task_if_needed_();
  void start_backlog_task_if_needed_();

  static void live_task_trampoline_(void *param);
  static void backlog_task_trampoline_(void *param);

  void run_live_task_();     // consumes live_queue_ and performs HTTPS posts
  void run_backlog_task_();  // scans directory and uploads oldest files

  bool upload_sample_http_(const Sample &s); // live POST, no gzip
  bool upload_file_http_(const std::string &path); // file POST, gzip optional

  // gzip utility (when enabled)
  bool gzip_file_to_temp_(const std::string &src, std::string &out_tmp_path, size_t &out_size);

  // fallback to file when live upload fails
  void fallback_to_file_(const Sample &s, int64_t now_ms);

  // scan log dir for *.csv or *.csv.gz oldest-first
  bool find_oldest_log_file_(std::string &out_path);

 protected:
  // config
  std::string upload_url_;
  std::string bearer_token_;
  std::string log_path_{"/sdcard/logs"};
  bool gzip_enabled_{true};
  uint32_t upload_interval_ms_{300000}; // still used to nudge backlog
  uint32_t live_throttle_ms_{30000};
  Backoff backoff_;

  // live throttle cache
  std::map<std::string, int64_t> last_live_sent_ms_;

  // queues & tasks
  QueueHandle_t live_queue_{nullptr};           // samples for live upload
  TaskHandle_t live_task_{nullptr};
  TaskHandle_t backlog_task_{nullptr};
  std::atomic<bool> task_live_running_{false};
  std::atomic<bool> task_backlog_running_{false};

  // file state
  FILE *cur_file_{nullptr};
  std::string cur_file_path_;
  int64_t cur_window_start_ms_{0}; // aligned to 30s boundary

  // timing
  int64_t last_upload_kick_ms_{0};

  // misc
  bool log_dir_ok_{false};
  bool warned_no_time_{false};
};

}  // namespace sdlog
}  // namespace esphome
