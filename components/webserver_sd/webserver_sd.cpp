#include "webserver_sd.h"

#include <map>

#include "esphome/components/network/util.h"
#include "esphome/core/helpers.h"
#include "esphome/core/log.h"

namespace esphome {
namespace webserver_sd {

static const char* TAG = "sd_file_server";

SDFileServer::SDFileServer(web_server_base::WebServerBase* base)
    : base_(base) {}

void SDFileServer::setup() { this->base_->add_handler(this); }

void SDFileServer::dump_config() {
  ESP_LOGCONFIG(TAG, "Webserver SD:");
  ESP_LOGCONFIG(TAG, "  Address: %s:%u", network::get_use_address().c_str(),
                this->base_->get_port());
  ESP_LOGCONFIG(TAG, "  Url Prefix: %s", this->url_prefix_.c_str());
  ESP_LOGCONFIG(TAG, "  Root Path: %s", this->root_path_.c_str());
  ESP_LOGCONFIG(TAG, "  Deletion Enabled: %s",
                TRUEFALSE(this->deletion_enabled_));
  ESP_LOGCONFIG(TAG, "  Download Enabled : %s",
                TRUEFALSE(this->download_enabled_));
  ESP_LOGCONFIG(TAG, "  Upload Enabled : %s", TRUEFALSE(this->upload_enabled_));
}

bool SDFileServer::canHandle(AsyncWebServerRequest* request) const {
  return str_startswith(std::string(request->url().c_str()),
                        this->build_prefix());
}

void SDFileServer::handleRequest(AsyncWebServerRequest* request) {
  if (!str_startswith(std::string(request->url().c_str()),
                      this->build_prefix()))
    return;

  auto method = request->method();
  std::string url = request->url().c_str();

  if (method == HTTP_GET) {
    // workaround for delete Detect ?delete (or &delete, delete=1, etc.)
    if (request->hasParam("delete")) {
      this->handle_delete(request);
      return;
    }

    this->handle_get(request);
  }
}

void SDFileServer::handleUpload(AsyncWebServerRequest* request,
                                const std::string& filename, size_t index,
                                uint8_t* data, size_t len, bool final) {
  if (!this->upload_enabled_) {
    request->send(401, "application/json",
                  "{ \"error\": \"file upload is disabled\" }");
    return;
  }

  std::string extracted =
      this->extract_path_from_url(std::string(request->url().c_str()));
  std::string path = this->build_absolute_path(extracted);

  if (index == 0 && !this->sd_mmc_->is_directory(path)) {
    auto response = request->beginResponse(
        401, "application/json", "{ \"error\": \"invalid upload folder\" }");
    response->addHeader("Connection", "close");
    request->send(response);
    return;
  }

  std::string file_name(filename.c_str());
  if (index == 0) {
    this->sd_mmc_->write_file(Path::join(path, file_name).c_str(), data, len);
    return;
  }

  this->sd_mmc_->append_file(Path::join(path, file_name).c_str(), data, len);

  if (final) {
    auto response = request->beginResponse(201, "text/html", "upload success");
    response->addHeader("Connection", "close");
    request->send(response);
  }
}

void SDFileServer::set_url_prefix(const std::string& prefix) {
  this->url_prefix_ = prefix;
}
void SDFileServer::set_root_path(const std::string& path) {
  this->root_path_ = path;
}
void SDFileServer::set_sd_mmc(sd_mmc::SdMmc* card) { this->sd_mmc_ = card; }
void SDFileServer::set_deletion_enabled(bool allow) {
  this->deletion_enabled_ = allow;
}
void SDFileServer::set_download_enabled(bool allow) {
  this->download_enabled_ = allow;
}
void SDFileServer::set_upload_enabled(bool allow) {
  this->upload_enabled_ = allow;
}

void SDFileServer::handle_get(AsyncWebServerRequest* request) const {
  std::string extracted =
      this->extract_path_from_url(std::string(request->url().c_str()));
  std::string path = this->build_absolute_path(extracted);

  if (!this->sd_mmc_->is_directory(path)) {
    handle_download(request, path);
    return;
  }
  handle_index(request, path);
}

#include <string>
#include <vector>

// Assuming necessary includes for Path, sd_mmc, AsyncWebServerRequest, etc.,
// are already present.

// Helper function to escape JSON strings (for names and URIs that might contain
// special characters)
std::string escape_json(const std::string& s) {
  std::string res;
  for (char c : s) {
    if (c == '"') {
      res += "\\\"";
    } else if (c == '\\') {
      res += "\\\\";
    } else if (c == '\n') {
      res += "\\n";
    } else if (c == '\r') {
      res += "\\r";
    } else if (c == '\t') {
      res += "\\t";
    } else {
      res += c;
    }
  }
  return res;
}

// Refactored to append a JSON object for a file/directory to the JSON string
void SDFileServer::append_json_row(std::string& json, bool& first,
                                   const sd_mmc::FileInfo& info) const {
  if (!first) {
    json += ",\n";
  }
  first = false;

  std::string file_name = Path::file_name(info.path);
  std::string uri =
      "/" + Path::join(this->url_prefix_,
                       Path::remove_root_path(info.path, this->root_path_));

  json += "  {\n";
  json += "    \"name\": \"" + escape_json(file_name) + "\",\n";
  json += "    \"is_directory\": " +
          (info.is_directory ? std::string("true") : std::string("false")) +
          ",\n";
  if (!info.is_directory) {
    json += "    \"size\": " + std::to_string(info.size) + ",\n";
  }
  json += "    \"uri\": \"" + escape_json(uri) + "\"\n";
  json += "  }";
}

// Refactored to handle JSON response instead of HTML
void SDFileServer::handle_index(AsyncWebServerRequest* request,
                                const std::string& path) const {
  AsyncResponseStream* response =
      request->beginResponseStream("application/json");

  // Build breadcrumbs array
  std::string current_path = "/";
  std::string relative_path = Path::join(
      this->url_prefix_, Path::remove_root_path(path, this->root_path_));
  std::vector<std::string> parts = Path::split_path(relative_path);

  std::string json = "{\n";

  // Add current path
  json += "  \"current_path\": \"" + escape_json(relative_path) + "\",\n";

  // Add enabled flags (for client-side handling)
  json += "  \"upload_enabled\": " +
          (this->upload_enabled_ ? std::string("true") : std::string("false")) +
          ",\n";
  json +=
      "  \"download_enabled\": " +
      (this->download_enabled_ ? std::string("true") : std::string("false")) +
      ",\n";
  json +=
      "  \"delete_enabled\": " +
      (this->deletion_enabled_ ? std::string("true") : std::string("false")) +
      ",\n";

  

  json += "  \"breadcrumbs\": [\n";
  bool first_breadcrumb = true;
  for (const auto& part : parts) {
    if (!part.empty()) {
      current_path = Path::join(current_path, part);
      if (!first_breadcrumb) {
        json += ",\n";
      }
      first_breadcrumb = false;
      json += "    {\n";
      json += "      \"name\": \"" + escape_json(part) + "\",\n";
      json += "      \"url\": \"" + escape_json(current_path) + "\"\n";
      json += "    }";
    }
  }
  json += "\n  ],\n";

  // Build files array
  json += "  \"items\": [\n";
  auto entries = this->sd_mmc_->list_directory_file_info(path, 0);
  bool first_file = true;
  for (const auto& entry : entries) {
    append_json_row(json, first_file, entry);
  }
  json += "\n  ]\n";

  json += "}";

  response->print(json.c_str());
  request->send(response);
}

void SDFileServer::handle_download(AsyncWebServerRequest* request,
                                   const std::string& path) const {
  if (!this->download_enabled_) {
    request->send(401, "application/json",
                  "{ \"error\": \"file download is disabled\" }");
    return;
  }

  auto file = this->sd_mmc_->read_file(path);
  if (file.size() == 0) {
    request->send(401, "application/json",
                  "{ \"error\": \"failed to read file\" }");
    return;
  }

#ifdef USE_ESP_IDF
  auto* response = request->beginResponse(200, Path::mime_type(path).c_str(),
                                          file.data(), file.size());
#else
  auto* response =
      request->beginResponseStream(Path::mime_type(path).c_str(), file.size());
  response->write(file.data(), file.size());
#endif

  request->send(response);
}

void SDFileServer::handle_delete(AsyncWebServerRequest* request) {
  if (!this->deletion_enabled_) {
    request->send(401, "application/json",
                  "{ \"error\": \"file deletion is disabled\" }");
    return;
  }

  std::string extracted =
      this->extract_path_from_url(std::string(request->url().c_str()));
  std::string path = this->build_absolute_path(extracted);

  if (this->sd_mmc_->is_directory(path)) {
    request->send(401, "application/json",
                  "{ \"error\": \"cannot delete a directory\" }");
    return;
  }

  if (this->sd_mmc_->delete_file(path)) {
    request->send(204, "application/json", "{}");
    return;
  }

  request->send(401, "application/json",
                "{ \"error\": \"failed to delete file\" }");
}

std::string SDFileServer::build_prefix() const {
  if (this->url_prefix_.empty() || this->url_prefix_[0] != '/')
    return "/" + this->url_prefix_;
  return this->url_prefix_;
}

std::string SDFileServer::extract_path_from_url(const std::string& url) const {
  std::string prefix = this->build_prefix();
  return url.substr(prefix.size(), url.size() - prefix.size());
}

std::string SDFileServer::build_absolute_path(std::string relative_path) const {
  if (relative_path.empty()) return this->root_path_;
  return Path::join(this->root_path_, relative_path);
}

std::string Path::file_name(const std::string& path) {
  size_t pos = path.rfind(separator);
  if (pos != std::string::npos) return path.substr(pos + 1);
  return "";
}

bool Path::is_absolute(const std::string& path) {
  return !path.empty() && path[0] == separator;
}

bool Path::trailing_slash(const std::string& path) {
  return !path.empty() && path.back() == separator;
}

std::string Path::join(const std::string& first, const std::string& second) {
  std::string result = first;
  if (!trailing_slash(first) && !is_absolute(second))
    result.push_back(separator);
  if (trailing_slash(first) && is_absolute(second)) result.pop_back();
  result.append(second);
  return result;
}

std::string Path::remove_root_path(std::string path, const std::string& root) {
  if (!str_startswith(path, root)) return path;
  if (path.size() == root.size() || path.size() < 2) return "/";
  return path.erase(0, root.size());
}

std::vector<std::string> Path::split_path(std::string path) {
  std::vector<std::string> parts;
  size_t pos = 0;
  while ((pos = path.find('/')) != std::string::npos) {
    std::string part = path.substr(0, pos);
    if (!part.empty()) parts.push_back(part);
    path.erase(0, pos + 1);
  }
  if (!path.empty()) parts.push_back(path);
  return parts;
}

std::string Path::extension(const std::string& file) {
  size_t pos = file.find_last_of('.');
  if (pos == std::string::npos) return "";
  return file.substr(pos + 1);
}

std::string Path::mime_type(const std::string& file) {
  static const std::map<std::string, std::string> file_types = {
      {"mp3", "audio/mpeg"},        {"wav", "audio/vnd.wav"},
      {"png", "image/png"},         {"jpg", "image/jpeg"},
      {"jpeg", "image/jpeg"},       {"bmp", "image/bmp"},
      {"txt", "text/plain"},        {"log", "text/plain"},
      {"csv", "text/csv"},          {"html", "text/html"},
      {"css", "text/css"},          {"js", "text/javascript"},
      {"json", "application/json"}, {"xml", "application/xml"},
      {"zip", "application/zip"},   {"gz", "application/gzip"},
      {"tar", "application/x-tar"}, {"mp4", "video/mp4"},
      {"avi", "video/x-msvideo"},   {"webm", "video/webm"}};

  std::string ext = Path::extension(file);
  ESP_LOGD(TAG, "ext : %s", ext.c_str());
  if (!ext.empty()) {
    std::transform(ext.begin(), ext.end(), ext.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    auto it = file_types.find(ext);
    if (it != file_types.end()) return it->second;
  }
  return "application/octet-stream";
}

}  // namespace webserver_sd
}  // namespace esphome