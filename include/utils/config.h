#ifndef CORNET_CONFIG_H
#define CORNET_CONFIG_H

#include <string>
#include <charconv>
#include <chrono>
#include <toml++/toml.h>

namespace cornet {
struct config_t {

  inline void load_from_path(std::string_view path) {
    root = toml::parse_file(path);
  }

  template<typename T>
  T& get(std::string_view key, T default_value) {
    return root.get_as<T>(key).value_or(default_value);
  }

  template<typename T>
  T& get(std::string_view key) {
    return root.get_as<T>(key);
  }

  auto operator[](std::string_view key) {
    return root[key];
  }

  static config_t& get() {
    static config_t s;
    return s;
  }

  static void load(std::string_view path) {
    get().load_from_path(path);
  }

  static inline std::chrono::nanoseconds to_nanoseconds(std::string_view str) {
    size_t num_pos = 0;
    while (num_pos < str.size() && (isdigit(str[num_pos]) || str[num_pos] == '.')) {
      ++num_pos;
    }

    if (num_pos == 0) {
      throw std::runtime_error("failed to parse to time:" + std::string(str));
    }

    double value = 0;
    auto [ptr, ec] = std::from_chars(str.data(), str.data() + num_pos, value);
    if (ec != std::errc()) {
      throw std::runtime_error("failed to parse to time:" + std::string(str));
    }

    std::string_view unit = str.substr(num_pos);

    if (unit == "ns" || unit == "nsec") {
      return std::chrono::nanoseconds(static_cast<int64_t>(value));
    } else if (unit == "us" || unit == "µs" || unit == "usec") {
      return std::chrono::microseconds(static_cast<int64_t>(value));
    } else if (unit == "ms" || unit == "msec") {
      return std::chrono::milliseconds(static_cast<int64_t>(value));
    } else if (unit == "s" || unit == "sec" || unit == "second") {
      return std::chrono::seconds(static_cast<int64_t>(value));
    } else if (unit == "m" || unit == "min" || unit == "minute") {
      return std::chrono::minutes(static_cast<int64_t>(value));
    } else if (unit == "h" || unit == "hr" || unit == "hour") {
      return std::chrono::hours(static_cast<int64_t>(value));
    } else {
      throw std::runtime_error("failed to parse to time:" + std::string(str));
    }
  }

private:
  toml::table root;
};
}


#endif //CORNET_CONFIG_H
