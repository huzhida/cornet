#ifndef CORNET_CONFIG_H
#define CORNET_CONFIG_H

#include <string>
#include <charconv>
#include <chrono>
#include <toml++/toml.h>

namespace cornet {

/**
 * @brief global configuration backed by TOML file.
 * Provides typed access to config values with defaults.
 */
struct config_t {

  /**
   * @brief load configuration from a TOML file
   * @param path file path to the TOML config
   */
  inline void load_from_path(std::string_view path) {
    root = toml::parse_file(path);
  }

  /**
   * @brief get a typed config value with a default fallback
   * @tparam T value type
   * @param key dotted config key
   * @param default_value fallback if key is missing
   * @return the config value or default
   */
  template<typename T>
  T get(std::string_view key, T default_value) {
    return root.get_as<T>(key).value_or(default_value);
  }

  /**
   * @brief get a typed config value as optional
   * @tparam T value type
   * @param key dotted config key
   * @return optional-like result
   */
  template<typename T>
  auto get(std::string_view key) {
    return root.get_as<T>(key);
  }

  /**
   * @brief subscript access to TOML table nodes
   * @param key top-level key
   * @return TOML node
   */
  auto operator[](std::string_view key) {
    return root[key];
  }

  /**
   * @brief get the global config singleton
   * @return singleton reference
   */
  static config_t& get() {
    static config_t s;
    return s;
  }

  /**
   * @brief load config from file into the global singleton
   * @param path file path to the TOML config
   */
  static void load(std::string_view path) {
    get().load_from_path(path);
  }

  /**
   * @brief parse a duration string (e.g. "10ms", "1s", "500us") to nanoseconds
   * @param str duration string with unit suffix
   * @return parsed duration in nanoseconds
   */
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
  // parsed TOML configuration table
  toml::table root;
};
}


#endif //CORNET_CONFIG_H
