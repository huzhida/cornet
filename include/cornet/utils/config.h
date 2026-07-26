#ifndef CORNET_CONFIG_H
#define CORNET_CONFIG_H

#include <string>
#include <charconv>
#include <chrono>
#include <unordered_map>
#include <toml++/toml.h>


namespace cornet {

/**
  * @brief parse a duration string (e.g. "10ms", "1s", "500us") to nanoseconds
  * @param str duration string with unit suffix
  * @return parsed duration in nanoseconds
  */
inline std::chrono::nanoseconds parse_time_str(std::string_view str) {
    size_t num_pos = 0;
    while (num_pos < str.size() && (std::isdigit(str[num_pos]) || str[num_pos] == '.')) {
        ++num_pos;
    }
    
    if (num_pos == 0) {
        throw std::runtime_error("failed to parse time: " + std::string(str));
    }
    
    double value = 0;
    auto [ptr, ec] = std::from_chars(str.data(), str.data() + num_pos, value);
    if (ec != std::errc()) {
        throw std::runtime_error("failed to parse time: " + std::string(str));
    }
    
    auto unit = str.substr(num_pos);
    
    static const std::unordered_map<std::string_view, std::chrono::nanoseconds> multipliers = {
        {"ns", std::chrono::nanoseconds(1)},
        {"nsec", std::chrono::nanoseconds(1)},
        {"us", std::chrono::microseconds(1)},
        {"µs", std::chrono::microseconds(1)},
        {"usec", std::chrono::microseconds(1)},
        {"ms", std::chrono::milliseconds(1)},
        {"msec", std::chrono::milliseconds(1)},
        {"s", std::chrono::seconds(1)},
        {"sec", std::chrono::seconds(1)},
        {"second", std::chrono::seconds(1)},
        {"m", std::chrono::minutes(1)},
        {"min", std::chrono::minutes(1)},
        {"minute", std::chrono::minutes(1)},
        {"h", std::chrono::hours(1)},
        {"hr", std::chrono::hours(1)},
        {"hour", std::chrono::hours(1)},
    };
    
    auto it = multipliers.find(unit);
    if (it != multipliers.end()) {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            value * it->second
        );
    }
    
    throw std::runtime_error("unknown time unit: " + std::string(unit));
}

struct config_t : public toml::table {
  config_t() = default;
  config_t(toml::table&& config) : toml::table(std::move(config)) {}

  static config_t from_file(std::string_view path) {
    return toml::parse_file(path);
  }
};

}


#endif //CORNET_CONFIG_H
