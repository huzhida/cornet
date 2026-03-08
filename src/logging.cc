#include "utils/logging.h"

#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/basic_file_sink.h>

namespace cornet::logging {

std::once_flag init_flag;

static void logging_init() {
  auto logging_conf = config::get()["cornet"]["logging"];
  if (!logging_conf) return;

  std::vector<spdlog::sink_ptr> sinks;

  if (auto stdout_conf = logging_conf["stdout"]) {
    auto level = spdlog::level::from_str(stdout_conf["level"].value_or("info"));
    auto pattern = stdout_conf["pattern"].value_or("%^%L%$ [%Y-%m-%d %T] %v");
    auto stdout_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    stdout_sink->set_level(level);
    stdout_sink->set_pattern(pattern);

    sinks.push_back(stdout_sink);
  }

  if (auto files_conf = logging_conf["files"].as_array()) {
    for (const auto& file_node : *files_conf) {
      auto file_table_ptr = file_node.as_table();
      if (!file_table_ptr) continue;
      auto& file = *file_table_ptr;

      std::string_view path = file["path"].value_or("");
      if (path.empty()) {
        SPDLOG_WARN("config => cornet.logging.files[*].path must be not empty");
        continue;
      }
      auto level = spdlog::level::from_str(file["level"].value_or("info"));
      auto pattern = file["pattern"].value_or("%L [%Y-%m-%d %T] %v");
      auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(std::string(path), true);
      file_sink->set_level(level);
      file_sink->set_pattern(pattern);
      sinks.push_back(file_sink);
    }
  }

  auto logger = std::make_shared<spdlog::logger>("cornet", sinks.begin(), sinks.end());
  spdlog::set_default_logger(logger);
}

void init() {
  std::call_once(init_flag, logging_init);
}

}