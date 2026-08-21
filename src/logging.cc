#include "cornet/utils/logging.h"

#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/basic_file_sink.h>

#include "cornet/utils/config.h"

namespace cornet::logging {

namespace {

// Human-facing layout: the historical default, good for interactive/dev use.
constexpr const char* kHumanPattern = "%^%L%$ [%Y-%m-%d %T %t %@] %v";
// Machine-facing layout: key=value fields, single line per event. Pairs with
// log collectors (loki / elasticsearch) that can tokenize k=v with a single
// regex. The message at %v is itself expected to use k=v pairs (the HTTP
// TRACE logger already does), so field extraction works end-to-end.
constexpr const char* kKvPattern =
    "ts=%Y-%m-%dT%H:%M:%S.%e%z level=%l tid=%t src=%@ msg=\"%v\"";

// Resolve which pattern to apply to a sink node. Priority: explicit
// `pattern` override > `format` keyword > per-sink default.
std::string resolve_pattern(toml::node_view<const toml::node> t, std::string_view default_format) {
  if (auto override_pat = t["pattern"].value<std::string_view>()) {
    return std::string(*override_pat);
  }
  auto fmt = t["format"].value_or(default_format);
  if (fmt == "kv") return kKvPattern;
  if (fmt == "human") return kHumanPattern;
  // Anything unknown still produces output; silently picking the structured
  // form here is what a confused user would prefer over unreadable bytes.
  return kKvPattern;
}

} // namespace

std::once_flag init_flag;

static void logging_init(const config_t& config) {
  auto logging_conf = config["cornet"]["logging"];
  if (!logging_conf) return;

  std::vector<spdlog::sink_ptr> sinks;

  if (auto stdout_conf = logging_conf["stdout"]) {
    auto level = spdlog::level::from_str(stdout_conf["level"].value_or("info"));
    auto stdout_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    stdout_sink->set_level(level);
    // stdout is what a developer stares at: stay human by default, let the
    // config switch to kv when piping into a collector.
    stdout_sink->set_pattern(resolve_pattern(stdout_conf, "human"));
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
      auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(std::string(path), true);
      file_sink->set_level(level);
      // Files go to collectors: default kv, let format="human" opt out.
      // Array iteration hands us a `const toml::node&`; wrap to node_view so
      // resolve_pattern speaks one interface regardless of source shape.
      file_sink->set_pattern(resolve_pattern(toml::node_view<const toml::node>(file_node), "kv"));
      sinks.push_back(file_sink);
    }
  }

  if (sinks.empty()) {
    spdlog::set_level(spdlog::level::off);
    return;
  }

  auto logger = std::make_shared<spdlog::logger>("cornet", sinks.begin(), sinks.end());
  spdlog::set_default_logger(logger);
}

void init(const cornet::config_t& config = {}) {
  std::call_once(init_flag, logging_init, config);
}

}