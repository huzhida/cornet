#include "common.h"
#include "cornet_bench.h"
#include "asio_cb_bench.h"
#include "asio_coro_bench.h"
#ifdef CORNET_BENCH_HTTP
#include "http_bench.h"
#endif

#include "cornet/utils/config.h"
#include "cornet/utils/logging.h"

#include <csignal>
#include <cstring>

using namespace bench;

/**
 * Row/scenario slicers so a single number can be taken under the profiler
 * instead of the whole table. --only matches against framework names,
 * --scenario against scenario names; both are substring, case-sensitive,
 * comma-separable.
 */
static bool matches_filter(const char* filter, std::string_view name) {
  if (!filter || !*filter) return true;
  std::string f(filter);
  size_t pos = 0;
  while (pos <= f.size()) {
    auto comma = f.find(',', pos);
    auto item = std::string_view(f).substr(pos, comma == std::string::npos
                                                  ? std::string_view::npos
                                                  : comma - pos);
    if (!item.empty() && name.find(item) != std::string_view::npos) return true;
    if (comma == std::string::npos) break;
    pos = comma + 1;
  }
  return false;
}

int main(int argc, char* argv[]) {
  signal(SIGPIPE, SIG_IGN);
  auto config = cornet::config_t::from_file("conf/default.toml");
  cornet::logging::init(config);
  signal(SIGPIPE, SIG_IGN);
  const char* only = nullptr;
  const char* scenario_filter = nullptr;
  for (int i = 1; i + 1 < argc; ++i) {
    if (std::strcmp(argv[i], "--only") == 0) only = argv[i + 1];
    if (std::strcmp(argv[i], "--scenario") == 0) scenario_filter = argv[i + 1];
  }
  auto all = default_scenarios();
  std::vector<scenario_t> scenarios;
  for (auto& sc : all) {
    if (matches_filter(scenario_filter, sc.name)) scenarios.push_back(sc);
  }
  if (scenarios.empty()) {
    fprintf(stderr, "bench: --scenario matched nothing (see common.h for names)\n");
    return 2;
  }

  printf("╔══════════════════════════════════════════════════════════════╗\n");
  printf("║              Cornet 网络框架性能基准测试                       ║\n");
  printf("╠══════════════════════════════════════════════════════════════╣\n");
  printf("║ 测试框架: Cornet                                              ║\n");
  printf("║           Asio(回调式/协程式)                                 ║\n");
#ifdef CORNET_BENCH_HTTP
  printf("║           Cornet HTTP/1.1 (仅服务端 / 完整栈)                 ║\n");
#endif
#ifdef CORNET_BENCH_TLS
  printf("║           Cornet HTTPS (TLS, 同口径)                          ║\n");
#endif
  printf("║ 测试模式: Echo (客户端发送→服务端回显→客户端接收)               ║\n");
  printf("║ 指标: RPS, 吞吐量, 延迟分布, 稳定性, 内存占用                   ║\n");
  printf("║ 每场景每框架运行 %d 轮, 取中位数结果                            ║\n", BENCH_ROUNDS);
  printf("╚══════════════════════════════════════════════════════════════╝\n");

  std::vector<result_t> all_results;
  // all_rounds[scenario_idx][framework_idx][round] = result
  std::vector<std::vector<std::vector<result_t>>> all_rounds;

  struct bench_entry {
    const char* name;
    bench_fn_t fn;
  };

  std::vector<bench_entry> benches = {
    {"Cornet",[&config](const scenario_t& s) { return run_cornet(s,config); }},
    {"Asio/Callback", [](const scenario_t& s) { return run_asio_callback(s); }},
    {"Asio/Coroutine", [](const scenario_t& s) { return run_asio_coro(s); }},
#ifdef CORNET_BENCH_HTTP
    {"Cornet/HTTPsrv", [&config](const scenario_t& s) { return run_cornet_http_server(s, config); }},
    {"Cornet/HTTP", [&config](const scenario_t& s) { return run_cornet_http(s, config); }},
#endif
#ifdef CORNET_BENCH_TLS
    {"Cornet/HTTPS", [&config](const scenario_t& s) { return run_cornet_https(s, config); }},
#endif
  };

  if (only && *only) {
    benches.erase(std::remove_if(benches.begin(), benches.end(),
                                 [&](const bench_entry& b) {
                                   return !matches_filter(only, b.name);
                                 }),
                  benches.end());
    if (benches.empty()) {
      fprintf(stderr, "bench: --only matched no framework (see --help rows)\n");
      return 2;
    }
  }

  for (size_t si = 0; si < scenarios.size(); ++si) {
    auto& scenario = scenarios[si];
    print_scenario_header(scenario);

    // Warmup
    printf("  预热中...\n");
    for (auto& b : benches) b.fn(scenario);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    print_result_header();

    std::vector<std::vector<result_t>> scenario_rounds; // [framework][round]

    for (size_t fi = 0; fi < benches.size(); ++fi) {
      std::vector<result_t> runs;
      for (int round = 0; round < BENCH_ROUNDS; ++round) {
        auto r = benches[fi].fn(scenario);
        runs.push_back(r);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
      auto median = median_result(runs);
      print_result(median);
      all_results.push_back(median);
      scenario_rounds.push_back(std::move(runs));
    }

    // 收集中位数结果用于图表
    std::vector<result_t> scenario_medians;
    for (auto& runs : scenario_rounds)
      if (!runs.empty()) scenario_medians.push_back(median_result(runs));

    // 性能可视化图表
    print_rps_latency_chart(scenario_medians);
    print_latency_profile_chart(scenario_medians);
    print_throughput_chart(scenario_medians);
    print_stability_chart(scenario_medians);

    all_rounds.push_back(std::move(scenario_rounds));
  }

  // 综合评估
  print_comprehensive_summary(all_results, all_rounds, scenarios);

#ifdef CORNET_BENCH_HTTP
  // HTTP 相对裸 send/recv 的开销
  print_http_overhead_summary(all_results);
#endif
#ifdef CORNET_BENCH_TLS
  // TLS 相对明文 HTTP 的开销
  print_tls_overhead_summary(all_results);
#endif

  return 0;
}
