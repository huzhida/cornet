#include "common.h"
#include "cornet_bench.h"
#include "asio_cb_bench.h"
#include "asio_coro_bench.h"
#include "libuv_bench.h"

#include "utils/config.h"
#include "utils/logging.h"

#include <iostream>
#include <map>
#include <csignal>

using namespace bench;

int main(int argc, char* argv[]) {
  signal(SIGPIPE, SIG_IGN);
  cornet::config_t::load("conf/default.toml");
  cornet::logging::init();
  signal(SIGPIPE, SIG_IGN);
  auto scenarios = default_scenarios();

  printf("╔══════════════════════════════════════════════════════════════╗\n");
  printf("║              Cornet 网络框架性能基准测试                    ║\n");
  printf("╠══════════════════════════════════════════════════════════════╣\n");
  printf("║ 测试框架: Cornet(Adaptive/RoundRobin/Batch)                ║\n");
  printf("║           Asio(回调式/协程式), Libuv                       ║\n");
  printf("║ 测试模式: Echo (客户端发送→服务端回显→客户端接收)          ║\n");
  printf("║ 指标: RPS, 吞吐量, 延迟分布, 稳定性, 内存占用             ║\n");
  printf("║ 每场景每框架运行 %d 轮, 取中位数结果                       ║\n", BENCH_ROUNDS);
  printf("╚══════════════════════════════════════════════════════════════╝\n");

  std::vector<result_t> all_results;
  // all_rounds[scenario_idx][framework_idx][round] = result
  std::vector<std::vector<std::vector<result_t>>> all_rounds;

  struct bench_entry {
    const char* name;
    bench_fn_t fn;
  };

  std::vector<bench_entry> benches = {
    {"Cornet/Adaptive",   [](const scenario_t& s) { return run_cornet(s, cornet::scheduler_type_t::Adaptive); }},
    {"Cornet/RoundRobin", [](const scenario_t& s) { return run_cornet(s, cornet::scheduler_type_t::RoundRobin); }},
    {"Cornet/Batch",      [](const scenario_t& s) { return run_cornet(s, cornet::scheduler_type_t::Batch); }},
    {"Asio/Callback",     [](const scenario_t& s) { return run_asio_callback(s); }},
    {"Asio/Coroutine",    [](const scenario_t& s) { return run_asio_coro(s); }},
    {"Libuv",             [](const scenario_t& s) { return run_libuv(s); }},
  };

  for (size_t si = 0; si < scenarios.size(); ++si) {
    auto& scenario = scenarios[si];
    print_scenario_header(scenario);

    // Warmup
    printf("  预热中...\n");
    run_cornet(scenario, cornet::scheduler_type_t::Adaptive);
    run_asio_callback(scenario);
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

    // 多轮波动展示
    print_rounds_variance(scenario_rounds);

    // 稳定性分析
    print_stability_analysis(all_results, scenario.name);

    // 场景推荐
    print_scenario_recommendation(all_results, scenario);

    all_rounds.push_back(std::move(scenario_rounds));
  }

  // 综合评估
  print_comprehensive_summary(all_results, all_rounds, scenarios);

  return 0;
}
