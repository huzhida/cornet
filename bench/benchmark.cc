#include "common.h"
#include "cornet_bench.h"
#include "asio_cb_bench.h"
#include "asio_coro_bench.h"
#include "libuv_bench.h"

#include "utils/config.h"
#include "utils/logging.h"

#include <iostream>
#include <map>

using namespace bench;

int main(int argc, char* argv[]) {
  cornet::config_t::load("conf/default.toml");
  cornet::logging::init();

  auto scenarios = default_scenarios();

  printf("╔══════════════════════════════════════════════════════════════╗\n");
  printf("║              Cornet 网络框架性能基准测试                    ║\n");
  printf("╠══════════════════════════════════════════════════════════════╣\n");
  printf("║ 测试框架: Cornet(Adaptive/RoundRobin/Batch)                ║\n");
  printf("║           Asio(回调式/协程式), Libuv                       ║\n");
  printf("║ 测试模式: Echo (客户端发送→服务端回显→客户端接收)          ║\n");
  printf("║ 指标: RPS, 吞吐量, 延迟分布, 稳定性, 内存占用             ║\n");
  printf("╚══════════════════════════════════════════════════════════════╝\n");

  std::vector<result_t> all_results;

  for (auto& scenario : scenarios) {
    print_scenario_header(scenario);

    // Warmup (不计入结果)
    printf("  预热中...\n");
    run_cornet(scenario, cornet::scheduler_type_t::Adaptive);
    run_asio_callback(scenario);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    print_result_header();

    // Cornet Adaptive
    {
      auto r = run_cornet(scenario, cornet::scheduler_type_t::Adaptive);
      print_result(r);
      all_results.push_back(r);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Cornet RoundRobin
    {
      auto r = run_cornet(scenario, cornet::scheduler_type_t::RoundRobin);
      print_result(r);
      all_results.push_back(r);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Cornet Batch
    {
      auto r = run_cornet(scenario, cornet::scheduler_type_t::Batch);
      print_result(r);
      all_results.push_back(r);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Asio Callback
    {
      auto r = run_asio_callback(scenario);
      print_result(r);
      all_results.push_back(r);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Asio Coroutine
    {
      auto r = run_asio_coro(scenario);
      print_result(r);
      all_results.push_back(r);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Libuv
    {
      auto r = run_libuv(scenario);
      print_result(r);
      all_results.push_back(r);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 稳定性分析
    print_stability_analysis(all_results, scenario.name);

    // 场景推荐
    print_scenario_recommendation(all_results, scenario);
  }

  // 综合总结
  printf("\n");
  printf("╔══════════════════════════════════════════════════════════════╗\n");
  printf("║                      综合评估                              ║\n");
  printf("╚══════════════════════════════════════════════════════════════╝\n\n");

  // 统计每个框架赢了多少场景
  std::map<std::string, int> rps_wins;
  std::map<std::string, int> latency_wins;
  std::map<std::string, double> avg_rps;
  std::map<std::string, int> framework_count;

  for (auto& scenario : scenarios) {
    const result_t* best_rps = nullptr;
    const result_t* best_lat = nullptr;
    for (auto& r : all_results) {
      if (r.scenario != scenario.name) continue;
      if (!best_rps || r.rps > best_rps->rps) best_rps = &r;
      if (!best_lat || r.p99_latency_us < best_lat->p99_latency_us) best_lat = &r;
      avg_rps[r.framework] += r.rps;
      framework_count[r.framework]++;
    }
    if (best_rps) rps_wins[best_rps->framework]++;
    if (best_lat) latency_wins[best_lat->framework]++;
  }

  printf("  [吞吐量冠军次数]\n");
  for (auto& [fw, wins] : rps_wins) {
    printf("    %-20s %d/%zu 场景\n", fw.c_str(), wins, scenarios.size());
  }

  printf("\n  [延迟冠军次数]\n");
  for (auto& [fw, wins] : latency_wins) {
    printf("    %-20s %d/%zu 场景\n", fw.c_str(), wins, scenarios.size());
  }

  printf("\n  [平均 RPS]\n");
  for (auto& [fw, total] : avg_rps) {
    printf("    %-20s %.0f\n", fw.c_str(), total / framework_count[fw]);
  }

  printf("\n  [适用场景建议]\n");

  // 按场景特点推荐最佳框架
  struct scene_category {
    const char* label;
    std::string scenario_name;
  };
  std::vector<scene_category> categories = {
    {"高并发小包 (IM/推送)",    "small_msg_high_conc"},
    {"常规请求响应 (Web API)",  "medium_msg"},
    {"大数据传输 (文件/流媒体)", "large_msg"},
    {"极限并发 (C10K+)",       "extreme_conc"},
    {"持续吞吐 (日志/管道)",    "sustained_throughput"},
  };

  for (auto& cat : categories) {
    const result_t* best = nullptr;
    for (auto& r : all_results) {
      if (r.scenario != cat.scenario_name) continue;
      if (!best || r.rps > best->rps) best = &r;
    }
    if (best) {
      printf("    %-28s → %s (%.0f RPS, P99=%.0fus)\n",
             cat.label, best->framework.c_str(), best->rps, best->p99_latency_us);
    }
  }

  return 0;
}
