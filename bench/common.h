#ifndef CORNET_BENCH_COMMON_H
#define CORNET_BENCH_COMMON_H

#include <chrono>
#include <string>
#include <vector>
#include <numeric>
#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <functional>
#include <cmath>
#include <fstream>

namespace bench {

struct scenario_t {
  std::string name;
  std::string description;
  int connections;
  int message_size;
  int total_messages;
};

struct result_t {
  std::string framework;
  std::string scenario;
  double rps{0};
  double throughput_mbps{0};
  double avg_latency_us{0};
  double p50_latency_us{0};
  double p95_latency_us{0};
  double p99_latency_us{0};
  double p999_latency_us{0};
  double stddev_latency_us{0};
  double jitter_us{0};
  int total_messages{0};
  int failed_messages{0};
  double elapsed_sec{0};
  size_t peak_rss_kb{0};
};

inline size_t get_peak_rss_kb() {
  std::ifstream f("/proc/self/status");
  std::string line;
  while (std::getline(f, line)) {
    if (line.rfind("VmHWM:", 0) == 0) {
      size_t val = 0;
      sscanf(line.c_str(), "VmHWM: %zu", &val);
      return val;
    }
  }
  return 0;
}

inline size_t get_current_rss_kb() {
  std::ifstream f("/proc/self/status");
  std::string line;
  while (std::getline(f, line)) {
    if (line.rfind("VmRSS:", 0) == 0) {
      size_t val = 0;
      sscanf(line.c_str(), "VmRSS: %zu", &val);
      return val;
    }
  }
  return 0;
}

struct latency_collector_t {
  std::vector<uint64_t> samples;
  std::atomic<int64_t> total_bytes{0};
  std::atomic<int64_t> total_messages{0};
  std::atomic<int64_t> failed_messages{0};

  void reserve(size_t n) { samples.reserve(n); }

  void record(uint64_t latency_us, int bytes) {
    samples.push_back(latency_us);
    total_bytes.fetch_add(bytes, std::memory_order_relaxed);
    total_messages.fetch_add(1, std::memory_order_relaxed);
  }

  void record_failure() {
    failed_messages.fetch_add(1, std::memory_order_relaxed);
  }

  result_t compute(const std::string& framework, const std::string& scenario, double elapsed_sec, size_t rss_before_kb) {
    result_t r;
    r.framework = framework;
    r.scenario = scenario;
    r.elapsed_sec = elapsed_sec;
    r.total_messages = total_messages.load();
    r.failed_messages = failed_messages.load();
    r.peak_rss_kb = get_current_rss_kb() - rss_before_kb;

    if (r.total_messages == 0 || elapsed_sec == 0) return r;

    r.rps = r.total_messages / elapsed_sec;
    r.throughput_mbps = (total_bytes.load() / (1024.0 * 1024.0)) / elapsed_sec;

    std::sort(samples.begin(), samples.end());
    size_t n = samples.size();
    if (n > 0) {
      double sum = 0;
      for (auto s : samples) sum += s;
      r.avg_latency_us = sum / n;
      r.p50_latency_us = samples[n * 50 / 100];
      r.p95_latency_us = samples[n * 95 / 100];
      r.p99_latency_us = samples[n * 99 / 100];
      r.p999_latency_us = samples[std::min(n - 1, n * 999 / 1000)];

      double variance = 0;
      for (auto s : samples) {
        double diff = s - r.avg_latency_us;
        variance += diff * diff;
      }
      r.stddev_latency_us = std::sqrt(variance / n);

      if (n > 1) {
        double jitter_sum = 0;
        for (size_t i = 1; i < n; ++i) {
          jitter_sum += std::abs((double)samples[i] - (double)samples[i-1]);
        }
        r.jitter_us = jitter_sum / (n - 1);
      }
    }
    return r;
  }

  void reset() {
    samples.clear();
    total_bytes = 0;
    total_messages = 0;
    failed_messages = 0;
  }
};

inline std::vector<scenario_t> default_scenarios() {
  return {
    {"small_msg_high_conc",
     "小消息高并发: 模拟即时通讯/推送场景, 大量连接传输小包",
     512, 64, 200000},
    {"medium_msg",
     "中等消息: 模拟典型 Web API 请求/响应, 中等连接数和消息体",
     128, 1024, 100000},
    {"large_msg",
     "大消息: 模拟文件传输/流媒体场景, 少连接但大数据块",
     32, 65536, 20000},
    {"extreme_conc",
     "极高并发: 压测连接数上限, 模拟 C10K+ 场景",
     2048, 128, 200000},
    {"sustained_throughput",
     "持续吞吐: 模拟数据管道/日志采集, 稳定中等连接持续传输",
     64, 4096, 100000},
  };
}

inline void print_scenario_header(const scenario_t& s) {
  printf("\n");
  printf("┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐\n");
  printf("│ 场景: %-30s                                                              │\n", s.name.c_str());
  printf("│ 描述: %-90s│\n", s.description.c_str());
  printf("│ 参数: 并发=%d  消息大小=%dB  总消息数=%d                                           │\n",
         s.connections, s.message_size, s.total_messages);
  printf("└─────────────────────────────────────────────────────────────────────────────────────────────────────┘\n");
}

inline void print_result_header() {
  printf("%-18s %8s %8s %8s %8s %8s %8s %8s %8s %8s %6s %8s\n",
         "框架", "RPS", "MB/s", "平均(us)", "P50", "P95", "P99", "P999", "标准差", "抖动", "失败", "内存(KB)");
  printf("%-18s %8s %8s %8s %8s %8s %8s %8s %8s %8s %6s %8s\n",
         "──────────", "──────", "──────", "──────", "──────", "──────", "──────", "──────", "──────", "──────", "────", "──────");
}

inline void print_result(const result_t& r) {
  printf("%-18s %8.0f %8.2f %8.0f %8.0f %8.0f %8.0f %8.0f %8.0f %8.0f %6d %8zu\n",
         r.framework.c_str(),
         r.rps, r.throughput_mbps,
         r.avg_latency_us, r.p50_latency_us, r.p95_latency_us,
         r.p99_latency_us, r.p999_latency_us, r.stddev_latency_us, r.jitter_us,
         r.failed_messages, r.peak_rss_kb);
}

inline void print_stability_analysis(const std::vector<result_t>& results, const std::string& scenario) {
  printf("\n  [稳定性分析]\n");
  for (auto& r : results) {
    if (r.scenario != scenario) continue;
    double cv = r.avg_latency_us > 0 ? (r.stddev_latency_us / r.avg_latency_us * 100) : 0;
    double tail_ratio = r.avg_latency_us > 0 ? (r.p99_latency_us / r.avg_latency_us) : 0;
    const char* stability;
    if (cv < 30 && tail_ratio < 3.0) stability = "优秀";
    else if (cv < 60 && tail_ratio < 5.0) stability = "良好";
    else if (cv < 100 && tail_ratio < 10.0) stability = "一般";
    else stability = "较差";

    printf("    %-18s 变异系数=%.1f%%  尾部放大=%.1fx  稳定性=%s\n",
           r.framework.c_str(), cv, tail_ratio, stability);
  }
}

inline void print_scenario_recommendation(const std::vector<result_t>& results, const scenario_t& scenario) {
  printf("\n  [场景推荐]\n");

  const result_t* best_rps = nullptr;
  const result_t* best_latency = nullptr;
  const result_t* best_stability = nullptr;
  const result_t* best_memory = nullptr;

  for (auto& r : results) {
    if (r.scenario != scenario.name) continue;
    if (!best_rps || r.rps > best_rps->rps) best_rps = &r;
    if (!best_latency || r.p99_latency_us < best_latency->p99_latency_us) best_latency = &r;
    if (!best_memory || r.peak_rss_kb < best_memory->peak_rss_kb) best_memory = &r;

    double cv = r.avg_latency_us > 0 ? (r.stddev_latency_us / r.avg_latency_us) : 999;
    double best_cv = best_stability && best_stability->avg_latency_us > 0
      ? (best_stability->stddev_latency_us / best_stability->avg_latency_us) : 999;
    if (!best_stability || cv < best_cv) best_stability = &r;
  }

  if (best_rps) printf("    最高吞吐: %s (%.0f RPS)\n", best_rps->framework.c_str(), best_rps->rps);
  if (best_latency) printf("    最低延迟: %s (P99=%.0fus)\n", best_latency->framework.c_str(), best_latency->p99_latency_us);
  if (best_stability) printf("    最稳定:   %s\n", best_stability->framework.c_str());
  if (best_memory) printf("    最省内存: %s (%zuKB)\n", best_memory->framework.c_str(), best_memory->peak_rss_kb);
}

using bench_fn_t = std::function<result_t(const scenario_t& scenario)>;

} // namespace bench

#endif // CORNET_BENCH_COMMON_H
