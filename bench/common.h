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
#include <map>
#include <thread>
#include <mutex>

namespace bench {

struct scenario_t {
  std::string name;
  std::string description;
  int connections;
  int message_size;
  int total_messages;
};

// Single RSS time-series sample point.
struct rss_sample_t {
  double timestamp_sec;  // elapsed seconds from benchmark start
  size_t rss_kb;          // process RSS in KB
};

// VmHWM reader for peak memory measurement.
static size_t get_vmmhwm_kb() {
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

// RSS profiler: background thread reads /proc/self/status periodically.
// Uses VmHWM (historical peak RSS) to measure incremental memory.
// Since VmHWM never resets within a process, each run's delta is measured
// from the VmHWM captured at the start of this specific run.
class rss_profiler_t {
public:
  void start() {
    // Capture VmHWM as baseline — this is the peak RSS before this run.
    // VmHWM is cumulative (never decreases), so the delta from this point
    // forward reflects memory allocated specifically during this run.
    baseline_ = get_vmmhwm_kb();
    start_time_ = std::chrono::steady_clock::now();
    running_.store(true);
    sample_thread_ = std::thread([this]() {
      while (running_.load(std::memory_order_acquire)) {
        size_t hwm = get_vmmhwm_kb();
        auto now = std::chrono::steady_clock::now();
        double elapsed = std::chrono::duration<double>(now - start_time_).count();
        size_t delta = hwm > baseline_ ? hwm - baseline_ : 0;
        std::lock_guard<std::mutex> lock(mu_);
        samples_.push_back({elapsed, delta});
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
      }
    });
  }

  void stop() {
    running_.store(false, std::memory_order_release);
    if (sample_thread_.joinable()) sample_thread_.join();
    compute_stats();
  }

  const std::vector<rss_sample_t>& samples() const { return samples_; }

  size_t peak_kb() const { return peak_kb_; }
  size_t min_kb() const { return min_kb_; }
  double mean_kb() const { return mean_kb_; }
  double stddev_kb() const { return stddev_kb_; }

  size_t baseline_kb() const { return baseline_; }

private:
  void compute_stats() {
    std::lock_guard<std::mutex> lock(mu_);
    if (samples_.empty()) {
      peak_kb_ = min_kb_ = mean_kb_ = stddev_kb_ = 0;
      return;
    }
    peak_kb_ = min_kb_ = samples_[0].rss_kb;
    double sum = 0;
    for (auto& s : samples_) {
      sum += s.rss_kb;
      if (s.rss_kb > peak_kb_) peak_kb_ = s.rss_kb;
      if (s.rss_kb < min_kb_) min_kb_ = s.rss_kb;
    }
    mean_kb_ = sum / samples_.size();
    double variance = 0;
    for (auto& s : samples_) {
      double diff = (double)s.rss_kb - mean_kb_;
      variance += diff * diff;
    }
    stddev_kb_ = std::sqrt(variance / samples_.size());
  }

  size_t baseline_ = 0;
  std::chrono::steady_clock::time_point start_time_;
  std::atomic<bool> running_{false};
  std::vector<rss_sample_t> samples_;
  std::mutex mu_;
  std::thread sample_thread_;
  size_t peak_kb_ = 0, min_kb_ = 0;
  double mean_kb_ = 0, stddev_kb_ = 0;
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
  size_t rss_mean_kb{0};
  size_t rss_min_kb{0};
  size_t rss_max_kb{0};
  double rss_stddev_kb{0};
  std::vector<rss_sample_t> rss_profile;  // time-series samples for plotting (delta from baseline)
  size_t rss_baseline_kb{0};              // absolute RSS at start of benchmark
};

// --- ASCII chart helpers ---

// Draw a horizontal bar: right-padded label + "|" + █ fill + value suffix.
// value/max_ref controls bar length (capped at max_len).
inline void draw_bar(const char* label, size_t label_width,
                     double value, double max_ref, size_t max_len) {
  size_t bar_len = max_ref > 0 ? static_cast<size_t>((value / max_ref) * max_len) : 0;
  if (bar_len > max_len) bar_len = max_len;
  printf("    %-*s |", static_cast<int>(label_width), label);
  for (size_t i = 0; i < bar_len; ++i) printf("█");
  printf(" %.2f", value);
}

// Find the maximum value from a lambda over result_t pointers.
// Returns at least 1.0 to avoid divide-by-zero.
inline double find_max(const std::vector<const result_t*>& items, auto fn) {
  double mx = 0.0;
  for (auto* r : items) {
    double v = fn(r);
    if (v > mx) mx = v;
  }
  return mx > 0.0 ? mx : 1.0;
}

// --- End chart helpers ---

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

  result_t compute(const std::string& framework, const std::string& scenario, double elapsed_sec, size_t hwm_before_kb) {
    result_t r;
    r.framework = framework;
    r.scenario = scenario;
    r.elapsed_sec = elapsed_sec;
    r.total_messages = total_messages.load();
    r.failed_messages = failed_messages.load();
    // Use VmHWM delta to measure peak memory allocated during the benchmark.
    // VmHWM tracks the highest RSS ever reached by the process and never resets.
    // Subtracting the baseline (captured before the benchmark) gives the actual
    // peak memory usage of this run, even when threads allocate/deallocate freely.
    size_t hwm_now = get_vmmhwm_kb();
    r.peak_rss_kb = hwm_now > hwm_before_kb ? hwm_now - hwm_before_kb : 0;

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

  // Merge RSS profiler data into a result_t after compute() has returned.
  void fill_profile(result_t& r, const rss_profiler_t& profiler) const {
    r.rss_mean_kb = size_t(profiler.mean_kb());
    r.rss_min_kb = profiler.min_kb();
    r.rss_max_kb = profiler.peak_kb();
    r.rss_stddev_kb = profiler.stddev_kb();
    r.rss_profile = profiler.samples();
    r.rss_baseline_kb = profiler.baseline_kb();
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
     512, 64, 100000},
    {"medium_msg",
     "中等消息: 模拟典型 Web API 请求/响应, 中等连接数和消息体",
     128, 1024, 50000},
    {"large_msg",
     "大消息: 模拟文件传输/流媒体场景, 少连接但大数据块",
     32, 65536, 10000},
    {"extreme_conc",
     "极高并发: 压测连接数上限, 模拟 C10K+ 场景",
     2048, 128, 100000},
    {"sustained_throughput",
     "持续吞吐: 模拟数据管道/日志采集, 稳定中等连接持续传输",
     64, 4096, 50000},
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
  printf("%-18s %8s %8s %8s %8s %8s %8s %8s %8s %8s %6s %8s %8s\n",
         "框架", "RPS", "MB/s", "平均(us)", "P50", "P95", "P99", "P999", "标准差", "抖动", "失败", "均值KB", "峰值KB");
  printf("%-18s %8s %8s %8s %8s %8s %8s %8s %8s %8s %6s %8s %8s\n",
         "──────────", "──────", "──────", "──────", "──────", "──────", "──────", "──────", "──────", "──────", "────", "──────", "──────");
}

inline void print_result(const result_t& r) {
  printf("%-18s %8.0f %8.2f %8.0f %8.0f %8.0f %8.0f %8.0f %8.0f %8.0f %6d %8zu %8zu\n",
         r.framework.c_str(),
         r.rps, r.throughput_mbps,
         r.avg_latency_us, r.p50_latency_us, r.p95_latency_us,
         r.p99_latency_us, r.p999_latency_us, r.stddev_latency_us, r.jitter_us,
         r.failed_messages, r.rss_mean_kb, r.rss_max_kb);
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

constexpr int BENCH_ROUNDS = 3;

// 多轮运行取中位数（按RPS排序取中间那次）
inline result_t median_result(std::vector<result_t>& runs) {
  std::sort(runs.begin(), runs.end(), [](auto& a, auto& b) { return a.rps < b.rps; });
  return runs[runs.size() / 2];
}

// 打印多轮运行的RPS波动范围
inline void print_rounds_variance(const std::vector<std::vector<result_t>>& all_runs) {
  printf("\n  [多轮运行波动 (3次RPS: min ~ median ~ max)]\n");
  for (auto& runs : all_runs) {
    if (runs.empty()) continue;
    auto sorted = runs;
    std::sort(sorted.begin(), sorted.end(), [](auto& a, auto& b) { return a.rps < b.rps; });
    double cv = 0;
    if (sorted.size() >= 2) {
      double mean = 0;
      for (auto& r : sorted) mean += r.rps;
      mean /= sorted.size();
      double var = 0;
      for (auto& r : sorted) { double d = r.rps - mean; var += d * d; }
      cv = mean > 0 ? std::sqrt(var / sorted.size()) / mean * 100 : 0;
    }
    printf("    %-18s %8.0f ~ %8.0f ~ %8.0f  (CV=%.1f%%)\n",
           sorted[0].framework.c_str(),
           sorted.front().rps, sorted[sorted.size()/2].rps, sorted.back().rps, cv);
  }
}

// Print detailed memory profile per framework/scenario: absolute RSS, delta,
// mean, peak, min, stddev, plus an ASCII mini chart.
inline void print_memory_profile(const std::vector<result_t>& results) {
  printf("\n  [内存使用详情]\n");
  printf("    %-18s %10s %10s %8s %8s %8s %6s\n",
         "框架", "基线KB", "均值增量", "峰值增量", "最低增量", "标准差", "样本");
  printf("    %-18s %10s %10s %8s %8s %8s %6s\n",
         "──────────", "────────", "────────", "──────", "──────", "──────", "────");

  for (auto& r : results) {
    if (r.rss_profile.empty()) continue;
    printf("    %-18s %10zu %10zu %8zu %8zu %8.0f %6zu\n",
           r.framework.c_str(),
           r.rss_baseline_kb, r.rss_mean_kb, r.rss_max_kb, r.rss_min_kb,
           r.rss_stddev_kb, r.rss_profile.size());
  }

  // Per-scenario memory chart: group by scenario so all frameworks share
  // the same peak_delta reference, making bar lengths proportional to each
  // framework's actual incremental memory.
  std::map<std::string, std::vector<const result_t*>> by_scenario;
  for (auto& r : results) {
    if (r.rss_profile.empty()) continue;
    by_scenario[r.scenario].push_back(&r);
  }

  for (auto& [scenario_name, items] : by_scenario) {
    printf("\n  [场景: %s]\n", scenario_name.c_str());
    size_t bar_max = 40;
    // Find the maximum peak delta across all frameworks for this scenario
    size_t peak_delta = 0;
    for (auto* r : items)
      if (r->rss_max_kb > peak_delta) peak_delta = r->rss_max_kb;
    if (peak_delta == 0) peak_delta = 1;

    for (auto* r : items) {
      size_t bar_len = (r->rss_max_kb * bar_max) / peak_delta;
      printf("    %-18s |", r->framework.c_str());
      for (size_t i = 0; i < bar_len; ++i) printf("█");
      printf(" +%zuKB", r->rss_max_kb);
      printf(" (基线 %zuKB)\n", r->rss_baseline_kb);
    }
  }
}

// Helper: compute CV% from a result_t pointer.
inline double calc_cv(const result_t* r) {
  return r->avg_latency_us > 0 ? (r->stddev_latency_us / r->avg_latency_us * 100) : 0;
}

// Helper: compute tail ratio (P99/P50) from a result_t pointer.
inline double calc_tail(const result_t* r) {
  return r->p50_latency_us > 0 ? (r->p99_latency_us / r->p50_latency_us) : 0;
}

// Helper: get stability tag string from CV% and tail ratio.
inline const char* stability_tag(double cv, double tail) {
  if (cv < 30 && tail < 3.0) return "优秀";
  if (cv < 60 && tail < 5.0) return "良好";
  if (cv < 100 && tail < 10.0) return "一般";
  return "较差";
}

// === Dual-axis RPS + latency grouped bar chart ===
inline void print_rps_latency_chart(const std::vector<result_t>& results) {
  if (results.empty()) return;

  std::map<std::string, std::vector<const result_t*>> by_scenario;
  for (auto& r : results) by_scenario[r.scenario].push_back(&r);

  for (auto& [scenario_name, items] : by_scenario) {
    printf("\n");
    printf("┌────────────────────────────────────────────────────────────────────────────┐\n");
    printf("│ 场景: %-53s │\n", scenario_name.c_str());
    printf("├────────────────────────────────────────────────────────────────────────────┤\n");
    printf("│ %-12s %-22s %-35s │\n", "框架", "RPS (左轴, req/s)", "延迟百分位 us (右轴)");
    printf("└────────────────────────────────────────────────────────────────────────────┘\n");

    const size_t rps_bar_max = 22;
    const size_t lat_bar_max = 16;
    double max_rps = find_max(items, [](const result_t* r) { return r->rps; });
    double max_lat = find_max(items, [](const result_t* r) { return r->p99_latency_us; });

    for (auto* r : items) {
      printf("│  %-12s |", r->framework.substr(0, 12).c_str());

      bool is_cornet = r->framework.rfind("Cornet", 0) == 0;
      const char* green = "\033[32m";
      const char* reset = "\033[0m";

      // RPS bar
      size_t rps_len = static_cast<size_t>((r->rps / max_rps) * rps_bar_max);
      if (rps_len > rps_bar_max) rps_len = rps_bar_max;
      if (is_cornet) printf("%s", green);
      for (size_t i = 0; i < rps_len; ++i) printf("█");
      for (size_t i = rps_len; i < rps_bar_max; ++i) printf(" ");
      if (is_cornet) printf("%s", reset);
      printf(" %5.0f", r->rps);

      printf("  ");

      // Latency sub-bars using ▓ DARK SHADE for visual distinction
      auto draw_lat_sub = [lat_bar_max, &max_lat, is_cornet, green, reset](const char* label, double val) {
        size_t len = static_cast<size_t>((val / max_lat) * lat_bar_max);
        if (len > lat_bar_max) len = lat_bar_max;
        printf("%s[", label);
        if (is_cornet) printf("%s", green);
        for (size_t i = 0; i < len; ++i) printf("▓");
        for (size_t i = len; i < lat_bar_max; ++i) printf(" ");
        if (is_cornet) printf("%s", reset);
        printf("]%.0f  ", val);
      };

      draw_lat_sub("均值", r->avg_latency_us);
      draw_lat_sub("P50", r->p50_latency_us);
      draw_lat_sub("P95", r->p95_latency_us);
      draw_lat_sub("P99", r->p99_latency_us);

      printf("│\n");
    }
    printf("\n");
  }
}

// === Latency percentile distribution chart ===
inline void print_latency_profile_chart(const std::vector<result_t>& results) {
  if (results.empty()) return;

  std::map<std::string, std::vector<const result_t*>> by_scenario;
  for (auto& r : results) by_scenario[r.scenario].push_back(&r);

  for (auto& [scenario_name, items] : by_scenario) {
    printf("\n");
    printf("┌──────────────────────────────────────────────────────────────────────────────────────────┐\n");
    printf("│ 场景: %-56s │\n", scenario_name.c_str());
    printf("├──────────────────────────────────────────────────────────────────────────────────────────┤\n");
    printf("│ %-12s %-8s %-8s %-8s %-8s %-10s │\n", "框架", "平均", "P50", "P95", "P99", "P999");
    printf("└──────────────────────────────────────────────────────────────────────────────────────────┘\n");

    const size_t bar_max = 30;
    double max_lat = find_max(items, [](const result_t* r) { return r->p999_latency_us; });

    for (auto* r : items) {
      printf("│  %-12s ", r->framework.substr(0, 12).c_str());

      bool is_cornet = r->framework.rfind("Cornet", 0) == 0;
      const char* green = "\033[32m";
      const char* reset = "\033[0m";

      auto draw_pctile = [bar_max, &max_lat, is_cornet, green, reset](const char* label, double val) {
        size_t len = static_cast<size_t>((val / max_lat) * bar_max);
        if (len > bar_max) len = bar_max;
        printf(" %-8s[", label);
        if (is_cornet) printf("%s", green);
        for (size_t i = 0; i < len; ++i) printf("█");
        for (size_t i = len; i < bar_max; ++i) printf(" ");
        if (is_cornet) printf("%s", reset);
        printf("]%.0f  ", val);
      };

      draw_pctile("均值", r->avg_latency_us);
      draw_pctile("P50", r->p50_latency_us);
      draw_pctile("P95", r->p95_latency_us);
      draw_pctile("P99", r->p99_latency_us);
      draw_pctile("P999", r->p999_latency_us);
      printf("│\n");
    }
    printf("\n");
  }
}

// === Throughput comparison chart ===
inline void print_throughput_chart(const std::vector<result_t>& results) {
  if (results.empty()) return;

  std::map<std::string, std::vector<const result_t*>> by_scenario;
  for (auto& r : results) by_scenario[r.scenario].push_back(&r);

  for (auto& [scenario_name, items] : by_scenario) {
    printf("\n  [场景: %s -- 吞吐量 MB/s]\n", scenario_name.c_str());

    const size_t bar_max = 40;
    double max_tp = find_max(items, [](const result_t* r) { return r->throughput_mbps; });
    const char* green = "\033[32m";
    const char* reset = "\033[0m";

    for (auto* r : items) {
      size_t bar_len = static_cast<size_t>((r->throughput_mbps / max_tp) * bar_max);
      if (bar_len > bar_max) bar_len = bar_max;
      printf("    %-18s |", r->framework.c_str());
      bool is_cornet = r->framework.rfind("Cornet", 0) == 0;
      if (is_cornet) printf("%s", green);
      for (size_t i = 0; i < bar_len; ++i) printf("█");
      if (is_cornet) printf("%s", reset);
      printf(" %6.2f MB/s (%.0f RPS)\n", r->throughput_mbps, r->rps);
    }
  }
}

// === Stability visualization chart ===
inline void print_stability_chart(const std::vector<result_t>& results) {
  if (results.empty()) return;

  std::map<std::string, std::vector<const result_t*>> by_scenario;
  for (auto& r : results) by_scenario[r.scenario].push_back(&r);

  for (auto& [scenario_name, items] : by_scenario) {
    printf("\n");
    printf("┌────────────────────────────────────────────────────────────────────────────┐\n");
    printf("│ 场景: %-53s │\n", scenario_name.c_str());
    printf("├────────────────────────────────────────────────────────────────────────────┤\n");

    const size_t bar_max = 30;

    // --- CV% chart ---
    double max_cv = find_max(items, calc_cv);
    const char* green = "\033[32m";
    const char* reset = "\033[0m";
    printf("\n  变异系数 CV%% (越低越稳定):\n");
    for (auto* r : items) {
      double cv = calc_cv(r);
      size_t len = static_cast<size_t>((cv / max_cv) * bar_max);
      if (len > bar_max) len = bar_max;
      printf("    %-18s |", r->framework.c_str());
      bool is_cornet = r->framework.rfind("Cornet", 0) == 0;
      if (is_cornet) printf("%s", green);
      for (size_t i = 0; i < len; ++i) printf("█");
      if (is_cornet) printf("%s", reset);
      printf(" %5.1f%%  [%s]\n", cv, stability_tag(cv, calc_tail(r)));
    }

    // --- Tail ratio chart ---
    double max_tail = find_max(items, calc_tail);
    printf("\n  尾部放大比 P99/P50 (越低越稳定):\n");
    for (auto* r : items) {
      double tail = calc_tail(r);
      size_t len = static_cast<size_t>((tail / max_tail) * bar_max);
      if (len > bar_max) len = bar_max;
      printf("    %-18s |", r->framework.c_str());
      bool is_cornet = r->framework.rfind("Cornet", 0) == 0;
      if (is_cornet) printf("%s", green);
      for (size_t i = 0; i < len; ++i) printf("█");
      if (is_cornet) printf("%s", reset);
      printf(" %5.1fx  [%s]\n", tail, stability_tag(calc_cv(r), tail));
    }

    // --- Star heatmap ---
    printf("\n  综合稳定性评级:\n");
    printf("    %-18s  CV%%     尾部比    评级\n", "框架");
    printf("    %-18s  ─────   ─────   ────────────\n", "──────────");

    for (auto* r : items) {
      double cv = calc_cv(r);
      double tail = calc_tail(r);

      // Per-metric ratings (evaluate independently)
      const char* cv_tag = stability_tag(cv, 0.0); // tail=0 always passes < 3.0
      if (cv >= 100) cv_tag = "较差";
      else if (cv >= 60) cv_tag = "一般";
      else if (cv >= 30) cv_tag = "良好";
      else cv_tag = "优秀";

      const char* tail_tag = stability_tag(0.0, tail); // cv=0 always passes < 30
      if (tail >= 10.0) tail_tag = "较差";
      else if (tail >= 5.0) tail_tag = "一般";
      else if (tail >= 3.0) tail_tag = "良好";
      else tail_tag = "优秀";

      // 4-star scale: CV<30→1, CV<60→1, tail<3→1, tail<5→1
      int stars = 0;
      if (cv < 30) stars++;
      if (cv < 60) stars++;
      if (tail < 3.0) stars++;
      if (tail < 5.0) stars++;

      printf("    %-18s [%-4s]  [%-4s]  ", r->framework.c_str(), cv_tag, tail_tag);
      for (int i = 0; i < stars; ++i) printf("★");
      for (int i = stars; i < 4; ++i) printf("☆");
      printf("\n");
    }
    printf("\n");
  }
}

// 综合评估：得分排名、置信度、延迟稳定性、内存效率
inline void print_comprehensive_summary(
    const std::vector<result_t>& all_results,
    const std::vector<std::vector<std::vector<result_t>>>& all_rounds, // [scenario][framework][round]
    const std::vector<scenario_t>& scenarios) {

  printf("\n");
  printf("╔══════════════════════════════════════════════════════════════╗\n");
  printf("║                      综合评估                              ║\n");
  printf("╚══════════════════════════════════════════════════════════════╝\n\n");

  // 收集所有框架名
  std::vector<std::string> frameworks;
  for (auto& r : all_results) {
    if (std::find(frameworks.begin(), frameworks.end(), r.framework) == frameworks.end())
      frameworks.push_back(r.framework);
  }

  // === 综合得分排名 ===
  printf("  [综合得分排名] (RPS 40%% + P99延迟 30%% + 稳定性 20%% + 内存 10%%)\n");
  std::map<std::string, double> total_score;
  for (auto& fw : frameworks) total_score[fw] = 0;

  for (auto& scenario : scenarios) {
    // 收集该场景所有结果
    std::vector<const result_t*> scene_results;
    for (auto& r : all_results)
      if (r.scenario == scenario.name) scene_results.push_back(&r);

    if (scene_results.empty()) continue;
    size_t n = scene_results.size();

    // 按各维度排名 (rank 0 = best)
    auto rank_by = [&](auto cmp) {
      std::vector<const result_t*> sorted = scene_results;
      std::sort(sorted.begin(), sorted.end(), cmp);
      std::map<std::string, int> ranks;
      for (size_t i = 0; i < sorted.size(); ++i) ranks[sorted[i]->framework] = i;
      return ranks;
    };

    auto rps_rank = rank_by([](auto* a, auto* b) { return a->rps > b->rps; });
    auto p99_rank = rank_by([](auto* a, auto* b) { return a->p99_latency_us < b->p99_latency_us; });
    auto mem_rank = rank_by([](auto* a, auto* b) { return a->peak_rss_kb < b->peak_rss_kb; });
    auto cv_rank = rank_by([](auto* a, auto* b) {
      double ca = a->avg_latency_us > 0 ? a->stddev_latency_us / a->avg_latency_us : 999;
      double cb = b->avg_latency_us > 0 ? b->stddev_latency_us / b->avg_latency_us : 999;
      return ca < cb;
    });

    for (auto& fw : frameworks) {
      // 归一化得分: (n - rank) / n, 越高越好
      double score = 0;
      if (rps_rank.count(fw)) score += 0.4 * (n - rps_rank[fw]) / (double)n;
      if (p99_rank.count(fw)) score += 0.3 * (n - p99_rank[fw]) / (double)n;
      if (cv_rank.count(fw))  score += 0.2 * (n - cv_rank[fw]) / (double)n;
      if (mem_rank.count(fw)) score += 0.1 * (n - mem_rank[fw]) / (double)n;
      total_score[fw] += score;
    }
  }

  // 排序输出
  std::vector<std::pair<std::string, double>> score_vec(total_score.begin(), total_score.end());
  std::sort(score_vec.begin(), score_vec.end(), [](auto& a, auto& b) { return a.second > b.second; });
  int rank = 1;
  for (auto& [fw, score] : score_vec) {
    printf("    #%d  %-20s  %.2f / %.1f\n", rank++, fw.c_str(), score, (double)scenarios.size());
  }

  // === 置信度分析 ===
  printf("\n  [多轮运行置信度] (CV<5%%=高可信, 5-15%%=中等, >15%%=低可信)\n");
  printf("    %-18s", "框架");
  for (auto& s : scenarios) printf(" %10s", s.name.substr(0, 10).c_str());
  printf("\n");

  for (size_t fi = 0; fi < frameworks.size(); ++fi) {
    printf("    %-18s", frameworks[fi].c_str());
    for (size_t si = 0; si < scenarios.size(); ++si) {
      if (si < all_rounds.size() && fi < all_rounds[si].size() && all_rounds[si][fi].size() >= 2) {
        auto& runs = all_rounds[si][fi];
        double mean = 0;
        for (auto& r : runs) mean += r.rps;
        mean /= runs.size();
        double var = 0;
        for (auto& r : runs) { double d = r.rps - mean; var += d * d; }
        double cv = mean > 0 ? std::sqrt(var / runs.size()) / mean * 100 : 0;
        const char* tag = cv < 5 ? "高" : cv < 15 ? "中" : "低";
        printf(" %5.1f%%(%s)", cv, tag);
      } else {
        printf(" %10s", "-");
      }
    }
    printf("\n");
  }

  // === 延迟稳定性对比 ===
  printf("\n  [延迟稳定性] (P99/P50 尾部放大比, 越低越稳定)\n");
  for (auto& fw : frameworks) {
    double sum_ratio = 0; int cnt = 0;
    for (auto& r : all_results) {
      if (r.framework == fw && r.p50_latency_us > 0) {
        sum_ratio += r.p99_latency_us / r.p50_latency_us;
        cnt++;
      }
    }
    double avg_ratio = cnt > 0 ? sum_ratio / cnt : 0;
    const char* grade = avg_ratio < 3 ? "优秀" : avg_ratio < 5 ? "良好" : avg_ratio < 10 ? "一般" : "较差";
    printf("    %-18s 平均P99/P50=%.1fx  %s\n", fw.c_str(), avg_ratio, grade);
  }

  // === 内存效率对比 ===
  printf("\n  [内存效率] (平均内存占用 & 每万RPS内存消耗)\n");
  printf("    %-18s %10s %14s\n", "框架", "平均内存KB", "KB/万RPS");
  for (auto& fw : frameworks) {
    double sum_mem = 0, sum_rps = 0; int cnt = 0;
    for (auto& r : all_results) {
      if (r.framework == fw) {
        sum_mem += r.peak_rss_kb;
        sum_rps += r.rps;
        cnt++;
      }
    }
    double avg_mem = cnt > 0 ? sum_mem / cnt : 0;
    double avg_rps = cnt > 0 ? sum_rps / cnt : 0;
    double mem_per_10k_rps = avg_rps > 0 ? avg_mem / (avg_rps / 10000.0) : 0;
    printf("    %-18s %10.0f %14.1f\n", fw.c_str(), avg_mem, mem_per_10k_rps);
  }

  // === 适用场景建议 ===
  printf("\n  [适用场景建议]\n");
  struct scene_category { const char* label; std::string scenario_name; };
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
}

} // namespace bench

#endif // CORNET_BENCH_COMMON_H
