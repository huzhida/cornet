#ifndef CORNET_SCHEDULE_POLICY_H
#define CORNET_SCHEDULE_POLICY_H

#include <chrono>
#include <memory>

#include "cornet/base/defines.h"
#include "cornet/utils/config.h"

namespace cornet {

struct context_t;

/**
 * @brief statistics collected during a single sched() cycle.
 * Used by policy::should_stop_cpu() and policy::on_sched_done() to
 * make scheduling decisions without exposing infrastructure details.
 */
struct sched_stats {
  std::chrono::steady_clock::time_point start;
  size_t tasks_resumed{0};
};

/**
 * @brief interface for injectable scheduling strategies.
 *
 * A policy answers three questions each sched() cycle:
 *   1. When should we stop resuming tasks and move to I/O?
 *      (should_stop_cpu)
 *   2. How long should we block waiting for I/O completions?
 *      (get_io_wait)
 *   3. What feedback should we collect after the cycle?
 *      (on_sched_done)
 *
 * Concrete policies carry only data members — no virtual functions
 * beyond the three listed above. This keeps them trivially copyable
 * and avoids the overhead of vtable dispatch on the hot path.
 */
struct schedule_policy_t {
  virtual ~schedule_policy_t() = default;

  /**
   * @brief decide whether to exit the CPU-resume phase.
   * @param stats running statistics for this sched() cycle
   * @return true to stop resuming and proceed to flush_io()
   */
  CORNET_NODISCARD inline bool should_stop_cpu(const sched_stats& stats) const {
    return default_should_stop_cpu(stats);
  }
  virtual bool default_should_stop_cpu(const sched_stats& stats) const = 0;

  /**
   * @brief determine the I/O wait timeout.
   * @return how long to block when waiting for CQEs
   */
  CORNET_NODISCARD inline std::chrono::nanoseconds get_io_wait() const {
    return default_get_io_wait();
  }
  virtual std::chrono::nanoseconds default_get_io_wait() const = 0;

  /**
   * @brief post-cycle callback for feedback-driven policies.
   * @param stats running statistics for this sched() cycle
   * @param cqes number of CQEs processed this cycle
   * @param inflight number of in-flight I/O tasks
   */
  inline void on_sched_done(const sched_stats& stats,
                             uint32_t cqes,
                             size_t inflight) const {
    return default_on_sched_done(stats, cqes, inflight);
  }
  virtual void default_on_sched_done(const sched_stats& stats,
                                     uint32_t cqes,
                                     size_t inflight) const = 0;
};

/**
 * @brief round-robin policy: process all ready tasks every cycle.
 *
 * Always resumes until the ready queue is empty, then flushes I/O.
 * No configurable parameters — the default 10ms I/O wait is used.
 */
struct round_robin_policy_t : schedule_policy_t {
  CORNET_NODISCARD bool default_should_stop_cpu(const sched_stats& /*stats*/) const {
    return false; // never stop early
  }
  std::chrono::nanoseconds default_get_io_wait() const {
    return std::chrono::milliseconds(10);
  }
  void default_on_sched_done(const sched_stats&, uint32_t, size_t) const {
    // no-op
  }
};

/**
 * @brief batch policy: resume at most max_batch tasks per cycle.
 *
 * Useful when the goal is to amortize I/O submission overhead by
 * processing a fixed number of coroutines before submitting pending SQEs.
 */
struct batch_policy_t : schedule_policy_t {
  size_t max_batch{32};

  CORNET_NODISCARD bool default_should_stop_cpu(const sched_stats& stats) const {
    return stats.tasks_resumed >= max_batch;
  }
  std::chrono::nanoseconds default_get_io_wait() const {
    return std::chrono::milliseconds(10);
  }
  void default_on_sched_done(const sched_stats&, uint32_t, size_t) const {
    // no-op
  }
};

/**
 * @brief time-slice policy: CPU and I/O each have a time budget.
 *
 * Resumes tasks until either the ready queue is empty or the CPU
 * budget expires. The I/O budget controls the wait timeout.
 */
struct time_slice_policy_t : schedule_policy_t {
  std::chrono::nanoseconds cpu_budget{10000000};  // 10ms
  std::chrono::nanoseconds io_budget{1000000};    // 1ms

  CORNET_NODISCARD bool default_should_stop_cpu(const sched_stats& stats) const {
    auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now() - stats.start);
    return elapsed > cpu_budget;
  }
  std::chrono::nanoseconds default_get_io_wait() const {
    return io_budget;
  }
  void default_on_sched_done(const sched_stats&, uint32_t, size_t) const {
    // no-op
  }
};

/**
 * @brief adaptive policy: dynamically adjusts CPU batch size and I/O
 * wait timeout based on feedback signals.
 *
 * I/O saturation (ratio of ready CQEs to inflight tasks) drives CPU
 * batch adjustments; activity levels drive I/O wait adjustments.
 * Automatically balances between CPU-bound and I/O-bound workloads
 * without configuration.
 */
struct adaptive_policy_t : schedule_policy_t {
  mutable size_t cpu_batch_{64};         // dynamic range: 8–1024
  mutable std::chrono::nanoseconds io_wait_{std::chrono::milliseconds(1)}; // 50us–10ms
  mutable double io_saturation_{0.0};

  CORNET_NODISCARD bool default_should_stop_cpu(const sched_stats& stats) const {
    return stats.tasks_resumed >= cpu_batch_;
  }
  std::chrono::nanoseconds default_get_io_wait() const {
    return io_wait_;
  }

  void default_on_sched_done(const sched_stats& stats,
                     uint32_t cqes,
                     size_t inflight) const {
    adapt(stats.tasks_resumed, cqes, inflight);
  }

private:
  void adapt(size_t resumed, uint32_t cqes_ready, size_t inflight) const {
    size_t still_inflight = inflight > cqes_ready ? inflight - cqes_ready : 0;
    double sat = still_inflight > 0
                   ? static_cast<double>(cqes_ready) / static_cast<double>(still_inflight)
                   : 0.0;
    io_saturation_ = io_saturation_ * 0.9 + sat * 0.1;

    // adjust cpu_batch based on I/O saturation
    if (io_saturation_ > 0.5 && cpu_batch_ > 8) {
      // I/O completions piling up, reduce CPU batch to process them faster
      cpu_batch_ = std::max(size_t(8), cpu_batch_ * 3 / 4);
    } else if (io_saturation_ < 0.1 && resumed >= cpu_batch_) {
      // I/O idle and CPU saturated, allow more CPU work per cycle
      cpu_batch_ = std::min(size_t(1024), cpu_batch_ + cpu_batch_ / 4 + 1);
    }

    // adjust wait timeout based on load
    if (resumed == 0 && cqes_ready == 0) {
      // nothing happening, increase wait to save CPU
      io_wait_ = std::min(std::chrono::nanoseconds(10000000), io_wait_ * 2);
    } else if (cqes_ready > 0 || resumed > 0) {
      // active workload, keep wait tight
      io_wait_ = std::max(std::chrono::nanoseconds(50000), io_wait_ * 3 / 4);
    }
  }
};

} // namespace cornet

#endif // CORNET_SCHEDULE_POLICY_H
