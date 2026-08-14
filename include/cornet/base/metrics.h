#ifndef CORNET_METRICS_H
#define CORNET_METRICS_H

#ifdef CORNET_METRICS
#include <chrono>
#include <algorithm>
#include <cstdio>

namespace cornet {



/**
 * @brief lightweight latency histogram with fixed buckets (in microseconds).
 * Tracks min, max, total, count for average calculation.
 */
struct latency_stats_t {
  uint64_t count{0};
  uint64_t total_us{0};
  uint64_t min_us{UINT64_MAX};
  uint64_t max_us{0};

  void record(uint64_t us) {
    count++;
    total_us += us;
    min_us = std::min(min_us, us);
    max_us = std::max(max_us, us);
  }

  uint64_t avg_us() const { return count > 0 ? total_us / count : 0; }

  void reset() {
    count = 0;
    total_us = 0;
    min_us = UINT64_MAX;
    max_us = 0;
  }
};

/**
 * @brief RAII timer that records elapsed time into a latency_stats_t on destruction.
 * If stats is nullptr, no timing is performed (zero overhead).
 */
struct scoped_timer_t {
  latency_stats_t* stats;
  std::chrono::steady_clock::time_point start;

  explicit scoped_timer_t(latency_stats_t* s)
    : stats(s), start(s ? std::chrono::steady_clock::now()
                        : std::chrono::steady_clock::time_point{}) {}

  ~scoped_timer_t() {
    if (stats) {
      auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - start).count();
      stats->record(elapsed);
    }
  }
};

/**
 * @brief per-context metrics for diagnosing performance issues.
 */
struct context_metrics_t {
  // io_uring submit metrics
  uint64_t submit_calls{0};
  uint64_t submit_sqes{0};
  uint64_t submit_failures{0};
  latency_stats_t submit_latency;

  // io_uring wait metrics
  uint64_t wait_calls{0};
  uint64_t wait_timeouts{0};
  uint64_t wait_cqes_processed{0};
  latency_stats_t wait_latency;

  // io_uring peek metrics
  uint64_t peek_calls{0};
  uint64_t peek_empty{0};
  uint64_t peek_cqes_processed{0};

  // get_sqe metrics
  uint64_t get_sqe_calls{0};
  uint64_t get_sqe_submit_forced{0};
  // SQ still full after a forced submit: the caller had to shed load
  uint64_t get_sqe_exhausted{0};

  // scheduler metrics
  uint64_t sched_cycles{0};
  uint64_t tasks_resumed{0};
  latency_stats_t sched_latency;

  // slot table metrics
  uint64_t slot_allocs{0};
  uint64_t slot_frees{0};
  uint64_t slot_stale_cqes{0};

  // task completion metrics
  uint64_t tasks_completed{0};
  uint64_t tasks_failed{0};

  void reset() {
    *this = context_metrics_t{};
  }

  void dump(FILE* out = stderr) const {
    fprintf(out, "=== cornet context metrics ===\n");
    fprintf(out, "[io_uring submit]\n");
    fprintf(out, "  calls: %lu, sqes: %lu, failures: %lu\n", submit_calls, submit_sqes, submit_failures);
    fprintf(out, "  latency(us): avg=%lu min=%lu max=%lu\n",
            submit_latency.avg_us(),
            submit_latency.count > 0 ? submit_latency.min_us : 0,
            submit_latency.max_us);
    fprintf(out, "[io_uring wait]\n");
    fprintf(out, "  calls: %lu, timeouts: %lu, cqes_processed: %lu\n", wait_calls, wait_timeouts, wait_cqes_processed);
    fprintf(out, "  latency(us): avg=%lu min=%lu max=%lu\n",
            wait_latency.avg_us(),
            wait_latency.count > 0 ? wait_latency.min_us : 0,
            wait_latency.max_us);
    fprintf(out, "[io_uring peek]\n");
    fprintf(out, "  calls: %lu, empty: %lu, cqes_processed: %lu\n", peek_calls, peek_empty, peek_cqes_processed);
    fprintf(out, "[get_sqe]\n");
    fprintf(out, "  calls: %lu, forced_submits: %lu, exhausted: %lu\n",
            get_sqe_calls, get_sqe_submit_forced, get_sqe_exhausted);
    fprintf(out, "[scheduler]\n");
    fprintf(out, "  cycles: %lu, tasks_resumed: %lu\n", sched_cycles, tasks_resumed);
    fprintf(out, "  latency(us): avg=%lu min=%lu max=%lu\n",
            sched_latency.avg_us(),
            sched_latency.count > 0 ? sched_latency.min_us : 0,
            sched_latency.max_us);
    fprintf(out, "[slots]\n");
    fprintf(out, "  allocs: %lu, frees: %lu, stale_cqes: %lu\n", slot_allocs, slot_frees, slot_stale_cqes);
    fprintf(out, "[tasks]\n");
    fprintf(out, "  completed: %lu, failed: %lu\n", tasks_completed, tasks_failed);
    fprintf(out, "==============================\n");
  }
};

#define CORNET_METRICS_ADD(val) do { \
  (val)++; \
} while(0)
#define CORNET_METRICS_ADD_N(val, n) do { \
  (val) += n; \
} while(0)
#define CORNET_METRICS_SCOPE_TIMER(latency) do { \
  scoped_timer_t timer(&latency); \
} while(0)

} // namespace cornet
#else

#define CORNET_METRICS_ADD(val)
#define CORNET_METRICS_ADD_N(val, n)
#define CORNET_METRICS_SCOPE_TIMER(latency)

#endif

#endif //CORNET_METRICS_H
