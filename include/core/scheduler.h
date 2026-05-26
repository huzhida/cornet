#ifndef CORNET_SCHEDULER_H
#define CORNET_SCHEDULER_H

#include <queue>
#include "task.h"


namespace cornet {

#define CORNET_REGISTER_SCHEDULER(name, cls) \
struct register_##cls { \
register_##cls() {                 \
cornet::scheduler_t::register_scheduler(name, cls::create);\
}\
} register_##cls##_instance;


struct context_t;
struct atask_t;
/**
 * @brief scheduler type
 */
enum class scheduler_type_t {
  UnKnown, RoundRobin, TimeSlice, Batch, Adaptive
};
/**
 * @brief get name from scheduler type
 * @param type scheduler type
 * @return scheduler name in const char*
 */
inline const char* scheduler_name(scheduler_type_t type) {
  switch (type) {
    case scheduler_type_t::RoundRobin: return "RoundRobin";
    case scheduler_type_t::Batch: return "Batch";
    case scheduler_type_t::TimeSlice: return "TimeSlice";
    case scheduler_type_t::Adaptive: return "Adaptive";
    default: return "UnknownScheduler";
  }
}

/**
 * @brief context scheduler, schedule and process task
 */
struct scheduler_t {
  using queue_t = std::queue<std::coroutine_handle<>>;

  virtual ~scheduler_t() = default;

  /**
   * @brief schedule interface
   * @param ctx owner context
   */
  virtual void sched(context_t& ctx) = 0;

  /**
   * @brief push coroutine handle to ready queue.
   * @param h coroutine handle
   */
  inline void schedule(std::coroutine_handle<> h) {
    ready_tasks.push(h);
  }

  /**
   * @brief whether scheduler idle
   * @return true for idle / false for busy
   */
  CORNET_NODISCARD inline bool idle() const {
    return ready_tasks.empty();
  }

  /**
   * @brief transfer tasks ownership to another scheduler
   * @param scheduler destination scheduler
   */
  void transfer_to(scheduler_t& scheduler);

  /**
   * @brief create scheduler by type
   * @param scheduler_type scheduler's type
   * @return scheduler instance
   */
  static std::unique_ptr<scheduler_t> scheduler(scheduler_type_t scheduler_type);

  /**
   * @brief register scheduler type
   * @param type scheduler type
   * @param create create factory function
   */
  static inline void register_scheduler(scheduler_type_t type, std::unique_ptr<scheduler_t> (*create)()) {
    registry[type] = {scheduler_name(type), create};
  }

  /**
   * @brief from name to scheduler type
   * @param name scheduler name
   * @return scheduler name correspond to name
   */
  static inline scheduler_type_t to_scheduler_type(std::string_view name) {
    for (const auto& kv : registry) {
      if (kv.second.first == name) {
        return kv.first;
      }
    }
    return scheduler_type_t::UnKnown;
  }

protected:
  // ready to resume queue
  queue_t ready_tasks;
  // resume one task from the ready queue
  void resume_one_task();
  // collect completed executor tasks into ready queue
  void process_async_tasks(context_t& ctx);
  /**
   * @brief common post-CPU-phase logic: submit SQEs, harvest CQEs, handle idle wait.
   * @param ctx owner context
   * @param wait_timeout how long to block when no CQEs ready and no CPU tasks pending
   * @return number of CQEs processed
   */
  uint32_t flush_io(context_t& ctx, std::chrono::nanoseconds wait_timeout = std::chrono::milliseconds(10));
  std::array<atask_t*, 32> async_tasks;

private:
  // scheduler registry
  static std::unordered_map<scheduler_type_t, std::pair<std::string, std::unique_ptr<scheduler_t>(*)()>> registry;
};

/**
 * @brief time-slice scheduler implementation
 * cpu / io has their own budget
 */
struct time_slice_scheduler_t : scheduler_t {
  static inline std::unique_ptr<scheduler_t> create() {
    return std::make_unique<time_slice_scheduler_t>();
  }

  time_slice_scheduler_t();

  void sched(context_t& ctx) final;

private:
  /**
   * @brief whether cpu timeout
   * @param start start time
   * @return true for yes/ false for no.
   */
  bool cpu_timeout(std::chrono::steady_clock::time_point& start) const;
  // cpu budget in ns
  std::chrono::nanoseconds cpu_budget{10000000};
  // io budget in ns
  std::chrono::nanoseconds io_budget{1000000};
};

/**
 * @brief round-robin scheduler implementation
 * process all cpu tasks until io_uring full or ready tasks empty, then wait io.
 */
struct round_robin_scheduler_t : scheduler_t {
  static inline std::unique_ptr<scheduler_t> create() {
    return std::make_unique<round_robin_scheduler_t>();
  }

  void sched(context_t& ctx) final;
};

/**
 * @brief batch scheduler implementation
 * try best to make batch then submit, then wait io.
 */
struct batch_scheduler_t : scheduler_t {
  batch_scheduler_t();

  static inline std::unique_ptr<scheduler_t> create() {
    return std::make_unique<batch_scheduler_t>();
  }

  void sched(context_t& ctx) final;
private:
  // batch size
  uint32_t batch_nr{32};
};

/**
 * @brief adaptive scheduler implementation.
 * Dynamically adjusts CPU batch size and I/O wait timeout based on feedback signals:
 * - I/O saturation: ratio of ready CQEs to inflight tasks
 * - CPU pressure: whether ready queue has pending tasks
 * Automatically balances between CPU-bound and I/O-bound workloads without configuration.
 */
struct adaptive_scheduler_t : scheduler_t {
  static inline std::unique_ptr<scheduler_t> create() {
    return std::make_unique<adaptive_scheduler_t>();
  }

  void sched(context_t& ctx) final;

private:
  void adapt(size_t resumed, uint32_t cqes_ready, size_t inflight);

  // dynamic parameters
  size_t cpu_batch_{64};
  std::chrono::nanoseconds io_wait_{std::chrono::milliseconds(1)};

  // feedback signals (exponential moving average)
  double io_saturation_{0.0};
};

} // cornet

#endif //CORNET_SCHEDULER_H
