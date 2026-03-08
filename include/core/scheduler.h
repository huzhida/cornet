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

enum class scheduler_type_t {
  UnKnown, RoundRobin, TimeSlice, Batch
};
inline const char* scheduler_name(scheduler_type_t type) {
  switch (type) {
    case scheduler_type_t::RoundRobin: return "RoundRobin";
    case scheduler_type_t::Batch: return "Batch";
    case scheduler_type_t::TimeSlice: return "TimeSlice";
    default: return "UnknownScheduler";
  }
}


struct scheduler_t {
  using queue_t = std::queue<std::coroutine_handle<>>;

  virtual ~scheduler_t() = default;

  virtual uint32_t sched(context_t& ctx) = 0;

  inline void schedule(std::coroutine_handle<> h) {
    ready_tasks.push(h);
  }

  template <typename T>
  CORNET_MAYBE_UNUSED inline void schedule(T&& task) {
    using R = std::decay_t<T>;
    if constexpr (std::is_pointer_v<R>) {
      static_assert(std::is_base_of_v<task_t, std::remove_pointer_t<R> >,
                    "T must be derived from task_t");
      ready_tasks.push(task->handle);
    } else {
      static_assert(std::is_base_of_v<task_t, R>,
                    "T must be derived from task_t");
      ready_tasks.push(task.handle);
      if constexpr (std::is_rvalue_reference_v<decltype(task)>) {
        active_tasks[task.handle.address()] = std::make_unique<R>(std::forward<T>(task));
      }
    }
  }

  inline bool idle() const {
    return ready_tasks.empty();
  }

  void transfer_to(scheduler_t& scheduler) {
    scheduler.active_tasks = std::move(active_tasks);
    scheduler.ready_tasks = std::move(ready_tasks);
  }

  static std::unique_ptr<scheduler_t> scheduler(scheduler_type_t scheduler_type);

  static inline void register_scheduler(scheduler_type_t type, std::unique_ptr<scheduler_t> (*create)()) {
    registry[type] = {scheduler_name(type), create};
  }

  static inline scheduler_type_t to_scheduler_type(std::string_view name) {
    for (const auto& kv : registry) {
      if (kv.second.first == name) {
        return kv.first;
      }
    }
    return scheduler_type_t::UnKnown;
  }

protected:
  queue_t ready_tasks;
  std::unordered_map<void*, std::unique_ptr<task_t> > active_tasks;

  inline void process_ready_task() {
    auto handle = ready_tasks.front();
    ready_tasks.pop();
    if (!handle.done())
      handle.resume();
    if (handle.done()) {
      active_tasks.erase(handle.address());
    }
  }

private:
  static std::unordered_map<scheduler_type_t, std::pair<std::string, std::unique_ptr<scheduler_t>(*)()>> registry;
};

struct time_slice_scheduler_t : scheduler_t {
  static inline std::unique_ptr<scheduler_t> create() {
    return std::make_unique<time_slice_scheduler_t>();
  }

  time_slice_scheduler_t();

  uint32_t sched(context_t& ctx) final;
  bool cpu_timeout(std::chrono::steady_clock::time_point& start) const;
private:
  std::chrono::nanoseconds cpu_budget{10000000};
  std::chrono::nanoseconds io_budget{1000000};
};

struct round_robin_scheduler_t : scheduler_t {
  static inline std::unique_ptr<scheduler_t> create() {
    return std::make_unique<round_robin_scheduler_t>();
  }

  uint32_t sched(context_t& ctx) final;
};

struct batch_scheduler_t : scheduler_t {
  batch_scheduler_t();

  static inline std::unique_ptr<scheduler_t> create() {
    return std::make_unique<batch_scheduler_t>();
  }

  uint32_t sched(context_t& ctx) final;
private:
  uint32_t batch_nr{32};
};

} // cornet

#endif //CORNET_SCHEDULER_H
