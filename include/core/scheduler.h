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
  RoundRobin, TimeSlice, Fair
};
inline const char* scheduler_type(scheduler_type_t type) {
  switch (type) {
    case scheduler_type_t::RoundRobin: return "RoundRobin";
    case scheduler_type_t::Fair: return "Fair";
    case scheduler_type_t::TimeSlice: return "TimeSlice";
    default: return "UnknownScheduler";
  }
}


struct scheduler_t {
  using queue_t = std::queue<std::coroutine_handle<>>;

  virtual ~scheduler_t() = default;

  virtual uint32_t sched(context_t& ctx) = 0;

  inline void schedule(std::coroutine_handle<> h) {
    pending_tasks.push(h);
  }

  template <typename T>
  CORNET_MAYBE_UNUSED inline void schedule(T&& task) {
    using R = std::decay_t<T>;
    if constexpr (std::is_pointer_v<R>) {
      static_assert(std::is_base_of_v<task_t, std::remove_pointer_t<R> >,
                    "T must be derived from task_t");
      pending_tasks.push(task->handle);
    } else {
      static_assert(std::is_base_of_v<task_t, R>,
                    "T must be derived from task_t");
      pending_tasks.push(task.handle);
      if constexpr (std::is_rvalue_reference_v<decltype(task)>) {
        active_tasks[task.handle.address()] = std::make_unique<R>(std::forward<T>(task));
      }
    }
  }

  inline bool idle() const {
    return pending_tasks.empty();
  }

  void transfer_to(scheduler_t& scheduler) {
    scheduler.active_tasks = std::move(active_tasks);
    scheduler.pending_tasks = std::move(pending_tasks);
  }

  static std::unique_ptr<scheduler_t> scheduler(scheduler_type_t type);

  static inline void register_scheduler(scheduler_type_t type, std::unique_ptr<scheduler_t> (*create)()) {
    registry[type] = create;
  }

protected:
  queue_t pending_tasks;
  std::unordered_map<void*, std::unique_ptr<task_t> > active_tasks;

private:
  static std::unordered_map<scheduler_type_t, std::unique_ptr<scheduler_t>(*)()> registry;
};

struct time_slice_scheduler_t : scheduler_t {
  static inline std::unique_ptr<scheduler_t> create() {
    return std::make_unique<time_slice_scheduler_t>();
  }

  uint32_t sched(context_t& ctx) final;
};

struct round_robin_scheduler_t : scheduler_t {
  static inline std::unique_ptr<scheduler_t> create() {
    return std::make_unique<round_robin_scheduler_t>();
  }

  uint32_t sched(context_t& ctx) final;
};

} // cornet

#endif //CORNET_SCHEDULER_H
