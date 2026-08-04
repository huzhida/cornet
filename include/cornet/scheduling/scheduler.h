#ifndef CORNET_SCHEDULER_H
#define CORNET_SCHEDULER_H

#include <vector>
#include <unordered_map>
#include <coroutine>
#include <array>
#include <memory>

#ifdef BLOCK_SIZE
#undef BLOCK_SIZE
#endif
#include <moodycamel/concurrentqueue.h>

#include "cornet/base/defines.h"
#include "cornet/utils/config.h"
#include "cornet/scheduling/task_tracker.h"
#include "cornet/scheduling/schedule_policy.h"


namespace cornet {

/**
 * @brief cache-friendly ring buffer queue for coroutine handles.
 * Contiguous memory layout for optimal cache performance on the hot scheduling path.
 * Power-of-2 capacity for branchless index wrapping via bitmask.
 */
struct ring_queue_t {
  ring_queue_t() : buf_(1024), mask_(1023), head_(0), tail_(0) {}

  void push(std::coroutine_handle<> h) {
    if (size() == buf_.size()) [[unlikely]] grow();
    buf_[tail_ & mask_] = h;
    tail_++;
  }

  std::coroutine_handle<> front() const {
    return buf_[head_ & mask_];
  }

  void pop() { head_++; }

  CORNET_NODISCARD bool empty() const { return head_ == tail_; }
  CORNET_NODISCARD size_t size() const { return tail_ - head_; }

private:
  void grow() {
    size_t old_cap = buf_.size();
    size_t new_cap = old_cap * 2;
    std::vector<std::coroutine_handle<>> new_buf(new_cap);
    for (size_t i = head_; i != tail_; i++) {
      new_buf[i & (new_cap - 1)] = buf_[i & mask_];
    }
    buf_ = std::move(new_buf);
    mask_ = new_cap - 1;
  }

  std::vector<std::coroutine_handle<>> buf_;
  size_t mask_;
  size_t head_;
  size_t tail_;
};

#define CORNET_REGISTER_SCHEDULER(name, factory_func) \
struct register_##factory_func { \
register_##factory_func() {                 \
cornet::scheduler_t::register_policy(name, factory_func);\
}\
} register_##factory_func##_instance;


struct config_t;
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
 * @brief scheduler with injectable scheduling policy.
 *
 * Infrastructure (ready queue, I/O flushing, async task collection)
 * is unified in this class. The scheduling strategy — when to stop
 * resuming tasks and when to flush I/O — is delegated to a
 * schedule_policy_t object.
 *
 * Switching policy at runtime is a simple pointer replacement;
 * no task queue transfer is needed.
 */
struct scheduler_t {
  using queue_t = ring_queue_t;
  using policy_factory_t = std::unique_ptr<schedule_policy_t> (*)(config_t*);

  scheduler_t(task_tracker_t& tracker, config_t* config)
    : tracker_(tracker), config_(config) {
    auto scheduler_type =
      to_scheduler_type(config ? config->at_path("cornet.context.scheduler.name").value_or("Adaptive") : "Adaptive");
    set_policy(scheduler_type);
  }

  scheduler_t(task_tracker_t& tracker, scheduler_type_t type, config_t* config = nullptr)
    : tracker_(tracker), config_(config) {
    set_policy(type);
  }

  virtual ~scheduler_t() = default;

  /**
   * @brief schedule interface: resume ready tasks, then flush I/O.
   * The concrete stopping condition is delegated to policy_->should_stop_cpu().
   * @param ctx owner context
   */
  void sched(context_t& ctx);

  /**
   * @brief swap the scheduling policy at runtime.
   * Preserves tracker_, ready_tasks, and all infrastructure.
   * Only the scheduling strategy changes.
   * @param type new scheduler type
   */
  void set_policy(scheduler_type_t type) {
    auto iter = registry.find(type);
    if (iter == registry.end()) {
      throw std::runtime_error("scheduler policy not registered: " +
                               std::string(scheduler_name(type)));
    }
    policy_ = iter->second.second(config_);
  }

  /**
   * @brief push coroutine handle to ready queue.
   * @param h coroutine handle
   */
  inline void schedule(std::coroutine_handle<> h) {
    ready_tasks_.push(h);
    tracker_.coroutine_add();
  }

  /**
   * @brief push coroutine handle to ready queue.
   * @param h coroutine handle
   */
  inline void schedule_remote(std::coroutine_handle<> h) {
    remote_queue_.enqueue(h);
  }

  /**
   * @brief whether scheduler idle
   * @return true for idle / false for busy
   */
  CORNET_NODISCARD inline bool idle() const {
    return ready_tasks_.empty();
  }

  /**
   * @brief register a scheduling policy by type.
   * @param type scheduler type
   * @param factory factory function that creates a schedule_policy_t
   */
  static inline void register_policy(scheduler_type_t type, policy_factory_t factory) {
    registry[type] = {scheduler_name(type), factory};
  }

  /**
   * @brief from name to scheduler type
   * @param name scheduler name
   * @return scheduler type corresponding to name
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
  // work tracker (owned by context_t, set after construction)
  task_tracker_t& tracker_;
  // ready to resume queue
  queue_t ready_tasks_;
  // MPSC queue for cross-thread task submission
  moodycamel::ConcurrentQueue<std::coroutine_handle<>> remote_queue_;
  // async tasks buffer
  std::array<atask_t*, 32> async_tasks;

  // resume one task from the ready queue
  void resume_one_task();
  // collect completed executor tasks into ready queue, returns how many moved
  uint32_t process_async_tasks(context_t& ctx);
  // drain the cross-thread queue into the ready queue, returns how many moved
  uint32_t harvest_remote();

private:
  // scheduling policy (injected, not inherited)
  std::unique_ptr<schedule_policy_t> policy_;
  // saved config pointer for use in set_policy and policy factory
  config_t* config_ = nullptr;
  // scheduler registry: type -> (name, policy_factory)
  static std::unordered_map<scheduler_type_t, std::pair<std::string, policy_factory_t>> registry;
};

} // cornet

#endif //CORNET_SCHEDULER_H
