#ifndef CORNET_CONTEXT_H
#define CORNET_CONTEXT_H

#include <queue>
#include "uring.h"
#include "task.h"
#include "utask.h"
#include "coro.h"
#include "ringbuffer.h"

namespace cornet {

struct context_t {
  using queue_t = std::queue<std::coroutine_handle<>>;
  struct scheduler_t {
    constexpr static const char* SCHEDULER_TYPE_TIME_SLICE = "Priority";
    constexpr static const char* SCHEDULER_TYPE_FAIR = "Fair";
    constexpr static const char* SCHEDULER_TYPE_ROUND_ROBIN = "RoundRobin";

    virtual ~scheduler_t() = default;
    virtual uint32_t sched(context_t& ctx) = 0;

    inline void schedule(std::coroutine_handle<> h) {
      pending_tasks.push(h);
    }
    template<typename T>
    CORNET_MAYBE_UNUSED inline void schedule(T&& task) {
      using R = std::decay_t<T>;
      if constexpr(std::is_pointer_v<R>) {
        static_assert(std::is_base_of_v<task_t, std::remove_pointer_t<R>>,
                       "T must be derived from task_t");
        std::coroutine_handle<> handle = task->handle;
        pending_tasks.push(handle);
      } else {
        static_assert(std::is_base_of_v<task_t, R>,
                      "T must be derived from task_t");
        std::coroutine_handle<> handle = task.handle;
        pending_tasks.push(handle);
        if constexpr (std::is_rvalue_reference_v<decltype(task)>) {
          active_tasks[handle.address()] = std::make_unique<R>(std::forward<T>(task));
        }
      }
    }
    inline bool idle() {
      return pending_tasks.empty();
    }

    static std::unique_ptr<scheduler_t> scheduler(const std::string& scheduler_type);
    static inline void register_scheduler(const char* name, std::unique_ptr<scheduler_t>(*create)()) {
      registry[name] = create;
    }
   protected:
    queue_t pending_tasks;
    std::unordered_map<void*, std::unique_ptr<task_t>> active_tasks;
   private:
    static std::unordered_map<std::string, std::unique_ptr<scheduler_t>(*)()> registry;
  };
  enum class state_t : uint32_t {
    Running, Canceling, Terminating, Terminated
  };

  ~context_t();
  context_t(const context_t&) = delete;
  context_t(context_t&& ctx) = delete;
  context_t& operator=(const context_t&) = delete;
  context_t& operator=(context_t&& ctx)  = delete;

  template<typename C> void sched(C&& c) {
    scheduler->schedule(std::forward<C>(c));
  }
  void run();
  void stop(bool cancel = true);
  uring_t& io_uring();
  CORNET_NODISCARD std::thread::id owner_thread() const;
  void set_cancel_task(utask_t* task);
  CORNET_NODISCARD utask_t* get_cancel_task() const;

  struct cancel_awaiter : utask_t {
    cancel_awaiter(context_t& ctx, void* user_data, int flags);
  };
  inline coro_t<int> cancel_io_tasks(void* user_data = nullptr, int flags = IORING_ASYNC_CANCEL_ANY) {
    co_return co_await cancel_awaiter{*this, user_data, flags};
  }

  static std::mutex contexts_mutex;
  static std::unordered_map<std::thread::id, context_t*> contexts;

  static inline context_t& context() {
    static thread_local context_t ctx;
    return ctx;
  }
  static inline context_t& from_thread(const std::thread& t) {
    std::lock_guard<std::mutex> guard(contexts_mutex);
    auto iter = contexts.at(t.get_id());
    return *iter;
  }

  static int process_utask(cqe_t cqe);

 private:
  context_t();
  std::atomic<state_t> state;
  uring_t uring;
  utask_t* cancel_task{nullptr};
  std::thread::id owner{std::this_thread::get_id()};
  std::unique_ptr<scheduler_t> scheduler;
};

struct time_slice_scheduler_t : context_t::scheduler_t {
  static inline std::unique_ptr<scheduler_t> create() {
    return std::make_unique<time_slice_scheduler_t>();
  }
  uint32_t sched(context_t& ctx) final;
};

struct round_robin_scheduler_t : context_t::scheduler_t {
  static inline std::unique_ptr<scheduler_t> create() {
    return std::make_unique<round_robin_scheduler_t>();
  }
  uint32_t sched(context_t& ctx) final;
};

} // cornet

#endif //CORNET_CONTEXT_H
