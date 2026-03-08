#ifndef CORNET_CONTEXT_H
#define CORNET_CONTEXT_H

#include "uring.h"
#include "task.h"
#include "utask.h"
#include "coro.h"
#include "scheduler.h"
#include "ringbuffer.h"

namespace cornet {

struct context_t {
  enum class state_t : uint32_t {
    Running, Canceling, Terminating, Terminated
  };

  ~context_t();

  context_t(const context_t&) = delete;

  context_t(context_t&& ctx) = delete;

  context_t& operator=(const context_t&) = delete;

  context_t& operator=(context_t&& ctx) = delete;

  template <typename C>
  void sched(C&& c) {
    scheduler->schedule(std::forward<C>(c));
  }

  void run();

  void stop(bool cancel = true);

  void set_cancel_task(utask_t* task);

  void set_scheduler_type(scheduler_type_t type);

  CORNET_NODISCARD uring_t& io_uring();

  CORNET_NODISCARD std::thread::id owner_thread() const;

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

  static inline std::optional<context_t*> from_thread(const std::thread& t) {
    std::lock_guard<std::mutex> guard(contexts_mutex);
    auto iter = contexts.find(t.get_id());
    if (iter == contexts.end()) {
      return nullptr;
    }
    return iter->second;
  }

  static int process_utask(cqe_t cqe);

private:
  context_t();

  std::atomic<state_t> state;
  scheduler_type_t scheduler_type{scheduler_type_t::RoundRobin};
  uring_t uring;
  utask_t* cancel_task{nullptr};
  std::thread::id owner{std::this_thread::get_id()};
  std::unique_ptr<scheduler_t> scheduler;
};

} // cornet

#endif //CORNET_CONTEXT_H