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
  /**
   * @brief context current state, represent
   */
  enum class state_t : uint32_t {
    // context is running
    Running,
    // context is trying to cancel io tasks.
    Canceling,
    // context is terminating, draining remain tasks.
    Terminating,
    // context terminated, all tasks done or error occupied.
    Terminated
  };

  ~context_t();

  context_t(const context_t&) = delete;

  context_t(context_t&& ctx) = delete;

  context_t& operator=(const context_t&) = delete;

  context_t& operator=(context_t&& ctx) = delete;

  /**
   * @brief push task to scheduler queue.
   * @tparam C task_t-like object, with C.handle (std::coroutine_handle) or is raw std::coroutine_handle
   * @param c task will be push
   */
  template <typename C>
  void sched(C&& c) {
    scheduler->schedule(std::forward<C>(c));
  }

  /**
   * @brief context start to resume task and wait io, run until tasks all complete or stop() called.
   */
  void run();

  /**
   * @brief cancel context io tasks or stop running.
   * @param cancel whether cancel io tasks, it makes all io_uring tasks to be cancels.
   */
  void stop(bool cancel = true);

  /**
   * @brief set cancel task to context.
   * @param task cancel task handle, it will be used by process cancel result.
   */
  void set_cancel_task(utask_t* task);

  /**
   * @brief set context scheduler type, new scheduler will take over schedule.
   * @param type new scheduler type.
   */
  void set_scheduler_type(scheduler_type_t type);

  /**
   * @brief return context_t owned io_uring wrapper.
   * @return context_t owned io_uring wrapper reference
   */
  CORNET_NODISCARD uring_t& io_uring();

  /**
   * @brief return context_t owner thread id
   * @return owner thread id
   */
  CORNET_NODISCARD std::thread::id owner_thread() const;

  /**
   * @brief get cancel task handle
   * @return cancel task ptr.
   */
  CORNET_NODISCARD utask_t* get_cancel_task() const;

  /**
   * @brief cancel awaiter, used for cancel io_uring async tasks.
   */
  struct cancel_awaiter : utask_t {
    cancel_awaiter(context_t& ctx, void* user_data, int flags);
  };

  /**
   * @brief cancel io_uring async tasks.
   * @param user_data
   * -------------------------------------------------------------------------------------------
   * flag                          user_data               comment
   * -------------------------------------------------------------------------------------------
   * IORING_ASYNC_CANCEL_ALL      | ptr                  | cancel all tasks match `user_data`.\n
   * IORING_ASYNC_CANCEL_FD       | fd                   | will cancel the first match fd task.\n
   * IORING_ASYNC_CANCEL_ANY      | nullptr              | will cancel all tasks.\n
   * IORING_ASYNC_CANCEL_FD_FIXED | registered fd index  | will cancel correspond fd.\n
   * -------------------------------------------------------------------------------------------
   * @param flags
   * IORING_ASYNC_CANCEL_ALL      Cancel all requests that match the given key
   * IORING_ASYNC_CANCEL_FD       Key off 'fd' for cancelation rather than the request 'user_data'
   * IORING_ASYNC_CANCEL_ANY      Match any request
   * IORING_ASYNC_CANCEL_FD_FIXED 'fd' passed in is a fixed descriptor
   * @return
   * -------------------------------------------------------------------------------------------
   * flag                          return
   * -------------------------------------------------------------------------------------------
   * IORING_ASYNC_CANCEL_ALL      | canceled task count \n
   * IORING_ASYNC_CANCEL_FD       | 0 for success / < 0 for failed \n
   * IORING_ASYNC_CANCEL_ANY      | 0 for success / < 0 for failed \n
   * IORING_ASYNC_CANCEL_FD_FIXED | 0 for success / < 0 for failed \n
   * -------------------------------------------------------------------------------------------
   */
  inline coro_t<int> cancel_io_tasks(void* user_data = nullptr, int flags = IORING_ASYNC_CANCEL_ANY) {
    co_return co_await cancel_awaiter{*this, user_data, flags};
  }

  /**
   * @brief thread-safety mutex for global contexts registry.
   */
  static std::mutex contexts_mutex;
  /**
   * @brief global contexts registry.
   */
  static std::unordered_map<std::thread::id, context_t*> contexts;

  /**
   * @brief return current thread owned context
   * @return thread owned context reference
   */
  static inline context_t& context() {
    static thread_local context_t ctx;
    return ctx;
  }

  /**
   * @brief return given thread owned context
   * @param t context owner thread
   * @return context owned by correspond thread
   */
  static inline context_t* from_thread(const std::thread& t) {
    std::lock_guard<std::mutex> guard(contexts_mutex);
    auto iter = contexts.find(t.get_id());
    if (iter == contexts.end()) {
      return nullptr;
    }
    return iter->second;
  }

  /**
   * @brief io_uring task processor
   * @param cqe complete queue entry on io_uring.
   * @return 0 for success / < 0 for failed
   */
  static int process_utask(cqe_t cqe);

private:
  context_t();

  // context current state
  std::atomic<state_t> state;
  // context current scheduler type
  scheduler_type_t scheduler_type{scheduler_type_t::RoundRobin};
  // context owned io_uring wrapper
  uring_t uring;
  // context cancel task handle
  utask_t* cancel_task{nullptr};
  // context owner thread id
  std::thread::id owner{std::this_thread::get_id()};
  // context scheduler
  std::unique_ptr<scheduler_t> scheduler;
};

} // cornet

#endif //CORNET_CONTEXT_H