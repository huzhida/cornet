#ifndef CORNET_CONTEXT_H
#define CORNET_CONTEXT_H

#include "uring.h"
#include "task.h"
#include "utask.h"
#include "coro.h"
#include "scheduler.h"
#include "utils/ringbuffer.h"

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
   * @brief push task-like to ready queue, will maintain r-value
   * @tparam T task-like type
   * @param task task-like object
   */
  template <typename T>
  CORNET_MAYBE_UNUSED inline void sched(T&& task) {
    using R = std::decay_t<T>;
    if constexpr (std::is_pointer_v<R>) {
      static_assert(std::is_base_of_v<task_t, std::remove_pointer_t<R> >,
                    "T must be derived from task_t");
      scheduler->schedule(task->handle);
    } else {
      static_assert(std::is_base_of_v<task_t, R>,
                    "T must be derived from task_t");
      if constexpr (std::is_rvalue_reference_v<decltype(task)>) {
        task.detach();
      }
      scheduler->schedule(task.handle);
    }
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
   * @brief set context scheduler type, new scheduler will take over schedule.
   * @param type new scheduler type.
   */
  void set_scheduler_type(scheduler_type_t type);

  /**
   * @brief return context_t owned io_uring wrapper.
   * @return context_t owned io_uring wrapper reference
   */
  CORNET_NODISCARD inline uring_t& io_uring() {
    return uring;
  }

  /**
   * @brief context idle or not
   * @return true for idle / false for busy
   */
  CORNET_NODISCARD inline bool idle() {
    return scheduler->idle() && uring.idle();
  }

  inline void switch_to(state_t s) {
    state.store(s, std::memory_order_release);
    SPDLOG_DEBUG("context switch to state:{}", to_string(s));
  }

  /**
   * @brief return context_t owner thread id
   * @return owner thread id
   */
  CORNET_NODISCARD std::thread::id owner_thread() const;


  /**
   * @brief cancel awaiter, used for cancel io_uring async tasks.
   */
  struct cancel_awaiter : utask_t {
    cancel_awaiter(context_t& ctx, void* user_data, int flags);
  };

  /**
   * @brief cancel io_uring async tasks.
   * @param user_data
   * -------------------------------------------------------------------------------------------\n
   * flag                          user_data               comment
   * -------------------------------------------------------------------------------------------\n
   * IORING_ASYNC_CANCEL_ALL      | ptr                  | cancel all tasks match `user_data`.\n
   * IORING_ASYNC_CANCEL_FD       | fd                   | will cancel the first match fd task.\n
   * IORING_ASYNC_CANCEL_ANY      | nullptr              | will cancel all tasks.\n
   * IORING_ASYNC_CANCEL_FD_FIXED | registered fd index  | will cancel correspond fd.\n
   * -------------------------------------------------------------------------------------------\n
   * @param flags
   * IORING_ASYNC_CANCEL_ALL      Cancel all requests that match the given key\n
   * IORING_ASYNC_CANCEL_FD       Key off 'fd' for cancelation rather than the request 'user_data'\n
   * IORING_ASYNC_CANCEL_ANY      Match any request\n
   * IORING_ASYNC_CANCEL_FD_FIXED 'fd' passed in is a fixed descriptor\n
   * @return
   * -------------------------------------------------------------------------------------------\n
   * flag                          return\n
   * -------------------------------------------------------------------------------------------\n
   * IORING_ASYNC_CANCEL_ALL      | canceled task count \n
   * IORING_ASYNC_CANCEL_FD       | 0 for success / < 0 for failed \n
   * IORING_ASYNC_CANCEL_ANY      | 0 for success / < 0 for failed \n
   * IORING_ASYNC_CANCEL_FD_FIXED | 0 for success / < 0 for failed \n
   * -------------------------------------------------------------------------------------------\n
   */
  inline coro_t<int> cancel_io_tasks(void* user_data = nullptr, int flags = IORING_ASYNC_CANCEL_ANY) {
    int canceled_nr = 0;
    while(!uring.idle()) {
      auto ret = co_await cancel_awaiter{*this, user_data, flags};
      if (ret > 0) {
        canceled_nr += ret;
      }else if(ret == -ENOENT || ret == 0) {
          co_return canceled_nr;
      } else {
        SPDLOG_ERROR("cancel tasks encountered error: {}", strerror(-ret));
        co_return ret;
      }
    }
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
   * @param ctx context reference
   * @param cqe complete queue entry on io_uring.
   * @return 0 for success / < 0 for failed
   */
  static int process_utask(context_t& ctx, cqe_t cqe);

  /**
   * @brief context state to string
   * @param s state
   * @return state string
   */
  static inline const char* to_string(state_t s) {
    switch (s) {
      case state_t::Running: return "Running";
      case state_t::Canceling: return "Canceling";
      case state_t::Terminating: return "Terminating";
      case state_t::Terminated: return "Terminated";
    }
    return "Unknown";
  }

private:
  context_t();

  // context owned io_uring wrapper
  uring_t uring;
  // context current state
  std::atomic<state_t> state;
  // context current scheduler type
  scheduler_type_t scheduler_type{scheduler_type_t::RoundRobin};
  // context owner thread id
  std::thread::id owner{std::this_thread::get_id()};
  // context scheduler
  std::unique_ptr<scheduler_t> scheduler;
};

} // cornet

#endif //CORNET_CONTEXT_H