#ifndef CORNET_CONTEXT_H
#define CORNET_CONTEXT_H

#include "cornet/base/defines.h"
#include <functional>

#include <spdlog/spdlog.h>

#ifdef CORNET_METRICS
#include "cornet/base/metrics.h"
#endif

#include "cornet/base/task.h"
#include "cornet/utils/config.h"
#include "cornet/io_uring/utask.h"
#include "cornet/io_uring/uring.h"
#include "cornet/io_uring/io_slot.h"
#include "cornet/io_uring/context_cancellation.h"
#include "cornet/coroutine/atask.h"
#include "cornet/coroutine/coro.h"
#include "cornet/scheduling/scheduler.h"
#include "cornet/scheduling/executor.h"
#include "cornet/coroutine/cancel.h"
#include "cornet/scheduling/task_tracker.h"

namespace cornet {

namespace detail {
/**
 * @brief helper to create a wrapper coroutine from a callable on the calling thread.
 * The callable (and its captures) are moved into the coroutine frame, preventing
 * lifetime issues with dangling references to local state (e.g. dangling this).
 */
template<typename F>
coro_t<void> make_remote_coro(F f) {
  co_await f();
}
} // namespace detail

struct context_t {
  /**
   * @brief context current state
   */
  enum class state_t : uint32_t {
    // context is running normally
    Running,
    // context is draining: no new connections, waiting for existing work to finish
    Draining,
    // context is canceling all pending io
    Canceling,
    // context terminated, all tasks done
    Terminated
  };
  
  context_t(config_t* config = nullptr);

  ~context_t();

  context_t(const context_t&) = delete;

  context_t(context_t&& ctx) = delete;

  context_t& operator=(const context_t&) = delete;

  context_t& operator=(context_t&& ctx) = delete;


  /**
   * @brief spawn a coroutine into the scheduler's ready queue.
   * Rvalue coroutines are detached (fire-and-forget, self-destructs on completion).
   * Lvalue coroutines are NOT detached (caller retains ownership and can read result after completion).
   * @tparam T task-like type (coro_t, task_t*, coroutine_handle)
   * @param task task-like object
   */
  template <typename T>
  CORNET_MAYBE_UNUSED inline void spawn(T&& task) {
    using R = std::decay_t<T>;
    if constexpr (std::is_pointer_v<R>) {
      static_assert(std::is_base_of_v<task_t, std::remove_pointer_t<R> >,
                    "T must be derived from task_t");
      scheduler_.schedule(task->handle);
    } else if constexpr (std::is_same_v<R, std::coroutine_handle<>>) {
      scheduler_.schedule(task);
    } else {
      static_assert(std::is_base_of_v<task_t, R>,
                    "T must be derived from task_t");
      if constexpr (std::is_rvalue_reference_v<decltype(task)>) {
        task.detach();
      }
      scheduler_.schedule(task.handle);
    }
  }

  /**
   * @brief submit a task or coroutine handle directly to this context's scheduler.
   * Thread-safe: can be called from any thread.
   * Accepts the same inputs as spawn() — task-like objects, coroutine handles,
   * or task pointers. The handle is enqueued directly and resumed
   * on this context's owner thread.
   * @tparam T task-like type (coro_t, task_t*, coroutine_handle)
   * @param task task-like object to schedule
   */
  template <typename T>
  CORNET_MAYBE_UNUSED inline void spawn_remote(T&& task)
    requires std::is_same_v<std::decay_t<T>, std::coroutine_handle<>>
          || std::is_base_of_v<task_t, std::decay_t<T>>
          || std::is_pointer_v<std::decay_t<T>>
  {
    using R = std::decay_t<T>;
    std::coroutine_handle<> h;
    if constexpr (std::is_pointer_v<R>) {
      static_assert(std::is_base_of_v<task_t, std::remove_pointer_t<R> >,
                    "T must be derived from task_t");
      h = task->handle;
    } else if constexpr (std::is_same_v<R, std::coroutine_handle<>>) {
      h = task;
    } else {
      static_assert(std::is_base_of_v<task_t, R>,
                    "T must be derived from task_t");
      if constexpr (std::is_rvalue_reference_v<decltype(task)>) {
        task.detach();
      }
      h = task.handle;
    }
    scheduler_.schedule_remote(h);
    wakeup();
  }

  /**
   * @brief submit a coroutine factory to be executed on this context's thread.
   * Thread-safe: can be called from any thread.
   * A wrapper coroutine is created on the calling thread to capture the callable,
   * then its handle is enqueued. On the owner thread, the wrapper resumes, calls
   * the factory to produce the target coroutine, and co_awaits it — keeping the
   * callable alive for the full lifetime of the inner coroutine.
   * @tparam F callable type that returns a task-like object or coroutine handle
   * @param fn callable to invoke on this context's thread
   */
  template <typename F>
  CORNET_MAYBE_UNUSED inline void spawn_remote(F&& fn)
    requires (!(std::is_same_v<std::decay_t<F>, std::coroutine_handle<>>
              || std::is_base_of_v<task_t, std::decay_t<F>>
              || std::is_pointer_v<std::decay_t<F>>))
  {
    auto wrapper = detail::make_remote_coro([f = std::decay_t<F>(std::forward<F>(fn))]() mutable {
      return f();
    });
    wrapper.detach();
    scheduler_.schedule_remote(wrapper.handle);
    wakeup();
  }

  /**
   * @brief submit a coroutine factory with arguments to be executed on this context's thread.
   * Thread-safe: can be called from any thread.
   * A wrapper coroutine is created on the calling thread to capture the callable
   * and arguments, then its handle is enqueued. On the owner thread, the wrapper
   * resumes, invokes the factory with the forwarded arguments, and co_awaits the result.
   * @tparam F callable type
   * @tparam Args argument types forwarded to the callable
   * @param fn callable to invoke on this context's thread
   * @param args arguments forwarded to the callable
   */
  template <typename F, typename... Args>
  CORNET_MAYBE_UNUSED inline void spawn_remote(F&& fn, Args&&... args)
    requires (sizeof...(Args) > 0)
  {
    auto wrapper = detail::make_remote_coro([f = std::decay_t<F>(std::forward<F>(fn)),
                                               ...as = std::decay_t<Args>(std::forward<Args>(args))]() mutable {
      return f(std::move(as)...);
    });
    wrapper.detach();
    scheduler_.schedule_remote(wrapper.handle);
    wakeup();
  }


  /**
   * @brief awaiter for executing a callable on the thread pool.
   * On await_suspend, submits work to executor. On completion, the scheduler
   * picks it up and resumes the coroutine with the result.
   * @tparam F callable type
   * @tparam R return type (deduced from F)
   */
  template<typename F, typename R = std::invoke_result_t<F>>
  struct async_awaiter {
    context_t& ctx_;
    async_task_t<std::decay_t<F>, R> task_;
    explicit async_awaiter(context_t& ctx, F&& f)
      : ctx_(ctx), task_(std::forward<F>(f)) {}
    bool await_ready() { return false; }
    void await_suspend(std::coroutine_handle<> h) {
      task_.handle = h;
      if (!ctx_.executor().add(&task_)) {
        task_.exception = std::make_exception_ptr(
            std::system_error(ENOBUFS, std::system_category(), "executor queue full"));
        ctx_.spawn(h);
      }
    }
    R await_resume() {
      if (task_.exception) std::rethrow_exception(task_.exception);
      if constexpr (!std::is_void_v<R>) return std::move(task_.result_);
    }
  };

  /**
   * @brief execute a callable on the thread pool and co_await the result.
   * Usage: auto result = co_await ctx.async([] { return heavy_work(); });
   * @tparam F callable type
   * @param f callable to execute on worker thread
   * @return async_awaiter that yields the callable's return value
   */
  template<typename F>
  auto async(F&& f) {
    return async_awaiter<F>{*this, std::forward<F>(f)};
  }

  /**
   * @brief context start to resume task and wait io, run until tasks all complete or stop() called.
   * The run loop exits when idle() returns true (no IO inflight and no ready tasks).
   * During shutdown, the loop transitions Draining → Canceling → Terminated.
   */
  void run();

  /**
   * @brief initiate graceful shutdown.
   * Transitions to Draining → waits for idle or timeout → Canceling → Terminated.
   * Thread-safe: can be called from any thread.
   * @param timeout max time to wait for existing work to finish before force-canceling
   */
  void shutdown(std::chrono::nanoseconds timeout = std::chrono::seconds(1));

  /**
   * @brief force stop immediately (no drain, no timeout).
   * Thread-safe.
   */
  void stop();

  /**
   * @brief wake up the owner thread if it's blocked in io_uring_wait.
   * Thread-safe.
   */
  void wakeup();

  /**
   * @brief whether the context running now.
   * Users should check this in accept loops to stop accepting new task.
   * @return true if not in Running state
   */
  CORNET_NODISCARD inline bool is_running() const {
    auto s = state_.load(std::memory_order_relaxed);
    return s == state_t::Running;
  }

  /**
   * @brief whether the context has terminated (run loop exited).
   * @return true if terminated
   */
  CORNET_NODISCARD inline bool is_terminated() const {
    return state_.load(std::memory_order_acquire) == state_t::Terminated;
  }

  /**
   * @brief register a callback for one or more signals.
   * Uses signalfd + io_uring for async signal delivery.
   * Must be called before run().
   * @param signals list of signal numbers to handle
   * @param handler callback invoked with the signal number
   */
  void on_signal(std::initializer_list<int> signals, std::function<void(int)> handler);

  /**
   * @brief generic io_uring awaiter that accepts any prep function.
   * Stores the prep callable and invokes it when the SQE is allocated.
   * @tparam F callable type with signature void(io_uring_sqe*)
   */
  template<typename F>
  struct generic_io_awaiter : utask_t {
    std::decay_t<F> prep_;
    generic_io_awaiter(context_t& ctx, F&& f) : prep_(std::forward<F>(f)) {
      this->ctx = &ctx;
      this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
        static_cast<generic_io_awaiter*>(self)->prep_(sqe);
      };
    }
  };

  /**
   * @brief submit a generic io_uring operation via a user-provided prep function.
   * Enables any io_uring op without framework changes.
   * Usage: auto result = co_await ctx.io([fd, buf, n](io_uring_sqe* sqe) {
   *            io_uring_prep_read(sqe, fd, buf, n, 0);
   *        });
   * @param f callable that fills the io_uring_sqe
   * @return generic_io_awaiter<F>
   */
  template<typename F>
  auto io(F&& f) {
    return generic_io_awaiter<F>{*this, std::forward<F>(f)};
  }

  /**
   * @brief fire-and-forget io_uring operation (no coroutine overhead).
   * Submits an SQE with user_data=nullptr, CQE result is silently discarded.
   * Usage: ctx.io_detach([](io_uring_sqe* sqe) { io_uring_prep_close(sqe, fd); });
   * @param f callable that fills the io_uring_sqe
   */
  template<typename F>
  void io_detach(F&& f) {
    auto* sqe = uring_.get_sqe();
    f(sqe);
    io_uring_sqe_set_data(sqe, nullptr);
  }

  /**
   * @brief set keep-alive mode. When true, the context will not auto-exit
   * when user tasks are idle. Only explicit shutdown()/stop() will terminate it.
   * Used by runtime_t to keep worker threads alive waiting for spawn_remote.
   * @param enabled whether to enable keep-alive
   */
  void set_keep_alive(bool enabled) { keep_alive_ = enabled; }

  /**
   * @brief RAII guard for persistent io work (watcher loops).
   * Ensures the persistent counter is always decremented, even on exception.
   */
  struct persistent_guard {
    context_t& ctx;
    explicit persistent_guard(context_t& ctx) : ctx(ctx) {
      ctx.tracker_.io_persistent_add(1);
    }
    ~persistent_guard() {
      ctx.tracker_.io_persistent_remove(1);
    }
    persistent_guard(const persistent_guard&) = delete;
  };

  /**
   * @brief return scheduler reference 
   */
  CORNET_NODISCARD inline scheduler_t& scheduler() {
    return this->scheduler_;
  }

  /**
   * @brief return context_t owned io_uring wrapper.
   * @return context_t owned io_uring wrapper reference
   */
  CORNET_NODISCARD inline uring_t& io_uring() {
    return uring_;
  }

  /**
   * @brief return context_t owned io slot table.
   * @return io slot table reference
   */
  CORNET_NODISCARD inline io_slot_table_t& io_slots() {
    return slots_;
  }

  #ifdef CORNET_METRICS
  /**
   * @brief return context metrics for performance diagnostics.
   * @return metrics reference
   */
  CORNET_NODISCARD inline context_metrics_t& metrics() {
    return metrics_;
  }
  #endif

  /**
   * @brief return context_t owned executor.
   * @return context_t owned executor reference
   */
  CORNET_NODISCARD inline executor_t& executor() {
    return executor_;
  }

  /**
   * @brief whether all user tasks are done (only persistent watchers remain).
   * Used to trigger the Terminated state transition.
   * When keep_alive is set, always returns false to prevent auto-exit.
   * @return true if no user IO inflight and no ready tasks
   */
  CORNET_NODISCARD inline bool user_idle() {
    if (keep_alive_) return false;
    return tracker_.user_idle();
  }

  /**
   * @brief whether the context is truly idle (nothing at all remains).
   * Used as the run loop exit condition.
   * @return true if no work inflight (including persistent)
   */
  CORNET_NODISCARD inline bool idle() {
    if (keep_alive_) return false;
    return tracker_.idle();
  }

  /**
   * @brief cancel awaiter, used for cancel io_uring async tasks.
   */
  struct cancel_awaiter : utask_t {
    void* user_data_;
    int flags_;
    cancel_awaiter(context_t& ctx, void* user_data, int flags);
  };

  /**
   * @brief context state to string
   * @param s state
   * @return state string
   */
  static inline const char* to_string(state_t s) {
    switch (s) {
      case state_t::Running: return "Running";
      case state_t::Draining: return "Draining";
      case state_t::Canceling: return "Canceling";
      case state_t::Terminated: return "Terminated";
    }
    return "Unknown";
  }


  /**
   * @brief return context config.
   * @return context config pointer
   */
  CORNET_NODISCARD inline config_t* config() {
    return this->config_;
  }

private:
  void switch_to(state_t s) {
    state_.store(s, std::memory_order_release);
    SPDLOG_DEBUG("context switch to state:{}", to_string(s));
  }
  // context config (read by scheduler, executor, uring)
  config_t* config_ = nullptr;
  // unified work counter: single source of truth for all in-flight work
  task_tracker_t tracker_;
  // context owned io_uring wrapper
  uring_t uring_;
  // context owned io slot table for safe user_data management
  io_slot_table_t slots_;
  // eventfd for cross-thread wakeup
  int wakeup_fd_{-1};
  // signalfd for async signal handling (-1 if not used)
  int signal_fd_{-1};
  // context current state
  std::atomic<state_t> state_{state_t::Terminated};
  // context scheduler (direct member, policy switchable at runtime)
  scheduler_t scheduler_;
  // context executor (thread pool for async offload)
  executor_t executor_;
  // per-signal handler callbacks
  std::unordered_map<int, std::function<void(int)>> signal_handlers_;
  // keep-alive flag: prevents auto-exit when user tasks are idle
  bool keep_alive_{false};

  #ifdef CORNET_METRICS
  // context performance metrics
  context_metrics_t metrics_;
  #endif

  // internal: signal watch coroutine
  coro_t<void> signal_watch_loop();
  // internal: wakeup eventfd watch coroutine
  coro_t<void> wakeup_watch_loop();

  // cancellation infrastructure (io_uring-specific, isolated for future backend replacement)
  context_cancellation_t cancellation_;
};

} // cornet

#endif //CORNET_CONTEXT_H
