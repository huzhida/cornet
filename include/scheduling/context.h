#ifndef CORNET_CONTEXT_H
#define CORNET_CONTEXT_H

#include "io_uring/uring.h"
#include "base/task.h"
#include "io_uring/utask.h"
#include "coroutine/atask.h"
#include "coroutine/coro.h"
#include "io_uring/awaiters.h"
#include "scheduling/scheduler.h"
#include "scheduling/executor.h"
#include "io_uring/io_slot.h"
#include "base/metrics.h"
#include "utils/config.h"
#include "utils/logging.h"
#include "io_uring/context_cancellation.h"
#include <functional>
#include <memory>
#include <optional>
#include <concurrentqueue/moodycamel/concurrentqueue.h>
#ifdef BLOCK_SIZE
#undef BLOCK_SIZE
#endif

namespace cornet {

namespace detail {
/**
 * @brief wrapper coroutine that moves a callable into its coroutine frame,
 * then co_awaits the coroutine produced by the callable.
 * This ensures the callable (and its captures) outlives the inner coroutine.
 */
template<typename F>
coro_t<void> spawn_remote_runner(F f) {
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
      scheduler->schedule(task->handle);
    } else if constexpr (std::is_same_v<R, std::coroutine_handle<>>) {
      scheduler->schedule(task);
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
   * @brief submit a coroutine factory to be executed on this context's thread.
   * Thread-safe: can be called from any thread.
   * The callable is invoked on this context's owner thread to produce a coroutine,
   * which is then spawned into the scheduler.
   * Uses a wrapper coroutine to move the callable into the coroutine frame,
   * preventing the lambda coroutine lifetime issue (dangling this).
   * @tparam F callable type that returns coro_t<void>
   * @param fn callable to invoke on this context's thread
   */
  template<typename F>
  void spawn_remote(F&& fn) {
    remote_queue_.enqueue([this, f = std::decay_t<F>(std::forward<F>(fn))]() mutable {
      this->spawn(detail::spawn_remote_runner(std::move(f)));
    });
    wakeup();
  }

  /**
   * @brief drain all pending remote tasks into the scheduler.
   * Called from the scheduler during flush_io. Single-consumer (owner thread only).
   */
  void drain_remote_queue();

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
    typed_atask_t<std::decay_t<F>, R> task_;
    explicit async_awaiter(context_t& ctx, F&& f)
      : ctx_(ctx), task_(std::forward<F>(f)) {}
    bool await_ready() { return false; }
    void await_suspend(std::coroutine_handle<> h) {
      task_.handle = h;
      ctx_.ensure_executor();
      if (!ctx_.async_executor()->add(&task_)) {
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
   * @brief whether the context is shutting down (draining, canceling, or terminated).
   * Users should check this in accept loops to stop accepting new connections.
   * @return true if not in Running state
   */
  CORNET_NODISCARD inline bool is_shutting_down() const {
    auto s = state.load(std::memory_order_acquire);
    return s != state_t::Running;
  }

  /**
   * @brief whether the context is in draining state (shutting down gracefully).
   * Users should check this in accept loops to stop accepting new connections.
   * @return true if draining or later state
   * @deprecated use is_shutting_down() instead
   */
  CORNET_NODISCARD inline bool is_draining() const {
    return is_shutting_down();
  }

  /**
   * @brief whether the context has terminated (run loop exited).
   * @return true if terminated
   */
  CORNET_NODISCARD inline bool is_terminated() const {
    return state.load(std::memory_order_acquire) == state_t::Terminated;
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
    auto* sqe = uring.get_sqe();
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
  void set_scheduler_type(scheduler_type_t type);

  /**
   * @brief return context_t owned io_uring wrapper.
   * @return context_t owned io_uring wrapper reference
   */
  CORNET_NODISCARD inline uring_t& io_uring() {
    return uring;
  }

  /**
   * @brief return context_t owned io slot table.
   * @return io slot table reference
   */
  CORNET_NODISCARD inline io_slot_table_t& io_slots() {
    return slots;
  }

  /**
   * @brief return context metrics for performance diagnostics.
   * @return metrics reference
   */
  CORNET_NODISCARD inline context_metrics_t& metrics() {
    return metrics_;
  }

  /**
   * @brief return context_t owned executor.
   * @return context_t owned executor reference
   */
  CORNET_NODISCARD inline std::unique_ptr<executor_t>& async_executor() {
    return executor;
  }

  /**
   * @brief whether all user tasks are done (only persistent watchers remain).
   * Used to trigger the Terminated state transition.
   * When keep_alive is set, always returns false to prevent auto-exit.
   * @return true if no user IO inflight and no ready tasks
   */
  CORNET_NODISCARD inline bool user_idle() {
    if (keep_alive_) return false;
    if (executor && !executor->idle()) return false;
    return scheduler->idle() && uring.user_idle();
  }

  /**
   * @brief whether the context is truly idle (nothing at all remains).
   * Used as the run loop exit condition.
   * @return true if no IO inflight (including persistent) and no ready tasks
   */
  CORNET_NODISCARD inline bool idle() {
    if (executor && !executor->idle()) return false;
    return scheduler->idle() && uring.idle();
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

  context_t();


  void ensure_executor() {
    if (!executor) {
      executor = std::make_unique<executor_t>(
          config_t::get()["cornet"]["context"]["executor"]["thread_nr"].value_or(1),
          config_t::get()["cornet"]["context"]["executor"]["max_task_nr"].value_or(16384)
      );
    }
  }

private:
  void switch_to(state_t s) {
    state.store(s, std::memory_order_release);
    SPDLOG_DEBUG("context switch to state:{}", to_string(s));
  }

  // context owned io_uring wrapper
  uring_t uring;
  // context owned io slot table for safe user_data management
  io_slot_table_t slots;
  // context performance metrics
  context_metrics_t metrics_;
  // eventfd for cross-thread wakeup
  int wakeup_fd{-1};
  // context current state
  std::atomic<state_t> state{state_t::Terminated};
  // context current scheduler type
  scheduler_type_t scheduler_type{scheduler_type_t::RoundRobin};
  // context scheduler
  std::unique_ptr<scheduler_t> scheduler;
  // context executor
  std::unique_ptr<executor_t> executor;
  // signalfd for async signal handling (-1 if not used)
  int signal_fd{-1};
  // per-signal handler callbacks
  std::unordered_map<int, std::function<void(int)>> signal_handlers;
  // MPSC queue for cross-thread task submission
  moodycamel::ConcurrentQueue<std::function<void()>> remote_queue_;
  // keep-alive flag: prevents auto-exit when user tasks are idle
  bool keep_alive_{false};

  // graceful shutdown deadline: when Draining and user_idle() is false,
  // this deadline triggers a forced Canceling transition
  std::optional<std::chrono::steady_clock::time_point> shutdown_deadline_;

  // internal: signal watch coroutine
  coro_t<void> signal_watch_loop();
  // internal: wakeup eventfd watch coroutine
  coro_t<void> wakeup_watch_loop();

  // cancellation infrastructure (io_uring-specific, isolated for future backend replacement)
  context_cancellation_t cancellation_;
};

} // cornet

#endif //CORNET_CONTEXT_H
