#ifndef CORNET_CONTEXT_H
#define CORNET_CONTEXT_H

#include "uring.h"
#ifndef IORING_ASYNC_CANCEL_ANY
#define IORING_ASYNC_CANCEL_ANY (1U << 2)
#endif
#include "task.h"
#include "utask.h"
#include "atask.h"
#include "coro.h"
#include "awaiters.h"
#include "scheduler.h"
#include "executor.h"
#include "io_slot.h"
#include "utils/metrics.h"
#include <functional>

namespace cornet {

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
   * @brief execute a callable on the thread pool and co_await the result.
   * Usage: auto result = co_await ctx.async([] { return heavy_work(); });
   * @tparam F callable type
   * @param f callable to execute on worker thread
   * @return async_awaiter that yields the callable's return value
   */
  template<typename F>
  auto async(F&& f);

  /**
   * @brief context start to resume task and wait io, run until tasks all complete or stop() called.
   */
  void run();

  /**
   * @brief initiate graceful shutdown.
   * Transitions to Draining → waits for idle or timeout → Canceling → Terminated.
   * Thread-safe: can be called from any thread.
   * @param timeout max time to wait for existing work to finish before force-canceling
   */
  void shutdown(std::chrono::nanoseconds timeout = std::chrono::seconds(5));

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
   * @brief whether the context is in draining state (shutting down gracefully).
   * Users should check this in accept loops to stop accepting new connections.
   * @return true if draining or later state
   */
  CORNET_NODISCARD inline bool is_draining() const {
    auto s = state.load(std::memory_order_acquire);
    return s != state_t::Running;
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
   * @brief submit a generic io_uring operation via a user-provided prep function.
   * Enables any io_uring op without framework changes.
   * Usage: auto result = co_await ctx.io([fd, buf, n](io_uring_sqe* sqe) {
   *            io_uring_prep_read(sqe, fd, buf, n, 0);
   *        });
   * @param f callable that fills the io_uring_sqe
   * @return generic_io_awaiter<F>
   */
  template<typename F>
  auto io(F&& f);

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
   * @brief context idle or not
   * @return true for idle / false for busy
   */
  CORNET_NODISCARD inline bool idle() {
    if (executor && !executor->idle()) return false;
    return scheduler->idle() && uring.idle();
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
    void* user_data_;
    int flags_;
    cancel_awaiter(context_t& ctx, void* user_data, int flags);
  };

  /**
   * @brief cancel all pending io_uring operations.
   * Uses IORING_ASYNC_CANCEL_ANY on 5.19+ kernels, falls back to
   * per-slot cancellation on older kernels.
   * @return expected<int>: canceled task count on success, error on failure
   */
  inline coro_t<expected<int>> cancel_pending_io() {
    int canceled_nr = 0;

    // Try CANCEL_ANY first (5.19+)
    if (!uring.idle()) {
      auto ret = co_await cancel_awaiter{*this, nullptr, IORING_ASYNC_CANCEL_ANY};
      if (!ret && ret.error().code == EINVAL) {
        // Kernel doesn't support CANCEL_ANY, fallback to per-slot cancel
        std::vector<uint64_t> active;
        slots.for_each_active([&](uint64_t sd) { active.push_back(sd); });
        for (auto sd : active) {
          auto r = co_await cancel_awaiter{*this, reinterpret_cast<void*>(sd), 0};
          if (r && *r > 0) canceled_nr += *r;
        }
        co_return canceled_nr;
      }
      // CANCEL_ANY supported
      if (!ret) {
        if (ret.error().code == ENOENT) co_return canceled_nr;
        co_return ret;
      }
      if (*ret > 0) canceled_nr += *ret;
    }

    // Continue with CANCEL_ANY
    while (!uring.idle()) {
      auto ret = co_await cancel_awaiter{*this, nullptr, IORING_ASYNC_CANCEL_ANY};
      if (!ret) {
        if (ret.error().code == ENOENT) co_return canceled_nr;
        co_return ret;
      }
      if (*ret == 0) co_return canceled_nr;
      canceled_nr += *ret;
    }
    co_return canceled_nr;
  }

  /**
   * @brief return current thread's context (thread-local singleton)
   * @return thread-local context reference
   */
  static inline context_t& current() {
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

private:
  context_t();

  void ensure_executor() {
    if (!executor) {
      executor = std::make_unique<executor_t>(
          config_t::get()["cornet"]["context"]["executor"]["thread_nr"].value_or(1),
          config_t::get()["cornet"]["context"]["executor"]["max_task_nr"].value_or(16384)
      );
    }
  }

  template<typename F, typename R>
  friend struct async_awaiter;

  void switch_to(state_t s) {
    state.store(s, std::memory_order_release);
    SPDLOG_DEBUG("context switch to state:{}", to_string(s));
  }

  // thread-safety mutex for global contexts registry
  static std::mutex contexts_mutex;
  // global contexts registry
  static std::unordered_map<std::thread::id, context_t*> contexts;
  // context owned io_uring wrapper
  uring_t uring;
  // context owned io slot table for safe user_data management
  io_slot_table_t slots;
  // context performance metrics
  context_metrics_t metrics_;
  // eventfd for cross-thread wakeup
  int wakeup_fd{-1};
  // context current state
  std::atomic<state_t> state;
  // context current scheduler type
  scheduler_type_t scheduler_type{scheduler_type_t::RoundRobin};
  // context owner thread id
  std::thread::id owner{std::this_thread::get_id()};
  // context scheduler
  std::unique_ptr<scheduler_t> scheduler;
  // context executor
  std::unique_ptr<executor_t> executor;
  // signalfd for async signal handling (-1 if not used)
  int signal_fd{-1};
  // per-signal handler callbacks
  std::unordered_map<int, std::function<void(int)>> signal_handlers;
  // shutdown timeout
  std::chrono::nanoseconds shutdown_timeout{std::chrono::seconds(5)};

  // internal: signal watch coroutine
  coro_t<void> signal_watch_loop();
  // internal: shutdown coroutine (drain → cancel → terminate)
  coro_t<void> shutdown_sequence();
};

/**
 * @brief generic io_uring awaiter that accepts any prep function.
 * @tparam F callable type with signature void(io_uring_sqe*)
 */
template<typename F>
struct generic_io_awaiter : utask_t {
  F prep_;

  generic_io_awaiter(context_t& ctx, F&& f) : prep_(std::forward<F>(f)) {
    this->ctx = &ctx;
    this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
      static_cast<generic_io_awaiter*>(self)->prep_(sqe);
    };
  }
};

template<typename F>
auto context_t::io(F&& f) {
  return generic_io_awaiter<std::decay_t<F>>{*this, std::forward<F>(f)};
}

/**
 * @brief awaiter for executing a callable on the thread pool.
 * On await_suspend, submits work to executor. On completion, the scheduler
 * picks it up and resumes the coroutine with the result.
 * @tparam F callable type
 * @tparam R return type
 */
template<typename F, typename R = std::invoke_result_t<F>>
struct async_awaiter {
  context_t& ctx_;
  typed_atask_t<F, R> task_;

  explicit async_awaiter(context_t& ctx, F&& f)
    : ctx_(ctx), task_(std::forward<F>(f)) {}

  bool await_ready() { return false; }

  void await_suspend(std::coroutine_handle<> h) {
    task_.handle = h;
    ctx_.ensure_executor();
    ctx_.executor->add(&task_);
  }

  R await_resume() {
    if (task_.exception) {
      std::rethrow_exception(task_.exception);
    }
    if constexpr (!std::is_void_v<R>) {
      return std::move(task_.result_);
    }
  }
};

template<typename F>
auto context_t::async(F&& f) {
  return async_awaiter<std::decay_t<F>>{*this, std::forward<F>(f)};
}

} // cornet

#endif //CORNET_CONTEXT_H
