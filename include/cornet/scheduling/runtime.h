#ifndef CORNET_RUNTIME_H
#define CORNET_RUNTIME_H

#include <thread>
#include <vector>
#include <functional>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <variant>
#include <memory>
#include <type_traits>
#include <chrono>

#include "cornet/scheduling/context.h"

namespace cornet {

// ---------------------------------------------------------------------------
//  Helpers for type extraction from coroutine factories
// ---------------------------------------------------------------------------

namespace detail {

/**
 * @brief Extract the return value type V from a coro_t<V>.
 * Used to deduce the return type of coroutine factories passed to submit().
 */
template<typename T> struct coro_return { using type = void; };
template<typename V>
struct coro_return<coro_t<V>> { using type = V; };
template<typename T>
using coro_return_t = typename coro_return<T>::type;

/**
 * @brief Shared state for task_future_t — thread-safe result/exception storage.
 * @tparam R return value type (specialized for void via std::monostate)
 *
 * Users create a shared_ptr<task_state_t<R>> when submitting a task.
 * The task thread calls set_value/set_error to store the result or exception.
 * The caller thread calls get() to block until completion and retrieve the result.
 */
template<typename R>
struct task_state_t {
    std::variant<std::monostate, R, std::exception_ptr> value_ = std::monostate{};
    bool done_ = false;
    std::condition_variable cv_;
    mutable std::mutex mtx_;

    /**
     * @brief Store a successful result and mark done.
     * Thread-safe: can be called from the executor thread or coroutine thread.
     */
    void set_value(R v) {
        std::lock_guard<std::mutex> lock(mtx_);
        value_ = std::move(v);
        done_ = true;
        cv_.notify_all();
    }

    /**
     * @brief Store an exception and mark done.
     * Thread-safe: can be called from the executor thread or coroutine thread.
     */
    void set_error(std::exception_ptr eptr) {
        std::lock_guard<std::mutex> lock(mtx_);
        value_ = std::move(eptr);
        done_ = true;
        cv_.notify_all();
    }

    /**
     * @brief Mark done without a value (used when value is monostate/void).
     * Thread-safe.
     */
    void set_done() {
        std::lock_guard<std::mutex> lock(mtx_);
        done_ = true;
        cv_.notify_all();
    }

    /**
     * @brief Block until the task completes, then return the result.
     * Rethrows any stored exception. Only callable once (moves the value out).
     * @return the stored result of type R
     */
    R get() {
        std::unique_lock<std::mutex> lock(mtx_);
        cv_.wait(lock, [this]{ return done_; });
        if (std::holds_alternative<std::exception_ptr>(value_))
            std::rethrow_exception(std::get<std::exception_ptr>(value_));
        if constexpr (!std::is_void_v<R>)
            return std::get<1>(std::move(value_));
    }

    /**
     * @brief Check whether the task has completed (non-blocking, with lock).
     * @return true if done, false otherwise
     */
    bool is_done() {
        std::lock_guard<std::mutex> lock(mtx_);
        return done_;
    }
};

/**
 * @brief Void specialization of task_state_t.
 * variant has no value slot (only monostate + exception_ptr).
 */
template<>
struct task_state_t<void> {
    std::variant<std::monostate, std::exception_ptr> value_ = std::monostate{};
    bool done_ = false;
    std::condition_variable cv_;
    mutable std::mutex mtx_;

    void set_error(std::exception_ptr eptr) {
        std::lock_guard<std::mutex> lock(mtx_);
        value_ = std::move(eptr);
        done_ = true;
        cv_.notify_all();
    }

    void set_done() {
        std::lock_guard<std::mutex> lock(mtx_);
        done_ = true;
        cv_.notify_all();
    }

    void get() {
        std::unique_lock<std::mutex> lock(mtx_);
        cv_.wait(lock, [this]{ return done_; });
        if (std::holds_alternative<std::exception_ptr>(value_))
            std::rethrow_exception(std::get<std::exception_ptr>(value_));
    }

    bool is_done() {
        std::lock_guard<std::mutex> lock(mtx_);
        return done_;
    }
};

/**
 * @brief Wrapper coroutine factory for submit().
 * Calls the user's coroutine factory with the context, captures the result or exception.
 * Note: exceptions thrown before the first co_await (e.g., in the factory's constructor)
 * will not be caught by this wrapper.
 * @tparam F callable type (the user's coroutine factory)
 * @tparam V return value type of the user's coroutine
 */
template<typename F, typename V>
coro_t<void> make_submit_wrapper(F f, context_t* ctx, std::shared_ptr<task_state_t<V>> state) {
    try {
        if constexpr (std::is_void_v<V>) {
            co_await f(*ctx);
        } else {
            auto result = co_await f(*ctx);
            state->set_value(std::move(result));
        }
    } catch (...) {
        state->set_error(std::current_exception());
    }
    state->set_done();
}

/**
 * @brief Wrapper coroutine for submit_async().
 * Offloads the callable to the context's executor via co_await ctx.async(),
 * then publishes the result or exception into the shared state.
 *
 * The async_task_t lives inside the async_awaiter on this coroutine's frame,
 * so it is released together with the frame. Never heap-allocate an atask_t:
 * the executor and the scheduler only pass such pointers around and rely on
 * the awaiter that owns them to free them.
 *
 * @tparam F callable type (takes no arguments, returns R)
 * @tparam R return value type of the callable
 */
template<typename F, typename R>
coro_t<void> make_async_wrapper(context_t* ctx, std::shared_ptr<task_state_t<R>> state, F f) {
    try {
        // f is a named frame parameter, not a temporary inside the co_await
        // expression — see context_t::async for why that distinction matters
        auto result = co_await ctx->async(std::move(f));
        state->set_value(std::move(result));
    } catch (...) {
        state->set_error(std::current_exception());
    }
    state->set_done();
}

/**
 * @brief Wrapper coroutine for spawn_async().
 * Same offload path as make_async_wrapper, but fire-and-forget: there is no
 * shared state and no caller to propagate to, so an escaping exception is logged
 * and swallowed.
 *
 * @tparam F callable type (takes no arguments, return value is discarded)
 */
template<typename F>
coro_t<void> make_spawn_async_wrapper(context_t* ctx, F f) {
    try {
        co_await ctx->async(std::move(f));
    } catch (...) {
        spdlog::warn("spawn_async task threw unhandled exception");
    }
}

} // namespace detail

// ---------------------------------------------------------------------------
//  task_future_t — shared future-like handle for runtime tasks
// ---------------------------------------------------------------------------

/**
 * @brief Future-like handle for tasks submitted via runtime_t::submit() and runtime_t::submit_async().
 * Provides blocking get() and non-blocking is_done() access to the task result.
 * @tparam R return value type
 *
 * Usage:
 *   auto f = rt.submit([](context_t& ctx) -> coro_t<int> { co_return 42; });
 *   int result = f.get();  // blocks until task completes
 */
template<typename R>
class task_future_t {
    std::shared_ptr<detail::task_state_t<R>> state_;
public:
    explicit task_future_t(std::shared_ptr<detail::task_state_t<R>> state)
        : state_(std::move(state)) {}

    /**
     * @brief Block until the task completes and return the result.
     * Rethrows any exception thrown by the task.
     * Only callable once — subsequent calls have undefined behavior (value is moved out).
     * @return the task's return value
     */
    R get() { return state_->get(); }

    /**
     * @brief Check whether the task has completed (non-blocking).
     * @return true if done, false otherwise
     */
    bool is_done() { return state_->is_done(); }
};

// ---------------------------------------------------------------------------
//  runtime_t — multi-threaded runtime managing N worker threads
// ---------------------------------------------------------------------------

/**
 * @brief multi-threaded runtime that manages N worker threads, each with its own context_t.
 * Thread-per-core / shared-nothing model. Coroutines never migrate between threads.
 * Cross-thread communication is via spawn_remote() only.
 *
 * Usage:
 *   runtime_t rt(4);
 *   rt.start([](size_t idx, context_t& ctx) {
 *       // per-thread initialization (e.g., spawn listeners)
 *   });
 *   rt.join();  // blocks until all threads exit
 *
 * High-level task submission (new):
 *   auto f = rt.submit([](context_t& ctx) -> coro_t<int> { co_return 42; });
 *   int result = f.get();
 *   rt.spawn_async([] { background_work(); });
 */
class runtime_t {
public:
  /**
   * @brief construct runtime with specified thread count and create all context_t instances.
   * @param thread_nr number of worker threads (default: hardware_concurrency)
   */
  explicit runtime_t(config_t* config = nullptr, size_t thread_nr = std::thread::hardware_concurrency());

  ~runtime_t();

  runtime_t(const runtime_t&) = delete;
  runtime_t& operator=(const runtime_t&) = delete;
  runtime_t(runtime_t&&) = delete;
  runtime_t& operator=(runtime_t&&) = delete;

  /**
   * @brief start all worker threads. Each thread calls run().
   * Blocks until all threads are ready to run.
   * @param init_fn per-thread initialization function called with (thread_index, context_t&).
   *                The context for each thread is created by the runtime,
   *                so it is passed directly to the init_fn.
   * @param keep_alive whether worker context keep alive
   */
  void start(std::function<void(size_t, context_t&)> init_fn = nullptr, bool keep_alive=true);

  /**
   * @brief initiate graceful shutdown on all contexts
   * @param timeout per-context shutdown timeout
   */
  void shutdown(std::chrono::nanoseconds timeout = std::chrono::seconds(5));

  /**
   * @brief initiate graceful shutdown on all contexts and wait for threads to finish.
   * This is a blocking call — equivalent to shutdown(timeout) + join().
   * @param timeout per-context shutdown timeout
   */
  void shutdown_and_join(std::chrono::nanoseconds timeout = std::chrono::seconds(5));

  /**
   * @brief forcefully stop all contexts.
   */
  void stop();

  /**
   * @brief forcefully stop all contexts and then join worker threads.
   */
  void stop_and_join();

  /**
   * @brief wait for all worker threads to finish (blocks caller).
   */
  void join();

  /**
   * @brief number of worker threads.
   */
  size_t size() const { return thread_nr_; }

  /**
   * @brief get the context_t for a specific worker thread.
   * Must be called after start() and before shutdown().
   * @param idx thread index (0 <= idx < size())
   * @return context_t& reference to the context
   */
  CORNET_NODISCARD context_t& context_at(size_t idx) const {
    if (idx >= contexts_.size()) throw std::out_of_range("context_at: idx out of range");
    return *contexts_[idx];
  }

  // =====================================================================
  //  High-level task submission APIs
  // =====================================================================

  /**
   * @brief submit a coroutine task to a worker context.
   * The callable takes a context_t& and returns a coro_t<V>.
   * Returns a task_future_t<V> that can be used to retrieve the result.
   *
   * Usage:
   *   auto f = rt.submit([](context_t& ctx) -> coro_t<int> {
   *       auto data = co_await read_file(ctx, "data.txt");
   *       co_return process(data);
   *   });
   *   int result = f.get();  // blocks until task completes
   *
   * @tparam F callable type that takes context_t& and returns coro_t<V>
   * @return task_future_t<V> with the coroutine's return value
   */
  template<typename F>
  auto submit(F&& f) -> task_future_t<detail::coro_return_t<std::decay_t<decltype(f(std::declval<context_t&>()))>>> {
    using V = detail::coro_return_t<std::decay_t<decltype(f(std::declval<context_t&>()))>>;

    auto& ctx = select_context();
    auto state = std::make_shared<detail::task_state_t<V>>();

    auto future = task_future_t<V>{state};
    auto coro = detail::make_submit_wrapper<F, V>(std::forward<F>(f), &ctx, std::move(state));
    ctx.spawn_remote(coro.detach());
    return future;
  }

  /**
   * @brief spawn a coroutine fire-and-forget on a worker context.
   * The callable takes a context_t& and returns a coro_t<V>.
   * No result is returned — the coroutine runs independently.
   *
   * The callable is moved into a wrapper coroutine and only invoked there, so a
   * temporary lambda stays alive for the whole run of the coroutine it produces.
   * Invoking the factory here instead and passing the resulting coro_t would leave
   * the closure — and everything its captures reference — dangling at the first
   * suspension point, since a lambda coroutine's frame only stores the closure
   * pointer, not the closure.
   *
   * Usage:
   *   rt.spawn([](context_t& ctx) -> coro_t<void> {
   *       co_await some_io_operation(ctx);
   *   });
   *
   * @tparam F callable type that takes context_t& and returns a coroutine
   */
  template<typename F, typename... Args>
  void spawn(F&& f, Args&&... args) {
    auto& ctx = select_context();
    ctx.spawn_remote([&ctx, f = std::decay_t<F>(std::forward<F>(f)),
                      ...as = std::decay_t<Args>(std::forward<Args>(args))]() mutable {
      return f(ctx, std::move(as)...);
    });
  }

  /**
   * @brief submit a CPU/blocking task to a worker context's executor.
   * The callable takes no arguments and returns R.
   * Returns a task_future_t<R> that can be used to retrieve the result.
   *
   * The callable is offloaded by a wrapper coroutine running on the selected
   * context, so the context must be started and running for the task to make
   * progress — same as submit().
   *
   * Usage:
   *   auto f = rt.submit_async([] { return heavy_computation(); });
   *   int result = f.get();  // blocks until task completes
   *
   * @tparam F callable type that returns R
   * @return task_future_t<R> with the callable's return value
   */
  template<typename F>
  auto submit_async(F&& f) -> task_future_t<std::invoke_result_t<std::decay_t<F>>> {
    using R = std::invoke_result_t<std::decay_t<F>>;

    auto& ctx = select_context();
    auto state = std::make_shared<detail::task_state_t<R>>();
    auto future = task_future_t<R>{state};

    auto coro = detail::make_async_wrapper<std::decay_t<F>, R>(
        &ctx, std::move(state), std::forward<F>(f));
    ctx.spawn_remote(coro.detach());
    return future;
  }

  /**
   * @brief submit a CPU/blocking task with forwarded arguments.
   * Returns a task_future_t<R> that can be used to retrieve the result.
   *
   * Usage:
   *   auto f = rt.submit_async([](int a, int b) { return a + b; }, 3, 4);
   *   int result = f.get();  // returns 7
   *
   * @tparam F callable type
   * @tparam Args argument types
   * @return task_future_t<R> with the callable's return value
   */
  template<typename F, typename... Args>
    requires (sizeof...(Args) > 0)
  auto submit_async(F&& f, Args&&... args)
    -> task_future_t<std::invoke_result_t<std::decay_t<F>, std::decay_t<Args>...>> {
    using R = std::invoke_result_t<std::decay_t<F>, std::decay_t<Args>...>;

    // bind the arguments into a nullary callable and reuse the no-arg overload
    return submit_async([f = std::decay_t<F>(std::forward<F>(f)),
                         ...as = std::decay_t<Args>(std::forward<Args>(args))]() mutable -> R {
      return f(std::move(as)...);
    });
  }

  /**
   * @brief spawn a CPU/blocking task fire-and-forget on a worker context's executor.
   * The callable takes no arguments; its return value is discarded.
   * Exceptions are caught and logged, never propagated.
   *
   * Usage:
   *   rt.spawn_async([] { background_cleanup(); });
   *
   * @tparam F callable type that returns R
   */
  template<typename F>
  void spawn_async(F&& f) {
    auto& ctx = select_context();

    auto coro = detail::make_spawn_async_wrapper<std::decay_t<F>>(&ctx, std::forward<F>(f));
    ctx.spawn_remote(coro.detach());
  }

private:
  /**
   * @brief select a context via round-robin.
   * Thread-safe: uses atomic increment.
   * @return reference to the selected context
   */
  context_t& select_context() {
    // Per-thread round-robin via thread_local to avoid shared atomic contention.
    // When called from a single thread (e.g. server thread dispatching connections),
    // this eliminates cache-line bouncing on next_index_.
    static thread_local size_t idx = 0;
    return *contexts_[idx++ % thread_nr_];
  }

  config_t* config_{nullptr};
  size_t thread_nr_;
  std::vector<std::thread> workers_;
  std::vector<std::unique_ptr<context_t>> contexts_;
  std::atomic<bool> stopped_{false};
  mutable std::mutex mutex_;
};

} // namespace cornet

#endif // CORNET_RUNTIME_H
