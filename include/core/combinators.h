#ifndef CORNET_COMBINATORS_H
#define CORNET_COMBINATORS_H

#include "context.h"
#include <tuple>
#include <memory>

namespace cornet {

/**
 * @brief sleep awaiter. Suspends the coroutine for the given duration using io_uring timeout.
 * Usage: co_await sleep(std::chrono::seconds(1));
 */
struct sleep_awaiter : utask_t {
  __kernel_timespec ts_;

  template<typename Rep, typename Period>
  explicit sleep_awaiter(std::chrono::duration<Rep, Period> duration) {
    this->ctx = &context_t::current();
    ts_ = to_kernel_timespec(duration);
    this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
      auto* t = static_cast<sleep_awaiter*>(self);
      io_uring_prep_timeout(sqe, &t->ts_, 0, 0);
    };
  }

  CORNET_NODISCARD CORNET_MAYBE_UNUSED expected<void> await_resume() const {
    if (value == -ETIME) {
      return {};
    }
    if (value < 0) {
      return unexpected(-value);
    }
    return {};
  }
};

/**
 * @brief create a sleep awaiter
 * @param duration how long to sleep
 * @return awaitable that completes after the duration
 */
template<typename Rep, typename Period>
sleep_awaiter sleep(std::chrono::duration<Rep, Period> duration) {
  return sleep_awaiter{duration};
}

/**
 * @brief timeout awaiter. Wraps a utask_t-based awaitable with a linked timeout.
 * Uses io_uring's IOSQE_IO_LINK + link_timeout to atomically cancel the operation on timeout.
 * If the operation completes before the timeout, returns its result.
 * If the timeout fires first, the operation is cancelled and returns ETIMEDOUT.
 *
 * Usage: auto result = co_await with_timeout(sock.recv(buf, n), 5s);
 */
template<typename Awaitable, typename Rep, typename Period>
struct timeout_awaiter {
  Awaitable op_;
  context_t* ctx_;
  __kernel_timespec ts_;
  uint64_t timeout_slot_{0};

  timeout_awaiter(Awaitable op, std::chrono::duration<Rep, Period> duration)
    : op_(std::move(op)), ctx_(&context_t::current()), ts_(to_kernel_timespec(duration)) {}

  bool await_ready() { return op_.await_ready(); }

  void await_suspend(std::coroutine_handle<> h) {
    op_.handle = h;
    auto& uring = ctx_->io_uring();
    auto& slots = ctx_->io_slots();

    io_uring_sqe* sqes[2];
    uring.get_sqes(sqes, 2);

    op_.slot_data = slots.alloc(&op_);
    op_.prepare_fn(&op_, sqes[0]);
    io_uring_sqe_set_data(sqes[0], reinterpret_cast<void*>(op_.slot_data));
    io_uring_sqe_set_flags(sqes[0], sqes[0]->flags | IOSQE_IO_LINK);

    io_uring_prep_link_timeout(sqes[1], &ts_, 0);
    io_uring_sqe_set_data(sqes[1], nullptr);
  }

  expected<int> await_resume() {
    if (op_.value == -ECANCELED) {
      return unexpected(ETIMEDOUT);
    }
    if (op_.value < 0) {
      return unexpected(-op_.value);
    }
    return op_.value;
  }
};

/**
 * @brief create a timeout-wrapped awaitable
 * @param op the io operation awaiter (recv_awaiter, send_awaiter, etc.)
 * @param duration timeout duration
 * @return awaitable that returns ETIMEDOUT on timeout
 */
template<typename Awaitable, typename Rep, typename Period>
requires std::derived_from<Awaitable, utask_t>
timeout_awaiter<Awaitable, Rep, Period> with_timeout(Awaitable op, std::chrono::duration<Rep, Period> duration) {
  return {std::move(op), duration};
}

/**
 * @brief coroutine-level with_cancel. Injects a canceler into cancelable_coro_t's promise
 * so all internal utask_t operations are automatically cancellable.
 */
template<typename V>
struct coro_cancellable_awaiter {
  cancelable_coro_t<V> coro_;
  canceler_t& canceler_;

  coro_cancellable_awaiter(cancelable_coro_t<V> coro, canceler_t& canceler)
    : coro_(std::move(coro)), canceler_(canceler) {}

  bool await_ready() { return coro_.done(); }

  std::coroutine_handle<> await_suspend(std::coroutine_handle<> parent) {
    auto h = coro_.native_handle();
    h.promise().canceler_ = &canceler_;
    h.promise().continuation = parent;
    return h;
  }

  V await_resume() { return coro_.value(); }
};

template<typename V>
coro_cancellable_awaiter<V> with_cancel(cancelable_coro_t<V> coro, canceler_t& canceler) {
  return {std::move(coro), canceler};
}

/**
 * @brief coroutine-level with_timeout. Injects a canceler into the coro's promise
 * and races it against a timer. If timeout fires first, cancels the coro's IO.
 * Returns coro_t<V> transparently — when V is expected<T>, timeout returns unexpected(ETIMEDOUT).
 */

namespace detail {
  template<typename T> struct is_expected : std::false_type {};
  template<typename T> struct is_expected<expected<T>> : std::true_type {};
  template<> struct is_expected<expected<void>> : std::true_type {};
}

template<typename V, typename Rep, typename Period>
cancelable_coro_t<V> with_timeout(cancelable_coro_t<V> coro, std::chrono::duration<Rep, Period> duration) {
  auto canceler = std::make_shared<canceler_t>();
  coro.native_handle().promise().canceler_ = canceler.get();

  auto timer = [](std::shared_ptr<canceler_t> c, std::chrono::duration<Rep, Period> d) -> coro_t<void> {
    sleep_awaiter sa{d};
    auto ret = co_await with_cancel(std::move(sa), *c);
    if (ret.has_value()) {
      c->cancel();
    }
  }(canceler, duration);

  auto& ctx = context_t::current();
  ctx.spawn(std::move(timer));

  if constexpr (std::is_void_v<V>) {
    co_await coro;
    canceler->cancel();
  } else {
    auto result = co_await coro;
    canceler->cancel();
    // if V is expected<T> and the result contains ECANCELED from timeout, convert to ETIMEDOUT
    if constexpr (detail::is_expected<V>::value) {
      if (!result && result.error().code == ECANCELED && canceler->is_cancelled()) {
        co_return unexpected(ETIMEDOUT);
      }
    }
    co_return std::move(result);
  }
}

namespace detail {

/**
 * @brief state for when_all/when_any with N coroutines
 */
template<typename... Ts>
struct when_all_state {
  std::tuple<expected<Ts>...> results;
  int remaining;
  std::coroutine_handle<> continuation{nullptr};

  when_all_state() : remaining(sizeof...(Ts)) {}
};

template<typename... Ts>
struct when_any_state {
  std::tuple<expected<Ts>...> results;
  bool done{false};
  int completed_index{-1};
  std::coroutine_handle<> continuation{nullptr};
  canceler_t* canceler{nullptr};

  when_any_state() = default;
  explicit when_any_state(canceler_t* c) : canceler(c) {}
};

template<size_t I, typename State, typename T>
coro_t<void> when_all_task(std::shared_ptr<State> state, coro_t<T> coro, context_t& ctx) {
  try {
    if constexpr (std::is_void_v<T>) {
      co_await coro;
      std::get<I>(state->results) = expected<void>{};
    } else {
      auto result = co_await coro;
      std::get<I>(state->results) = std::move(result);
    }
  } catch (const std::exception& e) {
    SPDLOG_ERROR("when_all: task {} threw exception: {}", I, e.what());
    std::get<I>(state->results) = unexpected(EFAULT, error_domain::exception);
  } catch (...) {
    SPDLOG_ERROR("when_all: task {} threw unknown exception", I);
    std::get<I>(state->results) = unexpected(EFAULT, error_domain::exception);
  }
  if (--state->remaining == 0) {
    if (state->continuation) {
      ctx.spawn(state->continuation);
    }
  }
  co_return;
}

template<size_t I, typename State, typename T>
coro_t<void> when_any_task(std::shared_ptr<State> state, coro_t<T> coro, context_t& ctx) {
  try {
    if constexpr (std::is_void_v<T>) {
      co_await coro;
      if (state->done) co_return;
      std::get<I>(state->results) = expected<void>{};
    } else {
      auto result = co_await coro;
      if (state->done) co_return;
      std::get<I>(state->results) = std::move(result);
    }
  } catch (const std::exception& e) {
    if (state->done) co_return;
    SPDLOG_ERROR("when_any: task {} threw exception: {}", I, e.what());
    std::get<I>(state->results) = unexpected(EFAULT, error_domain::exception);
  } catch (...) {
    if (state->done) co_return;
    SPDLOG_ERROR("when_any: task {} threw unknown exception", I);
    std::get<I>(state->results) = unexpected(EFAULT, error_domain::exception);
  }
  state->done = true;
  state->completed_index = I;
  if (state->canceler) {
    state->canceler->cancel();
  }
  if (state->continuation) {
    ctx.spawn(state->continuation);
  }
  co_return;
}

template<typename State, typename Tuple, size_t... Is>
void launch_all_impl(std::shared_ptr<State> state, Tuple& coros, context_t& ctx, std::index_sequence<Is...>) {
  (ctx.spawn(when_all_task<Is>(state, std::move(std::get<Is>(coros)), ctx)), ...);
}

template<typename State, typename Tuple, size_t... Is>
void launch_any_impl(std::shared_ptr<State> state, Tuple& coros, context_t& ctx, std::index_sequence<Is...>) {
  (ctx.spawn(when_any_task<Is>(state, std::move(std::get<Is>(coros)), ctx)), ...);
}

} // namespace detail

/**
 * @brief when_all result type
 */
template<typename... Ts>
struct when_all_result {
  std::tuple<expected<Ts>...> results;

  template<size_t I>
  auto& get() { return std::get<I>(results); }

  template<size_t I>
  const auto& get() const { return std::get<I>(results); }
};

/**
 * @brief when_any result type
 */
template<typename... Ts>
struct when_any_result {
  std::tuple<expected<Ts>...> results;
  int index{-1};

  template<size_t I>
  auto& get() { return std::get<I>(results); }

  template<size_t I>
  const auto& get() const { return std::get<I>(results); }
};

/**
 * @brief await all coroutines concurrently, resume when all complete.
 * Usage: auto result = co_await when_all(coro1(), coro2());
 * @return when_all_result containing all results
 */
template<typename... Ts>
auto when_all(coro_t<Ts>... coros) {
  struct awaiter {
    context_t& ctx_;
    std::shared_ptr<detail::when_all_state<Ts...>> state_;
    std::tuple<coro_t<Ts>...> coros_;

    ~awaiter() {
      if (state_) {
        state_->continuation = nullptr;
      }
    }

    bool await_ready() { return false; }

    void await_suspend(std::coroutine_handle<> h) {
      state_->continuation = h;
      detail::launch_all_impl(state_, coros_, ctx_, std::index_sequence_for<Ts...>{});
    }

    when_all_result<Ts...> await_resume() {
      return {std::move(state_->results)};
    }
  };

  auto& ctx = context_t::current();
  return awaiter{ctx, std::make_shared<detail::when_all_state<Ts...>>(), std::tuple{std::move(coros)...}};
}

/**
 * @brief await any coroutine concurrently, resume when the first one completes.
 * Remaining coroutines continue running but their results are discarded.
 * Usage: auto result = co_await when_any(coro1(), coro2());
 * @return when_any_result containing all results (only the completed one is valid)
 */
template<typename... Ts>
auto when_any(coro_t<Ts>... coros) {
  struct awaiter {
    context_t& ctx_;
    std::shared_ptr<detail::when_any_state<Ts...>> state_;
    std::tuple<coro_t<Ts>...> coros_;

    ~awaiter() {
      if (state_) {
        state_->continuation = nullptr;
      }
    }

    bool await_ready() { return false; }

    void await_suspend(std::coroutine_handle<> h) {
      state_->continuation = h;
      detail::launch_any_impl(state_, coros_, ctx_, std::index_sequence_for<Ts...>{});
    }

    when_any_result<Ts...> await_resume() {
      return {std::move(state_->results), state_->completed_index};
    }
  };

  auto& ctx = context_t::current();
  return awaiter{ctx, std::make_shared<detail::when_any_state<Ts...>>(), std::tuple{std::move(coros)...}};
}

/**
 * @brief await any coroutine concurrently with cancellation support.
 * When the first coroutine completes, the provided canceler is triggered,
 * cancelling any inflight IO operations that use with_cancel(op, canceler).
 * Usage:
 *   canceler_t canceler;
 *   auto result = co_await when_any(canceler, task_with_cancel(canceler), task_with_cancel(canceler));
 * @param canceler canceler to trigger on first completion
 * @return when_any_result containing all results (only the completed one is valid)
 */
template<typename... Ts>
auto when_any(canceler_t& canceler, coro_t<Ts>... coros) {
  struct awaiter {
    context_t& ctx_;
    std::shared_ptr<detail::when_any_state<Ts...>> state_;
    std::tuple<coro_t<Ts>...> coros_;

    ~awaiter() {
      if (state_) {
        state_->continuation = nullptr;
      }
    }

    bool await_ready() { return false; }

    void await_suspend(std::coroutine_handle<> h) {
      state_->continuation = h;
      detail::launch_any_impl(state_, coros_, ctx_, std::index_sequence_for<Ts...>{});
    }

    when_any_result<Ts...> await_resume() {
      return {std::move(state_->results), state_->completed_index};
    }
  };

  auto& ctx = context_t::current();
  auto state = std::make_shared<detail::when_any_state<Ts...>>(&canceler);
  return awaiter{ctx, std::move(state), std::tuple{std::move(coros)...}};
}

/**
 * @brief structured concurrency scope.
 * Guarantees that all child tasks spawned via scope.spawn() complete (or are cancelled)
 * before the scope exits. Provides structured lifetime management for concurrent tasks.
 *
 * Key guarantees:
 * - No child task outlives the scope
 * - If any child fails with an exception, all siblings are cancelled
 * - If the scope is cancelled externally, all children are cancelled
 * - The scope blocks (suspends) until all children finish
 *
 * Usage:
 *   co_await task_scope([](scope_t& scope) -> coro_t<void> {
 *       scope.spawn(task1());
 *       scope.spawn(task2());
 *       // scope exits here, waits for task1 and task2 to complete
 *   });
 *
 *   // with result collection:
 *   int result1, result2;
 *   co_await task_scope([&](scope_t& scope) -> coro_t<void> {
 *       scope.spawn(compute1(), result1);
 *       scope.spawn(compute2(), result2);
 *       co_return;
 *   });
 *   // result1 and result2 are safely populated here
 */
class scope_t {
public:
  explicit scope_t(context_t& ctx)
    : ctx_(ctx), canceler_() {}

  scope_t(context_t& ctx, canceler_t& parent)
    : ctx_(ctx), canceler_(parent) {}

  scope_t(const scope_t&) = delete;
  scope_t& operator=(const scope_t&) = delete;

  /**
   * @brief spawn a coroutine into this scope (fire-and-forget).
   * Works with any coro_t<T>. Return value is discarded.
   * @tparam T coroutine return type (can be void or any other type)
   */
  template<typename T>
  void spawn(coro_t<T> task) {
    active_count_++;
    ctx_.spawn(scoped_runner(std::move(task)));
  }

  /**
   * @brief spawn a callable (lambda) that returns a coroutine.
   * The callable is moved into the coroutine frame, solving the
   * "temporary lambda coroutine" lifetime issue.
   *
   * Usage (safe):
   *   scope.spawn([&count]() -> coro_t<void> {
   *       co_await sleep(100ms);
   *       count++;
   *   });
   *
   * Without this overload, the following is UNSAFE (dangling lambda):
   *   scope.spawn([&count]() -> coro_t<void> { ... }());  // BUG: lambda is temporary!
   *
   * @tparam F callable type that returns coro_t<something>
   */
  template<typename F, typename = std::enable_if_t<
    !std::is_same_v<std::decay_t<F>, coro_t<void>> &&
    !std::is_base_of_v<task_t, std::decay_t<F>>
  >>
  void spawn(F&& f) {
    active_count_++;
    ctx_.spawn(scoped_runner_fn(std::forward<F>(f)));
  }

  /**
   * @brief spawn a coroutine and store its result in the provided reference.
   * Safe because the scope guarantees the child completes before scope exits.
   * @tparam T coroutine return type (non-void)
   * @param task the coroutine to run
   * @param out reference to store the result (populated on success)
   */
  template<typename T>
  void spawn(coro_t<T> task, T& out) {
    active_count_++;
    ctx_.spawn(scoped_runner_with_result(std::move(task), out));
  }

  /**
   * @brief spawn a coroutine and store its result as expected<T>.
   * Captures both success and error cases without propagating exceptions.
   * @tparam T coroutine return type (non-void)
   * @param task the coroutine to run
   * @param out reference to expected<T> that receives the result or error
   */
  template<typename T>
  void spawn(coro_t<T> task, expected<T>& out) {
    active_count_++;
    ctx_.spawn(scoped_runner_with_expected(std::move(task), out));
  }

  /**
   * @brief cancel all child tasks in this scope.
   */
  void cancel() { canceler_.cancel(); }

  /**
   * @brief check if this scope has been cancelled.
   */
  CORNET_NODISCARD bool is_cancelled() const { return canceler_.is_cancelled(); }

  /**
   * @brief get the scope's canceler for use with with_cancel().
   */
  canceler_t& canceler() { return canceler_; }

  /**
   * @brief get the first error that occurred in any child task.
   */
  CORNET_NODISCARD const expected<void>& error() const { return first_error_; }

private:
  friend struct scope_join_awaiter;

  template<typename F>
  friend auto task_scope(F&& body);

  template<typename F>
  friend auto task_scope(canceler_t& parent, F&& body);

  /**
   * @brief runner for coro_t<void> tasks
   */
  coro_t<void> scoped_runner(coro_t<void> task) {
    try {
      co_await task;
    } catch (const std::exception& e) {
      if (first_error_.has_value()) {
        SPDLOG_ERROR("task_scope: child task threw exception: {}", e.what());
        first_error_ = unexpected(EFAULT, error_domain::exception);
        canceler_.cancel();
      }
    } catch (...) {
      if (first_error_.has_value()) {
        SPDLOG_ERROR("task_scope: child task threw unknown exception");
        first_error_ = unexpected(EFAULT, error_domain::exception);
        canceler_.cancel();
      }
    }
    on_child_done();
  }

  /**
   * @brief runner for coro_t<T> tasks (discard result)
   */
  template<typename T>
  coro_t<void> scoped_runner(coro_t<T> task) {
    try {
      co_await task;
    } catch (const std::exception& e) {
      if (first_error_.has_value()) {
        SPDLOG_ERROR("task_scope: child task threw exception: {}", e.what());
        first_error_ = unexpected(EFAULT, error_domain::exception);
        canceler_.cancel();
      }
    } catch (...) {
      if (first_error_.has_value()) {
        SPDLOG_ERROR("task_scope: child task threw unknown exception");
        first_error_ = unexpected(EFAULT, error_domain::exception);
        canceler_.cancel();
      }
    }
    on_child_done();
  }

  /**
   * @brief runner for coro_t<T> with result stored by reference
   */
  template<typename T>
  coro_t<void> scoped_runner_with_result(coro_t<T> task, T& out) {
    try {
      out = co_await task;
    } catch (const std::exception& e) {
      if (first_error_.has_value()) {
        SPDLOG_ERROR("task_scope: child task threw exception: {}", e.what());
        first_error_ = unexpected(EFAULT, error_domain::exception);
        canceler_.cancel();
      }
    } catch (...) {
      if (first_error_.has_value()) {
        SPDLOG_ERROR("task_scope: child task threw unknown exception");
        first_error_ = unexpected(EFAULT, error_domain::exception);
        canceler_.cancel();
      }
    }
    on_child_done();
  }

  /**
   * @brief runner for coro_t<T> with result stored as expected<T>
   */
  template<typename T>
  coro_t<void> scoped_runner_with_expected(coro_t<T> task, expected<T>& out) {
    try {
      out = co_await task;
    } catch (const std::exception& e) {
      SPDLOG_ERROR("task_scope: child task threw exception: {}", e.what());
      out = unexpected(EFAULT, error_domain::exception);
      if (first_error_.has_value()) {
        first_error_ = unexpected(EFAULT, error_domain::exception);
        canceler_.cancel();
      }
    } catch (...) {
      SPDLOG_ERROR("task_scope: child task threw unknown exception");
      out = unexpected(EFAULT, error_domain::exception);
      if (first_error_.has_value()) {
        first_error_ = unexpected(EFAULT, error_domain::exception);
        canceler_.cancel();
      }
    }
    on_child_done();
  }

  /**
   * @brief runner for callable (lambda) that returns a coroutine.
   * The callable is moved into the coroutine frame as a parameter,
   * ensuring it lives for the entire duration of the coroutine.
   * This avoids the "temporary lambda coroutine" dangling pointer issue.
   */
  template<typename F>
  coro_t<void> scoped_runner_fn(F f) {
    try {
      co_await f();
    } catch (const std::exception& e) {
      if (first_error_.has_value()) {
        SPDLOG_ERROR("task_scope: child task threw exception: {}", e.what());
        first_error_ = unexpected(EFAULT, error_domain::exception);
        canceler_.cancel();
      }
    } catch (...) {
      if (first_error_.has_value()) {
        SPDLOG_ERROR("task_scope: child task threw unknown exception");
        first_error_ = unexpected(EFAULT, error_domain::exception);
        canceler_.cancel();
      }
    }
    on_child_done();
  }

  void on_child_done() {
    active_count_--;
    if (active_count_ == 0 && waiter_) {
      ctx_.spawn(waiter_);
      waiter_ = nullptr;
    }
  }

  context_t& ctx_;
  canceler_t canceler_;
  int active_count_{0};
  std::coroutine_handle<> waiter_{nullptr};
  expected<void> first_error_{};
};

/**
 * @brief awaiter that suspends until all scope children complete.
 */
struct scope_join_awaiter {
  scope_t& scope_;

  bool await_ready() { return scope_.active_count_ == 0; }

  void await_suspend(std::coroutine_handle<> h) {
    scope_.waiter_ = h;
  }

  expected<void> await_resume() {
    return scope_.first_error_;
  }
};

/**
 * @brief create a structured concurrency scope and execute body within it.
 * The scope guarantees all spawned children complete before returning.
 *
 * Usage:
 *   auto result = co_await task_scope([](scope_t& scope) -> coro_t<void> {
 *       scope.spawn(handle_client(client1));
 *       scope.spawn(handle_client(client2));
 *       // all children are joined when body returns
 *   });
 *   if (!result) { handle error }
 *
 * @param body callable that receives scope_t& and returns coro_t<void>
 * @return awaitable that yields expected<void> (first child error, if any)
 */
template<typename F>
auto task_scope(F&& body) {
  struct awaiter {
    context_t& ctx_;
    std::decay_t<F> body_;
    std::unique_ptr<scope_t> scope_;

    bool await_ready() { return false; }

    void await_suspend(std::coroutine_handle<> h) {
      scope_ = std::make_unique<scope_t>(ctx_);
      auto runner = [](std::unique_ptr<scope_t>& scope, std::decay_t<F> body,
                       std::coroutine_handle<> continuation) -> coro_t<void> {
        co_await body(*scope);
        co_await scope_join_awaiter{*scope};
        scope->ctx_.spawn(continuation);
      };
      ctx_.spawn(runner(scope_, std::move(body_), h));
    }

    expected<void> await_resume() {
      return scope_->first_error_;
    }
  };

  auto& ctx = context_t::current();
  return awaiter{ctx, std::forward<F>(body), nullptr};
}

/**
 * @brief create a structured concurrency scope with a parent canceler.
 * If the parent canceler is triggered, all children in this scope are cancelled.
 *
 * Usage:
 *   canceler_t parent_canceler;
 *   auto result = co_await task_scope(parent_canceler, [](scope_t& scope) -> coro_t<void> {
 *       scope.spawn(handle_client(client));
 *   });
 *
 * @param parent parent canceler that can cancel this entire scope
 * @param body callable that receives scope_t& and returns coro_t<void>
 * @return awaitable that yields expected<void>
 */
template<typename F>
auto task_scope(canceler_t& parent, F&& body) {
  struct awaiter {
    context_t& ctx_;
    canceler_t& parent_;
    std::decay_t<F> body_;
    std::unique_ptr<scope_t> scope_;

    bool await_ready() { return false; }

    void await_suspend(std::coroutine_handle<> h) {
      scope_ = std::make_unique<scope_t>(ctx_, parent_);
      auto runner = [](std::unique_ptr<scope_t>& scope, std::decay_t<F> body,
                       std::coroutine_handle<> continuation) -> coro_t<void> {
        co_await body(*scope);
        co_await scope_join_awaiter{*scope};
        scope->ctx_.spawn(continuation);
      };
      ctx_.spawn(runner(scope_, std::move(body_), h));
    }

    expected<void> await_resume() {
      return scope_->first_error_;
    }
  };

  auto& ctx = context_t::current();
  return awaiter{ctx, parent, std::forward<F>(body), nullptr};
}

} // namespace cornet

#endif //CORNET_COMBINATORS_H
