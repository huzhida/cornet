#ifndef CORNET_SCOPE_H
#define CORNET_SCOPE_H

#include <coroutine>
#include <memory>
#include <optional>

#include <spdlog/spdlog.h>

#include "cornet/coroutine/cancel.h"
#include "cornet/scheduling/context.h"

namespace cornet {

class context_t;
template<typename T> struct coro_t;
class scope_t;

namespace detail {
template<typename F>
coro_t<void> scope_runner(std::shared_ptr<scope_t> scope, F body,
                          std::coroutine_handle<> continuation);
} // namespace detail

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
    : ctx_(ctx), canceler_(std::make_unique<canceler_t>(ctx)) {}

  scope_t(context_t& ctx, canceler_t& parent)
    : ctx_(ctx), canceler_(std::make_unique<canceler_t>(ctx, parent)) {}

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
    ctx_.spawn(scoped_runner(std::move(task), *this));
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
    ctx_.spawn(scoped_runner_fn(std::forward<F>(f), *this));
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
    ctx_.spawn(scoped_runner_with_result(std::move(task), out, *this));
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
    ctx_.spawn(scoped_runner_with_expected(std::move(task), out, *this));
  }

  /**
   * @brief cancel all child tasks in this scope.
   */
  void cancel() { canceler_->cancel(); }

  /**
   * @brief check if this scope has been cancelled.
   */
  CORNET_NODISCARD bool is_cancelled() const { return canceler_->is_cancelled(); }

  /**
   * @brief get the scope's canceler for use with with_cancel().
   */
  canceler_t* canceler() { return canceler_.get(); }

  /**
   * @brief get the first error that occurred in any child task.
   */
  CORNET_NODISCARD expected<void> error() const {
    return as_expected();
  }

private:
  friend struct scope_join_awaiter;

  template<typename F>
  friend auto task_scope(context_t& ctx, F&& body);

  template<typename F>
  friend auto task_scope(context_t& ctx, canceler_t& parent, F&& body);

  template<typename F>
  friend auto task_scope(canceler_t& parent, F&& body);

  template<typename F>
  friend coro_t<void> detail::scope_runner(std::shared_ptr<scope_t>, F,
                                           std::coroutine_handle<>);

  /**
   * @brief record a child's failure: keep the first one and cancel siblings.
   * Returns without cancelling when an error was already recorded.
   */
  void record_failure(const char* what) {
    if (first_error_) return;
    if (what) {
      SPDLOG_ERROR("task_scope: child task threw exception: {}", what);
    } else {
      SPDLOG_ERROR("task_scope: child task threw unknown exception");
    }
    first_error_ = error_t{EFAULT, error_domain::Exception};
    canceler_->cancel();
  }

  expected<void> as_expected() const {
    return first_error_ ? unexpected(*first_error_) : expected<void>{};
  }

  /**
   * @brief runner for coro_t<void> tasks
   */
  template<typename Scope>
  static coro_t<void> scoped_runner(coro_t<void> task, Scope& s) {
    try {
      co_await task;
    } catch (const std::exception& e) {
      s.record_failure(e.what());
    } catch (...) {
      s.record_failure(nullptr);
    }
    s.on_child_done();
  }

  /**
   * @brief runner for coro_t<T> tasks (discard result)
   */
  template<typename T, typename Scope>
  static coro_t<void> scoped_runner(coro_t<T> task, Scope& s) {
    try {
      co_await task;
    } catch (const std::exception& e) {
      s.record_failure(e.what());
    } catch (...) {
      s.record_failure(nullptr);
    }
    s.on_child_done();
  }

  /**
   * @brief runner for coro_t<T> with result stored by reference
   */
  template<typename T, typename Scope>
  static coro_t<void> scoped_runner_with_result(coro_t<T> task, T& out, Scope& s) {
    try {
      out = co_await task;
    } catch (const std::exception& e) {
      s.record_failure(e.what());
    } catch (...) {
      s.record_failure(nullptr);
    }
    s.on_child_done();
  }

  /**
   * @brief runner for coro_t<T> with result stored as expected<T>
   */
  template<typename T, typename Scope>
  static coro_t<void> scoped_runner_with_expected(coro_t<T> task, expected<T>& out, Scope& s) {
    try {
      out = co_await task;
    } catch (const std::exception& e) {
      SPDLOG_ERROR("task_scope: child task threw exception: {}", e.what());
      out = unexpected(EFAULT, error_domain::Exception);
      s.record_failure(nullptr);
    } catch (...) {
      SPDLOG_ERROR("task_scope: child task threw unknown exception");
      out = unexpected(EFAULT, error_domain::Exception);
      s.record_failure(nullptr);
    }
    s.on_child_done();
  }

  /**
   * @brief runner for callable (lambda) that returns a coroutine.
   * The callable is moved into the coroutine frame as a parameter,
   * ensuring it lives for the entire duration of the coroutine.
   * This avoids the "temporary lambda coroutine" dangling pointer issue.
   */
  template<typename F, typename Scope>
  static coro_t<void> scoped_runner_fn(F f, Scope& s) {
    try {
      co_await f();
    } catch (const std::exception& e) {
      s.record_failure(e.what());
    } catch (...) {
      s.record_failure(nullptr);
    }
    s.on_child_done();
  }

  void on_child_done() {
    active_count_--;
    if (active_count_ == 0 && waiter_) {
      ctx_.spawn(waiter_);
      waiter_ = nullptr;
    }
  }

  context_t& ctx_;
  std::unique_ptr<canceler_t> canceler_;
  int active_count_{0};
  std::coroutine_handle<> waiter_{nullptr};
  // nullopt until a child or the body fails; expected<void> cannot express
  // "no error recorded yet" (its default state is success), which used to
  // disable every catch block above.
  std::optional<error_t> first_error_;
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
    return scope_.as_expected();
  }
};

namespace detail {

/**
 * @brief shared body of both task_scope overloads: run body under the scope,
 * then join, then resume whoever awaited us. The join runs even when the body
 * throws, and the continuation is spawned unconditionally — the detached
 * runner used to swallow a body exception into its promise and never resume
 * the awaiting coroutine.
 *
 * The scope is shared, not unique: if the awaiting coroutine is destroyed
 * while the runner still stands between body and continuation, the runner's
 * reference must not dangle on a freed unique_ptr.
 */
template<typename F>
coro_t<void> scope_runner(std::shared_ptr<scope_t> scope, F body,
                          std::coroutine_handle<> continuation) {
  try {
    co_await body(*scope);
  } catch (const std::exception& e) {
    scope->record_failure(e.what());
  } catch (...) {
    scope->record_failure(nullptr);
  }
  co_await scope_join_awaiter{*scope};
  scope->ctx_.spawn(continuation);
}

} // namespace detail

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
auto task_scope(context_t& ctx, F&& body) {
  struct awaiter {
    context_t& ctx_;
    std::decay_t<F> body_;
    std::shared_ptr<scope_t> scope_;

    bool await_ready() { return false; }

    void await_suspend(std::coroutine_handle<> h) {
      scope_ = std::make_shared<scope_t>(ctx_);
      ctx_.spawn(detail::scope_runner(scope_, std::move(body_), h));
    }

    expected<void> await_resume() {
      return scope_->as_expected();
    }
  };

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
auto task_scope(context_t& ctx, canceler_t& parent, F&& body) {
  struct awaiter {
    context_t& ctx_;
    canceler_t& parent_;
    std::decay_t<F> body_;
    std::shared_ptr<scope_t> scope_;

    bool await_ready() { return false; }

    void await_suspend(std::coroutine_handle<> h) {
      scope_ = std::make_shared<scope_t>(ctx_, parent_);
      ctx_.spawn(detail::scope_runner(scope_, std::move(body_), h));
    }

    expected<void> await_resume() {
      return scope_->as_expected();
    }
  };

  return awaiter{ctx, parent, std::forward<F>(body), nullptr};
}

} // namespace cornet

#endif // CORNET_SCOPE_H
