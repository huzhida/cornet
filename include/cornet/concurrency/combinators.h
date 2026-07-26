#ifndef CORNET_COMBINATORS_H
#define CORNET_COMBINATORS_H

#include "cornet/coroutine/coro.h"
#include "cornet/coroutine/cancel.h"
#include "cornet/scheduling/context.h"
#include "cornet/concurrency/scope.h"

#include <tuple>
#include <memory>
#include <optional>

namespace cornet {

/**
 * @brief sleep awaiter. Suspends the coroutine for the given duration using io_uring timeout.
 * Usage: co_await sleep(std::chrono::seconds(1));
 */
struct sleep_awaiter : utask_t {
  __kernel_timespec ts_;

  template<typename Rep, typename Period>
  explicit sleep_awaiter(context_t& ctx, std::chrono::duration<Rep, Period> duration) {
    this->ctx = &ctx;
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
 * @param ctx context for io_uring operations
 * @param duration how long to sleep
 * @return awaitable that completes after the duration
 */
template<typename Rep, typename Period>
sleep_awaiter sleep(context_t& ctx, std::chrono::duration<Rep, Period> duration) {
  return sleep_awaiter{ctx, duration};
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
  using R = decltype(std::declval<Awaitable&>().await_resume());
  Awaitable op_;
  context_t* ctx_;
  __kernel_timespec ts_;
  bool timeout_armed_{false};

  timeout_awaiter(context_t& ctx, Awaitable op, std::chrono::duration<Rep, Period> duration)
    : op_(std::move(op)), ctx_(&ctx), ts_(to_kernel_timespec(duration)) {
    static_assert(std::is_constructible_v<R, unexpected>,
                  "with_timeout requires await_resume() to return an expected-like type constructible from unexpected");
  }

  bool await_ready() { return op_.await_ready(); }

  void await_suspend(std::coroutine_handle<> h) {
    timeout_armed_ = true;
    op_.handle = h;
    auto& uring = ctx_->io_uring();

    io_uring_sqe* sqes[2];
    uring.get_sqes(sqes, 2);

    op_.prepare_into(sqes[0]);
    io_uring_sqe_set_flags(sqes[0], sqes[0]->flags | IOSQE_IO_LINK);

    io_uring_prep_link_timeout(sqes[1], &ts_, 0);
    io_uring_sqe_set_data(sqes[1], nullptr);
  }

  R await_resume() {
    if (timeout_armed_ && op_.io_result() == -ECANCELED) {
      return unexpected(ETIMEDOUT);
    }
    return op_.await_resume();
  }
};

/**
 * @brief create a timeout-wrapped awaitable
 * @param ctx context for io_uring operations
 * @param op the io operation awaiter (recv_awaiter, send_awaiter, etc.)
 * @param duration timeout duration
 * @return awaitable preserving op.await_resume()'s return type; returns ETIMEDOUT on timeout
 */
template<typename Awaitable, typename Rep, typename Period>
requires std::derived_from<Awaitable, utask_t>
timeout_awaiter<Awaitable, Rep, Period> with_timeout(context_t& ctx, Awaitable op, std::chrono::duration<Rep, Period> duration) {
  return {ctx, std::move(op), duration};
}

/**
 * @brief coroutine-level with_cancel. Injects a canceler into cancelable_coro_t's promise
 * so all internal utask_t operations are automatically cancellable.
 */
template<typename V>
struct coro_cancellable_awaiter {
  cancelable_coro_t<V> coro_;
  canceler_t& canceler_;
  context_t& ctx_;

  coro_cancellable_awaiter(context_t& ctx, cancelable_coro_t<V> coro, canceler_t& canceler)
    : coro_(std::move(coro)), canceler_(canceler), ctx_(ctx) {}

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
coro_cancellable_awaiter<V> with_cancel(context_t& ctx, cancelable_coro_t<V> coro, canceler_t& canceler) {
  return {ctx, std::move(coro), canceler};
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

  template<typename V>
  struct timeout_state {
    struct canceler_deleter {
      void operator()(canceler_t* p) const {
        if (p) {
          p->cancel();  // cancel any remaining IO (timer) before destroying
          delete p;
        }
      }
    };
    std::unique_ptr<canceler_t, canceler_deleter> canceler;
    bool done{false};
    bool timed_out{false};
    std::coroutine_handle<> continuation{nullptr};
    std::optional<V> result;
    std::exception_ptr exception;
  };

  template<>
  struct timeout_state<void> {
    struct canceler_deleter {
      void operator()(canceler_t* p) const {
        if (p) {
          p->cancel();  // cancel any remaining IO (timer) before destroying
          delete p;
        }
      }
    };
    std::unique_ptr<canceler_t, canceler_deleter> canceler;
    bool done{false};
    bool timed_out{false};
    std::coroutine_handle<> continuation{nullptr};
    std::exception_ptr exception;
  };

  template<typename V>
  coro_t<void> timeout_target_task(context_t& ctx, std::shared_ptr<timeout_state<V>> state, cancelable_coro_t<V> coro) {
    try {
      coro.native_handle().promise().canceler_ = state->canceler.get();
      if constexpr (std::is_void_v<V>) {
        co_await coro;
        if (!state->done) {
          state->done = true;
          if (state->canceler) state->canceler->cancel();
          if (state->continuation) ctx.spawn(state->continuation);
        }
      } else {
        auto result = co_await coro;
        if (!state->done) {
          state->done = true;
          state->result.emplace(std::move(result));
          if (state->canceler) state->canceler->cancel();
          if (state->continuation) ctx.spawn(state->continuation);
        }
      }
    } catch (...) {
      if (!state->done) {
        state->done = true;
        state->exception = std::current_exception();
        if (state->canceler) state->canceler->cancel();
        if (state->continuation) ctx.spawn(state->continuation);
      }
    }
  }

  template<typename V, typename Rep, typename Period>
  coro_t<void> timeout_timer_task(context_t& ctx, std::shared_ptr<timeout_state<V>> state,
                                  std::chrono::duration<Rep, Period> duration) {
    auto ret = co_await with_cancel(ctx, sleep(ctx, duration), *state->canceler);
    if (ret && !state->done) {
      state->done = true;
      state->timed_out = true;
      if (state->canceler) state->canceler->cancel();
      if (state->continuation) ctx.spawn(state->continuation);
    }
  }
}

template<typename V>
struct coro_timeout_awaiter {
  std::shared_ptr<detail::timeout_state<V>> state_;
  context_t& ctx_;

  bool await_ready() const { return state_->done; }

  void await_suspend(std::coroutine_handle<> h) {
    state_->continuation = h;
    if (state_->done) {
      ctx_.spawn(h);
    }
  }

  V await_resume() {
    if constexpr (std::is_void_v<V>) {
      if (state_->exception) std::rethrow_exception(state_->exception);
      return;
    } else {
      if (state_->exception) std::rethrow_exception(state_->exception);
      if (state_->timed_out) {
        return unexpected(ETIMEDOUT);
      }
      return std::move(*state_->result);
    }
  }
};

template<typename V, typename Rep, typename Period>
coro_timeout_awaiter<V> with_timeout(context_t& ctx, cancelable_coro_t<V> coro, std::chrono::duration<Rep, Period> duration) {
  static_assert(std::is_void_v<V> || detail::is_expected<V>::value,
                "coroutine-level with_timeout requires ccoro_t<expected<T>> or ccoro_t<void>");

  auto state = std::make_shared<detail::timeout_state<V>>();
  using deleter_t = typename detail::timeout_state<V>::canceler_deleter;
  state->canceler = std::unique_ptr<canceler_t, deleter_t>(
      new canceler_t(ctx), deleter_t{});
  ctx.spawn(detail::timeout_target_task(ctx, state, std::move(coro)));
  ctx.spawn(detail::timeout_timer_task(ctx, state, duration));
  return {std::move(state), ctx};
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
coro_t<void> when_all_task(context_t& ctx, std::shared_ptr<State> state, coro_t<T> coro) {
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
coro_t<void> when_any_task(context_t& ctx, std::shared_ptr<State> state, coro_t<T> coro) {
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
void launch_all_impl(context_t& ctx, std::shared_ptr<State> state, Tuple& coros, std::index_sequence<Is...>) {
  (ctx.spawn(when_all_task<Is>(ctx, state, std::move(std::get<Is>(coros)))), ...);
}

template<typename State, typename Tuple, size_t... Is>
void launch_any_impl(context_t& ctx, std::shared_ptr<State> state, Tuple& coros, std::index_sequence<Is...>) {
  (ctx.spawn(when_any_task<Is>(ctx, state, std::move(std::get<Is>(coros)))), ...);
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
auto when_all(context_t& ctx, coro_t<Ts>... coros) {
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
      detail::launch_all_impl(ctx_, state_, coros_, std::index_sequence_for<Ts...>{});
    }

    when_all_result<Ts...> await_resume() {
      return {std::move(state_->results)};
    }
  };

  return awaiter{ctx, std::make_shared<detail::when_all_state<Ts...>>(), std::tuple{std::move(coros)...}};
}

/**
 * @brief await any coroutine concurrently, resume when the first one completes.
 * Remaining coroutines continue running but their results are discarded.
 * Usage: auto result = co_await when_any(coro1(), coro2());
 * @return when_any_result containing all results (only the completed one is valid)
 */
template<typename... Ts>
auto when_any(context_t& ctx, coro_t<Ts>... coros) {
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
      detail::launch_any_impl(ctx_, state_, coros_, std::index_sequence_for<Ts...>{});
    }

    when_any_result<Ts...> await_resume() {
      return {std::move(state_->results), state_->completed_index};
    }
  };

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
auto when_any(context_t& ctx, canceler_t& canceler, coro_t<Ts>... coros) {
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
      detail::launch_any_impl(ctx_, state_, coros_, std::index_sequence_for<Ts...>{});
    }

    when_any_result<Ts...> await_resume() {
      return {std::move(state_->results), state_->completed_index};
    }
  };

  auto state = std::make_shared<detail::when_any_state<Ts...>>(&canceler);
  return awaiter{ctx, std::move(state), std::tuple{std::move(coros)...}};
}

} // namespace cornet

#endif //CORNET_COMBINATORS_H
