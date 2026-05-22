#ifndef CORNET_COMBINATORS_H
#define CORNET_COMBINATORS_H

#include "context.h"
#include <tuple>
#include <atomic>
#include <memory>

namespace cornet {

/**
 * @brief sleep awaiter. Suspends the coroutine for the given duration using io_uring timeout.
 * Usage: co_await sleep(ctx, std::chrono::seconds(1));
 */
struct sleep_awaiter : utask_t {
  __kernel_timespec ts_;

  template<typename Rep, typename Period>
  sleep_awaiter(context_t& ctx, std::chrono::duration<Rep, Period> duration) {
    this->ctx = &ctx;
    ts_ = to_kernel_timespec(duration);
    this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
      auto* t = static_cast<sleep_awaiter*>(self);
      io_uring_prep_timeout(sqe, &t->ts_, 0, 0);
    };
  }

  CORNET_NODISCARD CORNET_MAYBE_UNUSED expected<int> await_resume() const {
    if (value == -ETIME) {
      return 0;
    }
    if (value < 0) {
      return unexpected(-value);
    }
    return 0;
  }
};

/**
 * @brief create a sleep awaiter
 * @param ctx owner context
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
 * Usage: auto result = co_await with_timeout(sock.recv(ctx, buf, n), ctx, 5s);
 */
template<typename Awaitable, typename Rep, typename Period>
struct timeout_awaiter {
  Awaitable op_;
  context_t* ctx_;
  __kernel_timespec ts_;
  uint64_t timeout_slot_{0};

  timeout_awaiter(Awaitable op, context_t& ctx, std::chrono::duration<Rep, Period> duration)
    : op_(std::move(op)), ctx_(&ctx), ts_(to_kernel_timespec(duration)) {}

  bool await_ready() { return op_.await_ready(); }

  void await_suspend(std::coroutine_handle<> h) {
    op_.handle = h;
    auto& uring = ctx_->io_uring();
    auto& slots = ctx_->io_slots();

    auto sqe1 = uring.get_sqe();
    op_.slot_data = slots.alloc(&op_);
    op_.prepare_fn(&op_, sqe1);
    io_uring_sqe_set_data(sqe1, reinterpret_cast<void*>(op_.slot_data));
    io_uring_sqe_set_flags(sqe1, sqe1->flags | IOSQE_IO_LINK);

    auto sqe2 = uring.get_sqe();
    io_uring_prep_link_timeout(sqe2, &ts_, 0);
    io_uring_sqe_set_data(sqe2, nullptr);
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
 * @param ctx owner context
 * @param duration timeout duration
 * @return awaitable that returns ETIMEDOUT on timeout
 */
template<typename Awaitable, typename Rep, typename Period>
timeout_awaiter<Awaitable, Rep, Period> with_timeout(Awaitable op, context_t& ctx, std::chrono::duration<Rep, Period> duration) {
  return {std::move(op), ctx, duration};
}

namespace detail {

/**
 * @brief shared state for when_all/when_any with N coroutines
 */
template<typename... Ts>
struct when_all_state {
  std::tuple<expected<Ts>...> results;
  std::atomic<int> remaining;
  std::coroutine_handle<> continuation{nullptr};

  when_all_state() : remaining(sizeof...(Ts)) {}
};

template<typename... Ts>
struct when_any_state {
  std::tuple<expected<Ts>...> results;
  std::atomic<bool> done{false};
  std::atomic<int> completed_index{-1};
  std::coroutine_handle<> continuation{nullptr};

  when_any_state() = default;
};

template<size_t I, typename State, typename T>
coro_t<void> when_all_task(std::shared_ptr<State> state, coro_t<T> coro, context_t& ctx) {
  if constexpr (std::is_void_v<T>) {
    co_await coro;
    std::get<I>(state->results) = expected<void>{};
  } else {
    auto result = co_await coro;
    std::get<I>(state->results) = std::move(result);
  }
  if (state->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    if (state->continuation) {
      ctx.sched(state->continuation);
    }
  }
  co_return;
}

template<size_t I, typename State, typename T>
coro_t<void> when_any_task(std::shared_ptr<State> state, coro_t<T> coro, context_t& ctx) {
  if constexpr (std::is_void_v<T>) {
    co_await coro;
    std::get<I>(state->results) = expected<void>{};
  } else {
    auto result = co_await coro;
    std::get<I>(state->results) = std::move(result);
  }
  bool expected_val = false;
  if (state->done.compare_exchange_strong(expected_val, true, std::memory_order_acq_rel)) {
    state->completed_index.store(I, std::memory_order_release);
    if (state->continuation) {
      ctx.sched(state->continuation);
    }
  }
  co_return;
}

template<typename State, typename Tuple, size_t... Is>
void launch_all_impl(std::shared_ptr<State>& state, Tuple& coros, context_t& ctx, std::index_sequence<Is...>) {
  (ctx.sched(when_all_task<Is>(state, std::move(std::get<Is>(coros)), ctx)), ...);
}

template<typename State, typename Tuple, size_t... Is>
void launch_all(std::shared_ptr<State>& state, Tuple& coros, context_t& ctx, std::index_sequence<Is...>) {
  launch_all_impl(state, coros, ctx, std::index_sequence<Is...>{});
}

template<typename State, typename Tuple, size_t... Is>
void launch_any_impl(std::shared_ptr<State>& state, Tuple& coros, context_t& ctx, std::index_sequence<Is...>) {
  (ctx.sched(when_any_task<Is>(state, std::move(std::get<Is>(coros)), ctx)), ...);
}

template<typename State, typename Tuple, size_t... Is>
void launch_any(std::shared_ptr<State>& state, Tuple& coros, context_t& ctx, std::index_sequence<Is...>) {
  launch_any_impl(state, coros, ctx, std::index_sequence<Is...>{});
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
 * Usage: auto result = co_await when_all(ctx, coro1(), coro2());
 * @return when_all_result containing all results
 */
template<typename... Ts>
auto when_all(context_t& ctx, coro_t<Ts>... coros) {
  struct awaiter {
    context_t& ctx_;
    std::shared_ptr<detail::when_all_state<Ts...>> state_;
    std::tuple<coro_t<Ts>...> coros_;

    bool await_ready() { return false; }

    void await_suspend(std::coroutine_handle<> h) {
      state_->continuation = h;
      launch_all(state_, coros_, ctx_, std::index_sequence_for<Ts...>{});
    }

    when_all_result<Ts...> await_resume() {
      return {std::move(state_->results)};
    }
  };

  return awaiter{ctx, std::make_shared<detail::when_all_state<Ts...>>(), std::tuple{std::move(coros)...}};
}

/**
 * @brief await any coroutine concurrently, resume when the first one completes.
 * Usage: auto result = co_await when_any(ctx, coro1(), coro2());
 * @return when_any_result containing all results (only the completed one is valid)
 */
template<typename... Ts>
auto when_any(context_t& ctx, coro_t<Ts>... coros) {
  struct awaiter {
    context_t& ctx_;
    std::shared_ptr<detail::when_any_state<Ts...>> state_;
    std::tuple<coro_t<Ts>...> coros_;

    bool await_ready() { return false; }

    void await_suspend(std::coroutine_handle<> h) {
      state_->continuation = h;
      launch_any(state_, coros_, ctx_, std::index_sequence_for<Ts...>{});
    }

    when_any_result<Ts...> await_resume() {
      return {std::move(state_->results), state_->completed_index.load(std::memory_order_acquire)};
    }
  };

  return awaiter{ctx, std::make_shared<detail::when_any_state<Ts...>>(), std::tuple{std::move(coros)...}};
}

} // namespace cornet

#endif //CORNET_COMBINATORS_H
