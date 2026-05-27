#ifndef CORNET_COMBINATORS_H
#define CORNET_COMBINATORS_H

#include "context.h"
#include <tuple>

namespace cornet {

// forward declaration
template<typename Awaitable>
struct cancellable_awaiter;

/**
 * @brief canceler. Supports single-task cancellation and hierarchical propagation.
 * Single-threaded, no atomic operations needed.
 *
 * Usage:
 *   canceler_t canceler;
 *   ctx.spawn(handle_client(sock, canceler));
 *   canceler.cancel();  // cancel the client coroutine
 *
 * Hierarchical:
 *   canceler_t parent;
 *   canceler_t child(parent);
 *   parent.cancel();  // propagates to child
 */
struct canceler_t {
  canceler_t() : ctx_(&context_t::current()) {}

  explicit canceler_t(canceler_t& parent)
    : ctx_(parent.ctx_), parent_(&parent) {
    next_sibling_ = parent.first_child_;
    parent.first_child_ = this;
  }

  ~canceler_t() {
    if (parent_) {
      // unlink from parent's children list
      auto** pp = &parent_->first_child_;
      while (*pp && *pp != this) {
        pp = &(*pp)->next_sibling_;
      }
      if (*pp) {
        *pp = next_sibling_;
      }
    }
  }

  canceler_t(const canceler_t&) = delete;
  canceler_t& operator=(const canceler_t&) = delete;

  /**
   * @brief cancel this canceler and all children.
   * If an IO operation is currently inflight, issues io_uring cancel for it.
   */
  void cancel() {
    if (cancelled_) return;
    cancelled_ = true;

    if (active_task_ && active_task_->slot_data != 0) {
      auto* sqe = ctx_->io_uring().get_sqe();
      io_uring_prep_cancel(sqe, reinterpret_cast<void*>(active_task_->slot_data), 0);
      io_uring_sqe_set_data(sqe, nullptr);
    }

    for (auto* child = first_child_; child; child = child->next_sibling_) {
      child->cancel();
    }
  }

  /**
   * @brief check if this canceler has been cancelled
   */
  CORNET_NODISCARD bool is_cancelled() const { return cancelled_; }

  /**
   * @brief reset canceler to reusable state
   */
  void reset() {
    cancelled_ = false;
    active_task_ = nullptr;
  }

private:
  bool cancelled_{false};
  utask_t* active_task_{nullptr};
  context_t* ctx_{nullptr};
  canceler_t* parent_{nullptr};
  canceler_t* first_child_{nullptr};
  canceler_t* next_sibling_{nullptr};

  template<typename Awaitable>
  friend struct cancellable_awaiter;
};

/**
 * @brief wraps a utask_t-based awaitable with cancellation support.
 * When the associated canceler is cancelled, the inflight io_uring operation
 * is automatically cancelled and the coroutine resumes with ECANCELED.
 *
 * Usage: auto n = co_await with_cancel(sock.recv(buf, 4096), canceler);
 */
template<typename Awaitable>
struct cancellable_awaiter {
  Awaitable op_;
  canceler_t& canceler_;
  bool submitted_{false};

  cancellable_awaiter(Awaitable op, canceler_t& canceler)
    : op_(std::move(op)), canceler_(canceler) {}

  bool await_ready() {
    if (canceler_.is_cancelled()) return true;
    return op_.await_ready();
  }

  bool await_suspend(std::coroutine_handle<> h) {
    if (canceler_.is_cancelled()) return false;
    canceler_.active_task_ = &op_;
    op_.await_suspend(h);
    submitted_ = true;
    return true;
  }

  auto await_resume() -> decltype(op_.await_resume()) {
    canceler_.active_task_ = nullptr;
    if (!submitted_) {
      return unexpected(ECANCELED);
    }
    return op_.await_resume();
  }
};

/**
 * @brief wrap any utask_t-derived awaiter with cancellation support
 * @param op the io operation awaiter
 * @param canceler the canceler to associate with
 * @return cancellable awaiter
 */
template<typename Awaitable>
cancellable_awaiter<Awaitable> with_cancel(Awaitable op, canceler_t& canceler) {
  return {std::move(op), canceler};
}

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
timeout_awaiter<Awaitable, Rep, Period> with_timeout(Awaitable op, std::chrono::duration<Rep, Period> duration) {
  return {std::move(op), duration};
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

  when_any_state() = default;
};

template<size_t I, typename State, typename T>
coro_t<void> when_all_task(State* state, coro_t<T> coro, context_t& ctx) {
  try {
    if constexpr (std::is_void_v<T>) {
      co_await coro;
      std::get<I>(state->results) = expected<void>{};
    } else {
      auto result = co_await coro;
      std::get<I>(state->results) = std::move(result);
    }
  } catch (...) {
    std::get<I>(state->results) = unexpected(ECANCELED, error_domain::internal);
  }
  if (--state->remaining == 0) {
    if (state->continuation) {
      ctx.spawn(state->continuation);
    }
  }
  co_return;
}

template<size_t I, typename State, typename T>
coro_t<void> when_any_task(State* state, coro_t<T> coro, context_t& ctx) {
  try {
    if constexpr (std::is_void_v<T>) {
      co_await coro;
      std::get<I>(state->results) = expected<void>{};
    } else {
      auto result = co_await coro;
      std::get<I>(state->results) = std::move(result);
    }
  } catch (...) {
    std::get<I>(state->results) = unexpected(ECANCELED, error_domain::internal);
  }
  if (!state->done) {
    state->done = true;
    state->completed_index = I;
    if (state->continuation) {
      ctx.spawn(state->continuation);
    }
  }
  co_return;
}

template<typename State, typename Tuple, size_t... Is>
void launch_all_impl(State* state, Tuple& coros, context_t& ctx, std::index_sequence<Is...>) {
  (ctx.spawn(when_all_task<Is>(state, std::move(std::get<Is>(coros)), ctx)), ...);
}

template<typename State, typename Tuple, size_t... Is>
void launch_any_impl(State* state, Tuple& coros, context_t& ctx, std::index_sequence<Is...>) {
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
    detail::when_all_state<Ts...> state_;
    std::tuple<coro_t<Ts>...> coros_;

    bool await_ready() { return false; }

    void await_suspend(std::coroutine_handle<> h) {
      state_.continuation = h;
      detail::launch_all_impl(&state_, coros_, ctx_, std::index_sequence_for<Ts...>{});
    }

    when_all_result<Ts...> await_resume() {
      return {std::move(state_.results)};
    }
  };

  auto& ctx = context_t::current();
  return awaiter{ctx, {}, std::tuple{std::move(coros)...}};
}

/**
 * @brief await any coroutine concurrently, resume when the first one completes.
 * Usage: auto result = co_await when_any(coro1(), coro2());
 * @return when_any_result containing all results (only the completed one is valid)
 */
template<typename... Ts>
auto when_any(coro_t<Ts>... coros) {
  struct awaiter {
    context_t& ctx_;
    detail::when_any_state<Ts...> state_;
    std::tuple<coro_t<Ts>...> coros_;

    bool await_ready() { return false; }

    void await_suspend(std::coroutine_handle<> h) {
      state_.continuation = h;
      detail::launch_any_impl(&state_, coros_, ctx_, std::index_sequence_for<Ts...>{});
    }

    when_any_result<Ts...> await_resume() {
      return {std::move(state_.results), state_.completed_index};
    }
  };

  auto& ctx = context_t::current();
  return awaiter{ctx, {}, std::tuple{std::move(coros)...}};
}

} // namespace cornet

#endif //CORNET_COMBINATORS_H
