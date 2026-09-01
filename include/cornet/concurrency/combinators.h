#ifndef CORNET_COMBINATORS_H
#define CORNET_COMBINATORS_H

#include "cornet/coroutine/coro.h"
#include "cornet/coroutine/cancel.h"
#include "cornet/scheduling/context.h"
#include "cornet/concurrency/deadline.h"
#include "cornet/concurrency/scope.h"
#include "cornet/concurrency/timer_wheel.h"

#include <tuple>
#include <memory>
#include <optional>
#include <utility>

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

  bool await_suspend(std::coroutine_handle<> h) {
    op_.handle = h;
    auto& uring = ctx_->io_uring();

    io_uring_sqe* sqes[2];
    if (!uring.get_sqes(sqes, 2)) {
      // The op and its link_timeout must land in the same batch, so a partial
      // acquire is not usable: fail fast rather than suspend on a wakeup that
      // will never arrive.
      op_.fail(ENOBUFS);
      return false;
    }
    timeout_armed_ = true;

    op_.prepare_into(sqes[0]);
    io_uring_sqe_set_flags(sqes[0], sqes[0]->flags | IOSQE_IO_LINK);

    io_uring_prep_link_timeout(sqes[1], &ts_, 0);
    io_uring_sqe_set_data(sqes[1], nullptr);
    return true;
  }

  R await_resume() {
    if (timeout_armed_ && op_.io_result() == -ECANCELED) {
      // ECANCELED on a linked op has two possible sources: our link_timeout
      // fired, or the op was reaped from outside (context shutdown runs a
      // CANCEL_ANY sweep). Reporting the latter as ETIMEDOUT would hide the
      // real reason from callers, logs and metrics: a graceful shutdown would
      // look like a burst of client timeouts, and callers could not tell "idle
      // peer" from "we are going down" — which is exactly the distinction a
      // server needs to decide whether to answer with Connection: close.
      if (ctx_ && !ctx_->is_running()) {
        return unexpected(ECANCELED);
      }
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
 * @brief Go-context-style WithDeadline over a caller-owned deadline_t: run
 * one op under the deadline's canceler, leave the deadline consistent on the
 * way out. Works for both with_cancel flavours (leaf utask ops and whole
 * ccoro_t's).
 *
 * The deadline owns every arm boundary — with a budget armed, ops simply
 * draw against the absolute end in sequence (an optional cap(d) before one
 * op tightens just that op's window):
 *
 *   deadline_t deadline{ctx, 50ms, 10s};
 *   auto c  = co_await with_deadline(ctx, tr.connect(ctx, addr), deadline);
 *   auto hs = co_await with_deadline(ctx, tr.start_tls(...), deadline);
 *
 * A fire keeps its latch: deadline.map(err) still reads ETIMEDOUT, and
 * later ops die instantly on the latched canceler — the expired-context
 * semantics, with reset() as the explicit way back. Lifecycle calls
 * (set_budget / cap / reset) go BETWEEN ops, never inside one.
 *
 * Multi-op windows (arm once, kill N awaits) and ops bound to CHILD scopes
 * stay manual: with_deadline only ever binds deadline.canceler() itself.
 */
template<typename Awaitable>
struct deadline_bound_awaiter {
  decltype(with_cancel(std::declval<context_t&>(), std::declval<Awaitable&&>(),
                       std::declval<canceler_t&>())) inner_;
  deadline_t* deadline_;

  bool await_ready() { return inner_.await_ready(); }

  auto await_suspend(std::coroutine_handle<> h) { return inner_.await_suspend(h); }

  decltype(auto) await_resume() {
    deadline_->restore_after_op();
    return inner_.await_resume();
  }
};

template<typename Awaitable>
deadline_bound_awaiter<Awaitable> with_deadline(context_t& ctx, Awaitable op,
                                                deadline_t& deadline) {
  return {with_cancel(ctx, std::move(op), deadline.canceler()), &deadline};
}

/**
 * @brief coroutine-level with_timeout. Races a cancelable coroutine against a
 * deadline on the context's shared timing wheel: if the deadline fires first,
 * the canceler injected into the coro's promise cancels its inflight IO.
 *
 * Division of labour among the deadline tools in cornet:
 * - op-level with_timeout (above): ONE leaf op, sub-tick precision, +1 SQE
 * - coroutine-level with_timeout (this): bound a whole user coroutine from
 *   outside; one heap canceler per call
 * - deadline_t (concurrency/deadline.h): a long-lived object's repeated
 *   protocol phases (connect/handshake/read/close); zero allocation, fires
 *   into the owner's own cancelers; with_deadline (above) is its one-op form.
 *   The http/ws modules standardize on it — new protocol code should not mix
 *   in the other two.
 *
 * Cost per call: one canceler_t allocation and one O(1) wheel arm. No wrapper
 * coroutines, no timer SQE, no cancel SQE on the success path — the target is
 * entered by symmetric transfer and its final suspend transfers straight back,
 * so neither side pays a scheduler round-trip. Compare the utask-level
 * with_timeout for sub-tick precision on a single op.
 */

namespace detail {
  /**
   * @brief shared machinery of the coroutine timeout awaiter (both void and
   * value flavours). Lives in the awaiting coroutine's frame.
   */
  template<typename V>
  struct coro_timeout_awaiter_base {
    context_t& ctx_;
    cancelable_coro_t<V> coro_;
    // Heap-allocated: unlike this awaiter, the canceler may still be referenced
    // by ops the target has in flight after we are gone — see the dtor's
    // orphan path and canceler_t::orphan().
    canceler_t* canceler_;
    timer_node_t node_{};
    std::chrono::nanoseconds timeout_;
    bool timed_out_{false};
    bool started_{false};

    template<typename Rep, typename Period>
    coro_timeout_awaiter_base(context_t& ctx, cancelable_coro_t<V> coro,
                              std::chrono::duration<Rep, Period> d)
      : ctx_(ctx), coro_(std::move(coro)), canceler_(new canceler_t(ctx)),
        timeout_(std::chrono::duration_cast<std::chrono::nanoseconds>(d)) {}

    coro_timeout_awaiter_base(const coro_timeout_awaiter_base&) = delete;
    coro_timeout_awaiter_base& operator=(const coro_timeout_awaiter_base&) = delete;
    coro_timeout_awaiter_base(coro_timeout_awaiter_base&&) = delete;
    coro_timeout_awaiter_base& operator=(coro_timeout_awaiter_base&&) = delete;

    ~coro_timeout_awaiter_base() {
      // A fired timer is already unlinked by the wheel; cancel() is a no-op on
      // an unarmed node, so this also covers "never armed".
      if (node_.armed()) ctx_.timeout_wheel().cancel(node_);
      if (!started_ || coro_.done()) {
        // Never co_awaited, or the target completed: nobody out there can
        // still hold a registrant on the canceler.
        delete canceler_;
        return;
      }
      // Orphan: our coroutine is being destroyed while the target still runs.
      // Stop the target's inflight IO, hand the target its own lifetime, and
      // let the canceler free itself once the last op resolves — the late
      // unlinks of those ops land on valid memory. The wheel node is already
      // disarmed above, so the expiry callback can never fire into freed
      // memory either.
      canceler_->cancel();
      coro_.detach();
      canceler_->orphan();
    }

    CORNET_MAYBE_UNUSED bool await_ready() const noexcept { return false; }

    CORNET_MAYBE_UNUSED std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) {
      started_ = true;
      auto child = coro_.native_handle();
      // All of the target's utask ops auto-register on our canceler
      // (await_transform), and its final suspend transfers back to our parent.
      child.promise().canceler_ = canceler_;
      child.promise().continuation = h;
      node_.owner = this;
      node_.on_expire = [](void* owner) {
        auto* self = static_cast<coro_timeout_awaiter_base*>(owner);
        self->timed_out_ = true;
        self->canceler_->cancel();
      };
      // Round up to whole milliseconds; arm() further ceils into ticks, so the
      // timer can only ever fire late, never early.
      using namespace std::chrono;
      const auto delay = duration_cast<milliseconds>(timeout_ + milliseconds(1) - nanoseconds(1));
      ctx_.timeout_wheel().arm(node_, delay);
      return child;
    }
  };
} // namespace detail

/**
 * @brief coroutine timeout awaiter for ccoro_t<expected<T>>.
 * Timeout is reported as unexpected(ETIMEDOUT); a target exception rethrows
 * and wins over the timeout race.
 */
template<typename V>
struct coro_timeout_awaiter : detail::coro_timeout_awaiter_base<V> {
  using detail::coro_timeout_awaiter_base<V>::coro_timeout_awaiter_base;

  V await_resume() {
    auto& prom = this->coro_.native_handle().promise();
    if (prom.value.index() == 2) {
      std::rethrow_exception(std::get<2>(std::move(prom.value)));
    }
    if (this->timed_out_) return unexpected(ETIMEDOUT);
    return this->coro_.value();
  }
};

/**
 * @brief coroutine timeout awaiter for ccoro_t<void>.
 * Yields expected<void> so a timeout is observable at all: previously a void
 * coroutine's deadline could fire without the caller seeing anything.
 */
template<>
struct coro_timeout_awaiter<void> : detail::coro_timeout_awaiter_base<void> {
  using detail::coro_timeout_awaiter_base<void>::coro_timeout_awaiter_base;

  expected<void> await_resume() {
    auto& prom = this->coro_.native_handle().promise();
    if (prom.value.index() == 1) {
      std::rethrow_exception(std::get<1>(std::move(prom.value)));
    }
    if (this->timed_out_) return unexpected(ETIMEDOUT);
    return {};
  }
};

template<typename V, typename Rep, typename Period>
coro_timeout_awaiter<V> with_timeout(context_t& ctx, cancelable_coro_t<V> coro, std::chrono::duration<Rep, Period> duration) {
  static_assert(std::is_void_v<V> || detail::is_expected_v<V>,
                "coroutine-level with_timeout requires ccoro_t<expected<T>> or ccoro_t<void>");
  return coro_timeout_awaiter<V>{ctx, std::move(coro), duration};
}

namespace detail {

/**
 * @brief yields the current coroutine's own handle without suspending.
 * The finishing when_* wrapper uses it to chain itself to the awaiting parent:
 * setting promise().continuation makes the framework's final_awaiter resume the
 * parent by symmetric transfer (constant stack) instead of a ready-queue hop.
 */
struct self_handle_awaiter {
  std::coroutine_handle<> self{nullptr};
  bool await_ready() const noexcept { return false; }
  bool await_suspend(std::coroutine_handle<> h) noexcept {
    self = h;
    return false;  // resume immediately
  }
  std::coroutine_handle<> await_resume() const noexcept { return self; }
};

/**
 * @brief chain this (detached, spawned) coroutine to `parent`: at final
 * suspend, control transfers straight into the parent.
 * Must be the last statement before the coroutine completes — anything
 * touching shared state after this point risks use-after-free, because the
 * resumed parent may delete that state before this coroutine's destroy runs.
 */
template<typename Promise>
void chain_to(std::coroutine_handle<> self, std::coroutine_handle<> parent) {
  std::coroutine_handle<Promise>::from_address(self.address()).promise().continuation = parent;
}

/**
 * @brief shared state for when_all/when_any with N coroutines.
 *
 * Heap-allocated exactly once per call and refcounted with a plain int: every
 * user (the awaiter and the N wrapper tasks) runs on the context's owner
 * thread, so shared_ptr's atomics would buy nothing. The awaiter holds one
 * ref and each wrapper one; whoever releases the last ref deletes the state.
 * The heap (rather than the awaiting frame) is what lets an abandoned awaiter
 * leave safely — wrappers may still write results after their parent is gone.
 */
template<typename... Ts>
struct when_all_state {
  std::tuple<result_slot_t<Ts>...> results;
  int remaining{int(sizeof...(Ts))};
  int refs{int(sizeof...(Ts)) + 1};
  std::coroutine_handle<> continuation{nullptr};
};

template<typename... Ts>
struct when_any_state {
  std::tuple<result_slot_t<Ts>...> results;
  bool done{false};
  int completed_index{-1};
  int refs{int(sizeof...(Ts)) + 1};
  std::coroutine_handle<> continuation{nullptr};
  canceler_t* canceler{nullptr};
};

template<size_t I, typename State, typename T>
coro_t<void> when_all_task(State* state, coro_t<T> coro) {
  try {
    if constexpr (std::is_void_v<T>) {
      co_await coro;
      std::get<I>(state->results) = expected<void>{};
    } else {
      std::get<I>(state->results) = co_await coro;
    }
  } catch (const std::exception& e) {
    SPDLOG_ERROR("when_all: task {} threw exception: {}", I, e.what());
    std::get<I>(state->results) = unexpected(EFAULT, error_domain::Exception);
  } catch (...) {
    SPDLOG_ERROR("when_all: task {} threw unknown exception", I);
    std::get<I>(state->results) = unexpected(EFAULT, error_domain::Exception);
  }
  std::coroutine_handle<> to_wake{nullptr};
  if (--state->remaining == 0) {
    to_wake = state->continuation;
    state->continuation = nullptr;
  }
  // Finishing protocol — must inline, not helper-into-a-coroutine: the chain
  // has to land on THIS coroutine's own promise for the detached final_suspend
  // to destroy us and transfer control straight into the parent. Unref before
  // chaining: the parent we are about to resume may drop the last ref itself.
  if (--state->refs == 0) delete state;
  if (to_wake) {
    auto self = co_await self_handle_awaiter{};
    chain_to<typename coro_t<void>::promise_type>(self, to_wake);
  }
  co_return;
}

template<size_t I, typename State, typename T>
coro_t<void> when_any_task(State* state, coro_t<T> coro) {
  try {
    if constexpr (std::is_void_v<T>) {
      co_await coro;
      if (!state->done) std::get<I>(state->results) = expected<void>{};
    } else {
      auto result = co_await coro;
      if (!state->done) std::get<I>(state->results) = std::move(result);
    }
  } catch (const std::exception& e) {
    if (!state->done) {
      SPDLOG_ERROR("when_any: task {} threw exception: {}", I, e.what());
      std::get<I>(state->results) = unexpected(EFAULT, error_domain::Exception);
    } else {
      // Losers' results are discarded by contract — including their exceptions.
      // Keep a trace: a silently-vanished losing task is a debugging black hole.
      SPDLOG_DEBUG("when_any: loser task {} threw (discarded): {}", I, e.what());
    }
  } catch (...) {
    if (!state->done) {
      SPDLOG_ERROR("when_any: task {} threw unknown exception", I);
      std::get<I>(state->results) = unexpected(EFAULT, error_domain::Exception);
    } else {
      SPDLOG_DEBUG("when_any: loser task {} threw unknown exception (discarded)", I);
    }
  }
  std::coroutine_handle<> to_wake{nullptr};
  if (!state->done) {
    state->done = true;
    state->completed_index = int(I);
    to_wake = state->continuation;
    state->continuation = nullptr;
    if (state->canceler) state->canceler->cancel();
  }
  // See when_all_task for why this protocol must inline here.
  if (--state->refs == 0) delete state;
  if (to_wake) {
    auto self = co_await self_handle_awaiter{};
    chain_to<typename coro_t<void>::promise_type>(self, to_wake);
  }
  co_return;
}

template<typename State, typename Tuple, size_t... Is>
void launch_all_impl(context_t& ctx, State* state, Tuple& coros, std::index_sequence<Is...>) {
  (ctx.spawn(when_all_task<Is>(state, std::move(std::get<Is>(coros)))), ...);
}

template<typename State, typename Tuple, size_t... Is>
void launch_any_impl(context_t& ctx, State* state, Tuple& coros, std::index_sequence<Is...>) {
  (ctx.spawn(when_any_task<Is>(state, std::move(std::get<Is>(coros)))), ...);
}

} // namespace detail

/**
 * @brief when_all result type
 * Each slot is expected<T>; a coroutine already returning expected<U>
 * flattens into a single expected<U> (see result_slot_t).
 */
template<typename... Ts>
struct when_all_result {
  std::tuple<detail::result_slot_t<Ts>...> results;

  template<size_t I>
  auto& get() { return std::get<I>(results); }

  template<size_t I>
  const auto& get() const { return std::get<I>(results); }
};

/**
 * @brief when_any result type
 * Each slot is expected<T>; a coroutine already returning expected<U>
 * flattens into a single expected<U> (see result_slot_t).
 */
template<typename... Ts>
struct when_any_result {
  std::tuple<detail::result_slot_t<Ts>...> results;
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
    awaiter(context_t& ctx, detail::when_all_state<Ts...>* s, std::tuple<coro_t<Ts>...> c)
      : ctx_(ctx), state_(s), coros_(std::move(c)) {}
    awaiter(const awaiter&) = delete;
    awaiter& operator=(const awaiter&) = delete;

    ~awaiter() {
      // If we go away before the children finish, they must not wake a dead
      // frame; they keep the state itself alive through their own refs.
      if (!state_) return;
      state_->continuation = nullptr;
      if (--state_->refs == 0) delete state_;
    }

    // Empty pack: nothing to wait for, resume inline without ever suspending.
    bool await_ready() const noexcept { return sizeof...(Ts) == 0; }

    void await_suspend(std::coroutine_handle<> h) {
      state_->continuation = h;
      detail::launch_all_impl(ctx_, state_, coros_, std::index_sequence_for<Ts...>{});
    }

    when_all_result<Ts...> await_resume() {
      return {std::move(state_->results)};
    }

    context_t& ctx_;
    detail::when_all_state<Ts...>* state_;
    std::tuple<coro_t<Ts>...> coros_;
  };

  return awaiter{ctx, new detail::when_all_state<Ts...>(), std::tuple{std::move(coros)...}};
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
    awaiter(context_t& ctx, detail::when_any_state<Ts...>* s, std::tuple<coro_t<Ts>...> c)
      : ctx_(ctx), state_(s), coros_(std::move(c)) {}
    awaiter(const awaiter&) = delete;
    awaiter& operator=(const awaiter&) = delete;

    ~awaiter() {
      if (!state_) return;
      state_->continuation = nullptr;
      if (--state_->refs == 0) delete state_;
    }

    bool await_ready() const noexcept { return sizeof...(Ts) == 0; }

    void await_suspend(std::coroutine_handle<> h) {
      state_->continuation = h;
      detail::launch_any_impl(ctx_, state_, coros_, std::index_sequence_for<Ts...>{});
    }

    when_any_result<Ts...> await_resume() {
      return {std::move(state_->results), state_->completed_index};
    }

    context_t& ctx_;
    detail::when_any_state<Ts...>* state_;
    std::tuple<coro_t<Ts>...> coros_;
  };

  return awaiter{ctx, new detail::when_any_state<Ts...>(), std::tuple{std::move(coros)...}};
}

/**
 * @brief await any coroutine concurrently with cancellation support.
 * When the first coroutine completes, the provided canceler is triggered.
 * To cancel inflight IO operations, pass the same canceler to with_cancel(op, canceler) as well.
 * Usage:
 *   canceler_t canceler;
 *   auto result = co_await when_any(canceler, task_with_cancel(canceler), task_with_cancel(canceler));
 * @param canceler canceler to trigger on first completion
 * @return when_any_result containing all results (only the completed one is valid)
 */
template<typename... Ts>
auto when_any(context_t& ctx, canceler_t& canceler, coro_t<Ts>... coros) {
  struct awaiter {
    awaiter(context_t& ctx, detail::when_any_state<Ts...>* s, std::tuple<coro_t<Ts>...> c)
      : ctx_(ctx), state_(s), coros_(std::move(c)) {}
    awaiter(const awaiter&) = delete;
    awaiter& operator=(const awaiter&) = delete;

    ~awaiter() {
      if (!state_) return;
      state_->continuation = nullptr;
      if (--state_->refs == 0) delete state_;
    }

    bool await_ready() const noexcept { return sizeof...(Ts) == 0; }

    void await_suspend(std::coroutine_handle<> h) {
      state_->continuation = h;
      detail::launch_any_impl(ctx_, state_, coros_, std::index_sequence_for<Ts...>{});
    }

    when_any_result<Ts...> await_resume() {
      return {std::move(state_->results), state_->completed_index};
    }

    context_t& ctx_;
    detail::when_any_state<Ts...>* state_;
    std::tuple<coro_t<Ts>...> coros_;
  };

  auto* state = new detail::when_any_state<Ts...>();
  state->canceler = &canceler;
  return awaiter{ctx, state, std::tuple{std::move(coros)...}};
}

} // namespace cornet

#endif //CORNET_COMBINATORS_H
