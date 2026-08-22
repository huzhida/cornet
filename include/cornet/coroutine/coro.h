#ifndef CORNET_CORO_H
#define CORNET_CORO_H

#include "cornet/base/task.h"
#include "cornet/coroutine/cancel.h"
#include "cornet/coroutine/frame_pool.h"

#include <concepts>
#include <coroutine>
#include <exception>
#include <stdexcept>
#include <iterator>

#include <type_traits>
#include <utility>
#include <variant>

namespace cornet {

/**
 * @brief base promise_type for value storage and exception handling
 * @tparam V return value type
 */
template <typename V>
struct base_promise_t : detail::frame_allocator_mixin_t {
  // variant return value: monostate (initial) | V (result) | exception_ptr (error)
  std::variant<std::monostate, V, std::exception_ptr> value;

  /**
   * @brief promise_type.return_value constraint
   * @param v return value
   */
  void return_value(V v) {
    value.template emplace<1>(std::move(v));
  }

  /**
   * @brief promise_type.unhandled_exception constraint
   */
  void unhandled_exception() {
    value.template emplace<2>(std::current_exception());
  }
};

template <>
struct base_promise_t<void> : detail::frame_allocator_mixin_t {
  // variant: monostate (initial) | exception_ptr (error)
  std::variant<std::monostate, std::exception_ptr> value;

  /**
   * @brief promise_type.return_void constraint
   */
  void return_void() {}

  /**
   * @brief promise_type.unhandled_exception constraint
   */
  void unhandled_exception() {
    value.template emplace<1>(std::current_exception());
  }
};

/**
 * @brief common final_awaiter for coroutine types.
 * Resumes the continuation (parent coroutine) or self-destructs if detached.
 * @tparam Promise the promise type of the coroutine
 */
template<typename Promise>
struct coro_final_awaiter {
  /**
   * @brief await.await_ready constraint
   * @return always false, need suspend to run final logic
   */
  CORNET_MAYBE_UNUSED bool await_ready() noexcept { return false; }

  /**
   * @brief on final suspend, resume parent or self-destruct
   * @param h current coroutine handle
   * @return the coroutine to resume next
   */
  CORNET_MAYBE_UNUSED std::coroutine_handle<> await_suspend(std::coroutine_handle<Promise> h) noexcept {
    std::coroutine_handle<> cont = h.promise().continuation;
    if (h.promise().detached) h.destroy();
    return cont ? cont : std::noop_coroutine();
  }

  CORNET_MAYBE_UNUSED void await_resume() noexcept {}
};

// forward declarations
template <typename V> struct coro_t;
template <typename V> struct cancelable_coro_t;

/**
 * @brief promise_type traits — maps Derived coroutine type to its promise_type.
 * Defined externally to break the circular dependency between CRTP base and derived.
 * @tparam Derived the concrete coroutine type (coro_t<V> or cancelable_coro_t<V>)
 */
template<typename Derived> struct coro_promise;

/**
 * @brief promise_type for coro_t — zero-overhead, no cancellation support
 */
template<typename V>
struct coro_promise<coro_t<V>> {
  struct type : base_promise_t<V> {
    // whether this coroutine is detached (self-destructs at final_suspend)
    bool detached{false};
    // parent coroutine to resume when this one completes
    std::coroutine_handle<> continuation;

    /**
     * @return coroutine implementation coro_t
     */
    coro_t<V> get_return_object() {
      return coro_t<V>{std::coroutine_handle<type>::from_promise(*this)};
    }

    /**
     * @brief whether need suspend on initial
     * @return awaitable
     */
    std::suspend_always initial_suspend() { return {}; }

    /**
     * @brief whether need suspend on final
     * @return awaitable
     */
    coro_final_awaiter<type> final_suspend() noexcept { return {}; }
  };
};

/**
 * @brief promise_type for cancelable_coro_t — with await_transform for automatic cancellation
 */
template<typename V>
struct coro_promise<cancelable_coro_t<V>> {
  struct type : base_promise_t<V> {
    // tag for SFINAE detection of cancelable promise
    using canceler_tag = void;
    // whether this coroutine is detached (self-destructs at final_suspend)
    bool detached{false};
    // parent coroutine to resume when this one completes
    std::coroutine_handle<> continuation;
    // associated canceler for automatic cancellation propagation
    canceler_t* canceler_{nullptr};

    /**
     * @brief await_transform for utask_t-derived awaitables.
     * Wraps with cancellable_awaiter for automatic cancellation propagation.
     */
    template<typename T>
    requires std::derived_from<std::decay_t<T>, utask_t>
    cancellable_awaiter<std::decay_t<T>> await_transform(T&& op) {
      return {std::forward<T>(op), canceler_};
    }

    /**
     * @brief cascade canceler into child cancelable_coro_t automatically.
     * When co_await-ing a ccoro_t inside another ccoro_t, the parent's canceler
     * is propagated to the child's promise so cancellation cascades.
     */
    template<typename T>
    requires requires { typename std::decay_t<T>::promise_type::canceler_tag; }
    auto await_transform(T&& coro) {
      if (canceler_) {
        coro.native_handle().promise().canceler_ = canceler_;
      }
      return std::move(coro).operator co_await();
    }

    /**
     * @brief passthrough for all other awaitables.
     * Handles coro_t (via operator co_await), suspend_always, etc.
     */
    template<typename T>
    requires (!std::derived_from<std::decay_t<T>, utask_t>
              && !requires { typename std::decay_t<T>::promise_type::canceler_tag; })
    T&& await_transform(T&& op) {
      return std::forward<T>(op);
    }

    /**
     * @return coroutine implementation cancelable_coro_t
     */
    cancelable_coro_t<V> get_return_object() {
      return cancelable_coro_t<V>{std::coroutine_handle<type>::from_promise(*this)};
    }

    /**
     * @brief whether need suspend on initial
     * @return awaitable
     */
    std::suspend_always initial_suspend() { return {}; }

    /**
     * @brief whether need suspend on final
     * @return awaitable
     */
    coro_final_awaiter<type> final_suspend() noexcept { return {}; }
  };
};

/**
 * @brief CRTP base for coroutine wrappers. Provides all common operations.
 * Derived types (coro_t, cancelable_coro_t) inherit this to share code.
 * Future modifications to common behavior only need to change this class.
 * @tparam V return value type
 * @tparam Derived the concrete coroutine type (CRTP)
 */
template<typename V, typename Derived>
struct basic_coro_t : task_t {
  using promise_type = typename coro_promise<Derived>::type;

  /**
   * @brief construct from coroutine handle
   * @param h native coroutine handle
   */
  explicit basic_coro_t(std::coroutine_handle<promise_type> h) { this->handle = h; }

  /**
   * @brief destructor. destroys coroutine frame unless detached.
   */
  ~basic_coro_t() {
    if (handle && !native_handle().promise().detached) handle.destroy();
  }

  basic_coro_t(const basic_coro_t&) = delete;

  /**
   * @brief move constructor. takes ownership of coroutine frame.
   */
  basic_coro_t(basic_coro_t&& c) noexcept {
    if (this != &c) {
      if (this->handle) this->handle.destroy();
      this->handle = std::exchange(c.handle, nullptr);
    }
  }

  basic_coro_t& operator=(const basic_coro_t&) = delete;

  /**
   * @brief move assignment. takes ownership of coroutine frame.
   */
  basic_coro_t& operator=(basic_coro_t&& c) noexcept {
    if (this != &c) {
      if (this->handle) this->handle.destroy();
      this->handle = std::exchange(c.handle, nullptr);
    }
    return *this;
  }

  /**
   * @brief implement co_await operator, for co_await.
   */
  auto operator co_await() {
    struct coro_awaiter {
      std::coroutine_handle<promise_type> handle;

      CORNET_MAYBE_UNUSED bool await_ready() {
        return !handle || handle.done();
      }

      CORNET_MAYBE_UNUSED std::coroutine_handle<> await_suspend(std::coroutine_handle<> parent) {
        handle.promise().continuation = parent;
        return handle;
      }

      CORNET_MAYBE_UNUSED V await_resume() {
        // await_ready() waves an empty (moved-from) handle straight through
        // to here; dereferencing it used to be UB.
        if (!handle) {
          throw std::logic_error("co_await on a moved-from coro_t");
        }
        if constexpr (std::is_void_v<V>) {
          if (handle.promise().value.index() == 1)
            std::rethrow_exception(std::get<1>(handle.promise().value));
        } else {
          if (handle.promise().value.index() == 2)
            std::rethrow_exception(std::get<2>(handle.promise().value));
          return std::get<1>(std::move(handle.promise().value));
        }
      }
    };
    return coro_awaiter{native_handle()};
  }

  /**
   * @brief resume current coroutine
   */
  CORNET_MAYBE_UNUSED void resume() {
    if (!handle || handle.done()) return;
    handle.resume();
  }

  /**
   * @brief whether current coroutine is done
   * @return true if coroutine is done (handle is non-null and handle.done() is true), false otherwise
   */
  bool done() { return handle && handle.done(); }

  /**
   * @brief hand the coroutine its own lifetime: it self-destructs at
   * final_suspend and this wrapper must never touch the frame again.
   *
   * Like unique_ptr::release, the handle leaves the wrapper as the return
   * value — keeping it here used to leave a dangling pointer behind once the
   * coroutine destroyed itself, after which this wrapper's destructor read
   * promise().detached from freed memory (and would double-destroy if that
   * byte reused as falsy).
   * @return the released coroutine handle (this wrapper is empty afterwards)
   */
  std::coroutine_handle<> detach() {
    if (!handle || handle.done()) return handle;
    native_handle().promise().detached = true;
    return std::exchange(handle, nullptr);
  }

  /**
   * @brief get the native typed coroutine handle
   * @return typed coroutine handle
   */
  std::coroutine_handle<promise_type> native_handle() {
    return std::coroutine_handle<promise_type>::from_address(handle.address());
  }

  /**
   * @brief get the coroutine's return value, rethrowing any stored exception
   * @return the return value of type V
   */
  V value() {
    auto& val = native_handle().promise().value;
    if constexpr (std::is_void_v<V>) {
      if (val.index() == 1) std::rethrow_exception(std::get<1>(val));
    } else {
      if (val.index() == 2) std::rethrow_exception(std::get<2>(val));
      return std::get<1>(std::move(val));
    }
  }
};

/**
 * @brief zero-overhead coroutine wrapper (no cancellation support)
 * @tparam V return value type
 */
template <typename V = void>
struct coro_t : basic_coro_t<V, coro_t<V>> {
  using basic_coro_t<V, coro_t<V>>::basic_coro_t;
  using promise_type = typename coro_promise<coro_t<V>>::type;
};

/**
 * @brief cancelable coroutine wrapper with await_transform for automatic cancellation.
 * All internal utask_t-based operations are automatically wrapped with cancellable_awaiter.
 * @tparam V return value type
 */
template <typename V = void>
struct cancelable_coro_t : basic_coro_t<V, cancelable_coro_t<V>> {
  using basic_coro_t<V, cancelable_coro_t<V>>::basic_coro_t;
  using promise_type = typename coro_promise<cancelable_coro_t<V>>::type;
};

template<typename V = void>
using ccoro_t = cancelable_coro_t<V>;

/**
 * @brief yield-able coroutine wrapper for generator pattern
 * @tparam V yield value type
 */
template <typename V>
struct generator_t : task_t {
  /**
   * @brief generator promise_type for co_yield support
   */
  struct promise_type : base_promise_t<void> {
    // current yielded value
    V current_value;

    /**
     * @return generator implementation generator_t
     */
    generator_t get_return_object() {
      return generator_t{std::coroutine_handle<promise_type>::from_promise(*this)};
    }

    /**
     * @brief whether need suspend on initial
     * @return awaitable
     */
    std::suspend_always initial_suspend() { return {}; }

    /**
     * @brief whether need suspend on final
     * @return awaitable
     */
    std::suspend_always final_suspend() noexcept { return {}; }

    /**
     * @brief for co_yield return value
     * @param value yield value
     * @return awaitable
     */
    std::suspend_always yield_value(V value) {
      current_value = std::move(value);
      return {};
    }
  };

  /**
   * @brief construct generator from coroutine handle
   */
  explicit generator_t(std::coroutine_handle<promise_type> h) { this->handle = h; }

  ~generator_t() { if (handle) handle.destroy(); }

  generator_t(const generator_t&) = delete;

  generator_t(generator_t&& g) noexcept {
    if (this != &g) {
      if (this->handle) this->handle.destroy();
      this->handle = std::exchange(g.handle, nullptr);
    }
  }

  generator_t& operator=(const generator_t&) = delete;

  generator_t& operator=(generator_t&& g) noexcept {
    if (this != &g) {
      if (this->handle) this->handle.destroy();
      this->handle = std::exchange(g.handle, nullptr);
    }
    return *this;
  }

  /**
   * @brief input iterator for range-based for loop over yielded values
   */
  struct iterator {
    // coroutine handle to resume/read from
    std::coroutine_handle<promise_type> handle;

    using iterator_category = std::input_iterator_tag;
    using value_type = V;
    using difference_type = std::ptrdiff_t;
    using pointer = V*;
    using reference = V&;

    V& operator*() { return handle.promise().current_value; }
    bool operator!=(std::default_sentinel_t) { return !handle.done(); }
    void operator++() { handle.resume(); }
  };

  /**
   * @brief begin iteration, resumes coroutine to get first value
   */
  iterator begin() {
    if (handle) handle.resume();
    // task_t::handle is coroutine_handle<> — aggregate init cannot convert it
    // to coroutine_handle<promise_type> implicitly, so rebuild it by address.
    return {std::coroutine_handle<promise_type>::from_address(handle.address())};
  }

  /**
   * @brief sentinel for end of iteration
   */
  std::default_sentinel_t end() { return {}; }
};

template<typename V>
using gen_t = generator_t<V>;

} // cornet

#endif //CORNET_CORO_H
