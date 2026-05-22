#ifndef CORNET_CORO_H
#define CORNET_CORO_H

#include <coroutine>
#include <utility>
#include <variant>
#include <exception>
#include <iterator>
#include <type_traits>
#include "task.h"
#include "utils/utils.h"

namespace cornet {

/**
 * @brief base promise_type
 * @tparam V return value type
 */
template <typename V>
struct base_promise_t {
  // variant return value
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
struct base_promise_t<void> {
  // variant return value
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
 * @brief coroutine wrapper
 * @tparam V return value type
 */
template <typename V>
struct coro_t : task_t {
  /**
   * @brief coroutine constraint promise_type
   */
  struct promise_type : base_promise_t<V> {
    // for co_await invoke
    bool detached{false};
    std::coroutine_handle<> continuation;

    /**
     * @return coroutine implementation coro_t
     */
    coro_t get_return_object() {
      return coro_t<V>{std::coroutine_handle<promise_type>::from_promise(*this)};
    }

    /**
     * @brief final_awaiter used for co_await, resume coroutine who invoke this coro_t
     */
    struct final_awaiter {
      /**
       * @brief await.await_ready constraint
       * @return whether need suspend
       */
      CORNET_MAYBE_UNUSED bool await_ready() noexcept { return false; }
      /**
       * @brief close to suspend, do something.
       * @param h std::coroutine_handle that who invoke co_await current coroutine
       * @return the coroutine need resume
       */
      CORNET_MAYBE_UNUSED std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> h) noexcept {
        if (h.promise().continuation) {
          return h.promise().continuation;
        }
        if (h.promise().detached) {
          h.destroy();
        }
        return std::noop_coroutine();
      }
      /**
       * @brief close to resume, do something.
       */
      CORNET_MAYBE_UNUSED void await_resume() noexcept {}
    };

    /**
     * @brief whether need suspend on initial
     * @return awaitable
     */
    std::suspend_always initial_suspend() { return {}; }

    /**
     * @brief whether need suspend on final
     * @return awaitable
     */
    final_awaiter final_suspend() noexcept {
      return {};
    }

  };

  explicit coro_t(std::coroutine_handle<promise_type> h) {
    this->handle = h;
  }

  ~coro_t() {
    if (handle && !native_handle().promise().detached) {
      handle.destroy();
    }
  }

  coro_t(const coro_t&) = delete;

  coro_t(coro_t&& c) noexcept {
    if (this != &c) {
      if (this->handle)
        this->handle.destroy();
      this->handle = std::exchange(c.handle, nullptr);
    }
  }

  coro_t& operator=(const coro_t&) = delete;

  coro_t& operator=(coro_t&& c) noexcept {
    if (this != &c) {
      if (this->handle)
        this->handle.destroy();
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
        if constexpr (std::is_void_v<V>) {
          if (handle.promise().value.index() == 1) {
            std::rethrow_exception(std::get<1>(handle.promise().value));
          }
        } else {
          if (handle.promise().value.index() == 2) {
            std::rethrow_exception(std::get<2>(handle.promise().value));
          }
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
    if (!handle || handle.done())
      return;
    handle.resume();
  }

  /**
   * @brief whether current coroutine done
   * @return true for done / false ...
   */
  bool done() {
    return handle && handle.done();
  }

  /**
   * @brief let coroutine take own their own lifecycle, coro_t destruct don't destroy coroutine_handle
   */
  void detach() {
    if (!handle || handle.done())
      return;
    native_handle().promise().detached = true;
  }

  std::coroutine_handle<promise_type> native_handle() {
    return std::coroutine_handle<promise_type>::from_address(handle.address());
  }

  V value() {
    auto& val = native_handle().promise().value;
    if constexpr (std::is_void_v<V>) {
      if (val.index() == 1) {
        std::rethrow_exception(std::get<1>(val));
      }
    } else {
      if (val.index() == 2) {
        std::rethrow_exception(std::get<2>(val));
      }
      return std::get<1>(std::move(val));
    }
  }
};


/**
 * @brief yield-able wrapper
 * @tparam V return value type
 */
template <typename V>
struct generator_t : task_t {
  struct promise_type : base_promise_t<void> {
    // current value
    V current_value;

    /**
     * @return generator implementation generator_t
     */
    generator_t get_return_object() {
      return generator_t<V>{std::coroutine_handle<promise_type>::from_promise(*this)};
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

  explicit generator_t(std::coroutine_handle<promise_type> h) {
    this->handle = h;
  }

  ~generator_t() {
    if (handle) {
      handle.destroy();
    }
  }

  generator_t(const generator_t&) = delete;

  generator_t(generator_t&& g) noexcept {
    if (this != &g) {
      if (this->handle)
        this->handle.destroy();
      this->handle = std::exchange(g.handle, nullptr);
    }
  }

  generator_t& operator=(const generator_t&) = delete;

  generator_t& operator=(generator_t&& g) noexcept {
    if (this != &g) {
      if (this->handle)
        this->handle.destroy();
      this->handle = std::exchange(g.handle, nullptr);
    }
    return *this;
  }

  /**
   * @brief iterator used for range.
   */
  struct iterator {
    std::coroutine_handle<promise_type> handle;

    using iterator_category = std::input_iterator_tag;
    using value_type = V;
    using difference_type = std::ptrdiff_t;
    using pointer = V*;
    using reference = V&;

    V& operator*() {
      return handle.promise().current_value;
    }

    bool operator!=(std::default_sentinel_t) { return !handle.done(); }
    void operator++() { handle.resume(); }
  };

  iterator begin() {
    if (handle)
      handle.resume();
    return {handle};
  }

  std::default_sentinel_t end() {
    return {};
  }

};

} // cornet

#endif //CORNET_CORO_H