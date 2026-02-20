#ifndef CORNET_CORO_H
#define CORNET_CORO_H

#include <coroutine>
#include <utility>
#include <variant>
#include <exception>
#include <iterator>
#include "task.h"
#include "utils.h"

namespace cornet {

template <typename V>
struct base_promise_t {
  std::variant<std::monostate, V, std::exception_ptr> value;

  void return_value(V v) {
    value.template emplace<1>(std::move(v));
  }

  void unhandled_exception() {
    value.template emplace<2>(std::current_exception());
  }
};

template <>
struct base_promise_t<void> {
  std::variant<std::monostate, std::exception_ptr> value;
  void return_void() {}

  void unhandled_exception() {
    value.template emplace<1>(std::current_exception());
  }
};

template <typename V>
struct coro_t : task_t {
  struct promise_type : base_promise_t<V> {
    std::coroutine_handle<> continuation;

    coro_t get_return_object() {
      return coro_t<V>{std::coroutine_handle<promise_type>::from_promise(*this)};
    }

    struct final_awaiter {
      CORNET_MAYBE_UNUSED bool await_ready() noexcept { return false; }
      CORNET_MAYBE_UNUSED std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> h) noexcept {
        if (h.promise().continuation) {
          return h.promise().continuation;
        }
        return std::noop_coroutine();
      }

      CORNET_MAYBE_UNUSED void await_resume() noexcept {}
    };

    std::suspend_always initial_suspend() { return {}; }

    final_awaiter final_suspend() noexcept {
      return {};
    }

  };

  explicit coro_t(std::coroutine_handle<promise_type> h) {
    this->handle = h;
  }

  ~coro_t() {
    if (handle)
      handle.destroy();
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
  }

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
        if (handle.promise().value.index() == 2) {
          std::rethrow_exception(std::get<2>(handle.promise().value));
        }
        return std::get<1>(handle.promise().value);
      }
    };
    return coro_awaiter{handle};
  }

  CORNET_MAYBE_UNUSED void resume() {
    if (!handle || handle.done())
      return;
    handle.resume();
  }

  bool done() {
    return handle && handle.done();
  }
};

template <typename V>
struct generator_t : task_t {
  struct promise_type : base_promise_t<void> {
    V current_value;

    generator_t get_return_object() {
      return generator_t<V>{std::coroutine_handle<promise_type>::from_promise(*this)};
    }

    std::suspend_always initial_suspend() { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; }

    std::suspend_always yield_value(V value) {
      current_value = std::move(value);
      return {};
    }
  };

  explicit generator_t(std::coroutine_handle<promise_type> h) {
    this->handle = handle;
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
  }

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