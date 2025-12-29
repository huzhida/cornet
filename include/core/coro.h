#ifndef CORNET_CORO_H
#define CORNET_CORO_H

#include <coroutine>
#include <utility>

namespace cornet {

/**
 *      @tparam C for coroutine type
 *      @tparam I for initial_suspend return type
 *      @tparam F for final_suspend return type
 */
template<typename I = std::suspend_always, typename F = std::suspend_always>
struct base_promise_t {
  auto initial_suspend() {return I{};}
  auto final_suspend() noexcept {return F{};}
};

/**
 *      @tparam C for coroutine type
 *      @tparam I for initial_suspend return type
 *      @tparam F for final_suspend return type
 */
template<typename C, typename I = std::suspend_always, typename F = std::suspend_always>
struct void_promise_t : public base_promise_t<I,F> {
  C get_return_object() {
    return C{std::coroutine_handle<void_promise_t>::from_promise(*this)};
  }
  void return_void() {}
  void unhandled_exception() {}
};

/**
 *      @tparam C for coroutine type
 *      @tparam V for return value type
 *      @tparam I for initial_suspend return type
 *      @tparam F for final_suspend return type
 */
template<typename C, typename V, typename I = std::suspend_always, typename F = std::suspend_always>
struct value_promise_t : public base_promise_t<I,F> {
  V value;
  C get_return_object() {
    return C{std::coroutine_handle<value_promise_t>::from_promise(*this)};
  }
  void return_value(V v) {value = v;}
  void unhandled_exception() {}
};

/**
 *      @tparam V for return value type
 *      @tparam I for initial_suspend return type
 *      @tparam F for final_suspend return type
 */
template<typename V = void, typename I = std::suspend_always, typename F = std::suspend_always>
struct coro {
  using promise_type = std::conditional_t<std::is_void_v<V>, void_promise_t<coro, I, F>, value_promise_t<coro, V, I, F>>;
  std::coroutine_handle<promise_type> handle;

  explicit coro(std::coroutine_handle<promise_type> h): handle(h) {}
  virtual ~coro() { if(handle) handle.destroy();}
  coro(const coro&) = delete;
  coro& operator=(const coro&) = delete;
  coro(coro&& other) noexcept : handle(std::exchange(other.handle, nullptr)) {}
  coro& operator=(coro&& other) noexcept {
    if (this == &other) return *this;
    if (handle) handle.destroy();
    handle = std::exchange(other.handle, nullptr);
    return *this;
  }
  void resume() {
    if (!handle || handle.done()) return;
    handle.resume();
  }
  void destroy() {
    if (!handle) return;
    handle.destroy();
  }
  bool done() const noexcept {
    return handle && handle.done();
  }
};

} // cornet

#endif //CORNET_CORO_H
