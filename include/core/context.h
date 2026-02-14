#ifndef CORNET_CONTEXT_H
#define CORNET_CONTEXT_H

#include <queue>
#include "uring.h"
#include "task.h"
#include "coro.h"
#include "ringbuffer.h"

namespace cornet {

struct context_t {
  context_t() = default;
  context_t(const context_t&) = delete;
  context_t(context_t&& ctx) noexcept;
  context_t& operator=(const context_t&) = delete;
  context_t& operator=(context_t&& ctx)  noexcept;
  template<typename C> void spawn(C&& c) {
    sched(std::forward<C>(c));
  }
  template<typename C> void sched(C&& c) {
    using T = std::decay_t<C>;
    if constexpr (std::is_same_v<T, std::coroutine_handle<>>) {
      ready_tasks.push(std::forward<C>(c));
    } else if constexpr(std::is_pointer_v<T>) {
      ready_tasks.push(c->handle);
    } else {
      ready_tasks.push(c.handle);
    }
  }
  void run();
  void stop();
  uring_t& io_uring();

 private:
  uring_t uring;
  std::queue<std::coroutine_handle<>> ready_tasks{};
  bool need_stop{false};
};

} // cornet

#endif //CORNET_CONTEXT_H
