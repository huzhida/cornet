#ifndef CORNET_CONTEXT_H
#define CORNET_CONTEXT_H

#include <queue>
#include "uring.h"
#include "task.h"
#include "coro.h"

namespace cornet {

struct context_t {
  uring_t uring;

  template<typename C>
  void spawn(C&& c) {
    resume(std::forward<C>(c));
  }
  template<typename C>
  void resume(C&& c) {
    using T = std::decay_t<C>;
    if constexpr (std::is_same_v<T, std::coroutine_handle<>>) {
      ready_tasks.push(std::forward<C>(c));
    } else if constexpr(std::is_pointer_v<T>) {
      ready_tasks.push(c->handle);
    } else {
      ready_tasks.push(c.handle);
    }
  }
  void run() {
    need_stop = true;
    while(!need_stop) {
      while(!ready_tasks.empty()) {
        auto handle = ready_tasks.front();
        ready_tasks.pop();
        handle.resume();
      }

      uring.wait_and_process_cqes([](cqe_t cqe) {
        auto task = reinterpret_cast<task_t*>(cqe->user_data);
        task->complete(task,cqe->res);
      }, 1, 1);
    }
  }
  void stop() {
    need_stop = true;
  }
 private:
  std::queue<std::coroutine_handle<>> ready_tasks;
  bool need_stop{false};
};

} // cornet

#endif //CORNET_CONTEXT_H
