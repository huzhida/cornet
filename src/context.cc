#include "core/context.h"

namespace cornet {
context_t::context_t(context_t &&ctx) noexcept {
  if (this != &ctx) {
    this->uring = std::move(ctx.uring);
    this->need_stop = ctx.need_stop;
    this->ready_tasks = std::move(ctx.ready_tasks);
  }
}
context_t &context_t::operator=(context_t &&ctx) noexcept {
  if (this != &ctx) {
    this->uring = std::move(ctx.uring);
    this->need_stop = ctx.need_stop;
    this->ready_tasks = std::move(ctx.ready_tasks);
  }
  return *this;
}
void context_t::run() {
  need_stop = false;
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
void context_t::stop() {
  need_stop = true;
}
uring_t &context_t::io_uring() {
  return uring;
}
} // cornet