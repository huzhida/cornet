#ifndef CORNET_AWAITER_H
#define CORNET_AWAITER_H

#include <coroutine>
#include <chrono>
#include <thread>
#include <optional>
#include "context.h"
#include "coro.h"

namespace cornet {

struct sleep_awaiter {
  int delay_seconds;
  std::coroutine_handle<> handle2;

  sleep_awaiter(int delay, std::coroutine_handle<> h) : delay_seconds(delay), handle2(h) {}
  bool await_ready() { return delay_seconds <= 0;}
  std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) {
    std::this_thread::sleep_for(std::chrono::seconds(delay_seconds));
    handle2.resume();
    return h;
  }
  void await_resume() {}
};

struct async_read : uring_task_t {
  async_read(context& ctx, int fd, void* buf, size_t size, uint64_t offset)
  : ctx(ctx), fd(fd), buf(buf), size(size), offset(offset) {}
  bool await_ready() { return false; }
  void await_suspend(std::coroutine_handle<> h) {
    this->handle = h;
    ctx.submit_async_read(this, fd, buf, size, offset);
  }
  int await_resume() { return value; }
 private:
  int fd;
  void* buf;
  size_t size;
  uint64_t offset;
  context& ctx;
};

struct async_write : uring_task_t {
  async_write(context& ctx, int fd, void* buf, size_t size, uint64_t offset)
  : ctx(ctx), fd(fd), buf(buf), size(size), offset(offset) {}
  bool await_ready() { return false; }
  void await_suspend(std::coroutine_handle<> h) {
    this->handle = h;
    ctx.submit_async_write(this, fd, buf, size, offset);
  }
  int await_resume() { return value; }
 private:
  int fd;
  void* buf;
  size_t size;
  uint64_t offset;
  context& ctx;
};

} // cornet

#endif //CORNET_AWAITER_H
