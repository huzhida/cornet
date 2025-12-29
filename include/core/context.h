#ifndef CORNET_CONTEXT_H
#define CORNET_CONTEXT_H

#include <coroutine>
#include <queue>
#include "uring.h"
namespace cornet {

struct uring_task_t {
  int32_t value;

  std::coroutine_handle<> handle;
  virtual void return_value(int32_t v) {
    value = v;
    handle.resume();
  }
};


class context {
  uring r;
  std::mutex m;
 public:
  void submit_async_read(uring_task_t* task, int fd, void* buf, uint32_t size, uint64_t offset) {
    std::lock_guard<std::mutex> guard(m);
    auto sqe = r.get_sqe();
    sqe.with_data(task).prep_read(fd, buf, size, offset);
    r.submit();
  }

  void submit_async_write(uring_task_t* task, int fd, void* buf, uint32_t size, uint64_t offset) {
    std::lock_guard<std::mutex> guard(m);
    auto sqe = r.get_sqe();
    sqe.with_data(task).prep_write(fd, buf, size, offset);
    r.submit();
  }

  void submit_async_send(uring_task_t* task, int sockfd, void* buf, uint32_t size, int flags) {
    std::lock_guard<std::mutex> guard(m);
    auto sqe = r.get_sqe();
    sqe.with_data(task).prep_send(sockfd, buf, size, flags);
    r.submit();
  }

  void submit_async_recv(uring_task_t* task, int sockfd, void* buf, uint32_t size, int flags) {
    std::lock_guard<std::mutex> guard(m);
    auto sqe = r.get_sqe();
    sqe.with_data(task).prep_recv(sockfd, buf, size, flags);
    r.submit();
  }

  void loop() {
    while(true) {
      auto cqes = r.wait_cqes(1);
      for (auto cqe : cqes) {
        ((uring_task_t*)cqe->user_data)->return_value(cqe->res);
      }
    }
  }
};

} // cornet

#endif //CORNET_CONTEXT_H
