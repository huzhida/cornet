
#include <fcntl.h>
#include <iostream>
#include <chrono>

#ifndef CORNET_UTILS_H
#define CORNET_UTILS_H

#include <spdlog/spdlog.h>

#define CORNET_UNIX_CHECK(expr, ...) \
  if ((expr) < 0) {             \
    SPDLOG_ERROR("Call {} error: {} {}", #expr, strerror(errno), errno); \
    __VA_ARGS__;                                      \
  }
#endif


#ifndef CORNET_URING_H
#define CORNET_URING_H

#include <liburing.h>

namespace cornet {

class uring {
 public:
  using CQE = io_uring_cqe;
  struct SQE {
    explicit SQE(io_uring_sqe* sqe): sqe(sqe) {}
    SQE& with_data(void* user_data) {
      io_uring_sqe_set_data(sqe, user_data);
      return *this;
    }
    SQE& with_flags(uint32_t flags) {
      io_uring_sqe_set_flags(sqe, flags);
      return *this;
    }
    void prep_read(int fd, void* buf, uint32_t nbytes, uint64_t offset) const {
      io_uring_prep_read(sqe, fd, buf ,nbytes, offset);
    }
    void prep_readv(int fd, iovec* iovecs, int nr_vecs, uint64_t offset) const {
      io_uring_prep_readv(sqe, fd, iovecs, nr_vecs, offset);
    }
    void prep_write(int fd, void* buf, uint32_t nbytes, uint64_t offset) const {
      io_uring_prep_write(sqe, fd, buf ,nbytes, offset);
    }
    void prep_writev(int fd, iovec* iovecs, int nr_vecs, uint64_t offset) const {
      io_uring_prep_writev(sqe, fd, iovecs, nr_vecs, offset);
    }
    void prep_send(int sockfd, void* buf, size_t len, int flags) const {
      io_uring_prep_send(sqe, sockfd, buf , len, flags);
    }
    void prep_recv(int sockfd, void* buf, size_t len, int flags) const {
      io_uring_prep_recv(sqe, sockfd, buf , len, flags);
    }
    void prep_accept(int sockfd, sockaddr* addr, socklen_t* addrlen, int flags) const {
      io_uring_prep_accept(sqe, sockfd, addr, addrlen, flags);
    }
    void prep_connect(int sockfd, sockaddr* addr, socklen_t addrlen) const {
      io_uring_prep_connect(sqe, sockfd, addr, addrlen);
    }
   private:
    io_uring_sqe* sqe;
  };

  explicit uring(uint32_t entries = 32, uint32_t flags = 0) {
    CORNET_UNIX_CHECK(io_uring_queue_init(entries, &_ring, flags))
  }
  ~uring() {
    io_uring_queue_exit(&_ring);
  }
  SQE get_sqe() {
    return SQE{io_uring_get_sqe(&_ring)};
  }
  std::vector<CQE*> wait_cqes(int wait_nr = 1, int timeout_s = 0, int timeout_ns = 0, sigset_t* mask = nullptr) {
    io_uring_cqe* cqe;
    __kernel_timespec ts{timeout_s, timeout_ns};
    CORNET_UNIX_CHECK(io_uring_wait_cqes(&_ring,
                                         &cqe,wait_nr,
                                         timeout_ns && timeout_ns ? &ts : nullptr,
                                         mask),
                      return {});

    std::vector<CQE*> cqes;
    uint32_t head;
    io_uring_for_each_cqe(&_ring, head, cqe) {
      cqes.emplace_back(cqe);
    }
    return cqes;
  }
  void submit() {
    CORNET_UNIX_CHECK(io_uring_submit(&_ring));
  }
  io_uring _ring{};
};

} // cornet

#endif //CORNET_URING_H




#ifndef CORNET_CONTEXT_H
#define CORNET_CONTEXT_H

#include <coroutine>
#include <queue>
namespace cornet {

struct uring_task_t {
  int32_t value{};

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
      auto cqes = r.wait_cqes(1,-1);
      for (auto cqe : cqes) {
        ((uring_task_t*)cqe->user_data)->return_value(cqe->res);
        io_uring_cqe_seen(&r._ring, cqe);
      }
    }
  }
};

} // cornet

#endif //CORNET_CONTEXT_H


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

#ifndef CORNET_AWAITER_H
#define CORNET_AWAITER_H

#include <coroutine>
#include <chrono>
#include <thread>
#include <optional>

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
  int fd;
  void* buf;
  size_t size;
  uint64_t offset;
  context& ctx;

  async_read(context& ctx, int fd, void* buf, size_t size, uint64_t offset)
      : ctx(ctx), fd(fd), buf(buf), size(size), offset(offset) {

  }
  bool await_ready() { return false;}
  void await_suspend(std::coroutine_handle<> h) {
    this->handle = h;
    ctx.submit_async_read(this, fd, buf, size, offset);
  }
  int await_resume() {
    return value;
  }
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



using namespace cornet;

coro<int, std::suspend_never> read_some(context& ctx) {
  constexpr const char* file = "./main.cc";
  constexpr const char* writef = "./main_copy.cc";
  char buffer[40960] = {0};
  auto fd = open(file, O_RDONLY);
  auto fd2 = open(writef, O_CREAT | O_WRONLY);
  auto size = co_await async_read(ctx, fd, buffer, 4096, 0);
  auto size2 = co_await async_write(ctx, fd2, buffer, 4096, 0);
  std::cout << std::string(buffer) << std::endl;
  close(fd);
  close(fd2);
  co_return size;
}

int main() {
  context ctx;
  auto co = read_some(ctx);
  std::thread t([&ctx] {
    ctx.loop();
  });
  auto start = std::chrono::steady_clock::now();
  while(!co.done()){
    std::this_thread::yield();
//    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  std::cout << "elapsed:" << std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count() << "us"  << std::endl;
  auto size = co.handle.promise().value;
  std::cout << size << std::endl;
  t.join();
}