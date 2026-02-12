#ifndef CORNET_SOCKET_H
#define CORNET_SOCKET_H

#include "context.h"
#include <coroutine>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace cornet {

namespace tcp::v4 {

sockaddr_in to_address(const std::string& ip, uint32_t port) {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = inet_addr(ip.c_str());
  return addr;
}

class socket_t {
 public:
  socket_t() {
    CORNET_UNIX_CHECK(fd = socket());
  }
  ~socket_t() {
    if (fd != -1) {
      ::close(fd);
      fd = -1;
    }
  }
  socket_t(int fd, sockaddr_in* addr) : fd(fd), addr(*addr) {}
  socket_t(const std::string& ip, uint32_t port) {
    CORNET_UNIX_CHECK(fd = socket());
    addr = to_address(ip, port);
  }

  static inline int socket() {
    return ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  }

  bool listen() {
    CORNET_UNIX_CHECK(::bind(fd, (sockaddr*)&addr, sizeof(addr)), return false;);
    CORNET_UNIX_CHECK(::listen(fd, 2048), return false;);
    return true;
  }
  auto accept(context_t& ctx, sockaddr_in* raddr, socklen_t* len, int flag) {
    struct accept_awaiter : task_t {
      accept_awaiter(context_t& ctx, int fd, sockaddr_in* addr, socklen_t* len, int flag) : ctx(ctx), task_t(complete) {
        ctx.uring.new_sqe().with_data(this).prep_accept(fd, (sockaddr*)addr, len, flag);
      }
      bool await_ready() {
        return false;
      }
      void await_suspend(std::coroutine_handle<> handle) {
        this->handle = handle;
        ctx.uring.submit();
      }
      int await_resume() {
        return fd;
      }

      static inline void complete(task_t* self, int ret) {
        auto* awaiter = reinterpret_cast<accept_awaiter*>(self);
        awaiter->fd = ret;
        awaiter->ctx.resume(awaiter);
      }
      int fd{-1};
      context_t& ctx;
    };
    return accept_awaiter{ctx, fd, raddr, len, flag};
  }
  auto connect(context_t& ctx, sockaddr_in* raddr, socklen_t len, int flag) {
    struct accept_awaiter : task_t {
      accept_awaiter(context_t& ctx, int fd, sockaddr_in* addr, socklen_t len, int flag) : ctx(ctx), task_t(complete) {
        ctx.uring.new_sqe().with_data(this).prep_connect(fd, (sockaddr*)addr, len);
      }
      bool await_ready() {
        return false;
      }
      void await_suspend(std::coroutine_handle<> handle) {
        this->handle = handle;
        ctx.uring.submit();
      }
      int await_resume() {
        return ret;
      }

      static inline void complete(task_t* self, int ret) {
        auto* awaiter = reinterpret_cast<accept_awaiter*>(self);
        awaiter->ret = ret;
        awaiter->ctx.resume(awaiter);
      }
      int ret{0};
      context_t& ctx;
    };
    return accept_awaiter{ctx, fd, raddr, len, flag};
  }
  auto recv(context_t& ctx, void* buf, uint32_t nbytes, int flag = 0) {
    struct recv_awaiter : task_t {
      recv_awaiter(context_t& ctx, int fd, void* buf, uint32_t nbytes, int flag) : ctx(ctx), task_t(complete) {
        ctx.uring.new_sqe().with_data(this).prep_recv(fd, buf, nbytes, flag);
      }
      bool await_ready() {
        return false;
      }
      void await_suspend(std::coroutine_handle<> handle) {
        this->handle = handle;
        ctx.uring.submit();
      }
      int await_resume() {
        return value;
      }
      static inline void complete(task_t* self, int ret) {
        auto* awaiter = reinterpret_cast<recv_awaiter*>(self);
        awaiter->value = ret;
        awaiter->ctx.resume(awaiter);
      }
      context_t& ctx;
      int value{};
    };
    return recv_awaiter{ctx, this->fd, buf, nbytes, flag};
  }
  auto send(context_t& ctx, void* buf, uint32_t nbytes, int flag = 0) {
    struct send_awaiter : task_t {
      send_awaiter(context_t& ctx, int fd, void* buf, uint32_t nbytes, int flag) : ctx(ctx), task_t(complete) {
        ctx.uring.new_sqe().with_data(this).prep_send(fd, buf, nbytes, flag);
      }
      bool await_ready() {
        return false;
      }
      void await_suspend(std::coroutine_handle<> handle) {
        this->handle = handle;
        ctx.uring.submit();
      }
      int await_resume() {
        return value;
      }
      static inline void complete(task_t* self, int ret) {
        auto* awaiter = reinterpret_cast<send_awaiter*>(self);
        awaiter->value = ret;
        awaiter->ctx.resume(awaiter);
      }
      context_t& ctx;
      int value{};
    };
    return send_awaiter{ctx, this->fd, buf, nbytes, flag};
  }
  auto close(context_t& ctx) {
    struct close_awaiter : task_t {
      close_awaiter(context_t& ctx, int fd) : ctx(ctx), task_t(complete) {
        ctx.uring.new_sqe().with_data(this).prep_close(fd);
      }
      bool await_ready() {
        return false;
      }
      void await_suspend(std::coroutine_handle<> handle) {
        this->handle = handle;
        ctx.uring.submit();
      }
      int await_resume() {
        return value;
      }
      static inline void complete(task_t* self, int ret) {
        auto awaiter = reinterpret_cast<close_awaiter*>(self);
        awaiter->value = ret;
        awaiter->ctx.resume(awaiter);
      }
      int value;
      context_t& ctx;
    };
    return close_awaiter{ctx, fd};
  }
  int fd{-1};
  sockaddr_in addr{};
};
}
} // cornet

#endif //CORNET_SOCKET_H
