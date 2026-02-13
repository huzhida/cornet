#ifndef CORNET_SOCKET_H
#define CORNET_SOCKET_H

#include "context.h"
#include <coroutine>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace cornet {

namespace tcp::v4 {

inline sockaddr_in to_address(const std::string& ip, uint32_t port) {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = inet_addr(ip.c_str());
  return addr;
}

class socket_t {
 public:
  socket_t() {
    if((fd = socket()) < 0) {
      SPDLOG_ERROR("create socket failed with error: {}", strerror(errno));
    }
  }
  ~socket_t() {
    if (fd != -1) {
      ::close(fd);
      fd = -1;
    }
  }
  socket_t(const socket_t&) = default;
  socket_t(socket_t&& s) noexcept {
    if (this != &s) {
      this->fd = s.fd;
      this->addr = s.addr;
      s.fd = -1;
    }
  }
  socket_t& operator=(const socket_t&) = delete;
  socket_t& operator=(socket_t&& s) noexcept {
    if (this != &s) {
      this->fd = s.fd;
      this->addr = s.addr;
      s.fd = -1;
    }
    return *this;
  }

  static inline int socket() {
    return ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  }

  bool listen(const std::string& ip, unsigned port) {
    addr = to_address(ip, port);
    if (::bind(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
      SPDLOG_ERROR("bind to {}:{} failed with error:{}", ip, port, strerror(errno));
      return false;
    }
    if (::listen(fd, 2048) < 0) {
      SPDLOG_ERROR("listen to {}:{} failed with error:{}", ip, port, strerror(errno));
      return false;
    }
    return true;
  }
  auto accept(context_t& ctx, int flag = 0) {
    struct accept_awaiter : task_t {
      accept_awaiter(context_t& ctx, int fd, int flag) : ctx(ctx), task_t(complete) {
        ctx.io_uring().new_sqe().prep_accept(fd, (sockaddr*)&addr, &addr_len, flag).with_data(this);
      }
      bool await_ready() {
        return false;
      }
      void await_suspend(std::coroutine_handle<> handle) {
        this->handle = handle;
        ctx.io_uring().submit();
      }
      socket_t await_resume() {
        return {fd, &addr};
      }

      static inline void complete(task_t* self, int ret) {
        auto* awaiter = reinterpret_cast<accept_awaiter*>(self);
        awaiter->fd = ret;
        awaiter->ctx.sched(awaiter);
      }
      int fd{-1};
      sockaddr_in addr{};
      socklen_t addr_len{};
      context_t& ctx;
    };
    return accept_awaiter{ctx, fd, flag};
  }
  auto connect(context_t& ctx, const std::string& ip, unsigned port, int flag = 0) {
    struct connect_awaiter : task_t {
      connect_awaiter(context_t& ctx, int fd, const std::string& ip, unsigned port, int flag) : ctx(ctx), task_t(complete) {
        addr = to_address(ip, port);
        ctx.io_uring().new_sqe().prep_connect(fd, (sockaddr*)&addr, sizeof(addr)).with_data(this);
      }
      bool await_ready() {
        return false;
      }
      void await_suspend(std::coroutine_handle<> handle) {
        this->handle = handle;
        ctx.io_uring().submit();
      }
      int await_resume() {
        return ret;
      }

      static inline void complete(task_t* self, int ret) {
        auto* awaiter = reinterpret_cast<connect_awaiter*>(self);
        awaiter->ret = ret;
        awaiter->ctx.sched(awaiter);
      }
      int ret{0};
      context_t& ctx;
      sockaddr_in addr;
    };
    return connect_awaiter{ctx, fd, ip, port, flag};
  }
  auto recv(context_t& ctx, void* buf, uint32_t nbytes, int flag = 0) {
    struct recv_awaiter : task_t {
      recv_awaiter(context_t& ctx, int fd, void* buf, uint32_t nbytes, int flag) : ctx(ctx), task_t(complete) {
        ctx.io_uring().new_sqe().prep_recv(fd, buf, nbytes, flag).with_data(this);
      }
      bool await_ready() {
        return false;
      }
      void await_suspend(std::coroutine_handle<> handle) {
        this->handle = handle;
        ctx.io_uring().submit();
      }
      int await_resume() {
        return value;
      }
      static inline void complete(task_t* self, int ret) {
        auto* awaiter = reinterpret_cast<recv_awaiter*>(self);
        awaiter->value = ret;
        awaiter->ctx.sched(awaiter);
      }
      context_t& ctx;
      int value{};
    };
    return recv_awaiter{ctx, this->fd, buf, nbytes, flag};
  }
  auto send(context_t& ctx, void* buf, uint32_t nbytes, int flag = 0) {
    struct send_awaiter : task_t {
      send_awaiter(context_t& ctx, int fd, void* buf, uint32_t nbytes, int flag) : ctx(ctx), task_t(complete) {
        ctx.io_uring().new_sqe().prep_send(fd, buf, nbytes, flag).with_data(this);
      }
      bool await_ready() {
        return false;
      }
      void await_suspend(std::coroutine_handle<> handle) {
        this->handle = handle;
        ctx.io_uring().submit();
      }
      int await_resume() {
        return value;
      }
      static inline void complete(task_t* self, int ret) {
        auto* awaiter = reinterpret_cast<send_awaiter*>(self);
        awaiter->value = ret;
        awaiter->ctx.sched(awaiter);
      }
      context_t& ctx;
      int value{};
    };
    return send_awaiter{ctx, this->fd, buf, nbytes, flag};
  }
  auto close(context_t& ctx) {
    struct close_awaiter : task_t {
      close_awaiter(context_t& ctx, int fd) : ctx(ctx), task_t(complete) {
        ctx.io_uring().new_sqe().prep_close(fd).with_data(this);
      }
      bool await_ready() {
        return false;
      }
      void await_suspend(std::coroutine_handle<> handle) {
        this->handle = handle;
        ctx.io_uring().submit();
      }
      int await_resume() {
        return value;
      }
      static inline void complete(task_t* self, int ret) {
        auto awaiter = reinterpret_cast<close_awaiter*>(self);
        awaiter->value = ret;
        awaiter->ctx.sched(awaiter);
      }
      int value;
      context_t& ctx;
    };
    return close_awaiter{ctx, fd};
  }

 private:
  int fd{-1};
  sockaddr_in addr{};

  socket_t(int fd, sockaddr_in* addr) : fd(fd), addr(*addr) {}
};
}
} // cornet

#endif //CORNET_SOCKET_H
