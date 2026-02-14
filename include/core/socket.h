#ifndef CORNET_SOCKET_H
#define CORNET_SOCKET_H

#include "context.h"
#include <coroutine>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace cornet::tcp::v4 {

inline sockaddr_in to_address(const std::string& ip, uint32_t port) {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = inet_addr(ip.c_str());
  return addr;
}

class socket_t {
 public:
  socket_t();
  ~socket_t();
  socket_t(const socket_t&) = default;
  socket_t(socket_t&& s) noexcept;
  socket_t& operator=(const socket_t&) = delete;
  socket_t& operator=(socket_t&& s) noexcept;

  static inline int socket() {
    return ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  }

  bool listen(const std::string& ip, unsigned port);
  struct accept_awaiter : uring_task_t {
    accept_awaiter(context_t& ctx, int fd, int flag) : uring_task_t(ctx, complete) {
      ctx.io_uring().new_sqe().prep_accept(fd, (sockaddr*)&addr, &addr_len, flag).with_data(this);
    }
    CORNET_MAYBE_UNUSED inline socket_t await_resume() {
      return {fd, &addr};
    }
    static inline void complete(task_t* self, cqe_t cqe) {
      auto* awaiter = reinterpret_cast<accept_awaiter*>(self);
      awaiter->fd = cqe->res;
      awaiter->ctx.sched(awaiter);
    }
    int fd{-1};
    sockaddr_in addr{};
    socklen_t addr_len{};
  };
  accept_awaiter accept(context_t& ctx, int flag = 0) const;
  struct connect_awaiter : uring_task_t {
    connect_awaiter(context_t& ctx, int fd, const std::string& ip, unsigned port) : uring_task_t(ctx, complete) {
      addr = to_address(ip, port);
      ctx.io_uring().new_sqe().prep_connect(fd, (sockaddr*)&addr, sizeof(addr)).with_data(this);
    }
    CORNET_MAYBE_UNUSED inline int await_resume() const {
      return ret;
    }

    static inline void complete(task_t* self, cqe_t cqe) {
      auto* awaiter = reinterpret_cast<connect_awaiter*>(self);
      awaiter->ret = cqe->res;
      awaiter->ctx.sched(awaiter);
    }
    int ret{0};
    sockaddr_in addr{};
  };
  connect_awaiter connect(context_t& ctx, const std::string& ip, unsigned port) const;
  struct recv_awaiter : uring_task_t {
    recv_awaiter(context_t& ctx, int fd, void* buf, uint32_t nbytes, int flag) : uring_task_t(ctx, complete) {
      ctx.io_uring().new_sqe().prep_recv(fd, buf, nbytes, flag).with_data(this);
    }
    CORNET_MAYBE_UNUSED inline int await_resume() const {
      return value;
    }
    static inline void complete(task_t* self, cqe_t cqe) {
      auto* awaiter = reinterpret_cast<recv_awaiter*>(self);
      awaiter->value = cqe->res;
      awaiter->ctx.sched(awaiter);
    }
    int value{};
  };
  recv_awaiter recv(context_t& ctx, void* buf, uint32_t nbytes, int flag = 0) const;
  struct send_awaiter : uring_task_t {
    send_awaiter(context_t& ctx, int fd, void* buf, uint32_t nbytes, int flag) : uring_task_t(ctx, complete) {
      ctx.io_uring().new_sqe().prep_send(fd, buf, nbytes, flag).with_data(this);
    }
    CORNET_MAYBE_UNUSED inline int await_resume() const {
      return value;
    }
    static inline void complete(task_t* self, cqe_t cqe) {
      auto* awaiter = reinterpret_cast<send_awaiter*>(self);
      awaiter->value = cqe->res;
      awaiter->ctx.sched(awaiter);
    }
    int value{};
  };
  send_awaiter send(context_t& ctx, void* buf, uint32_t nbytes, int flag = 0) const;
  struct close_awaiter : uring_task_t {
    close_awaiter(context_t& ctx, int fd) : uring_task_t(ctx, complete) {
      ctx.io_uring().new_sqe().prep_close(fd).with_data(this);
    }
    CORNET_MAYBE_UNUSED inline int await_resume() const {
      return value;
    }
    static inline void complete(task_t* self, cqe_t cqe) {
      auto awaiter = reinterpret_cast<close_awaiter*>(self);
      awaiter->value = cqe->res;
      awaiter->ctx.sched(awaiter);
    }
    int value{};
  };
  close_awaiter close(context_t& ctx) const;

 private:
  int fd{-1};
  sockaddr_in addr{};

  socket_t(int fd, sockaddr_in* addr) : fd(fd), addr(*addr) {}
};
} // cornet

#endif //CORNET_SOCKET_H
