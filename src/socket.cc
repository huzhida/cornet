#include "core/socket.h"

namespace cornet::tcp::v4 {
socket_t::socket_t() {
  if((fd = socket()) < 0) {
    SPDLOG_ERROR("create socket failed with error: {}", strerror(errno));
  }
}
bool socket_t::listen(const std::string &ip, unsigned int port) {
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
socket_t::~socket_t() {
  if (fd != -1) {
    ::close(fd);
    fd = -1;
  }
}
socket_t::socket_t(socket_t &&s) noexcept  {
  if (this != &s) {
    this->fd = s.fd;
    this->addr = s.addr;
    s.fd = -1;
  }
}
socket_t &socket_t::operator=(socket_t &&s) noexcept {
  if (this != &s) {
    this->fd = s.fd;
    this->addr = s.addr;
    s.fd = -1;
  }
  return *this;
}
auto socket_t::accept(context_t &ctx, int flag) {
  struct accept_awaiter : uring_task_t {
    accept_awaiter(context_t& ctx, int fd, int flag) : uring_task_t(ctx, complete) {
      ctx.io_uring().new_sqe().prep_accept(fd, (sockaddr*)&addr, &addr_len, flag).with_data(this);
    }
    CORNET_MAYBE_UNUSED socket_t await_resume() {
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
  };
  return accept_awaiter{ctx, fd, flag};
}
auto socket_t::connect(context_t &ctx, const std::string &ip, unsigned int port, int flag) {
  struct connect_awaiter : uring_task_t {
    connect_awaiter(context_t& ctx, int fd, const std::string& ip, unsigned port, int flag) : uring_task_t(ctx, complete) {
      addr = to_address(ip, port);
      ctx.io_uring().new_sqe().prep_connect(fd, (sockaddr*)&addr, sizeof(addr)).with_data(this);
    }
    CORNET_MAYBE_UNUSED int await_resume() const {
      return ret;
    }

    static inline void complete(task_t* self, int ret) {
      auto* awaiter = reinterpret_cast<connect_awaiter*>(self);
      awaiter->ret = ret;
      awaiter->ctx.sched(awaiter);
    }
    int ret{0};
    sockaddr_in addr{};
  };
  return connect_awaiter{ctx, fd, ip, port, flag};
}
auto socket_t::recv(context_t &ctx, void *buf, uint32_t nbytes, int flag)  {
  struct recv_awaiter : uring_task_t {
    recv_awaiter(context_t& ctx, int fd, void* buf, uint32_t nbytes, int flag) : uring_task_t(ctx, complete) {
      ctx.io_uring().new_sqe().prep_recv(fd, buf, nbytes, flag).with_data(this);
    }
    int await_resume() {
      return value;
    }
    static inline void complete(task_t* self, int ret) {
      auto* awaiter = reinterpret_cast<recv_awaiter*>(self);
      awaiter->value = ret;
      awaiter->ctx.sched(awaiter);
    }
    int value{};
  };
  return recv_awaiter{ctx, this->fd, buf, nbytes, flag};
}
auto socket_t::send(context_t &ctx, void *buf, uint32_t nbytes, int flag)  {
  struct send_awaiter : uring_task_t {
    send_awaiter(context_t& ctx, int fd, void* buf, uint32_t nbytes, int flag) : uring_task_t(ctx, complete) {
      ctx.io_uring().new_sqe().prep_send(fd, buf, nbytes, flag).with_data(this);
    }
    int await_resume() const {
      return value;
    }
    static inline void complete(task_t* self, int ret) {
      auto* awaiter = reinterpret_cast<send_awaiter*>(self);
      awaiter->value = ret;
      awaiter->ctx.sched(awaiter);
    }
    int value{};
  };
  return send_awaiter{ctx, this->fd, buf, nbytes, flag};
}
auto socket_t::close(context_t &ctx){
  struct close_awaiter : uring_task_t {
    close_awaiter(context_t& ctx, int fd) : uring_task_t(ctx, complete) {
      ctx.io_uring().new_sqe().prep_close(fd).with_data(this);
    }
    int await_resume() {
      return value;
    }
    static inline void complete(task_t* self, int ret) {
      auto awaiter = reinterpret_cast<close_awaiter*>(self);
      awaiter->value = ret;
      awaiter->ctx.sched(awaiter);
    }
    int value{};
  };
  return close_awaiter{ctx, fd};
}
} // cornet