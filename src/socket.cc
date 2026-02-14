#include "core/socket.h"

namespace cornet::tcp::v4 {
socket_t::socket_t() {
  if((fd = socket()) < 0) {
    SPDLOG_ERROR("create socket failed with error: {}", strerror(errno));
  }
}
socket_t::socket_t(int fd) : fd(fd) {

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
    s.fd = -1;
  }
}
socket_t &socket_t::operator=(socket_t &&s) noexcept {
  if (this != &s) {
    this->fd = s.fd;
    s.fd = -1;
  }
  return *this;
}
void socket_t::address_reuse(bool on) {
  int reuse = on ? 1 : 0;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (void*)&reuse, sizeof(reuse));
}
void socket_t::port_reuse(bool on) {
  int reuse = on ? 1 : 0;
  setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, (void*)&reuse, sizeof(reuse));
}
bool socket_t::listen(const std::string &ip, unsigned int port) const {
  sockaddr_in addr = to_address(ip, port);
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

socket_t::accept_awaiter::accept_awaiter(context_t &ctx, int fd, sockaddr_in *addr, int flag) : uring_task_t(ctx) {
  ctx.io_uring().new_sqe().prep_accept(fd, (sockaddr*)&addr, &addr_len, flag).with_data(this);
}
socket_t::accept_awaiter socket_t::accept(context_t &ctx, sockaddr_in* addr, int flag) const {
  return accept_awaiter{ctx, fd, addr, flag};
}

socket_t::connect_awaiter::connect_awaiter(context_t &ctx, int fd, const std::string &ip, unsigned int port) : uring_task_t(ctx) {
  addr = to_address(ip, port);
  ctx.io_uring().new_sqe().prep_connect(fd, (sockaddr*)&addr, sizeof(addr)).with_data(this);
}
socket_t::connect_awaiter socket_t::connect(context_t &ctx, const std::string &ip, unsigned int port) const {
  return connect_awaiter{ctx, fd, ip, port};
}

socket_t::recv_awaiter::recv_awaiter(context_t &ctx, int fd, void *buf, uint32_t nbytes, int flag) : uring_task_t(ctx) {
  ctx.io_uring().new_sqe().prep_recv(fd, buf, nbytes, flag).with_data(this);
}
socket_t::recv_awaiter socket_t::recv(context_t &ctx, void *buf, uint32_t nbytes, int flag) const {
  return recv_awaiter{ctx, this->fd, buf, nbytes, flag};
}

socket_t::send_awaiter::send_awaiter(context_t &ctx, int fd, void *buf, uint32_t nbytes, int flag) : uring_task_t(ctx) {
  ctx.io_uring().new_sqe().prep_send(fd, buf, nbytes, flag).with_data(this);
}
socket_t::send_awaiter socket_t::send(context_t &ctx, void *buf, uint32_t nbytes, int flag) const  {
  return send_awaiter{ctx, this->fd, buf, nbytes, flag};
}

socket_t::close_awaiter::close_awaiter(context_t &ctx, int fd) : uring_task_t(ctx) {
  ctx.io_uring().new_sqe().prep_close(fd).with_data(this);
}
socket_t::close_awaiter socket_t::close(context_t &ctx) const {
  return close_awaiter{ctx, fd};
}

} // cornet