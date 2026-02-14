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

socket_t::accept_awaiter socket_t::accept(context_t &ctx, int flag) const {
  return accept_awaiter{ctx, fd, flag};
}

socket_t::connect_awaiter socket_t::connect(context_t &ctx, const std::string &ip, unsigned int port) const {
  return connect_awaiter{ctx, fd, ip, port};
}

socket_t::recv_awaiter socket_t::recv(context_t &ctx, void *buf, uint32_t nbytes, int flag) const {
  return recv_awaiter{ctx, this->fd, buf, nbytes, flag};
}
socket_t::send_awaiter socket_t::send(context_t &ctx, void *buf, uint32_t nbytes, int flag) const  {
  return send_awaiter{ctx, this->fd, buf, nbytes, flag};
}

socket_t::close_awaiter socket_t::close(context_t &ctx) const {
  return close_awaiter{ctx, fd};
}
} // cornet