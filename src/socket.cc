#include "core/socket.h"

namespace cornet {
socklen_t to_address(const std::string& ip, const std::string& port, sockaddr_storage& addr, int family, int type, int flag) {
  struct addrinfo hints{};
  struct addrinfo* res;

  hints.ai_family = family;
  hints.ai_socktype = type;
  hints.ai_flags = flag;

  int n = getaddrinfo(ip.c_str(), port.c_str(), &hints, &res);
  if (n != 0) {
    SPDLOG_ERROR("get {}:{} address info failed, with error:{}", ip, port, gai_strerror(n));
    return 0;
  }
  if (res) {
    socklen_t ret = res->ai_addrlen;
    std::memcpy(&addr, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);
    return ret;
  }
  return 0;
}
socklen_t to_address(const std::string& path, sockaddr_storage& addr){
  auto* u = (sockaddr_un*)&addr;
  if(path.size() > sizeof(u->sun_path)) {
    SPDLOG_ERROR("path length is overflow unix local socket permit range.");
    return 0;
  }
  u->sun_family = AF_UNIX;
  std::strncpy(u->sun_path, path.c_str(), sizeof(u->sun_path) - 1);
  return sizeof(sockaddr_un);
}
socket_t::socket_t(int fd) : fd(fd) {
  if (fd < 0) {
    CORNET_FATAL("socket bad file descriptor");
  }
}
socket_t::~socket_t() {
  if (fd != -1) {
    ::close(fd);
    fd = -1;
  }
}
socket_t::socket_t(socket_t&& s) noexcept {
  if (this != &s) {
    this->fd = s.fd;
    s.fd = -1;
  }
}
socket_t& socket_t::operator=(socket_t&& s) noexcept {
  if (this != &s) {
    if (this->fd != -1) {
      ::close(this->fd);
    }
    this->fd = s.fd;
    s.fd = -1;
  }
  return *this;
}
int socket_t::native_fd() const {
  return fd;
}
void socket_t::address_reuse(bool on) const {
  int reuse = on ? 1 : 0;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (void*)&reuse, sizeof(reuse));
}
void socket_t::port_reuse(bool on) const {
  int reuse = on ? 1 : 0;
  setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, (void*)&reuse, sizeof(reuse));
}

socket_t::close_awaiter::close_awaiter(context_t& ctx, int fd) : fd_(fd) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<close_awaiter*>(self);
    io_uring_prep_close(sqe, t->fd_);
  };
}

socket_t::accept_awaiter::accept_awaiter(context_t& ctx, int fd, sockaddr* addr, socklen_t* len, int flag)
  : fd_(fd), addr_(addr), addr_len_(len), flag_(flag) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<accept_awaiter*>(self);
    io_uring_prep_accept(sqe, t->fd_, t->addr_, t->addr_len_, t->flag_);
  };
}

socket_t::connect_awaiter::connect_awaiter(context_t& ctx, int fd, const std::string& ip, const std::string& port, int domain, int type)
  : fd_(fd) {
  this->ctx = &ctx;
  socklen_ = to_address(ip, port, addr, domain, type, AI_ADDRCONFIG | AI_V4MAPPED);
  if (socklen_ == 0) {
    this->completed = true;
    this->value = -EINVAL;
    return;
  }
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<connect_awaiter*>(self);
    io_uring_prep_connect(sqe, t->fd_, (sockaddr*)&t->addr, t->socklen_);
  };
}
socket_t::connect_awaiter::connect_awaiter(context_t& ctx, int fd, const std::string& path)
  : fd_(fd) {
  this->ctx = &ctx;
  socklen_ = to_address(path, addr);
  if (socklen_ == 0) {
    this->completed = true;
    this->value = -EINVAL;
    return;
  }
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<connect_awaiter*>(self);
    io_uring_prep_connect(sqe, t->fd_, (sockaddr*)&t->addr, t->socklen_);
  };
}

socket_t::recv_awaiter::recv_awaiter(context_t& ctx, int fd, void* buf, uint32_t nbytes, int flag)
  : fd_(fd), buf_(buf), nbytes_(nbytes), flag_(flag) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<recv_awaiter*>(self);
    io_uring_prep_recv(sqe, t->fd_, t->buf_, t->nbytes_, t->flag_);
  };
}

socket_t::send_awaiter::send_awaiter(context_t& ctx, int fd, void* buf, uint32_t nbytes, int flag)
  : fd_(fd), buf_(buf), nbytes_(nbytes), flag_(flag) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<send_awaiter*>(self);
    io_uring_prep_send(sqe, t->fd_, t->buf_, t->nbytes_, t->flag_);
  };
}

socket_t::sendmsg_awaiter::sendmsg_awaiter(context_t &ctx, int fd, struct msghdr *msg, int flags)
  : fd_(fd), msg_(msg), flags_(flags) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<sendmsg_awaiter*>(self);
    io_uring_prep_sendmsg(sqe, t->fd_, t->msg_, t->flags_);
  };
}

socket_t::recvmsg_awaiter::recvmsg_awaiter(context_t &ctx, int fd, struct msghdr *msg, int flags)
  : fd_(fd), msg_(msg), flags_(flags) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<recvmsg_awaiter*>(self);
    io_uring_prep_recvmsg(sqe, t->fd_, t->msg_, t->flags_);
  };
}

socket_t::close_awaiter socket_t::close(context_t& ctx) const {
  return close_awaiter{ctx, fd};
}
socket_t::connect_awaiter socket_t::connect(context_t& ctx, const std::string& ip, const std::string& port) const {
  return connect_awaiter{ctx, fd, ip, port, domain, type};
}
socket_t::recv_awaiter socket_t::recv(context_t& ctx, void* buf, uint32_t nbytes, int flag) const {
  return recv_awaiter{ctx, fd, buf, nbytes, flag};
}
socket_t::send_awaiter socket_t::send(context_t& ctx, void* buf, uint32_t nbytes, int flag) const {
  return send_awaiter{ctx, fd, buf, nbytes, flag};
}
expected<void> socket_t::bind(const std::string& address, const std::string& port) const {
  sockaddr_storage addr{};
  socklen_t socklen;
  if (this->domain == AF_UNIX) {
    ::unlink(address.c_str());
    socklen = to_address(address, addr);
    if (socklen == 0) {
      return unexpected(EINVAL);
    }
  } else {
    socklen = to_address(address, port, addr, domain, type, AI_PASSIVE | AI_NUMERICHOST);
    if (socklen == 0) {
      return unexpected(EINVAL);
    }
  }

  if (::bind(fd, (sockaddr*)&addr, socklen) < 0) {
    return unexpected(errno);
  }
  return {};
}

namespace tcp {
socket_t::socket_t(int fd) : cornet::socket_t(fd) {
  type = SOCK_STREAM;
  protocol = IPPROTO_TCP;
}
expected<void> socket_t::listen(const std::string& address, const std::string& port) const {
  auto ret = bind(address, port);
  if (!ret) return ret;
  if (::listen(fd, 2048) < 0) {
    return unexpected(errno);
  }
  return {};
}
socket_t::accept_awaiter socket_t::accept(context_t& ctx, sockaddr* addr, socklen_t* socklen, int flag) const {
  return accept_awaiter{ctx, fd, addr, socklen, flag};
}
coro_t<expected<socket_t>> socket_t::accept(context_t &ctx, int flag) const {
  sockaddr_storage addr{};
  socklen_t len{};
  auto result = co_await accept(ctx, (sockaddr*)&addr, &len, flag);
  if (!result) {
    co_return unexpected(result.error());
  }
  auto socket = tcp::socket_t(*result);
  socket.domain = addr.ss_family;
  co_return socket;
}
} // cornet::net::tcp
namespace udp {
socket_t::socket_t(int fd) : cornet::socket_t(fd) {
  type = SOCK_DGRAM;
  protocol = IPPROTO_UDP;
}
coro_t<expected<int>> socket_t::sendto(context_t &ctx,void *buf,size_t nbytes, sockaddr *addr, socklen_t socklen,int flag) const {
  struct iovec iov{};
  struct msghdr msg{};

  iov.iov_base = buf;
  iov.iov_len = nbytes;
  msg.msg_name = addr;
  msg.msg_namelen = socklen;
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;

  co_return co_await sendmsg_awaiter(ctx, fd, &msg, flag);
}
coro_t<expected<int>> socket_t::recvfrom(context_t &ctx,void *buf,size_t nbytes, sockaddr *addr, socklen_t *socklen,int flag) const {
  struct iovec iov{};
  struct msghdr msg{};

  iov.iov_base = buf;
  iov.iov_len = nbytes;
  msg.msg_name = addr;
  msg.msg_namelen = *socklen;
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;
  auto ret = co_await recvmsg_awaiter(ctx, fd, &msg, flag);
  if (!ret) {
    co_return unexpected(ret.error());
  }
  *socklen = msg.msg_namelen;
  co_return *ret;
}
auto socket_t::sendmsg(context_t &ctx, struct msghdr *msg, int flags) const {
  return sendmsg_awaiter{ctx, fd, msg, flags};
}
auto socket_t::recvmsg(context_t &ctx, struct msghdr *msg, int flags) const {
  return recvmsg_awaiter{ctx, fd, msg, flags};
}
} // cornet::net::udp
} // cornet

namespace cornet::tcp::local {
socket_t::socket_t() : cornet::tcp::socket_t(::socket(AF_UNIX, SOCK_STREAM, 0)) {
  domain = AF_UNIX;
}
socket_t::socket_t(int fd) : cornet::tcp::socket_t(fd) {
  domain = AF_UNIX;
}
} // cornet::tcp::local

namespace cornet::tcp::v4 {
socket_t::socket_t() : cornet::tcp::socket_t(::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) {
  domain = AF_INET;
}
socket_t::socket_t(int fd) : cornet::tcp::socket_t(fd) {
  domain = AF_INET;
}
} // cornet::tcp::v4

namespace cornet::tcp::v6 {
socket_t::socket_t() : cornet::tcp::socket_t(::socket(AF_INET6, SOCK_STREAM, IPPROTO_TCP)) {
  domain = AF_INET6;
}
socket_t::socket_t(int fd) : cornet::tcp::socket_t(fd) {
  domain = AF_INET6;
}
void socket_t::v6_only(bool on) const {
  int flag = on ? 1 : 0;
  int ret = setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &flag, sizeof(flag));
  if (ret < 0) {
    SPDLOG_WARN("set socket to {} failed with error: {}", flag ? "v6 only" : "v4 & v6", strerror(errno));
  }
}
} // cornet::tcp::v6

namespace cornet::udp::local {
socket_t::socket_t() : cornet::udp::socket_t(::socket(AF_UNIX, SOCK_DGRAM, 0)) {
  domain = AF_UNIX;
}
socket_t::socket_t(int fd) : cornet::udp::socket_t(fd) {
  domain = AF_UNIX;
}
} // cornet::udp::local

namespace cornet::udp::v4 {
socket_t::socket_t() : cornet::udp::socket_t(::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) {
  domain = AF_INET;
}
socket_t::socket_t(int fd) : cornet::udp::socket_t(fd) {
  domain = AF_INET;
}
} // cornet::udp::v4

namespace cornet::udp::v6 {
socket_t::socket_t() : cornet::udp::socket_t(::socket(AF_INET6, SOCK_DGRAM, IPPROTO_UDP)) {
  domain = AF_INET6;
}
socket_t::socket_t(int fd) : cornet::udp::socket_t(fd) {
  domain = AF_INET6;
}
} // cornet::udp::v6
