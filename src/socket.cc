#include "cornet/net/socket.h"

#include <spdlog/spdlog.h>

#include "cornet/scheduling/context.h"

namespace cornet {

expected<socklen_t> to_address(std::string_view ip, uint16_t port, sockaddr_storage& addr, int family, int type, int flag) {
  struct addrinfo hints{};
  struct addrinfo* res;

  hints.ai_family = family;
  hints.ai_socktype = type;
  hints.ai_flags = flag;

  std::string ip_str(ip);
  std::string port_str = std::to_string(port);

  int n = getaddrinfo(ip_str.c_str(), port_str.c_str(), &hints, &res);
  if (n != 0) {
    return unexpected(n, error_domain::Resolve);
  }
  if (res) {
    socklen_t ret = res->ai_addrlen;
    std::memcpy(&addr, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);
    return ret;
  }
  return 0;
}

expected<socklen_t> to_address(std::string_view path, sockaddr_storage& addr) {
  auto* u = (sockaddr_un*)&addr;
  if (path.size() > sizeof(u->sun_path) - 1) {
    SPDLOG_ERROR("path length overflow unix local socket permit range.");
    return unexpected(ENAMETOOLONG);
  }
  u->sun_family = AF_UNIX;
  std::memcpy(u->sun_path, path.data(), path.size());
  u->sun_path[path.size()] = '\0';
  return sizeof(sockaddr_un);
}

coro_t<expected<resolved_address>> resolve(context_t& ctx, std::string_view host, uint16_t port, int family, int type) {
  std::string host_str(host);
  std::string port_str = std::to_string(port);

  // The closure is a named local, not a temporary inside the co_await expression: gcc
  // (11 and 12) gives such a temporary two frame slots and destroys the one it never
  // constructed, so the captured strings get freed from a stale interior pointer.
  auto job = [host_str, port_str, family, type]() -> expected<resolved_address> {
    struct addrinfo hints{};
    struct addrinfo* res;
    hints.ai_family = family;
    hints.ai_socktype = type;
    hints.ai_flags = AI_ADDRCONFIG | AI_V4MAPPED;

    int n = getaddrinfo(host_str.c_str(), port_str.c_str(), &hints, &res);
    if (n != 0) {
      return unexpected(n, error_domain::Resolve);
    }
    if (!res) {
      return unexpected(EAI_NONAME, error_domain::Resolve);
    }
    resolved_address r;
    r.socklen = res->ai_addrlen;
    std::memcpy(&r.addr, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);
    return r;
  };

  auto result = co_await ctx.async(std::move(job));

  co_return result;
}

socket_t::socket_t(int fd) : fd(fd) {
  if (fd < 0) {
    SPDLOG_ERROR("socket bad file descriptor");
    throw std::runtime_error("socket bad file descriptor");
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
int socket_t::release() {
  int released = fd;
  fd = -1;
  return released;
}
void socket_t::address_reuse(bool on) const {
  int reuse = on ? 1 : 0;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (void*)&reuse, sizeof(reuse));
}
void socket_t::port_reuse(bool on) const {
  int reuse = on ? 1 : 0;
  setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, (void*)&reuse, sizeof(reuse));
}

socket_t::accept_awaiter::accept_awaiter(context_t& ctx, int fd, sockaddr* addr, socklen_t* len, int flag)
  : fd_(fd), addr_(addr), addr_len_(len), flag_(flag) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<accept_awaiter*>(self);
    io_uring_prep_accept(sqe, t->fd_, t->addr_, t->addr_len_, t->flag_);
  };
}
socket_t::connect_awaiter::connect_awaiter(context_t& ctx, int fd, std::string_view path)
  : fd_(fd) {
  this->ctx = &ctx;
  auto socklen = to_address(path, addr);
  if (!socklen) {
    this->completed = true;
    this->value = -socklen.error().code;
    return;
  }
  socklen_ = *socklen;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<connect_awaiter*>(self);
    io_uring_prep_connect(sqe, t->fd_, (sockaddr*)&t->addr, t->socklen_);
  };
}

socket_t::connect_awaiter::connect_awaiter(context_t& ctx, int fd, const resolved_address& resolved)
  : fd_(fd) {
  this->ctx = &ctx;
  addr = resolved.addr;
  socklen_ = resolved.socklen;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<connect_awaiter*>(self);
    io_uring_prep_connect(sqe, t->fd_, (sockaddr*)&t->addr, t->socklen_);
  };
}

socket_t::recv_awaiter::recv_awaiter(context_t& ctx, int fd, void* buf, size_t nbytes, int flag)
  : fd_(fd), buf_(buf), nbytes_(nbytes), flag_(flag) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<recv_awaiter*>(self);
    io_uring_prep_recv(sqe, t->fd_, t->buf_, t->nbytes_, t->flag_);
  };
}

socket_t::send_awaiter::send_awaiter(context_t& ctx, int fd, const void* buf, size_t nbytes, int flag)
  : fd_(fd), buf_(buf), nbytes_(nbytes), flag_(flag) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<send_awaiter*>(self);
    io_uring_prep_send(sqe, t->fd_, t->buf_, t->nbytes_, t->flag_);
  };
}

socket_t::sendmsg_awaiter::sendmsg_awaiter(context_t& ctx, int fd, struct msghdr *msg, int flags)
  : fd_(fd), msg_(msg), flags_(flags) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<sendmsg_awaiter*>(self);
    io_uring_prep_sendmsg(sqe, t->fd_, t->msg_, t->flags_);
  };
}

socket_t::recvmsg_awaiter::recvmsg_awaiter(context_t& ctx, int fd, struct msghdr *msg, int flags)
  : fd_(fd), msg_(msg), flags_(flags) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<recvmsg_awaiter*>(self);
    io_uring_prep_recvmsg(sqe, t->fd_, t->msg_, t->flags_);
  };
}

socket_t::writev_awaiter::writev_awaiter(context_t& ctx, int fd, const struct iovec* iov, size_t iov_len, int flags)
  : fd_(fd), flags_(flags) {
  this->ctx = &ctx;
  msg_ = {};
  // msg_iov is non-const in the ABI, but sendmsg never writes through it
  msg_.msg_iov = const_cast<struct iovec*>(iov);
  msg_.msg_iovlen = iov_len;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<writev_awaiter*>(self);
    io_uring_prep_sendmsg(sqe, t->fd_, &t->msg_, t->flags_);
  };
}

socket_t::sendto_awaiter::sendto_awaiter(context_t& ctx, int fd, void* buf, size_t nbytes, sockaddr* addr, socklen_t socklen, int flag)
  : fd_(fd), flag_(flag) {
  this->ctx = &ctx;
  iov_ = {buf, nbytes};
  msg_ = {};
  msg_.msg_name = addr;
  msg_.msg_namelen = socklen;
  msg_.msg_iov = &iov_;
  msg_.msg_iovlen = 1;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<sendto_awaiter*>(self);
    t->msg_.msg_iov = &t->iov_;
    io_uring_prep_sendmsg(sqe, t->fd_, &t->msg_, t->flag_);
  };
}

socket_t::recvfrom_awaiter::recvfrom_awaiter(context_t& ctx, int fd, void* buf, size_t nbytes, sockaddr* addr, socklen_t* socklen, int flag)
  : fd_(fd), user_socklen_(socklen), flag_(flag) {
  this->ctx = &ctx;
  iov_ = {buf, nbytes};
  msg_ = {};
  msg_.msg_name = addr;
  msg_.msg_namelen = *socklen;
  msg_.msg_iov = &iov_;
  msg_.msg_iovlen = 1;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<recvfrom_awaiter*>(self);
    t->msg_.msg_iov = &t->iov_;
    io_uring_prep_recvmsg(sqe, t->fd_, &t->msg_, t->flag_);
  };
}

cornet::close_awaiter socket_t::close(context_t& ctx) {
  close_awaiter awaiter{ctx, fd};
  fd = -1;
  return awaiter;
}
cornet::shutdown_awaiter socket_t::shutdown(context_t& ctx, int how) const {
  return shutdown_awaiter{ctx, fd, how};
}
ccoro_t<expected<void>> socket_t::connect(context_t& ctx, std::string_view host, uint16_t port) const {
  // fast path: numeric IP address, no DNS needed
  resolved_address fast{};
  auto socklen = to_address(host, port, fast.addr, domain, type, AI_NUMERICHOST);
  if (socklen) {
    fast.socklen = socklen.value();
    co_return co_await connect(ctx, fast);
  }

  // slow path: hostname, async DNS resolve via thread pool
  auto resolved = co_await resolve(ctx, host, port, domain, type);
  if (!resolved) {
    co_return unexpected(resolved.error());
  }
  co_return co_await connect(ctx, *resolved);
}

coro_t<expected<void>> socket_t::connect(context_t& ctx, std::string_view host, uint16_t port, canceler_t& canceler) const {
  // fast path: numeric IP address, no DNS needed
  resolved_address fast{};
  auto socklen = to_address(host, port, fast.addr, domain, type, AI_NUMERICHOST);
  if (socklen) {
    fast.socklen = socklen.value();
    co_return co_await with_cancel(ctx, connect(ctx, fast), canceler);
  }

  // slow path: hostname, async DNS resolve via thread pool
  auto resolved = co_await resolve(ctx, host, port, domain, type);
  if (!resolved) {
    co_return unexpected(resolved.error());
  }
  if (canceler.is_cancelled()) {
    co_return unexpected(ECANCELED);
  }
  co_return co_await with_cancel(ctx, connect(ctx, *resolved), canceler);
}
socket_t::connect_awaiter socket_t::connect(context_t& ctx, const resolved_address& resolved) const {
  return connect_awaiter{ctx, fd, resolved};
}
socket_t::recv_awaiter socket_t::recv(context_t& ctx, void* buf, size_t nbytes, int flag) const {
  return recv_awaiter{ctx, fd, buf, nbytes, flag};
}
socket_t::send_awaiter socket_t::send(context_t& ctx, const void* buf, size_t nbytes, int flag) const {
  return send_awaiter{ctx, fd, buf, nbytes, flag};
}
socket_t::writev_awaiter socket_t::writev(context_t& ctx, const struct iovec* iov, size_t iov_len, int flags) const {
  return writev_awaiter{ctx, fd, iov, iov_len, flags};
}
socket_t::sendmsg_awaiter socket_t::sendmsg(context_t& ctx, struct msghdr* msg, int flags) const {
  return sendmsg_awaiter{ctx, fd, msg, flags};
}
socket_t::recvmsg_awaiter socket_t::recvmsg(context_t& ctx, struct msghdr* msg, int flags) const {
  return recvmsg_awaiter{ctx, fd, msg, flags};
}
expected<void> socket_t::bind(std::string_view address, uint16_t port) const {
  sockaddr_storage addr{};
  socklen_t socklen;
  if (this->domain == AF_UNIX) {
    ::unlink(std::string(address).c_str());
    auto socklen_ = to_address(address, addr);
    if (!socklen_) {
      return unexpected(socklen_.error());
    }
    socklen = socklen_.value();
  } else {
    auto socklen_ = to_address(address, port, addr, domain, type, AI_PASSIVE | AI_NUMERICHOST);
    if (!socklen_) {
      return unexpected(socklen_.error());
    }
    socklen = socklen_.value();
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
expected<void> socket_t::listen(std::string_view address, uint16_t port) const {
  auto ret = bind(address, port);
  if (!ret) return ret;
  if (::listen(fd, 2048) < 0) {
    return unexpected(errno);
  }
  return {};
}
socket_t::tcp_accept_awaiter::tcp_accept_awaiter(context_t& ctx, int fd, int flag)
  : fd_(fd), flag_(flag) {
  this->ctx = &ctx;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<tcp_accept_awaiter*>(self);
    io_uring_prep_accept(sqe, t->fd_, (sockaddr*)&t->addr_, &t->len_, t->flag_);
  };
}
socket_t::accept_awaiter socket_t::accept(context_t& ctx, sockaddr* addr, socklen_t* socklen, int flag) const {
  return accept_awaiter{ctx, fd, addr, socklen, flag};
}
socket_t::tcp_accept_awaiter socket_t::accept(context_t& ctx, int flag) const {
  return tcp_accept_awaiter{ctx, fd, flag};
}
} // cornet::tcp

namespace udp {
socket_t::socket_t(int fd) : cornet::socket_t(fd) {
  type = SOCK_DGRAM;
  protocol = IPPROTO_UDP;
}
socket_t::sendto_awaiter socket_t::sendto(context_t& ctx, void *buf, size_t nbytes, sockaddr *addr, socklen_t socklen, int flag) const {
  return sendto_awaiter{ctx, fd, buf, nbytes, addr, socklen, flag};
}
socket_t::recvfrom_awaiter socket_t::recvfrom(context_t& ctx, void *buf, size_t nbytes, sockaddr *addr, socklen_t *socklen, int flag) const {
  return recvfrom_awaiter{ctx, fd, buf, nbytes, addr, socklen, flag};
}
} // cornet::udp
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
