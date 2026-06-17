#include "core/socket.h"
#include "core/combinators.h"

namespace cornet {

socklen_t to_address(std::string_view ip, uint16_t port, sockaddr_storage& addr, int family, int type, int flag) {
  struct addrinfo hints{};
  struct addrinfo* res;

  hints.ai_family = family;
  hints.ai_socktype = type;
  hints.ai_flags = flag;

  std::string ip_str(ip);
  std::string port_str = std::to_string(port);

  int n = getaddrinfo(ip_str.c_str(), port_str.c_str(), &hints, &res);
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

socklen_t to_address(std::string_view path, sockaddr_storage& addr) {
  auto* u = (sockaddr_un*)&addr;
  if (path.size() > sizeof(u->sun_path) - 1) {
    SPDLOG_ERROR("path length overflow unix local socket permit range.");
    return 0;
  }
  u->sun_family = AF_UNIX;
  std::memcpy(u->sun_path, path.data(), path.size());
  u->sun_path[path.size()] = '\0';
  return sizeof(sockaddr_un);
}

coro_t<expected<resolved_address>> resolve(std::string_view host, uint16_t port, int family, int type) {
  auto& ctx = context_t::current();
  std::string host_str(host);
  std::string port_str = std::to_string(port);

  auto result = co_await ctx.async([host_str, port_str, family, type]() -> expected<resolved_address> {
    struct addrinfo hints{};
    struct addrinfo* res;
    hints.ai_family = family;
    hints.ai_socktype = type;
    hints.ai_flags = AI_ADDRCONFIG | AI_V4MAPPED;

    int n = getaddrinfo(host_str.c_str(), port_str.c_str(), &hints, &res);
    if (n != 0) {
      return unexpected(n, error_domain::resolve);
    }
    if (!res) {
      return unexpected(EAI_NONAME, error_domain::resolve);
    }
    resolved_address r;
    r.socklen = res->ai_addrlen;
    std::memcpy(&r.addr, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);
    return r;
  });

  co_return result;
}

socket_t::socket_t(int fd) : fd(fd) {
  if (fd < 0) {
    CORNET_FATAL("socket bad file descriptor");
  }
}
socket_t::~socket_t() {
  if (fd != -1) {
    async_close(fd);
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
      auto& ctx = context_t::current();
      if (std::this_thread::get_id() == ctx.owner_thread()) {
        async_close(this->fd);
      } else {
        ::close(this->fd);
      }
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

socket_t::accept_awaiter::accept_awaiter(int fd, sockaddr* addr, socklen_t* len, int flag)
  : fd_(fd), addr_(addr), addr_len_(len), flag_(flag) {
  this->ctx = &context_t::current();
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<accept_awaiter*>(self);
    io_uring_prep_accept(sqe, t->fd_, t->addr_, t->addr_len_, t->flag_);
  };
}

socket_t::connect_awaiter::connect_awaiter(int fd, std::string_view ip, uint16_t port, int domain, int type)
  : fd_(fd) {
  this->ctx = &context_t::current();
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
socket_t::connect_awaiter::connect_awaiter(int fd, std::string_view path)
  : fd_(fd) {
  this->ctx = &context_t::current();
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

socket_t::connect_awaiter::connect_awaiter(int fd, const resolved_address& resolved)
  : fd_(fd) {
  this->ctx = &context_t::current();
  addr = resolved.addr;
  socklen_ = resolved.socklen;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<connect_awaiter*>(self);
    io_uring_prep_connect(sqe, t->fd_, (sockaddr*)&t->addr, t->socklen_);
  };
}

socket_t::recv_awaiter::recv_awaiter(int fd, void* buf, size_t nbytes, int flag)
  : fd_(fd), buf_(buf), nbytes_(nbytes), flag_(flag) {
  this->ctx = &context_t::current();
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<recv_awaiter*>(self);
    io_uring_prep_recv(sqe, t->fd_, t->buf_, t->nbytes_, t->flag_);
  };
}

socket_t::send_awaiter::send_awaiter(int fd, const void* buf, size_t nbytes, int flag)
  : fd_(fd), buf_(buf), nbytes_(nbytes), flag_(flag) {
  this->ctx = &context_t::current();
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<send_awaiter*>(self);
    io_uring_prep_send(sqe, t->fd_, t->buf_, t->nbytes_, t->flag_);
  };
}

socket_t::sendmsg_awaiter::sendmsg_awaiter(int fd, struct msghdr *msg, int flags)
  : fd_(fd), msg_(msg), flags_(flags) {
  this->ctx = &context_t::current();
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<sendmsg_awaiter*>(self);
    io_uring_prep_sendmsg(sqe, t->fd_, t->msg_, t->flags_);
  };
}

socket_t::recvmsg_awaiter::recvmsg_awaiter(int fd, struct msghdr *msg, int flags)
  : fd_(fd), msg_(msg), flags_(flags) {
  this->ctx = &context_t::current();
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<recvmsg_awaiter*>(self);
    io_uring_prep_recvmsg(sqe, t->fd_, t->msg_, t->flags_);
  };
}

socket_t::sendto_awaiter::sendto_awaiter(int fd, void* buf, size_t nbytes, sockaddr* addr, socklen_t socklen, int flag)
  : fd_(fd) {
  this->ctx = &context_t::current();
  iov_ = {buf, nbytes};
  msg_ = {};
  msg_.msg_name = addr;
  msg_.msg_namelen = socklen;
  msg_.msg_iov = &iov_;
  msg_.msg_iovlen = 1;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<sendto_awaiter*>(self);
    t->msg_.msg_iov = &t->iov_;
    io_uring_prep_sendmsg(sqe, t->fd_, &t->msg_, 0);
  };
}

socket_t::recvfrom_awaiter::recvfrom_awaiter(int fd, void* buf, size_t nbytes, sockaddr* addr, socklen_t* socklen, int flag)
  : fd_(fd), user_socklen_(socklen) {
  this->ctx = &context_t::current();
  iov_ = {buf, nbytes};
  msg_ = {};
  msg_.msg_name = addr;
  msg_.msg_namelen = *socklen;
  msg_.msg_iov = &iov_;
  msg_.msg_iovlen = 1;
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<recvfrom_awaiter*>(self);
    t->msg_.msg_iov = &t->iov_;
    io_uring_prep_recvmsg(sqe, t->fd_, &t->msg_, 0);
  };
}

cornet::close_awaiter socket_t::close() const {
  return close_awaiter{fd};
}
cornet::shutdown_awaiter socket_t::shutdown(int how) const {
  return shutdown_awaiter{fd, how};
}
coro_t<expected<void>> socket_t::connect(std::string_view host, uint16_t port) const {
  // fast path: numeric IP address, no DNS needed
  resolved_address fast{};
  fast.socklen = to_address(host, port, fast.addr, domain, type, AI_NUMERICHOST);
  if (fast.socklen > 0) {
    co_return co_await connect(fast);
  }

  // slow path: hostname, async DNS resolve via thread pool
  auto resolved = co_await resolve(host, port, domain, type);
  if (!resolved) {
    co_return unexpected(resolved.error());
  }
  co_return co_await connect(*resolved);
}

coro_t<expected<void>> socket_t::connect(std::string_view host, uint16_t port, canceler_t& canceler) const {
  // fast path: numeric IP address, no DNS needed
  resolved_address fast{};
  fast.socklen = to_address(host, port, fast.addr, domain, type, AI_NUMERICHOST);
  if (fast.socklen > 0) {
    co_return co_await with_cancel(connect(fast), canceler);
  }

  // slow path: hostname, async DNS resolve via thread pool
  auto resolved = co_await resolve(host, port, domain, type);
  if (!resolved) {
    co_return unexpected(resolved.error());
  }
  if (canceler.is_cancelled()) {
    co_return unexpected(ECANCELED);
  }
  co_return co_await with_cancel(connect(*resolved), canceler);
}

coro_t<expected<void>> socket_t::connect(std::string_view host, uint16_t port, std::chrono::nanoseconds timeout) const {
  // fast path: numeric IP address, no DNS needed
  resolved_address fast{};
  fast.socklen = to_address(host, port, fast.addr, domain, type, AI_NUMERICHOST);
  if (fast.socklen > 0) {
    auto ret = co_await with_timeout(connect(fast), timeout);
    if (!ret) co_return unexpected(ret.error());
    co_return {};
  }

  // slow path: hostname, async DNS resolve via thread pool
  auto resolved = co_await resolve(host, port, domain, type);
  if (!resolved) {
    co_return unexpected(resolved.error());
  }
  auto ret = co_await with_timeout(connect(*resolved), timeout);
  if (!ret) co_return unexpected(ret.error());
  co_return {};
}
socket_t::connect_awaiter socket_t::connect(const resolved_address& resolved) const {
  return connect_awaiter{fd, resolved};
}
socket_t::recv_awaiter socket_t::recv(void* buf, size_t nbytes, int flag) const {
  return recv_awaiter{fd, buf, nbytes, flag};
}
socket_t::send_awaiter socket_t::send(const void* buf, size_t nbytes, int flag) const {
  return send_awaiter{fd, buf, nbytes, flag};
}
expected<void> socket_t::bind(std::string_view address, uint16_t port) const {
  sockaddr_storage addr{};
  socklen_t socklen;
  if (this->domain == AF_UNIX) {
    ::unlink(std::string(address).c_str());
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
expected<void> socket_t::listen(std::string_view address, uint16_t port) const {
  auto ret = bind(address, port);
  if (!ret) return ret;
  if (::listen(fd, 2048) < 0) {
    return unexpected(errno);
  }
  return {};
}
socket_t::tcp_accept_awaiter::tcp_accept_awaiter(int fd, int flag)
  : fd_(fd), flag_(flag) {
  this->ctx = &context_t::current();
  this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
    auto* t = static_cast<tcp_accept_awaiter*>(self);
    io_uring_prep_accept(sqe, t->fd_, (sockaddr*)&t->addr_, &t->len_, t->flag_);
  };
}
socket_t::accept_awaiter socket_t::accept(sockaddr* addr, socklen_t* socklen, int flag) const {
  return accept_awaiter{fd, addr, socklen, flag};
}
socket_t::tcp_accept_awaiter socket_t::accept(int flag) const {
  return tcp_accept_awaiter{fd, flag};
}
} // cornet::tcp

namespace udp {
socket_t::socket_t(int fd) : cornet::socket_t(fd) {
  type = SOCK_DGRAM;
  protocol = IPPROTO_UDP;
}
socket_t::sendto_awaiter socket_t::sendto(void *buf, size_t nbytes, sockaddr *addr, socklen_t socklen, int flag) const {
  return sendto_awaiter{fd, buf, nbytes, addr, socklen, flag};
}
socket_t::recvfrom_awaiter socket_t::recvfrom(void *buf, size_t nbytes, sockaddr *addr, socklen_t *socklen, int flag) const {
  return recvfrom_awaiter{fd, buf, nbytes, addr, socklen, flag};
}
socket_t::sendmsg_awaiter socket_t::sendmsg(struct msghdr *msg, int flags) const {
  return sendmsg_awaiter{fd, msg, flags};
}
socket_t::recvmsg_awaiter socket_t::recvmsg(struct msghdr *msg, int flags) const {
  return recvmsg_awaiter{fd, msg, flags};
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
