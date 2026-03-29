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
socket_t::close_awaiter::close_awaiter(context_t& ctx, int fd) : utask_t(ctx) {
  sqe.prep_close(fd).with_data(this);
}
socket_t::accept_awaiter::accept_awaiter(context_t& ctx, int fd, sockaddr* addr, socklen_t* len, int flag) : utask_t(ctx) {
  sqe.prep_accept(fd, addr, len, flag).with_data(this);
}
socket_t::connect_awaiter::connect_awaiter(context_t& ctx, int fd, const std::string& ip, const std::string& port, int domain, int type) : utask_t(ctx) {
  socklen_t socklen = to_address(ip, port, addr, domain, type, AI_ADDRCONFIG | AI_V4MAPPED);
  if (socklen == 0) {
    CORNET_FATAL("failed to get address info on {}:{}", ip, port);
  }
  sqe.prep_connect(fd, (sockaddr*)&addr, socklen).with_data(this);
}
socket_t::connect_awaiter::connect_awaiter(context_t& ctx, int fd, const std::string& path) : utask_t(ctx) {
  socklen_t socklen = to_address(path, addr);
  if (socklen == 0) {
    CORNET_FATAL("failed to get address info on {}", path);
  }
  sqe.prep_connect(fd, (sockaddr*)&addr, socklen).with_data(this);
}
socket_t::recv_awaiter::recv_awaiter(context_t& ctx, int fd, void* buf, uint32_t nbytes, int flag) : utask_t(ctx) {
  sqe.prep_recv(fd, buf, nbytes, flag).with_data(this);
}
socket_t::send_awaiter::send_awaiter(context_t& ctx, int fd, void* buf, uint32_t nbytes, int flag) : utask_t(ctx) {
  sqe.prep_send(fd, buf, nbytes, flag).with_data(this);
}
socket_t::sendmsg_awaiter::sendmsg_awaiter(context_t &ctx, int fd, struct msghdr *msg, int flags) : utask_t(ctx) {
  sqe.prep_sendmsg(fd, msg, flags).with_data(this);
}
socket_t::recvmsg_awaiter::recvmsg_awaiter(context_t &ctx, int fd, struct msghdr *msg, int flags) : utask_t(ctx) {
  sqe.prep_recvmsg(fd, msg, flags).with_data(this);
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
bool socket_t::bind(const std::string& address, const std::string& port) const {
  sockaddr_storage addr{};
  socklen_t socklen;
  if (this->domain == AF_UNIX) {
    ::unlink(address.c_str());
    socklen = to_address(address, addr);
    if (socklen == 0) {
      CORNET_FATAL("failed to get address info on {}", address);
    }
  } else {
    socklen = to_address(address, port, addr, domain, type, AI_PASSIVE | AI_NUMERICHOST);
    if (socklen == 0) {
      CORNET_FATAL("failed to get address info on {}:{}", address, port);
    }
  }

  if (::bind(fd, (sockaddr*)&addr, socklen) < 0) {
    SPDLOG_ERROR("bind to {}:{} failed with error: {}", address, port, strerror(errno));
    return false;
  }
  return true;
}

namespace tcp {
socket_t::socket_t(int fd) : cornet::socket_t(fd) {
  type = SOCK_STREAM;
  protocol = IPPROTO_TCP;
}
bool socket_t::listen(const std::string& address, const std::string& port) const {
  if (!bind(address, port)) return false;
  if (::listen(fd, 2048) < 0) {
    if (domain == AF_UNIX) {
      SPDLOG_ERROR("listen on {} failed with error: {}", address, strerror(errno));
    } else {
      SPDLOG_ERROR("listen on {}:{} failed with error: {}", address, port, strerror(errno));
    }
    return false;
  }
  return true;
}
socket_t::accept_awaiter socket_t::accept(context_t& ctx, sockaddr* addr, socklen_t* socklen, int flag) const {
  return accept_awaiter{ctx, fd, addr, socklen, flag};
}
coro_t<socket_t> socket_t::accept(context_t &ctx, int flag) const {
  sockaddr_storage addr{};
  socklen_t len{};
  int client_fd = co_await accept(ctx, (sockaddr*)&addr, &len, flag);
  auto socket = tcp::socket_t(client_fd);
  socket.domain = addr.ss_family;
  co_return socket;
}
} // cornet::net::tcp
namespace udp {
socket_t::socket_t(int fd) : cornet::socket_t(fd) {
  type = SOCK_DGRAM;
  protocol = IPPROTO_UDP;
}
coro_t<int> socket_t::sendto(context_t &ctx,void *buf,size_t nbytes, sockaddr *addr, socklen_t socklen,int flag) const {
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
coro_t<int> socket_t::recvfrom(context_t &ctx,void *buf,size_t nbytes, sockaddr *addr, socklen_t *socklen,int flag) const {
  struct iovec iov{};
  struct msghdr msg{};

  iov.iov_base = buf;
  iov.iov_len = nbytes;
  msg.msg_name = addr;
  msg.msg_namelen = *socklen;
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;
  int ret = co_await recvmsg_awaiter(ctx, fd, &msg, flag);
  *socklen = msg.msg_namelen;
  co_return ret;
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