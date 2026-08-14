#ifndef CORNET_SOCKET_H
#define CORNET_SOCKET_H

#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/un.h>

#include "cornet/coroutine/coro.h"
#include "cornet/coroutine/cancel.h"
#include "cornet/io_uring/awaiters.h"

namespace cornet {

struct canceler_t;

expected<socklen_t> to_address(std::string_view address, uint16_t port, sockaddr_storage& addr,
                int family = AF_UNSPEC, int type = SOCK_STREAM, int flag = AI_NUMERICHOST);
expected<socklen_t> to_address(std::string_view path, sockaddr_storage& addr);

/**
 * @brief resolve result containing resolved address and its length
 */
struct resolved_address {
  sockaddr_storage addr{};
  socklen_t socklen{0};

  explicit operator bool() const { return socklen > 0; }
};

/**
 * @brief async DNS resolve. Offloads getaddrinfo to thread pool to avoid blocking the event loop.
 * @param host hostname or IP address
 * @param port port number
 * @param family address family (AF_INET, AF_INET6, AF_UNSPEC)
 * @param type socket type (SOCK_STREAM, SOCK_DGRAM)
 * @return resolved address on success, error on failure
 */
coro_t<expected<resolved_address>> resolve(context_t& ctx, std::string_view host, uint16_t port,
                                         int family = AF_UNSPEC, int type = SOCK_STREAM);

class socket_t {
 public:
  /**
   * @brief accept awaiter for io_uring_prep_accept
   */
  struct accept_awaiter : utask_t {
    accept_awaiter(context_t& ctx, int fd, sockaddr* addr, socklen_t* addr_len, int flag);
   private:
    int fd_;
    sockaddr* addr_;
    socklen_t* addr_len_;
    int flag_;
  };

  /**
   * @brief connect awaiter for io_uring_prep_connect
   */
  struct connect_awaiter : utask_t {
    connect_awaiter(context_t& ctx, int fd, std::string_view ip, uint16_t port, int domain, int type);
    connect_awaiter(context_t& ctx, int fd, std::string_view path);
    connect_awaiter(context_t& ctx, int fd, const resolved_address& resolved);

    CORNET_NODISCARD CORNET_MAYBE_UNUSED expected<void> await_resume() const {
      if (value < 0) return unexpected(-value);
      return {};
    }
   private:
    int fd_;
    sockaddr_storage addr{};
    socklen_t socklen_{};
  };

  /**
   * @brief send awaiter for io_uring_prep_send
   */
  struct send_awaiter : utask_t {
    send_awaiter(context_t& ctx, int fd, const void* buf, size_t nbytes, int flag);
   private:
    int fd_;
    const void* buf_;
    size_t nbytes_;
    int flag_;
  };

  /**
   * @brief recv awaiter for io_uring_prep_recv
   */
  struct recv_awaiter : utask_t {
    recv_awaiter(context_t& ctx, int fd, void* buf, size_t nbytes, int flag);
   private:
    int fd_;
    void* buf_;
    size_t nbytes_;
    int flag_;
  };

  /**
   * @brief sendmsg awaiter for io_uring_prep_sendmsg
   */
  struct sendmsg_awaiter : utask_t {
    sendmsg_awaiter(context_t& ctx, int fd, struct msghdr* msg, int flags);
   private:
    int fd_;
    struct msghdr* msg_;
    int flags_;
  };

  /**
   * @brief recvmsg awaiter for io_uring_prep_recvmsg
   */
  struct recvmsg_awaiter : utask_t {
    recvmsg_awaiter(context_t& ctx, int fd, struct msghdr* msg, int flags);
   private:
    int fd_;
    struct msghdr* msg_;
    int flags_;
  };

  /**
   * @brief writev awaiter with embedded msghdr.
   * Gather-writes several buffers in one syscall. The msghdr is owned by the
   * awaiter, so callers never keep one alive across the CQE themselves; only
   * the iovec array must outlive the operation (typically a member of the
   * object driving the write loop, not a local).
   *
   * Usage: auto n = co_await sock.writev(ctx, iov, iov_len);
   */
  struct writev_awaiter : utask_t {
    writev_awaiter(context_t& ctx, int fd, const struct iovec* iov, size_t iov_len, int flags = 0);
   private:
    int fd_;
    int flags_;
    struct msghdr msg_;
  };

  /**
   * @brief sendto awaiter with embedded iovec/msghdr
   */
  struct sendto_awaiter : utask_t {
    sendto_awaiter(context_t& ctx, int fd, void* buf, size_t nbytes, sockaddr* addr, socklen_t socklen, int flag);
   private:
    int fd_;
    int flag_;
    struct iovec iov_;
    struct msghdr msg_;
  };

  /**
   * @brief recvfrom awaiter with embedded iovec/msghdr
   */
  struct recvfrom_awaiter : utask_t {
    recvfrom_awaiter(context_t& ctx, int fd, void* buf, size_t nbytes, sockaddr* addr, socklen_t* socklen, int flag);

    CORNET_NODISCARD CORNET_MAYBE_UNUSED expected<int> await_resume() const {
      if (value < 0) return unexpected(-value);
      *user_socklen_ = msg_.msg_namelen;
      return value;
    }
   private:
    int fd_;
    int flag_;
    struct iovec iov_;
    struct msghdr msg_;
    socklen_t* user_socklen_;
  };

  ~socket_t();
  socket_t(const socket_t&) = delete;
  socket_t& operator=(const socket_t&) = delete;
  socket_t(socket_t&&) noexcept;
  socket_t& operator=(socket_t&&) noexcept;

  /**
   * @brief get native file descriptor
   */
  int native_fd() const;
  /**
   * @brief give up ownership of the descriptor.
   *
   * Needed whenever the fd is handed to something else that will close it — a
   * fire-and-forget io_uring close, for example. Without this the socket's own
   * destructor closes it a second time, and by then the number may already have
   * been reassigned to a different connection.
   * @return the descriptor, now unowned; -1 if there was none
   */
  int release();
  /**
   * @brief set socket option reuse address
   */
  void address_reuse(bool on) const;
  /**
   * @brief set socket option reuse port
   */
  void port_reuse(bool on) const;
  inline int getpeername(sockaddr* addr, socklen_t* socklen) {
    return ::getpeername(fd, addr, socklen);
  }
  inline int getsockname(sockaddr* addr, socklen_t* socklen) {
    return ::getsockname(fd, addr, socklen);
  }
  /**
   * @brief async close socket
   */
  close_awaiter close(context_t& ctx);
  /**
   * @brief async shutdown socket (half-close).
   * @param ctx context for io_uring operations
   * @param how SHUT_RD, SHUT_WR, or SHUT_RDWR
   */
  shutdown_awaiter shutdown(context_t& ctx, int how) const;
  /**
   * @brief async connect with async DNS resolve.
   * @param ctx context for io_uring operations
   * @param host hostname or IP address
   * @param port port number
   * @return expected<void> on success, error on failure
   */
  ccoro_t<expected<void>> connect(context_t& ctx, std::string_view host, uint16_t port) const;
  /**
   * @brief async connect with cancellation support.
   */
  coro_t<expected<void>> connect(context_t& ctx, std::string_view host, uint16_t port, canceler_t& canceler) const;
  /**
   * @brief async connect to pre-resolved address.
   * @param ctx context for io_uring operations
   * @param resolved resolve result from cornet::resolve()
   * @return connect_awaiter
   */
  connect_awaiter connect(context_t& ctx, const resolved_address& resolved) const;
  /**
   * @brief async recv from peer
   */
  recv_awaiter recv(context_t& ctx, void* buf, size_t nbytes, int flag = 0) const;
  /**
   * @brief async send to peer
   */
  send_awaiter send(context_t& ctx, const void* buf, size_t nbytes, int flag = 0) const;
  /**
   * @brief async gather-write to peer (single syscall for several buffers).
   * The iovec array must stay valid until the operation completes.
   */
  writev_awaiter writev(context_t& ctx, const struct iovec* iov, size_t iov_len, int flags = 0) const;
  /**
   * @brief async sendmsg to peer. The msghdr must stay valid until completion;
   * prefer writev() unless ancillary data is needed.
   */
  sendmsg_awaiter sendmsg(context_t& ctx, struct msghdr* msg, int flags = 0) const;
  /**
   * @brief async recvmsg from peer. The msghdr must stay valid until completion.
   */
  recvmsg_awaiter recvmsg(context_t& ctx, struct msghdr* msg, int flags = 0) const;
  /**
   * @brief sync bind address:port to socket
   */
  expected<void> bind(std::string_view address, uint16_t port) const;
 protected:
  explicit socket_t(int fd);

  int fd{-1};
  int domain{};
  int type{};
  int protocol{};
}; // cornet::socket_t

namespace tcp {
class socket_t : public cornet::socket_t {
 protected:
  explicit socket_t(int fd);
 public:
  /**
   * @brief accept awaiter that returns a tcp socket directly
   */
  struct tcp_accept_awaiter : utask_t {
    tcp_accept_awaiter(context_t& ctx, int fd, int flag);

    CORNET_NODISCARD CORNET_MAYBE_UNUSED expected<socket_t> await_resume() const {
      if (value < 0) return unexpected(-value);
      auto sock = tcp::socket_t(value);
      sock.domain = addr_.ss_family;
      return sock;
    }
   private:
    int fd_;
    sockaddr_storage addr_{};
    socklen_t len_{sizeof(sockaddr_storage)};
    int flag_;
  };

  /**
   * @brief accept a connection, returning a new tcp socket.
   */
  tcp_accept_awaiter accept(context_t& ctx, int flag = 0) const;
  /**
   * @brief accept a connection with raw sockaddr.
   */
  accept_awaiter accept(context_t& ctx, sockaddr* addr, socklen_t* socklen, int flag = 0) const;
  /**
   * @brief sync listen on address:port
   */
  expected<void> listen(std::string_view address, uint16_t port) const;
};
} // cornet::tcp

namespace udp {
class socket_t : public cornet::socket_t {
 protected:
  explicit socket_t(int fd);
 public:
  sendto_awaiter sendto(context_t& ctx, void* buf, size_t nbytes, sockaddr* addr, socklen_t socklen, int flag = 0) const;
  recvfrom_awaiter recvfrom(context_t& ctx, void* buf, size_t nbytes, sockaddr* addr, socklen_t* socklen, int flag = 0) const;
};
} // cornet::udp

} // cornet

namespace cornet::tcp::local {
class socket_t : public cornet::tcp::socket_t {
 public:
  socket_t();
  explicit socket_t(int fd);

  CORNET_NODISCARD inline expected<void> bind(std::string_view address) {
    return cornet::tcp::socket_t::bind(address, 0);
  }
  CORNET_NODISCARD inline expected<void> listen(std::string_view address) const {
    return cornet::tcp::socket_t::listen(address, 0);
  }
  CORNET_NODISCARD inline connect_awaiter connect(context_t& ctx, std::string_view address) const {
    return connect_awaiter{ctx, fd, address};
  }
 private:
  using cornet::tcp::socket_t::listen;
  using cornet::tcp::socket_t::bind;
  using cornet::tcp::socket_t::connect;
};
} // cornet::tcp::local

namespace cornet::udp::local {
class socket_t : public cornet::udp::socket_t {
 public:
  socket_t();
  explicit socket_t(int fd);

  CORNET_NODISCARD inline expected<void> bind(std::string_view address) {
    return cornet::udp::socket_t::bind(address, 0);
  }
  CORNET_NODISCARD inline connect_awaiter connect(context_t& ctx, std::string_view address) const {
    return connect_awaiter{ctx, fd, address};
  }
 private:
  using cornet::udp::socket_t::bind;
  using cornet::udp::socket_t::connect;
};
} // cornet::udp::local

namespace cornet::tcp::v4 {
class socket_t : public cornet::tcp::socket_t {
public:
  socket_t();
  explicit socket_t(int fd);
};
} // cornet::tcp::v4

namespace cornet::tcp::v6 {
class socket_t : public cornet::tcp::socket_t {
 public:
  socket_t();
  explicit socket_t(int fd);
  void v6_only(bool on) const;
};
} // cornet::tcp::v6

namespace cornet::udp::v4 {
class socket_t : public cornet::udp::socket_t {
public:
  socket_t();
  explicit socket_t(int fd);
};
} // cornet::udp::v4

namespace cornet::udp::v6 {
class socket_t : public cornet::udp::socket_t {
public:
  socket_t();
  explicit socket_t(int fd);
};
} // cornet::udp::v6

#endif //CORNET_SOCKET_H
