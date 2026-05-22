#ifndef CORNET_SOCKET_H
#define CORNET_SOCKET_H

#include "context.h"
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/un.h>

namespace cornet {

socklen_t to_address(const std::string& address, const std::string& port, sockaddr_storage& addr,
                int family = AF_UNSPEC, int type = SOCK_STREAM, int flag = AI_NUMERICHOST);
socklen_t to_address(const std::string& path, sockaddr_storage& addr);

class socket_t {
 public:
  /**
   * @brief accept awaiter for io_uring_prep_accept
   */
  struct accept_awaiter : utask_t {
    int fd_;
    sockaddr* addr_;
    socklen_t* addr_len_;
    int flag_;
    accept_awaiter(context_t& ctx, int fd, sockaddr* addr, socklen_t* addr_len, int flag);
  };

  /**
   * @brief close awaiter for io_uring_prep_close
   */
  struct close_awaiter : utask_t {
    int fd_;
    close_awaiter(context_t& ctx, int fd);
  };

  /**
   * @brief connect awaiter for io_uring_prep_connect
   */
  struct connect_awaiter : utask_t {
    int fd_;
    sockaddr_storage addr{};
    socklen_t socklen_{};
    connect_awaiter(context_t& ctx, int fd, const std::string& ip, const std::string& port, int domain, int type);
    connect_awaiter(context_t& ctx, int fd, const std::string& path);
  };

  /**
   * @brief send awaiter for io_uring_prep_send
   */
  struct send_awaiter : utask_t {
    int fd_;
    void* buf_;
    uint32_t nbytes_;
    int flag_;
    send_awaiter(context_t& ctx, int fd, void* buf, uint32_t nbytes, int flag);
  };

  /**
   * @brief recv awaiter for io_uring_prep_recv
   */
  struct recv_awaiter : utask_t {
    int fd_;
    void* buf_;
    uint32_t nbytes_;
    int flag_;
    recv_awaiter(context_t& ctx, int fd, void* buf, uint32_t nbytes, int flag);
  };

  /**
   * @brief sendmsg awaiter for io_uring_prep_sendmsg
   */
  struct sendmsg_awaiter : utask_t {
    int fd_;
    struct msghdr* msg_;
    int flags_;
    sendmsg_awaiter(context_t& ctx, int fd, struct msghdr* msg, int flags);
  };

  /**
   * @brief recvmsg awaiter for io_uring_prep_recvmsg
   */
  struct recvmsg_awaiter : utask_t {
    int fd_;
    struct msghdr* msg_;
    int flags_;
    recvmsg_awaiter(context_t& ctx, int fd, struct msghdr* msg, int flags);
  };

  ~socket_t();
  socket_t(const socket_t&) = delete;
  socket_t& operator=(const socket_t&) = delete;
  socket_t(socket_t&&) noexcept;
  socket_t& operator=(socket_t&&) noexcept;

  /**
   * @brief get native file descriptor
   * @return native file descriptor
   */
  int native_fd() const;
  /**
   * @brief set socket option reuse address
   * @param on reuse or not
   */
  void address_reuse(bool on) const;
  /**
   * @brief set socket option reuse port
   * @param on reuse or not
   */
  void port_reuse(bool on) const;
  inline int getpeername(sockaddr* addr, socklen_t* socklen) {
    return ::getpeername(fd, addr, socklen);
  }
  inline int getsockname(sockaddr* addr, socklen_t* socklen) {
    return ::getsockname(fd, addr, socklen);
  }
  /**
   * @brief close socket
   * @param ctx owner context
   * @return co_await -> value (system call return)
   */
  close_awaiter close(context_t& ctx) const;
  /**
   * @brief connect to ip:port
   * @param ctx owner context
   * @param address ip address
   * @param port port
   * @return co_await -> value (system call return)
   */
  connect_awaiter connect(context_t& ctx, const std::string& address, const std::string& port) const;
  /**
   * @brief async recv from peer
   * @param ctx owner context
   * @param buf recv to this buffer
   * @param nbytes recv bytes count
   * @param flag MSG_* flags
   * @return co_await -> value (system call return)
   */
  recv_awaiter recv(context_t& ctx, void* buf, uint32_t nbytes, int flag = 0) const;
  /**
   * @brief async send to peer
   * @param ctx owner context
   * @param buf the buffer need send to peer
   * @param nbytes send bytes count
   * @param flag MSG_* flags
   * @return co_await -> value (system call return)
   */
  send_awaiter send(context_t& ctx, void* buf, uint32_t nbytes, int flag = 0) const;
  /**
   * @brief bind address:port to socket
   * @param address ip address
   * @param port port
   * @return bind ok?
   */
  bool bind(const std::string& address, const std::string& port) const;
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
   * @brief listen on address:port, or listen on unix path if is local socket
   * @param address address
   * @param port port, local socket will ignore this argument.
   * @return listen ok?
   */
  CORNET_NODISCARD bool listen(const std::string& address, const std::string& port) const;
  /**
   * @brief accept new socket from client
   * @param ctx owner context
   * @param addr client address
   * @param socklen address length
   * @param flag SOCK_NONBLOCK / SOCK_CLOEXEC
   * @return co_await -> value (system call return)
   */
  accept_awaiter accept(context_t& ctx, sockaddr* addr, socklen_t* socklen, int flag = 0) const;
  coro_t<socket_t> accept(context_t& ctx, int flag = 0) const;
};
} // cornet::net::tcp

namespace udp {
class socket_t : public cornet::socket_t {
 protected:
  explicit socket_t(int fd);
 public:
  /**
   * @brief sendto wrapper
   * @param ctx context reference
   * @param buf send buffer
   * @param nbtyes send bytes count
   * @param addr address to send
   * @param socklen address length
   * @param flag sendto flag
   * @return coroutine return int
   */
  coro_t<int> sendto(context_t& ctx, void* buf, size_t nbtyes, sockaddr* addr, socklen_t socklen, int flag = 0) const;
  /**
   * @brief recvfrom wrapper
   * @param ctx context reference
   * @param buf recv buffer
   * @param nbytes recv bytes count
   * @param addr address to recv
   * @param socklen address length
   * @param flag recvfrom flag
   * @return coroutine return int
   */
  coro_t<int> recvfrom(context_t& ctx, void* buf, size_t nbytes, sockaddr* addr, socklen_t* socklen, int flag = 0) const;
  /**
   * @brief sendmsg wrapper
   * @param ctx context reference
   * @param msg msghdr struct, usage see manpage.
   * @param flags sendmsg flags
   * @return sendmsg awaiter
   */
  auto sendmsg(context_t& ctx, struct msghdr* msg, int flags) const;
  /**
   * @brief recvmsg wrapper
   * @param ctx context reference
   * @param msg msghdr struct, usage see manpage.
   * @param flags recvmsg flags
   * @return recvmsg awaiter
   */
  auto recvmsg(context_t& ctx, struct msghdr* msg, int flags) const;
};
} // cornet::net::udp

} // cornet

namespace cornet::tcp::local {
 class socket_t : public cornet::tcp::socket_t {
 public:
  socket_t();
  explicit socket_t(int fd);

  CORNET_NODISCARD inline bool bind(const std::string& address) {
    return cornet::tcp::socket_t::bind(address, "");
  }
  CORNET_NODISCARD inline bool listen(const std::string& address) const {
    return cornet::tcp::socket_t::listen(address, "");
  }
  CORNET_NODISCARD inline connect_awaiter connect(context_t& ctx, const std::string& address) const {
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

  CORNET_NODISCARD inline bool bind(const std::string& address) {
    return cornet::udp::socket_t::bind(address, "");
  }
  CORNET_NODISCARD inline connect_awaiter connect(context_t& ctx, const std::string& address) const {
    return connect_awaiter{ctx, fd, address};
  }
 private:
  using cornet::udp::socket_t::bind;
  using cornet::udp::socket_t::connect;
};
} // cornet::tcp::local

namespace cornet::tcp::v4 {
/**
 * @brief tcp v4 socket wrapper
 */
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
