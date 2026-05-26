#ifndef CORNET_SOCKET_H
#define CORNET_SOCKET_H

#include "context.h"
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/un.h>

namespace cornet {

socklen_t to_address(std::string_view address, uint16_t port, sockaddr_storage& addr,
                int family = AF_UNSPEC, int type = SOCK_STREAM, int flag = AI_NUMERICHOST);
socklen_t to_address(std::string_view path, sockaddr_storage& addr);

class socket_t {
 public:
  /**
   * @brief accept awaiter for io_uring_prep_accept
   */
  struct accept_awaiter : utask_t {
    accept_awaiter(int fd, sockaddr* addr, socklen_t* addr_len, int flag);
   private:
    int fd_;
    sockaddr* addr_;
    socklen_t* addr_len_;
    int flag_;
  };

  /**
   * @brief close awaiter for io_uring_prep_close
   */
  struct close_awaiter : utask_t {
    close_awaiter(int fd);

    CORNET_NODISCARD CORNET_MAYBE_UNUSED expected<void> await_resume() const {
      if (value < 0) return unexpected(-value);
      return {};
    }
   private:
    int fd_;
  };

  /**
   * @brief connect awaiter for io_uring_prep_connect
   */
  struct connect_awaiter : utask_t {
    connect_awaiter(int fd, std::string_view ip, uint16_t port, int domain, int type);
    connect_awaiter(int fd, std::string_view path);

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
    send_awaiter(int fd, const void* buf, size_t nbytes, int flag);
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
    recv_awaiter(int fd, void* buf, size_t nbytes, int flag);
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
    sendmsg_awaiter(int fd, struct msghdr* msg, int flags);
   private:
    int fd_;
    struct msghdr* msg_;
    int flags_;
  };

  /**
   * @brief recvmsg awaiter for io_uring_prep_recvmsg
   */
  struct recvmsg_awaiter : utask_t {
    recvmsg_awaiter(int fd, struct msghdr* msg, int flags);
   private:
    int fd_;
    struct msghdr* msg_;
    int flags_;
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
  close_awaiter close() const;
  /**
   * @brief async connect to ip:port
   */
  connect_awaiter connect(std::string_view address, uint16_t port) const;
  /**
   * @brief async recv from peer
   */
  recv_awaiter recv(void* buf, size_t nbytes, int flag = 0) const;
  /**
   * @brief async send to peer
   */
  send_awaiter send(const void* buf, size_t nbytes, int flag = 0) const;
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
   * @brief sync listen on address:port
   */
  CORNET_NODISCARD expected<void> listen(std::string_view address, uint16_t port) const;
  /**
   * @brief async accept (raw, returns fd)
   */
  accept_awaiter accept(sockaddr* addr, socklen_t* socklen, int flag = 0) const;
  /**
   * @brief async accept (high-level, returns socket)
   */
  coro_t<expected<socket_t>> accept(int flag = 0) const;
};
} // cornet::tcp

namespace udp {
class socket_t : public cornet::socket_t {
 protected:
  explicit socket_t(int fd);
 public:
  coro_t<expected<int>> sendto(void* buf, size_t nbytes, sockaddr* addr, socklen_t socklen, int flag = 0) const;
  coro_t<expected<int>> recvfrom(void* buf, size_t nbytes, sockaddr* addr, socklen_t* socklen, int flag = 0) const;
  auto sendmsg(struct msghdr* msg, int flags) const;
  auto recvmsg(struct msghdr* msg, int flags) const;
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
  CORNET_NODISCARD inline connect_awaiter connect(std::string_view address) const {
    return connect_awaiter{fd, address};
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
  CORNET_NODISCARD inline connect_awaiter connect(std::string_view address) const {
    return connect_awaiter{fd, address};
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
