#ifndef CORNET_SOCKET_H
#define CORNET_SOCKET_H

#include "context.h"
#include <netinet/in.h>
#include <arpa/inet.h>

namespace cornet::tcp::v4 {

/**
 * @brief convert ip:port to sockaddr_in struct.
 * @param ip ip address
 * @param port port
 * @return sockaddr_in struct for ip:port
 */
inline sockaddr_in to_address(const std::string& ip, uint32_t port) {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = inet_addr(ip.c_str());
  return addr;
}

/**
 * @brief tcp v4 socket wrapper
 */
class socket_t {
public:
  socket_t();

  explicit socket_t(int fd);

  ~socket_t();

  socket_t(const socket_t&) = delete;

  socket_t(socket_t&& s) noexcept;

  socket_t& operator=(const socket_t&) = delete;

  socket_t& operator=(socket_t&& s) noexcept;

  static inline int socket() {
    return ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  }

  /**
   * @brief get native fd for socket.
   * @return native fd
   */
  CORNET_MAYBE_UNUSED int native_fd() const;
  /**
   * @brief set address reuse
   * @param on reuse or not.
   */
  CORNET_MAYBE_UNUSED void address_reuse(bool on) const;
  /**
   * @brief set port reuse
   * @param on reuse or not.
   */
  CORNET_MAYBE_UNUSED void port_reuse(bool on) const;
  /**
   * @brief listen on ip:port
   * @param ip ip address
   * @param port port
   * @return listen ok?
   */
  CORNET_NODISCARD bool listen(const std::string& ip, unsigned port) const;

  /**
   * @brief accept awaiter for io_uring_prep_accept
   */
  struct accept_awaiter : utask_t {
    accept_awaiter(context_t& ctx, int fd, sockaddr_in* addr, int flag);

    socklen_t addr_len{};
  };
  /**
   * @brief accept new socket from client
   * @param ctx owner context
   * @param addr client address
   * @param flag
   * SOCK_NONBLOCK non-block accept
   * SOCK_CLOEXEC  close on exec-like system call, avoid fd leak
   * @return co_await -> value (system call return)
   */
  accept_awaiter accept(context_t& ctx, sockaddr_in* addr, int flag = 0) const;

  /**
   * @brief connect awaiter for io_uring_prep_connect
   */
  struct connect_awaiter : utask_t {
    connect_awaiter(context_t& ctx, int fd, const std::string& ip, unsigned port);

    sockaddr_in addr{};
  };
  /**
   * @brief connect to ip:port
   * @param ctx owner context
   * @param ip ip address
   * @param port port
   * @return co_await -> value (system call return)
   */
  connect_awaiter connect(context_t& ctx, const std::string& ip, unsigned port) const;

  /**
   * @brief recv awaiter for io_uring_prep_recv
   */
  struct recv_awaiter : utask_t {
    recv_awaiter(context_t& ctx, int fd, void* buf, uint32_t nbytes, int flag);
  };
  /**
   * @brief async recv from peer
   * @param ctx owner context
   * @param buf recv to this buffer
   * @param nbytes recv bytes count
   * @param flag
   * MSG_DONTWAIT non-block recv, return -1 and EAGAIN/EWOULDBLOCK will be set errno.
   * MSG_PEEK     only see msg, dont clear kernel buffer
   * MSG_WAITALL  wait until nbytes come, but return when peer close
   * MSG_NOSIGNAL ignore signal like PIPE
   * ...
   * @return co_await -> value (system call return)
   */
  recv_awaiter recv(context_t& ctx, void* buf, uint32_t nbytes, int flag = 0) const;

  /**
   * @brief send awaiter for io_uring_prep_send
   */
  struct send_awaiter : utask_t {
    send_awaiter(context_t& ctx, int fd, void* buf, uint32_t nbytes, int flag);
  };
  /**
   * @brief async send to peer
   * @param ctx owner context
   * @param buf the buffer need send to peer
   * @param nbytes send bytes count
   * @param flag
   * MSG_DONTWAIT non-block send, return -1 and EAGAIN/EWOULDBLOCK will be set errno.
   * MSG_NOSIGNAL ignore signal like PIPE
   * ...
   * @return co_await -> value (system call return)
   */
  send_awaiter send(context_t& ctx, void* buf, uint32_t nbytes, int flag = 0) const;

  /**
   * @brief close awaiter for io_uring_prep_close
   */
  struct close_awaiter : utask_t {
    close_awaiter(context_t& ctx, int fd);
  };
  /**
   * @brief close socket
   * @param ctx owner context
   * @return co_await -> value (system call return)
   */
  close_awaiter close(context_t& ctx) const;

private:
  int fd{-1};
};
} // cornet

#endif //CORNET_SOCKET_H