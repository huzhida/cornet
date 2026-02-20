#ifndef CORNET_SOCKET_H
#define CORNET_SOCKET_H

#include "context.h"
#include <coroutine>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace cornet::tcp::v4 {

inline sockaddr_in to_address(const std::string& ip, uint32_t port) {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = inet_addr(ip.c_str());
  return addr;
}

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

  CORNET_MAYBE_UNUSED void address_reuse(bool on) const;

  CORNET_MAYBE_UNUSED void port_reuse(bool on) const;

  CORNET_NODISCARD bool listen(const std::string& ip, unsigned port) const;

  struct accept_awaiter : utask_t {
    accept_awaiter(context_t& ctx, int fd, sockaddr_in* addr, int flag);

    socklen_t addr_len{};
  };

  accept_awaiter accept(context_t& ctx, sockaddr_in* addr, int flag = 0) const;

  struct connect_awaiter : utask_t {
    connect_awaiter(context_t& ctx, int fd, const std::string& ip, unsigned port);

    sockaddr_in addr{};
  };

  connect_awaiter connect(context_t& ctx, const std::string& ip, unsigned port) const;

  struct recv_awaiter : utask_t {
    recv_awaiter(context_t& ctx, int fd, void* buf, uint32_t nbytes, int flag);
  };

  recv_awaiter recv(context_t& ctx, void* buf, uint32_t nbytes, int flag = 0) const;

  struct send_awaiter : utask_t {
    send_awaiter(context_t& ctx, int fd, void* buf, uint32_t nbytes, int flag);
  };

  send_awaiter send(context_t& ctx, void* buf, uint32_t nbytes, int flag = 0) const;

  struct close_awaiter : utask_t {
    close_awaiter(context_t& ctx, int fd);
  };

  close_awaiter close(context_t& ctx) const;

private:
  int fd{-1};
};
} // cornet

#endif //CORNET_SOCKET_H