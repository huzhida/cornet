#ifndef CORNET_SOCKET_H
#define CORNET_SOCKET_H

#include "context.h"
#include <coroutine>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace cornet {

namespace tcp::v4 {

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
  ~socket_t();
  socket_t(const socket_t&) = default;
  socket_t(socket_t&& s) noexcept;
  socket_t& operator=(const socket_t&) = delete;
  socket_t& operator=(socket_t&& s) noexcept;

  static inline int socket() {
    return ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  }

  bool listen(const std::string& ip, unsigned port);
  auto accept(context_t& ctx, int flag = 0);
  auto connect(context_t& ctx, const std::string& ip, unsigned port, int flag = 0);
  auto recv(context_t& ctx, void* buf, uint32_t nbytes, int flag = 0);
  auto send(context_t& ctx, void* buf, uint32_t nbytes, int flag = 0);
  auto close(context_t& ctx);

 private:
  int fd{-1};
  sockaddr_in addr{};

  socket_t(int fd, sockaddr_in* addr) : fd(fd), addr(*addr) {}
};
}
} // cornet

#endif //CORNET_SOCKET_H
