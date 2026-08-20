#ifndef CORNET_TESTS_EPHEMERAL_PORT_H
#define CORNET_TESTS_EPHEMERAL_PORT_H

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <cstdint>

/**
 * @brief read the port a socket is bound to (0 on failure / not inet).
 */
template <typename Sock>
uint16_t bound_port(const Sock& sock) {
  sockaddr_storage ss{};
  socklen_t len = sizeof(ss);
  if (sock.getsockname(reinterpret_cast<sockaddr*>(&ss), &len) != 0) return 0;
  if (ss.ss_family == AF_INET6) {
    return ntohs(reinterpret_cast<sockaddr_in6*>(&ss)->sin6_port);
  }
  if (ss.ss_family == AF_INET) {
    return ntohs(reinterpret_cast<sockaddr_in*>(&ss)->sin_port);
  }
  return 0;
}

/**
 * @brief bind the listener to an ephemeral port and return it (0 on failure).
 *
 * Hardcoded port numbers collide with leftover listeners on reruns and with
 * parallel test jobs, producing listen() failures that have nothing to do
 * with the behavior under test. Works for any socket whose listen() takes
 * (ip, port) — tcp::v4::socket_t, tcp::v6::socket_t ("::1"), etc.
 */
template <typename Sock>
uint16_t listen_ephemeral(Sock& sock, const char* ip = "127.0.0.1") {
  if (!sock.listen(ip, uint16_t{0}).has_value()) return 0;
  return bound_port(sock);
}

/**
 * @brief UDP twin of listen_ephemeral(): bind(ip, 0) and report the port the
 * kernel picked. Note SO_REUSEPORT does NOT make hardcoded ports safe for
 * tests — a leftover process would silently steal half the datagrams.
 */
template <typename Sock>
uint16_t bind_ephemeral(Sock& sock, const char* ip = "127.0.0.1") {
  if (!sock.bind(ip, uint16_t{0}).has_value()) return 0;
  return bound_port(sock);
}

#endif // CORNET_TESTS_EPHEMERAL_PORT_H
