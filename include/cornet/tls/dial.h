#ifndef CORNET_TLS_DIAL_H
#define CORNET_TLS_DIAL_H

#include <chrono>
#include <memory>
#include <string_view>

#include "cornet/base/expected.h"
#include "cornet/concurrency/deadline.h"
#include "cornet/coroutine/cancel.h"
#include "cornet/net/socket.h"
#include "cornet/tls/context.h"
#include "cornet/tls/transport.h"

namespace cornet {

struct context_t;

namespace tls {

/**
 * @brief the pieces of a dial that protocols actually tune.
 */
struct dial_options_t {
  // per-phase bounds; the overall budget (if any) lives on the caller's
  // deadline_t and these arms are clamped against it
  std::chrono::milliseconds connect_timeout{};
  std::chrono::milliseconds handshake_timeout{};
  // null => plain TCP, nothing else in here is consulted
  std::shared_ptr<tls_context_t> tls_cx{};
  // SNI + certificate verification name; only meaningful with tls_cx set.
  // The caller computes the fallback (host vs. override) because only it
  // knows its own options.
  std::string_view server_name{};
  bool tcp_nodelay{true};
};

/**
 * @brief the dial skeleton every client shares: family socket, TCP_NODELAY,
 * a bounded connect, a bounded TLS handshake — and on any failure, abandon
 * the fd so no caller ever hand-copies cleanup again.
 *
 * Why references instead of ownership: the deadline/canceler controllers
 * have per-module homes (the http client keeps them on its connection object,
 * the websocket dial keeps them in the connect() frame), and resolutions flow
 * through per-module caches before reaching here. Moving those in would
 * duplicate exactly the variance the callers already have.
 *
 * On success returns a connected (and, if tls_cx, TLS-established) transport;
 * every failure is reported with deadline.map(), so an expired deadline reads
 * ETIMEDOUT while a peer/reset error keeps its real reason.
 */
CORNET_NODISCARD coro_t<expected<transport_t>> dial(
    context_t& ctx, deadline_t& deadline, const resolved_address& addr,
    const dial_options_t& opt);

} // namespace tls

} // namespace cornet

#endif // CORNET_TLS_DIAL_H
