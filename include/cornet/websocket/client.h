#ifndef CORNET_WEBSOCKET_CLIENT_H
#define CORNET_WEBSOCKET_CLIENT_H

#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "cornet/base/defines.h"
#include "cornet/base/expected.h"
#include "cornet/coroutine/coro.h"
#include "cornet/tls/context.h"
#include "cornet/websocket/session.h"

namespace cornet {

struct context_t;

namespace websocket {

/**
 * @brief client tunables. Built programmatically; there is no TOML loading
 * here because a client is usually constructed per call, not per process.
 */
struct client_options_t {
  // required for wss:// urls; built via tls::tls_context_t::make_client
  std::shared_ptr<tls::tls_context_t> tls{};
  // covers connect, the TLS handshake and the HTTP upgrade exchange together
  std::chrono::milliseconds handshake_timeout{10000};
  // SNI and certificate verification name override; empty uses the url host
  std::string tls_server_name{};
  // subprotocols to offer, in preference order; the wire joins them with ", "
  std::vector<std::string> subprotocols{};
  // session budgets once the handshake succeeds
  session_options_t session{};
};

/**
 * @brief connect, handshake, and hand back a ready websocket session.
 *
 * One call does DNS, the TCP connect, TLS for wss:// (with SNI/verification
 * from options), the HTTP upgrade and its validation — including the
 * Sec-WebSocket-Accept hash and the rule that a server may not select
 * extensions or subprotocols the client never offered. Bytes read past the
 * 101 belong to the session (a hot peer may already have sent frames).
 *
 * @param url ws:// or wss:// absolute url
 * @return the session (client role: outgoing frames are masked), or a system
 *         error (connect/tls), an http error (bad url/scheme), or
 *         websocket_error_t::HandshakeFailed for a refused/invalid handshake
 */
CORNET_NODISCARD coro_t<expected<std::unique_ptr<session_t>>> connect(
    context_t& ctx, std::string_view url, client_options_t opt = {});

} // namespace websocket
} // namespace cornet

#endif // CORNET_WEBSOCKET_CLIENT_H
