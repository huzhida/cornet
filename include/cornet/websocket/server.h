#ifndef CORNET_WEBSOCKET_SERVER_H
#define CORNET_WEBSOCKET_SERVER_H

#include <functional>
#include <string_view>

#include "cornet/base/defines.h"
#include "cornet/base/expected.h"
#include "cornet/coroutine/coro.h"
#include "cornet/http/common/protocol.h"

namespace cornet::http {

class request_t;

} // namespace cornet::http

namespace cornet::websocket {

class session_t;

/**
 * @brief a websocket route's session handler. Runs once the 101 is on the
 * wire; the session lives exactly as long as this coroutine.
 */
using ws_handler_t = std::function<coro_t<void>(session_t&)>;

/**
 * @brief knobs the pre-handshake guard may turn.
 */
struct accept_info_t {
  // set to the subprotocol to echo in the 101; it must be one the client
  // offered (the guard has the offer list in the request headers)
  std::string_view subprotocol{};
  // the refusal status when the guard returns false
  http::status_t refuse_with{http::status_t::Forbidden};
};

/**
 * @brief synchronous pre-handshake guard, run after the RFC 6455 §4.2.1
 * checks and before the 101 is staged. Returning false refuses the upgrade
 * with accept_info_t::refuse_with; the session handler never runs.
 *
 * Deliberately synchronous: this is an admission check (Origin, a token, an
 * offered-subprotocol pick), and a guard that needs IO should have run as an
 * http filter earlier in the chain — which is still available, since filters
 * run for websocket routes exactly as for plain ones.
 */
using ws_accept_t = std::function<bool(http::request_t&, accept_info_t&)>;

/**
 * @brief validate the websocket half of an upgrade request.
 *
 * llhttp has already vouched for the HTTP half (GET, Connection: upgrade, an
 * Upgrade header — it flags nothing else as an upgrade). What remains is
 * RFC 6455 §4.2.1 items 2..4: the Upgrade value, the version and the key.
 * @return the Sec-WebSocket-Key (a view into the request buffers), or
 *         websocket_error_t::HandshakeFailed
 */
CORNET_NODISCARD expected<std::string_view> validate_request(const http::request_t& req);

} // namespace cornet::websocket

#endif // CORNET_WEBSOCKET_SERVER_H
