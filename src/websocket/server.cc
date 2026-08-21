#include "cornet/websocket/server.h"

#include "cornet/http/server/message.h"
#include "cornet/websocket/common/handshake.h"
#include "cornet/websocket/common/protocol.h"

namespace cornet::websocket {

expected<std::string_view> validate_request(const http::request_t& req) {
  // llhttp flags a message as an upgrade only when Connection carries the
  // upgrade token and an Upgrade header exists, so those two are proven.
  // What is left is RFC 6455 §4.2.1: the method, the Upgrade value, the
  // version and the key.
  if (req.method() != http::method_t::Get) {
    return websocket_unexpected(websocket_error_t::HandshakeFailed);
  }
  if (!http::iequals(req.headers().get(http::field_t::Upgrade), "websocket")) {
    return websocket_unexpected(websocket_error_t::HandshakeFailed);
  }
  if (req.headers().get(kSecWebSocketVersion) != "13") {
    return websocket_unexpected(websocket_error_t::HandshakeFailed);
  }
  auto key = req.headers().get(kSecWebSocketKey);
  if (!key_valid(key)) {
    return websocket_unexpected(websocket_error_t::HandshakeFailed);
  }
  return key;
}

} // namespace cornet::websocket
