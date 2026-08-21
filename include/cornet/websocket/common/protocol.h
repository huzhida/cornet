#ifndef CORNET_WEBSOCKET_COMMON_PROTOCOL_H
#define CORNET_WEBSOCKET_COMMON_PROTOCOL_H

#include <cstdint>
#include <string_view>

#include "cornet/base/defines.h"
#include "cornet/base/expected.h"

namespace cornet::websocket {

// ─────────────────────────── enums ───────────────────────────

/**
 * @brief frame opcode, RFC 6455 §5.2. Values are the wire values.
 */
enum class opcode_t : uint8_t {
  Continue = 0x0,   // continuation of a fragmented message
  Text     = 0x1,
  Binary   = 0x2,
  Close    = 0x8,
  Ping     = 0x9,
  Pong     = 0xA,
};

/**
 * @brief which end of the connection this side is. The role decides masking
 * policy: a client masks every frame it sends and refuses masked frames it
 * receives; a server is the mirror image (RFC 6455 §5.1, §5.3).
 */
enum class role_t : uint8_t { Client, Server };

inline bool opcode_is_control(opcode_t op) { return uint8_t(op) >= 0x8; }
inline bool opcode_is_data(opcode_t op) {
  return op == opcode_t::Text || op == opcode_t::Binary;
}

/**
 * @brief close status code, RFC 6455 §7.4.1. Values are the wire values.
 */
enum class close_code_t : uint16_t {
  Normal               = 1000,
  GoingAway            = 1001,
  ProtocolError        = 1002,
  UnsupportedData      = 1003,
  NoStatus             = 1005,   // pseudo: peer sent no code; never on the wire
  Abnormal             = 1006,   // pseudo: connection dropped; never on the wire
  InvalidPayload       = 1007,
  PolicyViolation      = 1008,
  MessageTooBig        = 1009,
  ExtensionExpected    = 1010,
  UnexpectedCondition  = 1011,
};

/**
 * @brief whether a close code may legally appear on the wire.
 *
 * Codes below 1000 are undefined outright, and 1004/1005/1006/1015 are
 * reserved as local pseudo-statuses — a peer that sends one is broken
 * (RFC 6455 §7.4.1, §7.4.2). Ranges 1016–2999 are assigned-reserved and pass;
 * 3000–4999 are application space.
 */
inline bool close_code_valid(uint16_t code) {
  if (code < 1000) return false;
  if (code >= 5000) return false;
  return code != 1004 && code != 1005 && code != 1006 && code != 1015;
}

// ───────────────────────────── errors ─────────────────────────────

/**
 * @brief websocket-layer error codes carried in error_domain::Websocket.
 */
enum class websocket_error_t : int {
  ReservedBits = 1,         // RSV1..3 set with no negotiated extension
  InvalidOpcode,            // reserved opcode value on the wire
  UnmaskedFrame,            // server received an unmasked client frame
  MaskedFrame,              // client received a masked server frame
  FragmentedControl,        // control frame with FIN clear
  ControlTooLarge,          // control payload over 125 bytes
  NonCanonicalLength,       // extended length in a wider form than needed / MSB set
  MessageTooLarge,          // message exceeded max_message_bytes
  ContinuationExpected,     // a new data frame arrived mid-fragment
  ContinuationUnexpected,   // Continue frame with no message open
  InvalidCloseCode,         // close code that must never appear on the wire
  HandshakeFailed,          // opening handshake missing or contradicting fields
  Closed,                   // session used after the close handshake completed
};

/**
 * @brief build an error_t in the Websocket domain.
 */
inline error_t websocket_error(websocket_error_t e) {
  return error_t{int(e), error_domain::Websocket};
}

/**
 * @brief build an unexpected in the Websocket domain.
 */
inline unexpected websocket_unexpected(websocket_error_t e) {
  return unexpected(int(e), error_domain::Websocket);
}

/**
 * @brief render a Websocket-domain code. Registered into the core's resolver
 * slot so error_t::message() works without the core linking this module.
 */
const char* websocket_error_name(int code);

} // namespace cornet::websocket

#endif // CORNET_WEBSOCKET_COMMON_PROTOCOL_H
