#ifndef CORNET_WEBSOCKET_COMMON_HANDSHAKE_H
#define CORNET_WEBSOCKET_COMMON_HANDSHAKE_H

#include <cstdint>
#include <string_view>

#include "cornet/base/defines.h"
#include "cornet/base/expected.h"
#include "cornet/http/common/serializer.h"

namespace cornet::websocket {

/**
 * @brief the magic GUID the accept hash appends (RFC 6455 §1.3).
 */
inline constexpr std::string_view kRfc6455Guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

inline constexpr uint32_t kAcceptLen = 28;  // base64 of a 20-byte SHA-1 digest
inline constexpr uint32_t kKeyLen    = 24;  // base64 of a 16-byte nonce

/**
 * @brief Sec-WebSocket-Accept for a client key: base64(SHA-1(key + GUID)).
 * @param out at least kAcceptLen bytes; not NUL-terminated
 */
void accept_key(std::string_view key, char* out);

/**
 * @brief a fresh Sec-WebSocket-Key: 16 unpredictable bytes, base64-encoded.
 * @param out at least kKeyLen bytes; not NUL-terminated
 */
void make_key(char* out);

/**
 * @brief whether a Sec-WebSocket-Key value is well-formed: exactly the base64
 * encoding of 16 bytes. The value is never decoded for use — this only keeps
 * a garbage header from producing a garbage handshake.
 */
CORNET_NODISCARD bool key_valid(std::string_view key);

/**
 * @brief standard base64 (RFC 4648 §4, '=' padding).
 * @param out room for 4 * ceil(len / 3) bytes
 * @return bytes written
 */
uint32_t base64_encode(const void* data, uint32_t len, char* out);

/**
 * @brief decode standard base64.
 * @return false on malformed input or insufficient capacity
 */
CORNET_NODISCARD bool base64_decode(std::string_view in, void* out, uint32_t cap);

/**
 * @brief fill `buf` from the kernel CSPRNG (getrandom(2), no flags).
 *
 * Shared with the session: frame masks must be as unpredictable as the
 * handshake nonce (RFC 6455 §5.3), so both draw from the same source.
 */
void random_bytes(void* buf, size_t len);

/**
 * @brief frame the server's 101 Switching Protocols answer.
 *
 * Emitted as one header block from pre-composed constants plus the accept
 * value — framing it never involves the response machinery because a 101
 * forbids both a body and every framing header that would describe one.
 * `subprotocol` is omitted entirely when empty (RFC 6455 §4.2.2).
 */
void frame_handshake_response(http::out_buffer_t& out, std::string_view key,
                              std::string_view subprotocol = {});

/**
 * @brief frame the client's opening GET request.
 *
 * Written directly rather than by the http client because an upgrade request
 * has an unusual contract: Connection carries a token, there is no body, and
 * the bytes after the blank line belong to a different protocol from the
 * moment they are read. Keeping it a pure framing function (like
 * frame_request_head) means it is byte-testable without a socket.
 *
 * @param path path as written; "/" is emitted when it is empty
 * @param query query without the '?'; skipped when empty
 * @param subprotocols comma-joined offer list, e.g. "chat, super"; omitted when empty
 */
void frame_handshake_request(http::out_buffer_t& out, std::string_view host,
                             std::string_view path, std::string_view query,
                             std::string_view key, std::string_view subprotocols);

// ── the handshake headers as they appear on the wire, pre-composed ──

inline constexpr std::string_view kHdrUpgradeWebSocket = "Upgrade: websocket\r\n";
inline constexpr std::string_view kHdrConnUpgrade = "Connection: Upgrade\r\n";
inline constexpr std::string_view kSecWebSocketKey = "Sec-WebSocket-Key";
inline constexpr std::string_view kSecWebSocketAccept = "Sec-WebSocket-Accept";
inline constexpr std::string_view kSecWebSocketProtocol = "Sec-WebSocket-Protocol";
inline constexpr std::string_view kSecWebSocketVersion = "Sec-WebSocket-Version";
inline constexpr std::string_view kSecWebSocketExtensions = "Sec-WebSocket-Extensions";

} // namespace cornet::websocket

#endif // CORNET_WEBSOCKET_COMMON_HANDSHAKE_H
