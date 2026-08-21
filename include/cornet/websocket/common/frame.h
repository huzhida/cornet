#ifndef CORNET_WEBSOCKET_COMMON_FRAME_H
#define CORNET_WEBSOCKET_COMMON_FRAME_H

#include <cstdint>
#include <string_view>

#include "cornet/base/defines.h"
#include "cornet/base/expected.h"
#include "cornet/websocket/common/protocol.h"

namespace cornet::websocket {

// 2-byte base header + 8-byte extended length + 4-byte masking key (RFC 6455 §5.2)
inline constexpr uint32_t kMaxFrameHeaderLen = 14;
inline constexpr uint32_t kMaxControlPayload = 125;

/**
 * @brief one parsed frame header.
 */
struct frame_t {
  bool     fin{false};
  opcode_t opcode{opcode_t::Continue};
  uint64_t payload_len{0};
  bool     masked{false};
  // the 4 wire bytes packed little-endian: payload[i] is XORed with byte
  // (i % 4) of this word, which is exactly how apply_mask consumes it
  uint32_t mask_key{0};
  // bytes the header occupies on the wire, 2..kMaxFrameHeaderLen
  uint32_t header_len{0};
};

/**
 * @brief incremental frame-header parser. Stateless across frames: every
 * call parses the header at the start of the window handed to it.
 *
 * The split between header and payload is deliberate: parse() returns as
 * soon as the header is validated, and the caller compares the window
 * against header_len + payload_len to know whether the payload has fully
 * arrived. A payload streamed in runs (only the session's aggregation
 * buffer needs it contiguous) never forces this parser to buffer.
 *
 * Structural RFC 6455 rules are enforced here — reserved bits, reserved
 * opcodes, control frame shape, canonical length forms, and the two masking
 * rules that depend on role. Message-level rules (fragmentation ordering,
 * close-code validity, size limits) belong to the session, which has the
 * context this layer lacks.
 */
class frame_decoder_t {
 public:
  enum class result_t : uint8_t { NeedMore, Frame, Error };

  explicit frame_decoder_t(role_t role) : role_(role) {}

  /**
   * @brief parse one frame header at the start of `window`.
   * @return Frame with `out` filled when the header is complete and valid;
   *         NeedMore when the window is short of the full header;
   *         Error on a violated MUST, with error() saying which
   */
  CORNET_NODISCARD result_t parse(std::string_view window, frame_t& out);

  CORNET_NODISCARD error_t error() const { return err_; }

 private:
  role_t  role_;
  error_t err_{};
};

/**
 * @brief unmask `len` bytes at `data` in place.
 *
 * Word-wise XOR on the packed key (see frame_t::mask_key). `offset` is the
 * payload position the run starts at, and continues the 4-byte key cycle —
 * a payload processed in several runs passes its running offset so the two
 * halves of a split run are consistent.
 */
void apply_mask(uint32_t key, char* data, size_t len, uint64_t offset = 0);

/**
 * @brief serialize a frame header.
 * @param out at least kMaxFrameHeaderLen bytes
 * @param mask_key packed little-endian masking key; 0 emits an unmasked header
 * @return bytes written, 2..kMaxFrameHeaderLen
 */
uint32_t write_frame_header(char* out, bool fin, opcode_t op, uint64_t payload_len,
                            uint32_t mask_key);

/**
 * @brief render an opcode for logs.
 */
const char* opcode_name(opcode_t op);

} // namespace cornet::websocket

#endif // CORNET_WEBSOCKET_COMMON_FRAME_H
