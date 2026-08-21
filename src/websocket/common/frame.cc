#include "cornet/websocket/common/frame.h"

#include <cstring>

namespace cornet::websocket {

static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__,
              "apply_mask and mask_key packing assume a little-endian host");

frame_decoder_t::result_t frame_decoder_t::parse(std::string_view window, frame_t& out) {
  if (err_) return result_t::Error;
  if (window.size() < 2) return result_t::NeedMore;

  auto fail = [this](websocket_error_t e) {
    err_ = websocket_error(e);
    return result_t::Error;
  };

  const auto b0 = uint8_t(window[0]);
  const auto b1 = uint8_t(window[1]);

  // No extension is ever negotiated, so every RSV bit must be clear
  // (RFC 6455 §5.2). Checking first matters: an RSV-theft attempt by a
  // middlebox is indistinguishable from garbage without it.
  if (b0 & 0x70) return fail(websocket_error_t::ReservedBits);

  const auto op = opcode_t(b0 & 0x0F);
  switch (op) {
    case opcode_t::Continue:
    case opcode_t::Text:
    case opcode_t::Binary:
    case opcode_t::Close:
    case opcode_t::Ping:
    case opcode_t::Pong:
      break;
    default:
      return fail(websocket_error_t::InvalidOpcode);
  }

  const bool fin = (b0 & 0x80) != 0;
  const bool masked = (b1 & 0x80) != 0;
  uint64_t len = b1 & 0x7F;

  // Masking policy is role-mirrored (RFC 6455 §5.1): a server MUST refuse an
  // unmasked client frame, a client MUST refuse a masked server frame.
  if (role_ == role_t::Server && !masked) return fail(websocket_error_t::UnmaskedFrame);
  if (role_ == role_t::Client && masked) return fail(websocket_error_t::MaskedFrame);

  // Control frames never fragment and never exceed one small payload
  // (RFC 6455 §5.5): both rules let them be handled inline between message
  // frames without disturbing the message in flight.
  if (opcode_is_control(op)) {
    if (!fin) return fail(websocket_error_t::FragmentedControl);
    if (len > kMaxControlPayload) return fail(websocket_error_t::ControlTooLarge);
  }

  uint32_t off = 2;
  if (len == 126) {
    if (window.size() < 4) return result_t::NeedMore;
    len = uint64_t(uint8_t(window[off])) << 8 | uint8_t(window[off + 1]);
    off += 2;
    // RFC 6455 §5.2: the minimal number of bytes MUST be used
    if (len <= kMaxControlPayload) return fail(websocket_error_t::NonCanonicalLength);
  } else if (len == 127) {
    if (window.size() < 10) return result_t::NeedMore;
    uint64_t v = 0;
    for (uint32_t i = 0; i < 8; ++i) v = v << 8 | uint8_t(window[off + i]);
    off += 8;
    if (v > 0x7FFFFFFFFFFFFFFFull) return fail(websocket_error_t::NonCanonicalLength);
    if (v <= 0xFFFFull) return fail(websocket_error_t::NonCanonicalLength);
    len = v;
  }

  uint32_t mask_key = 0;
  if (masked) {
    if (window.size() < size_t(off) + 4) return result_t::NeedMore;
    std::memcpy(&mask_key, window.data() + off, 4);
    off += 4;
  }

  out.fin = fin;
  out.opcode = op;
  out.payload_len = len;
  out.masked = masked;
  out.mask_key = mask_key;
  out.header_len = off;
  return result_t::Frame;
}

void apply_mask(uint32_t key, char* data, size_t len, uint64_t offset) {
  size_t i = 0;
  // head: byte-wise until the payload position reaches a key boundary
  for (; i < len && (offset + i) & 3; ++i) {
    data[i] ^= char(key >> (((offset + i) & 3) * 8));
  }
  // body: whole words — from here on the position is 4-aligned, so the
  // packed key applies unchanged
  for (; i + 4 <= len; i += 4) {
    uint32_t v;
    std::memcpy(&v, data + i, 4);
    v ^= key;
    std::memcpy(data + i, &v, 4);
  }
  // tail
  for (uint32_t t = 0; i < len; ++i, ++t) {
    data[i] ^= char(key >> (t * 8));
  }
}

uint32_t write_frame_header(char* out, bool fin, opcode_t op, uint64_t payload_len,
                            uint32_t mask_key) {
  out[0] = char((fin ? 0x80 : 0x00) | uint8_t(op));
  const uint8_t mask_bit = mask_key ? 0x80 : 0x00;
  uint32_t off;
  if (payload_len <= kMaxControlPayload) {
    out[1] = char(mask_bit | uint8_t(payload_len));
    off = 2;
  } else if (payload_len <= 0xFFFFull) {
    out[1] = char(mask_bit | 126);
    out[2] = char(payload_len >> 8);
    out[3] = char(payload_len);
    off = 4;
  } else {
    out[1] = char(mask_bit | 127);
    for (uint32_t i = 0; i < 8; ++i) out[2 + i] = char(payload_len >> (56 - 8 * i));
    off = 10;
  }
  if (mask_key) {
    std::memcpy(out + off, &mask_key, 4);
    off += 4;
  }
  return off;
}

const char* opcode_name(opcode_t op) {
  switch (op) {
    case opcode_t::Continue: return "continue";
    case opcode_t::Text:     return "text";
    case opcode_t::Binary:   return "binary";
    case opcode_t::Close:    return "close";
    case opcode_t::Ping:     return "ping";
    case opcode_t::Pong:     return "pong";
    default:                 return "unknown";
  }
}

} // namespace cornet::websocket
