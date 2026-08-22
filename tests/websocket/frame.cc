#include <gtest/gtest.h>

#include <cstring>
#include <string>

#include "cornet/websocket/common/frame.h"

using namespace cornet;

namespace {

// build a wire frame by hand: header + (optionally masked) payload
std::string wire(bool fin, uint8_t op, std::string_view payload,
                 const char* mask /*4 bytes or nullptr*/) {
  std::string out;
  websocket::frame_t f{};
  char hdr[websocket::kMaxFrameHeaderLen];
  uint32_t key = 0;
  if (mask) std::memcpy(&key, mask, 4);
  auto n = websocket::write_frame_header(hdr, fin, websocket::opcode_t(op),
                                         payload.size(), key);
  out.append(hdr, n);
  std::string body(payload);
  if (mask) websocket::apply_mask(key, body.data(), body.size());
  out.append(body);
  return out;
}

websocket::frame_t parse_ok(websocket::role_t role, std::string_view bytes) {
  websocket::frame_decoder_t dec(role);
  websocket::frame_t f;
  EXPECT_EQ(dec.parse(bytes, f), websocket::frame_decoder_t::result_t::Frame)
      << "error: " << dec.error().message();
  return f;
}

websocket::websocket_error_t parse_err(websocket::role_t role, std::string_view bytes) {
  websocket::frame_decoder_t dec(role);
  websocket::frame_t f;
  EXPECT_EQ(dec.parse(bytes, f), websocket::frame_decoder_t::result_t::Error);
  return websocket::websocket_error_t(dec.error().code);
}

} // namespace

// RFC 6455 §5.7 example: a masked single-frame text "Hello" from a client.
TEST(ws_frame, masked_text_hello) {
  const char bytes[] = {'\x81', '\x85', '\x37', '\xfa', '\x21', '\x3d',
                        '\x7f', '\x9f', '\x4d', '\x51', '\x58'};
  auto f = parse_ok(websocket::role_t::Server, std::string_view(bytes, sizeof(bytes)));
  EXPECT_TRUE(f.fin);
  EXPECT_EQ(f.opcode, websocket::opcode_t::Text);
  EXPECT_TRUE(f.masked);
  EXPECT_EQ(f.payload_len, 5u);
  EXPECT_EQ(f.header_len, 6u);

  std::string payload(bytes + f.header_len, f.payload_len);
  websocket::apply_mask(f.mask_key, payload.data(), payload.size());
  EXPECT_EQ(payload, "Hello");
}

TEST(ws_frame, unmasked_ping_rfc_shape) {
  // RFC 6455 §5.7: unmasked ping "Hello" as a server sends it
  auto w = wire(true, 0x9, "Hello", nullptr);
  ASSERT_EQ(w.size(), 2u + 5u);
  EXPECT_EQ(w[0], '\x89');
  EXPECT_EQ(w[1], '\x05');
  auto f = parse_ok(websocket::role_t::Client, w);
  EXPECT_EQ(f.opcode, websocket::opcode_t::Ping);
  EXPECT_FALSE(f.masked);
  EXPECT_EQ(f.payload_len, 5u);
}

TEST(ws_frame, masking_policy_by_role) {
  auto masked = wire(true, 0x1, "abc", "MASK");
  auto plain = wire(true, 0x1, "abc", nullptr);

  // server refuses unmasked client frames, client refuses masked server frames
  EXPECT_EQ(parse_err(websocket::role_t::Server, plain),
            websocket::websocket_error_t::UnmaskedFrame);
  EXPECT_EQ(parse_err(websocket::role_t::Client, masked),
            websocket::websocket_error_t::MaskedFrame);

  // and the mirror images parse fine
  parse_ok(websocket::role_t::Server, masked);
  parse_ok(websocket::role_t::Client, plain);
}

TEST(ws_frame, structural_violations) {
  // Pre-allocated buffer: the lambda below returns a string_view into this
  // so the data outlives each call (the old version stored the array inside
  // the lambda body, yielding a dangling string_view – only visible under
  // -O3 where the stack is reused between calls).
  char buf[8]{};
  auto with_first_byte = [&](uint8_t b0, uint8_t b1) {
    buf[0] = char(b0);
    buf[1] = char(b1);
    std::memset(buf + 2, 0, 6);
    return std::string_view(buf, sizeof(buf));
  };
  // RSV1 must be clear when no extension was negotiated
  EXPECT_EQ(parse_err(websocket::role_t::Client, with_first_byte(0xC1, 0x0)),
            websocket::websocket_error_t::ReservedBits);
  // opcodes 0x3..0x7 and 0xB..0xF are reserved
  EXPECT_EQ(parse_err(websocket::role_t::Client, with_first_byte(0x83, 0x0)),
            websocket::websocket_error_t::InvalidOpcode);
  EXPECT_EQ(parse_err(websocket::role_t::Client, with_first_byte(0x8B, 0x0)),
            websocket::websocket_error_t::InvalidOpcode);
  // a control frame never fragments and never exceeds 125 bytes of payload
  EXPECT_EQ(parse_err(websocket::role_t::Client, with_first_byte(0x09, 0x0)),
            websocket::websocket_error_t::FragmentedControl);
  EXPECT_EQ(parse_err(websocket::role_t::Client,
                      wire(true, 0x9, std::string(126, 'x'), nullptr)),
            websocket::websocket_error_t::ControlTooLarge);
}

TEST(ws_frame, extended_lengths_roundtrip) {
  for (size_t len : {126u, 127u, 1000u, 65535u, 65536u, 1u << 20}) {
    std::string body(len, 'q');
    auto w = wire(true, 0x2, body, nullptr);
    auto f = parse_ok(websocket::role_t::Client, w);
    EXPECT_EQ(f.payload_len, len) << "len=" << len;
    EXPECT_EQ(f.header_len + len, w.size()) << "header says more than the wire holds";
    EXPECT_EQ(w.substr(f.header_len), body);
  }
}

TEST(ws_frame, non_canonical_lengths_rejected) {
  // 126 form for a payload that fits in 7 bits; 127 form for one that fits in 16
  const char short_ext[] = {'\x81', '\x7e', '\x00', '\x7d'};
  EXPECT_EQ(parse_err(websocket::role_t::Client, std::string_view(short_ext, 4)),
            websocket::websocket_error_t::NonCanonicalLength);
  const char wide_ext[] = {'\x81', '\x7f', '\x00', '\x00', '\x00', '\x00',
                           '\x00', '\x00', '\xff', '\xff'};
  EXPECT_EQ(parse_err(websocket::role_t::Client, std::string_view(wide_ext, 10)),
            websocket::websocket_error_t::NonCanonicalLength);
  // the 64-bit form's MSB must stay clear (RFC 6455 §5.2)
  const char msb[] = {'\x81', '\x7f', '\xff', '\xff', '\xff', '\xff',
                      '\xff', '\xff', '\xff', '\xff'};
  EXPECT_EQ(parse_err(websocket::role_t::Client, std::string_view(msb, 10)),
            websocket::websocket_error_t::NonCanonicalLength);
}

TEST(ws_frame, need_more_until_header_complete) {
  auto w = wire(true, 0x2, std::string(70000, 'z'), "MASK");  // 127-form + mask
  websocket::frame_decoder_t dec(websocket::role_t::Server);
  websocket::frame_t f;
  for (size_t cut : {size_t(0), size_t(1), size_t(2), size_t(9), size_t(10), size_t(13)}) {
    EXPECT_EQ(dec.parse(std::string_view(w).substr(0, cut), f),
              websocket::frame_decoder_t::result_t::NeedMore)
        << "cut=" << cut;
  }
  EXPECT_EQ(dec.parse(w, f), websocket::frame_decoder_t::result_t::Frame);
  EXPECT_EQ(f.header_len, 14u);
  EXPECT_EQ(f.payload_len, 70000u);
}

TEST(ws_frame, continue_and_close_opcodes) {
  auto f = parse_ok(websocket::role_t::Client, wire(false, 0x0, "part", nullptr));
  EXPECT_FALSE(f.fin);
  EXPECT_EQ(f.opcode, websocket::opcode_t::Continue);

  char code[2] = {'\x03', '\xe8'};  // 1000 BE
  auto c = parse_ok(websocket::role_t::Client,
                    wire(true, 0x8, std::string_view(code, 2), nullptr));
  EXPECT_EQ(c.opcode, websocket::opcode_t::Close);
  EXPECT_EQ(c.payload_len, 2u);
}

TEST(ws_frame, apply_mask_offset_continuity) {
  // masking a buffer in two runs must equal masking it in one
  char mask[4] = {'\x12', '\x34', '\x56', '\x78'};
  uint32_t key;
  std::memcpy(&key, mask, 4);

  std::string whole(1000, '\0');
  for (size_t i = 0; i < whole.size(); ++i) whole[i] = char(i * 31 + 7);
  std::string one = whole, two = whole;

  websocket::apply_mask(key, one.data(), one.size());
  for (size_t cut : {size_t(1), size_t(2), size_t(3), size_t(4), size_t(5), size_t(777)}) {
    two = whole;
    websocket::apply_mask(key, two.data(), cut);
    websocket::apply_mask(key, two.data() + cut, two.size() - cut, cut);
    EXPECT_EQ(two, one) << "cut=" << cut;
  }

  // XOR is its own inverse: unmasking restores the plaintext
  websocket::apply_mask(key, one.data(), one.size());
  EXPECT_EQ(one, whole);
}
