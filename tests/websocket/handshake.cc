#include <gtest/gtest.h>

#include <string>

#include "cornet/websocket/common/handshake.h"

using namespace cornet;

namespace {

http::out_buffer_t make_out(http::buffer_lease_t& lease) {
  lease = http::buffer_pool_t::local().acquire(4096);
  http::out_buffer_t out;
  out.reset(std::move(lease));
  return out;
}

} // namespace

// RFC 6455 §1.3: the one vector every implementation is expected to reproduce.
TEST(ws_handshake, accept_key_rfc_vector) {
  char accept[websocket::kAcceptLen];
  websocket::accept_key("dGhlIHNhbXBsZSBub25jZQ==", accept);
  EXPECT_EQ(std::string_view(accept, websocket::kAcceptLen), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
}

// Empty key still hashes (the header's presence was checked elsewhere).
TEST(ws_handshake, accept_key_empty) {
  // base64(SHA-1(GUID)) — cross-checked against a reference SHA-1
  char accept[websocket::kAcceptLen];
  websocket::accept_key("", accept);
  EXPECT_EQ(std::string_view(accept, websocket::kAcceptLen), "Kfh9QIsMVZcl6xEPYxPHzW8SZ8w=");
}

TEST(ws_handshake, make_key_shape_and_uniqueness) {
  char a[websocket::kKeyLen], b[websocket::kKeyLen];
  websocket::make_key(a);
  websocket::make_key(b);
  EXPECT_TRUE(websocket::key_valid(std::string_view(a, websocket::kKeyLen)));
  EXPECT_TRUE(websocket::key_valid(std::string_view(b, websocket::kKeyLen)));
  EXPECT_NE(std::string_view(a, websocket::kKeyLen), std::string_view(b, websocket::kKeyLen));
}

TEST(ws_handshake, key_valid) {
  EXPECT_TRUE(websocket::key_valid("dGhlIHNhbXBsZSBub25jZQ=="));
  EXPECT_FALSE(websocket::key_valid(""));
  EXPECT_FALSE(websocket::key_valid("abc"));
  EXPECT_FALSE(websocket::key_valid("dGhlIHNhbXBsZSBub25jZQ="));   // truncated
  EXPECT_FALSE(websocket::key_valid("dGhlIHNhbXBsZSBub25jQ=="));   // wrong padding shape
  EXPECT_FALSE(websocket::key_valid("dGhlIH*hYBsZSBub25jZQ=="));   // outside alphabet
  EXPECT_FALSE(websocket::key_valid(std::string_view("dGhlIHNhbXBsZSBub25jZQ==\0", 25)));
}

TEST(ws_handshake, base64_roundtrip) {
  const std::string raw = [] {
    std::string s;
    for (int i = 0; i < 256; ++i) s.push_back(char(i));
    return s;
  }();
  std::string enc(4 * ((raw.size() + 2) / 3), '\0');
  auto n = websocket::base64_encode(raw.data(), uint32_t(raw.size()), enc.data());
  enc.resize(n);
  std::string dec(raw.size(), '\0');
  ASSERT_TRUE(websocket::base64_decode(enc, dec.data(), uint32_t(dec.size())));
  EXPECT_EQ(dec, raw);
}

TEST(ws_handshake, base64_vectors) {
  // RFC 4648 §10 vectors
  char out[16];
  auto enc = [&](std::string_view in) {
    auto n = websocket::base64_encode(in.data(), uint32_t(in.size()), out);
    return std::string_view(out, n);
  };
  EXPECT_EQ(enc(""), "");
  EXPECT_EQ(enc("f"), "Zg==");
  EXPECT_EQ(enc("fo"), "Zm8=");
  EXPECT_EQ(enc("foo"), "Zm9v");
  EXPECT_EQ(enc("foob"), "Zm9vYg==");
  EXPECT_EQ(enc("fooba"), "Zm9vYmE=");
  EXPECT_EQ(enc("foobar"), "Zm9vYmFy");

  const std::string raw = "<<?!??!>>";
  char small[2];
  EXPECT_FALSE(websocket::base64_decode("Zm9vYmFy", small, sizeof(small)));
}

TEST(ws_handshake, frame_response_rfc_shape) {
  http::buffer_lease_t lease;
  auto out = make_out(lease);
  websocket::frame_handshake_response(out, "dGhlIHNhbXBsZSBub25jZQ==");
  ASSERT_FALSE(out.failed());
  EXPECT_EQ(out.view(),
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n"
            "\r\n");
}

TEST(ws_handshake, frame_response_with_subprotocol) {
  http::buffer_lease_t lease;
  auto out = make_out(lease);
  websocket::frame_handshake_response(out, "dGhlIHNhbXBsZSBub25jZQ==", "chat");
  ASSERT_FALSE(out.failed());
  EXPECT_EQ(out.view(),
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n"
            "Sec-WebSocket-Protocol: chat\r\n"
            "\r\n");
}

TEST(ws_handshake, frame_request_byte_exact) {
  http::buffer_lease_t lease;
  auto out = make_out(lease);
  websocket::frame_handshake_request(out, "server.example.com", "/chat", {},
                                     "dGhlIHNhbXBsZSBub25jZQ==", "chat, superchat");
  ASSERT_FALSE(out.failed());
  EXPECT_EQ(out.view(),
            "GET /chat HTTP/1.1\r\n"
            "Host: server.example.com\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "Sec-WebSocket-Protocol: chat, superchat\r\n"
            "\r\n");
}

TEST(ws_handshake, frame_request_defaults_and_query) {
  http::buffer_lease_t lease;
  auto out = make_out(lease);
  websocket::frame_handshake_request(out, "h", {}, "a=1", "AAAAAAAAAAAAAAAAAAAAAA==", {});
  ASSERT_FALSE(out.failed());
  EXPECT_EQ(out.view(),
            "GET /?a=1 HTTP/1.1\r\n"
            "Host: h\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Key: AAAAAAAAAAAAAAAAAAAAAA==\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n");
}
