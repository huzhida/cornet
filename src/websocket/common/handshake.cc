#include "cornet/websocket/common/handshake.h"

#include "cornet/websocket/common/protocol.h"

#include <sys/random.h>

#include <array>
#include <cstring>

namespace cornet::websocket {

namespace {

// ─────────────────────────────── SHA-1 ───────────────────────────────
// Compact FIPS 180-1 implementation. It exists for exactly one digest —
// SHA-1(key + GUID) per handshake — so there is no streaming API beyond
// what that needs. OpenSSL stays inside the tls module: pulling it in
// here would hand every cornet_http user a transitive OpenSSL link for
// one 64-block hash.

class sha1_t {
 public:
  sha1_t() {
    h_[0] = 0x67452301u;
    h_[1] = 0xEFCDAB89u;
    h_[2] = 0x98BADCFEu;
    h_[3] = 0x10325476u;
    h_[4] = 0xC3D2E1F0u;
  }

  void update(const void* data, size_t len) {
    auto* p = static_cast<const uint8_t*>(data);
    total_ += len;
    while (len > 0) {
      size_t take = 64 - used_ < len ? 64 - used_ : len;
      std::memcpy(block_ + used_, p, take);
      used_ += take;
      p += take;
      len -= take;
      if (used_ == 64) {
        compress(block_);
        used_ = 0;
      }
    }
  }

  void final(uint8_t out[20]) {
    uint64_t bits = total_ * 8;
    uint8_t pad = 0x80;
    update(&pad, 1);
    std::array<uint8_t, 64> zeros{};
    // pad with zeros until the block has exactly 8 bytes left for the length
    size_t want = used_ <= 56 ? 56 - used_ : 64 + 56 - used_;
    update(zeros.data(), want);
    uint8_t len_be[8];
    for (uint32_t i = 0; i < 8; ++i) len_be[i] = uint8_t(bits >> (56 - 8 * i));
    update(len_be, 8);
    for (uint32_t i = 0; i < 5; ++i) {
      out[4 * i]     = uint8_t(h_[i] >> 24);
      out[4 * i + 1] = uint8_t(h_[i] >> 16);
      out[4 * i + 2] = uint8_t(h_[i] >> 8);
      out[4 * i + 3] = uint8_t(h_[i]);
    }
  }

 private:
  static uint32_t rol(uint32_t v, uint32_t n) { return v << n | v >> (32 - n); }

  void compress(const uint8_t block[64]) {
    uint32_t w[80];
    for (uint32_t i = 0; i < 16; ++i) {
      w[i] = uint32_t(block[4 * i]) << 24 | uint32_t(block[4 * i + 1]) << 16 |
             uint32_t(block[4 * i + 2]) << 8 | uint32_t(block[4 * i + 3]);
    }
    for (uint32_t i = 16; i < 80; ++i) w[i] = rol(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);

    uint32_t a = h_[0], b = h_[1], c = h_[2], d = h_[3], e = h_[4];
    for (uint32_t i = 0; i < 80; ++i) {
      uint32_t f, k;
      if (i < 20) {
        f = (b & c) | (~b & d);
        k = 0x5A827999u;
      } else if (i < 40) {
        f = b ^ c ^ d;
        k = 0x6ED9EBA1u;
      } else if (i < 60) {
        f = (b & c) | (b & d) | (c & d);
        k = 0x8F1BBCDCu;
      } else {
        f = b ^ c ^ d;
        k = 0xCA62C1D6u;
      }
      uint32_t t = rol(a, 5) + f + e + k + w[i];
      e = d;
      d = c;
      c = rol(b, 30);
      b = a;
      a = t;
    }
    h_[0] += a;
    h_[1] += b;
    h_[2] += c;
    h_[3] += d;
    h_[4] += e;
  }

  uint32_t h_[5];
  uint8_t  block_[64];
  size_t   used_{0};
  uint64_t total_{0};
};

} // namespace

void random_bytes(void* buf, size_t len) {
  // getrandom(2) with no flags: safe at any point in the process lifetime and
  // never touches an fd — which an io_uring program must be careful about,
  // since a shared fd table is how accidental closes cascade.
  auto* p = static_cast<uint8_t*>(buf);
  while (len > 0) {
    ssize_t n = ::getrandom(p, len, 0);
    if (n > 0) {
      p += n;
      len -= size_t(n);
    }
  }
}

// ─────────────────────────────── base64 ───────────────────────────────

uint32_t base64_encode(const void* data, uint32_t len, char* out) {
  static constexpr char kAlphabet[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  auto* in = static_cast<const uint8_t*>(data);
  uint32_t o = 0;
  uint32_t i = 0;
  for (; i + 3 <= len; i += 3) {
    uint32_t v = uint32_t(in[i]) << 16 | uint32_t(in[i + 1]) << 8 | in[i + 2];
    out[o++] = kAlphabet[v >> 18];
    out[o++] = kAlphabet[v >> 12 & 0x3F];
    out[o++] = kAlphabet[v >> 6 & 0x3F];
    out[o++] = kAlphabet[v & 0x3F];
  }
  if (i < len) {
    uint32_t v = uint32_t(in[i]) << 16;
    const bool two = i + 1 < len;
    if (two) v |= uint32_t(in[i + 1]) << 8;
    out[o++] = kAlphabet[v >> 18];
    out[o++] = kAlphabet[v >> 12 & 0x3F];
    out[o++] = two ? kAlphabet[v >> 6 & 0x3F] : '=';
    out[o++] = '=';
  }
  return o;
}

bool base64_decode(std::string_view in, void* out, uint32_t cap) {
  if (in.empty() || in.size() % 4 != 0) return false;
  auto val = [](char c) -> int {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return 26 + (c - 'a');
    if (c >= '0' && c <= '9') return 52 + (c - '0');
    if (c == '+') return 62;
    if (c == '/') return 63;
    if (c == '=') return 0;
    return -1;
  };
  auto* dst = static_cast<uint8_t*>(out);
  uint32_t o = 0;
  for (size_t i = 0; i < in.size(); i += 4) {
    // padding belongs to the final group only, at most two of it
    const bool last = i + 4 == in.size();
    int pad = 0;
    int v[4];
    for (uint32_t j = 0; j < 4; ++j) {
      char c = in[i + j];
      if (c == '=') {
        if (!last || j < 2) return false;
        ++pad;
      } else if (pad) {
        return false;
      }
      v[j] = val(c);
      if (v[j] < 0) return false;
    }
    if (pad > 2) return false;
    uint32_t word = uint32_t(v[0]) << 18 | uint32_t(v[1]) << 12 |
                    uint32_t(v[2]) << 6 | uint32_t(v[3]);
    const uint32_t bytes = 3 - uint32_t(pad);
    if (o + bytes > cap) return false;
    dst[o++] = uint8_t(word >> 16);
    if (bytes > 1) dst[o++] = uint8_t(word >> 8);
    if (bytes > 2) dst[o++] = uint8_t(word);
  }
  return true;
}

// ─────────────────────────────── keys ───────────────────────────────

void accept_key(std::string_view key, char* out) {
  sha1_t sha;
  sha.update(key.data(), key.size());
  sha.update(kRfc6455Guid.data(), kRfc6455Guid.size());
  uint8_t digest[20];
  sha.final(digest);
  base64_encode(digest, sizeof(digest), out);
}

void make_key(char* out) {
  uint8_t nonce[16];
  random_bytes(nonce, sizeof(nonce));
  base64_encode(nonce, sizeof(nonce), out);
}

bool key_valid(std::string_view key) {
  if (key.size() != kKeyLen) return false;
  uint8_t nonce[16];
  return base64_decode(key, nonce, sizeof(nonce));
}

// ──────────────────────────── handshake wire ────────────────────────────

void frame_handshake_response(http::out_buffer_t& out, std::string_view key,
                              std::string_view subprotocol) {
  // The same discipline as the response machinery: a value with CR/LF in it
  // is a smuggled header, not a header
  for (char c : subprotocol) {
    if (c == '\r' || c == '\n') {
      out.fail(http::http_error(http::http_error_t::InvalidHeader));
      return;
    }
  }
  // The status line is a pre-rendered constant; the only computed value in
  // the whole block is the accept hash.
  uint32_t n = 0;
  auto* line = http::status_line(http::status_t::SwitchingProtocols, n);
  out.put(line, n);
  out.put(kHdrUpgradeWebSocket);
  out.put(kHdrConnUpgrade);
  out.put(kSecWebSocketAccept);
  out.put(": ", 2);
  char accept[kAcceptLen];
  accept_key(key, accept);
  out.put(accept, kAcceptLen);
  out.put_crlf();
  if (!subprotocol.empty()) {
    out.put(kSecWebSocketProtocol);
    out.put(": ", 2);
    out.put(subprotocol);
    out.put_crlf();
  }
  out.put_crlf();
}

void frame_handshake_request(http::out_buffer_t& out, std::string_view host,
                             std::string_view path, std::string_view query,
                             std::string_view key, std::string_view subprotocols) {
  http::serializer_t::request_line(out, http::method_t::Get, path, query,
                                   http::version_t::Http11);
  http::serializer_t::header(out, http::field_t::Host, host);
  out.put(kHdrUpgradeWebSocket);
  out.put(kHdrConnUpgrade);
  out.put(kSecWebSocketKey);
  out.put(": ", 2);
  out.put(key);
  out.put_crlf();
  // 13 is the only version this implementation speaks, so it is a constant.
  out.put("Sec-WebSocket-Version: 13\r\n", 27);
  if (!subprotocols.empty()) {
    out.put(kSecWebSocketProtocol);
    out.put(": ", 2);
    out.put(subprotocols);
    out.put_crlf();
  }
  out.put_crlf();
}

// ─────────────────────────────── errors ───────────────────────────────

const char* websocket_error_name(int code) {
  switch (websocket_error_t(code)) {
    case websocket_error_t::ReservedBits:           return "reserved frame bits set";
    case websocket_error_t::InvalidOpcode:          return "reserved opcode on the wire";
    case websocket_error_t::UnmaskedFrame:          return "unmasked frame from a client";
    case websocket_error_t::MaskedFrame:            return "masked frame from a server";
    case websocket_error_t::FragmentedControl:      return "fragmented control frame";
    case websocket_error_t::ControlTooLarge:        return "control payload over 125 bytes";
    case websocket_error_t::NonCanonicalLength:     return "frame length not in minimal form";
    case websocket_error_t::MessageTooLarge:        return "message exceeded the size limit";
    case websocket_error_t::ContinuationExpected:   return "new message started mid-fragment";
    case websocket_error_t::ContinuationUnexpected: return "continue frame with no open message";
    case websocket_error_t::InvalidCloseCode:       return "close code that must not be sent";
    case websocket_error_t::HandshakeFailed:        return "opening handshake failed";
    case websocket_error_t::Closed:                 return "session already closed";
    default:                                        return "unknown websocket error";
  }
}

namespace {

// Same load-time registration pattern as the http and tls tables: the core
// renders Websocket-domain codes through this slot without linking the module.
const bool kResolverRegistered = [] {
  websocket_message_resolver() = &websocket_error_name;
  return true;
}();

} // namespace

} // namespace cornet::websocket
