#include "cornet/http/serializer.h"

namespace cornet::http {

namespace {

// two-digit pairs "00".."99", so the conversion below emits two digits per step
constexpr char kDigitPairs[201] =
    "00010203040506070809"
    "10111213141516171819"
    "20212223242526272829"
    "30313233343536373839"
    "40414243444546474849"
    "50515253545556575859"
    "60616263646566676869"
    "70717273747576777879"
    "80818283848586878889"
    "90919293949596979899";

constexpr char kHexDigits[] = "0123456789abcdef";

} // namespace

uint32_t write_u64(char* out, uint64_t v) {
  if (v == 0) {
    out[0] = '0';
    return 1;
  }
  // Build backwards into a scratch buffer, two digits at a time: one division per
  // two digits instead of one per digit.
  char tmp[20];
  char* p = tmp + sizeof(tmp);
  while (v >= 100) {
    auto idx = uint32_t(v % 100) * 2;
    v /= 100;
    *--p = kDigitPairs[idx + 1];
    *--p = kDigitPairs[idx];
  }
  if (v >= 10) {
    auto idx = uint32_t(v) * 2;
    *--p = kDigitPairs[idx + 1];
    *--p = kDigitPairs[idx];
  } else {
    *--p = char('0' + v);
  }
  auto n = uint32_t(tmp + sizeof(tmp) - p);
  std::memcpy(out, p, n);
  return n;
}

uint32_t write_chunk_size(char* out, uint64_t v) {
  char tmp[16];
  char* p = tmp + sizeof(tmp);
  if (v == 0) {
    *--p = '0';
  } else {
    while (v) {
      *--p = kHexDigits[v & 0xf];
      v >>= 4;
    }
  }
  auto n = uint32_t(tmp + sizeof(tmp) - p);
  std::memcpy(out, p, n);
  out[n] = '\r';
  out[n + 1] = '\n';
  return n + 2;
}

void serializer_t::status_line(out_buffer_t& out, status_t s) {
  uint32_t len = 0;
  if (const char* line = http::status_line(s, len)) {
    // The whole line is a constant for every status we know, so emitting it is
    // one memcpy rather than a format call.
    out.put(line, len);
    return;
  }
  // Not tabulated: compose it. Rare enough that the extra work is irrelevant.
  out.put("HTTP/1.1 ", 9);
  out.put_u64(uint16_t(s));
  out.put_byte(' ');
  out.put(reason_phrase(s));
  out.put_crlf();
}

void serializer_t::header(out_buffer_t& out, field_t f, std::string_view value) {
  uint32_t len = 0;
  if (const char* prefix = field_prefix(f, len)) {
    out.put(prefix, len);
  }
  out.put(value);
  out.put_crlf();
}

void serializer_t::header(out_buffer_t& out, std::string_view name, std::string_view value) {
  out.put(name);
  out.put(": ", 2);
  out.put(value);
  out.put_crlf();
}

void serializer_t::header_u64(out_buffer_t& out, field_t f, uint64_t value) {
  uint32_t len = 0;
  if (const char* prefix = field_prefix(f, len)) {
    out.put(prefix, len);
  }
  out.put_u64(value);
  out.put_crlf();
}

void serializer_t::date_header(out_buffer_t& out, const char* imf_date, uint32_t len) {
  if (!imf_date || len == 0) return;
  uint32_t plen = 0;
  const char* prefix = field_prefix(field_t::Date, plen);
  out.put(prefix, plen);
  out.put(imf_date, len);
  out.put_crlf();
}

} // namespace cornet::http
