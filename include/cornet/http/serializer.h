#ifndef CORNET_HTTP_SERIALIZER_H
#define CORNET_HTTP_SERIALIZER_H

#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>

#include "cornet/http/buffer.h"
#include "cornet/http/common.h"

namespace cornet::http {

/**
 * @brief write u64 as decimal into a caller buffer.
 *
 * snprintf would consult the locale and run a format-string interpreter for what
 * is a handful of divisions; Content-Length is written on every single response.
 * @param out at least 20 bytes
 * @return number of digits written
 */
uint32_t write_u64(char* out, uint64_t v);

/**
 * @brief write a chunked-transfer size line, e.g. "1f4\r\n".
 * @param out at least 18 bytes
 * @return bytes written
 */
uint32_t write_chunk_size(char* out, uint64_t v);

/**
 * @brief append-only output buffer for one connection's pending responses.
 *
 * Holds the head (status line plus headers) and any small bodies that were copied
 * in. Bodies that were not copied are referenced by the iovec list the connection
 * assembles, so a large response never passes through this buffer.
 *
 * Failures latch instead of being reported per call: a response is written by a
 * chain of a dozen small calls, and making each one return a checked error would
 * make handlers unreadable while adding nothing — there is exactly one useful
 * reaction to "the response did not fit", and it belongs at the end.
 */
class out_buffer_t {
 public:
  out_buffer_t() = default;

  void reset(buffer_lease_t lease) {
    lease_ = std::move(lease);
    size_ = 0;
    err_ = {};
  }

  void release() {
    lease_.release();
    size_ = 0;
    err_ = {};
  }

  void clear() {
    size_ = 0;
    err_ = {};
  }

  CORNET_NODISCARD bool     attached() const { return lease_.valid(); }
  CORNET_NODISCARD char*    data() const { return lease_.data(); }
  CORNET_NODISCARD uint32_t size() const { return size_; }
  CORNET_NODISCARD uint32_t capacity() const { return lease_.capacity(); }
  CORNET_NODISCARD uint32_t remaining() const {
    return lease_.valid() ? lease_.capacity() - size_ : 0;
  }

  CORNET_NODISCARD error_t error() const { return err_; }
  CORNET_NODISCARD bool failed() const { return bool(err_); }

  /**
   * @brief mark the buffer as failed; subsequent writes become no-ops.
   */
  void fail(error_t e) {
    if (!err_) err_ = e;
  }

  /**
   * @brief append bytes. No-op once failed.
   */
  void put(const char* data, uint32_t len) {
    if (err_) return;
    if (len > remaining()) {
      err_ = http_error(http_error_t::OutputOverflow);
      return;
    }
    std::memcpy(lease_.data() + size_, data, len);
    size_ += len;
  }

  void put(std::string_view s) { put(s.data(), uint32_t(s.size())); }

  void put_byte(char c) { put(&c, 1); }

  void put_crlf() { put("\r\n", 2); }

  void put_u64(uint64_t v) {
    if (err_) return;
    if (remaining() < 20) {
      err_ = http_error(http_error_t::OutputOverflow);
      return;
    }
    size_ += write_u64(lease_.data() + size_, v);
  }

  /**
   * @brief reserve a fixed run of bytes to be filled in later.
   * Used for Content-Length when the body size is not yet known.
   * @return offset of the reserved run, or 0 with the buffer failed
   */
  CORNET_NODISCARD uint32_t reserve(uint32_t len) {
    if (err_) return 0;
    if (len > remaining()) {
      err_ = http_error(http_error_t::OutputOverflow);
      return 0;
    }
    uint32_t off = size_;
    std::memset(lease_.data() + off, ' ', len);
    size_ += len;
    return off;
  }

  /**
   * @brief overwrite a previously reserved run, right-aligned and space-padded.
   */
  void patch_u64(uint32_t off, uint32_t len, uint64_t v) {
    if (err_ || !lease_.valid()) return;
    char tmp[20];
    uint32_t n = write_u64(tmp, v);
    if (n > len) {
      err_ = http_error(http_error_t::OutputOverflow);
      return;
    }
    char* dst = lease_.data() + off;
    std::memset(dst, ' ', len - n);
    std::memcpy(dst + (len - n), tmp, n);
  }

  CORNET_NODISCARD std::string_view view() const {
    return size_ ? std::string_view(lease_.data(), size_) : std::string_view{};
  }

  CORNET_NODISCARD std::string_view view(uint32_t off, uint32_t len) const {
    return {lease_.data() + off, len};
  }

 private:
  buffer_lease_t lease_;
  uint32_t size_{0};
  error_t  err_{};
};

/**
 * @brief writes status lines and headers into an out_buffer_t.
 *
 * Everything here is a memcpy of a pre-rendered constant or a hand-rolled integer
 * conversion. Nothing formats.
 */
class serializer_t {
 public:
  /**
   * @brief write "HTTP/1.1 <code> <reason>\r\n".
   * Falls back to composing the line for statuses outside the table.
   */
  static void status_line(out_buffer_t& out, status_t s);

  /**
   * @brief write "Name: value\r\n" for a known field.
   */
  static void header(out_buffer_t& out, field_t f, std::string_view value);

  /**
   * @brief write "name: value\r\n" for an arbitrary name.
   */
  static void header(out_buffer_t& out, std::string_view name, std::string_view value);

  /**
   * @brief write "Name: <number>\r\n".
   */
  static void header_u64(out_buffer_t& out, field_t f, uint64_t value);

  /**
   * @brief write "Date: <imf-fixdate>\r\n" from a pre-rendered string.
   * The string comes from the context's coarse clock, so this is a memcpy.
   */
  static void date_header(out_buffer_t& out, const char* imf_date, uint32_t len);

  /**
   * @brief terminate the header section.
   */
  static void end_headers(out_buffer_t& out) { out.put_crlf(); }
};

} // namespace cornet::http

#endif // CORNET_HTTP_SERIALIZER_H
