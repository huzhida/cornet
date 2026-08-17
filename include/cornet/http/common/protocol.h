#ifndef CORNET_HTTP_COMMON_PROTOCOL_H
#define CORNET_HTTP_COMMON_PROTOCOL_H

#include <cstdint>
#include <cstring>
#include <string_view>

#include "cornet/base/defines.h"
#include "cornet/base/expected.h"

namespace cornet::http {

// ─────────────────────────── enums ───────────────────────────
// Enumerators are PascalCase, matching context_t::state_t (see docs §1.3).

/**
 * @brief HTTP request method. Values mirror llhttp_method so the mapping from
 * the parser is a range check plus a table lookup, never a string compare.
 */
enum class method_t : uint8_t {
  Delete = 0, Get = 1, Head = 2, Post = 3, Put = 4,
  Connect = 5, Options = 6, Trace = 7, Patch = 28,
  Unknown = 0xff,
};

/**
 * @brief protocol version. Only 1.0 and 1.1 are in scope.
 */
enum class version_t : uint8_t { Http10, Http11, Unknown };

/**
 * @brief response status. Carries the wire value so serialization is direct.
 */
enum class status_t : uint16_t {
  Continue = 100, SwitchingProtocols = 101,

  Ok = 200, Created = 201, Accepted = 202, NoContent = 204,
  ResetContent = 205, PartialContent = 206,

  MovedPermanently = 301, Found = 302, SeeOther = 303, NotModified = 304,
  TemporaryRedirect = 307, PermanentRedirect = 308,

  BadRequest = 400, Unauthorized = 401, Forbidden = 403, NotFound = 404,
  MethodNotAllowed = 405, NotAcceptable = 406, RequestTimeout = 408,
  Conflict = 409, Gone = 410, LengthRequired = 411, PreconditionFailed = 412,
  ContentTooLarge = 413, UriTooLong = 414, UnsupportedMediaType = 415,
  ExpectationFailed = 417, UnprocessableContent = 422, UpgradeRequired = 426,
  TooManyRequests = 429, RequestHeaderFieldsTooLarge = 431,

  InternalServerError = 500, NotImplemented = 501, BadGateway = 502,
  ServiceUnavailable = 503, GatewayTimeout = 504, HttpVersionNotSupported = 505,
};

/**
 * @brief well-known header names, so hot-path checks become integer compares.
 * Anything not listed maps to Other and is matched by name when asked for.
 */
enum class field_t : uint8_t {
  Host = 0,
  ContentLength,
  ContentType,
  TransferEncoding,
  Connection,
  Accept,
  AcceptEncoding,
  AcceptLanguage,
  UserAgent,
  Expect,
  Upgrade,
  Date,
  Server,
  Location,
  Cookie,
  SetCookie,
  Authorization,
  Referer,
  Range,
  IfNoneMatch,
  IfModifiedSince,
  ETag,
  CacheControl,
  Origin,
  Other,          // keep last: kFieldCount depends on it
};

// number of enumerated fields excluding Other
inline constexpr uint32_t kFieldCount = uint32_t(field_t::Other);
static_assert(kFieldCount <= 32, "field_bitmap_ in headers_t is a uint32_t");

// ─────────────────────── status / method text ───────────────────────

/**
 * @brief reason phrase for a status code, e.g. "Not Found".
 * @return the phrase, or "Unknown" for codes outside the table
 */
const char* reason_phrase(status_t s);

/**
 * @brief the full pre-rendered status line for HTTP/1.1, terminated by CRLF,
 * e.g. "HTTP/1.1 404 Not Found\r\n".
 *
 * Serializing a response should never format this: it is a fixed string per
 * status, so the write path is one memcpy.
 * @param s status code
 * @param len receives the length in bytes
 * @return pointer to a static string, or nullptr if the status is not tabulated
 */
const char* status_line(status_t s, uint32_t& len);

/**
 * @brief uppercase method name, e.g. "GET".
 */
const char* method_name(method_t m);

/**
 * @brief map an llhttp method value to method_t.
 * @param raw llhttp_method value
 */
method_t method_from_raw(uint8_t raw);

/**
 * @brief whether a response to this method may carry a body at all.
 * HEAD responses must report Content-Length yet send no body.
 */
inline bool method_expects_body(method_t m) { return m != method_t::Head; }

/**
 * @brief whether this status forbids a message body entirely.
 * 1xx / 204 / 304 must send neither a body nor Content-Length; emitting one is
 * a framing error that desynchronizes keep-alive connections.
 */
inline bool status_forbids_body(status_t s) {
  auto code = uint16_t(s);
  return (code >= 100 && code < 200) || code == 204 || code == 304;
}

/**
 * @brief whether this is an interim (1xx) response.
 *
 * A client must swallow these and keep reading: `100 Continue` and `103 Early
 * Hints` arrive *before* the real response and are not the answer to anything.
 */
inline bool status_is_informational(status_t s) {
  auto code = uint16_t(s);
  return code >= 100 && code < 200;
}

/**
 * @brief whether this field must never be honoured from a chunked trailer.
 *
 * RFC 9110 §6.5.1 forbids framing, routing, request modifiers, authentication and
 * response control data in trailers — they all get interpreted long before the
 * trailer arrives, so accepting one there is how request smuggling and
 * after-the-fact header spoofing start. Such a trailer is dropped, not rejected: a
 * sloppy peer should not cost an otherwise valid message.
 */
bool field_forbidden_in_trailer(field_t f);

/**
 * @brief whether a failed request with this method may be retried automatically.
 *
 * Only relevant to the client: a keep-alive pool always races the peer closing an
 * idle connection, and replaying a non-idempotent request to paper over that race
 * would be worse than the error.
 */
inline bool method_is_idempotent(method_t m) {
  switch (m) {
    case method_t::Get:
    case method_t::Head:
    case method_t::Put:
    case method_t::Delete:
    case method_t::Options:
    case method_t::Trace:
      return true;
    default:
      return false;
  }
}

// ───────────────────────── header name text ─────────────────────────

/**
 * @brief canonical header name for a field, e.g. "Content-Length".
 */
const char* field_name(field_t f);

/**
 * @brief canonical header name followed by ": ", e.g. "Content-Length: ".
 * Lets the writer emit a name with a single memcpy.
 * @param f field
 * @param len receives the length including the colon and space
 */
const char* field_prefix(field_t f, uint32_t& len);

/**
 * @brief identify a header name using SWAR (SIMD Within A Register).
 *
 * Reads the name 8 bytes at a time into a uint64_t, lowercases the whole word
 * with a single OR (bit 0x20 is the ASCII case bit), and compares as integers.
 * A per-character branch table costs one unpredictable branch per character;
 * this costs one switch on length plus one or two integer compares.
 *
 * @param name header name, not NUL-terminated
 * @return the matching field, or field_t::Other
 */
field_t field_from_name(std::string_view name);

/**
 * @brief case-insensitive ASCII compare, for header names outside the table.
 */
bool iequals(std::string_view a, std::string_view b);

// ───────────────────────────── errors ─────────────────────────────

/**
 * @brief HTTP-layer error codes carried in error_domain::Http.
 *
 * Values below kProtocolErrorBase are llhttp errno values passed through
 * verbatim, so a parser failure keeps its precise identity; values at or above
 * the base are conditions this module detects itself.
 */
inline constexpr int kProtocolErrorBase = 1000;

enum class http_error_t : int {
  HeaderTooLarge   = kProtocolErrorBase + 1,   // exceeds max_header_bytes
  TooManyHeaders   = kProtocolErrorBase + 2,
  BodyTooLarge     = kProtocolErrorBase + 3,   // exceeds max_body_bytes
  BadContentLength = kProtocolErrorBase + 4,
  UnsupportedVersion = kProtocolErrorBase + 5,
  OutputOverflow   = kProtocolErrorBase + 6,   // response exceeded its buffer
  InvalidState     = kProtocolErrorBase + 7,   // API misuse, e.g. body after finish
  BadUpgrade       = kProtocolErrorBase + 8,
  // ── client side ──
  BadUrl             = kProtocolErrorBase + 9,   // unparseable request url
  UnsupportedScheme  = kProtocolErrorBase + 10,  // a scheme this build cannot speak
  TooManyRedirects   = kProtocolErrorBase + 11,
  PoolExhausted      = kProtocolErrorBase + 12,  // no connection available in time
  ResponseIncomplete = kProtocolErrorBase + 13,  // peer closed mid-message
};

/**
 * @brief build an error_t in the HTTP domain.
 */
inline error_t http_error(http_error_t e) {
  return error_t{int(e), error_domain::Http};
}

/**
 * @brief build an unexpected in the HTTP domain.
 */
inline unexpected http_unexpected(http_error_t e) {
  return unexpected(int(e), error_domain::Http);
}

/**
 * @brief render an HTTP-domain code. Registered into the core's resolver so
 * error_t::message() works without the core linking against llhttp.
 */
const char* http_error_name(int code);

/**
 * @brief the status code a protocol error should be answered with.
 */
status_t status_for_error(error_t err);

} // namespace cornet::http

#endif // CORNET_HTTP_COMMON_PROTOCOL_H
