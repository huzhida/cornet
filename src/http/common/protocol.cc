#include "cornet/http/common/protocol.h"

#include <llhttp.h>

namespace cornet::http {

namespace {

// ─────────────────────────── SWAR helpers ───────────────────────────

// ASCII case bit. OR-ing it lowercases letters and leaves digits, '-' and '_'
// unchanged (they all already have 0x20 set), which is the entire character set
// a valid header name can use.
constexpr uint64_t kLower = 0x2020202020202020ull;

/**
 * @brief pack up to 8 bytes of a name into a little-endian word, lowercased.
 * constexpr so the comparison values are computed at compile time.
 */
constexpr uint64_t pack(std::string_view s) {
  uint64_t v = 0;
  const size_t n = s.size() < 8 ? s.size() : 8;
  for (size_t i = 0; i < n; ++i) {
    v |= (uint64_t(uint8_t(s[i])) | 0x20ull) << (8 * i);
  }
  return v;
}

/**
 * @brief load exactly N bytes and lowercase them in one OR.
 * N is a compile-time constant, so this is a single unaligned load plus mask.
 */
template <size_t N>
inline uint64_t ld(const char* p) {
  static_assert(N >= 1 && N <= 8, "SWAR word is 8 bytes");
  uint64_t v = 0;
  std::memcpy(&v, p, N);
  v |= kLower;
  if constexpr (N < 8) {
    v &= (uint64_t(1) << (8 * N)) - 1;
  }
  return v;
}

// For names longer than 8 bytes, compare the first word and the *last* word.
// The two windows overlap for lengths 9..15, which is harmless and cheaper than
// a third load plus a tail mask.
#define HTTP_M8(p, lit, len) \
  (ld<8>(p) == pack(lit) && ld<8>((p) + (len) - 8) == pack(std::string_view(lit).substr((len) - 8)))

} // namespace

field_t field_from_name(std::string_view name) {
  const char* p = name.data();
  switch (name.size()) {
    case 4:
      if (ld<4>(p) == pack("host")) return field_t::Host;
      if (ld<4>(p) == pack("date")) return field_t::Date;
      if (ld<4>(p) == pack("etag")) return field_t::ETag;
      break;
    case 5:
      if (ld<5>(p) == pack("range")) return field_t::Range;
      break;
    case 6:
      if (ld<6>(p) == pack("accept")) return field_t::Accept;
      if (ld<6>(p) == pack("expect")) return field_t::Expect;
      if (ld<6>(p) == pack("server")) return field_t::Server;
      if (ld<6>(p) == pack("cookie")) return field_t::Cookie;
      if (ld<6>(p) == pack("origin")) return field_t::Origin;
      break;
    case 7:
      if (ld<7>(p) == pack("upgrade")) return field_t::Upgrade;
      if (ld<7>(p) == pack("referer")) return field_t::Referer;
      break;
    case 8:
      if (ld<8>(p) == pack("location")) return field_t::Location;
      break;
    case 10:
      if (HTTP_M8(p, "connection", 10)) return field_t::Connection;
      if (HTTP_M8(p, "user-agent", 10)) return field_t::UserAgent;
      if (HTTP_M8(p, "set-cookie", 10)) return field_t::SetCookie;
      break;
    case 12:
      if (HTTP_M8(p, "content-type", 12)) return field_t::ContentType;
      break;
    case 13:
      if (HTTP_M8(p, "authorization", 13)) return field_t::Authorization;
      if (HTTP_M8(p, "if-none-match", 13)) return field_t::IfNoneMatch;
      if (HTTP_M8(p, "cache-control", 13)) return field_t::CacheControl;
      break;
    case 14:
      if (HTTP_M8(p, "content-length", 14)) return field_t::ContentLength;
      break;
    case 15:
      if (HTTP_M8(p, "accept-encoding", 15)) return field_t::AcceptEncoding;
      if (HTTP_M8(p, "accept-language", 15)) return field_t::AcceptLanguage;
      break;
    case 17:
      if (HTTP_M8(p, "transfer-encoding", 17)) return field_t::TransferEncoding;
      if (HTTP_M8(p, "if-modified-since", 17)) return field_t::IfModifiedSince;
      break;
    default:
      break;
  }
  return field_t::Other;
}

#undef HTTP_M8

bool iequals(std::string_view a, std::string_view b) {
  if (a.size() != b.size()) return false;
  for (size_t i = 0; i < a.size(); ++i) {
    if ((uint8_t(a[i]) | 0x20) != (uint8_t(b[i]) | 0x20)) return false;
  }
  return true;
}

// ─────────────────────────── name tables ───────────────────────────

namespace {

struct field_text_t {
  const char* name;
  const char* prefix;   // name + ": "
  uint32_t    prefix_len;
};

#define HTTP_FIELD(literal) {literal, literal ": ", uint32_t(sizeof(literal ": ") - 1)}

// indexed by field_t, so order must match the enum exactly
constexpr field_text_t kFieldText[] = {
  HTTP_FIELD("Host"),
  HTTP_FIELD("Content-Length"),
  HTTP_FIELD("Content-Type"),
  HTTP_FIELD("Transfer-Encoding"),
  HTTP_FIELD("Connection"),
  HTTP_FIELD("Accept"),
  HTTP_FIELD("Accept-Encoding"),
  HTTP_FIELD("Accept-Language"),
  HTTP_FIELD("User-Agent"),
  HTTP_FIELD("Expect"),
  HTTP_FIELD("Upgrade"),
  HTTP_FIELD("Date"),
  HTTP_FIELD("Server"),
  HTTP_FIELD("Location"),
  HTTP_FIELD("Cookie"),
  HTTP_FIELD("Set-Cookie"),
  HTTP_FIELD("Authorization"),
  HTTP_FIELD("Referer"),
  HTTP_FIELD("Range"),
  HTTP_FIELD("If-None-Match"),
  HTTP_FIELD("If-Modified-Since"),
  HTTP_FIELD("ETag"),
  HTTP_FIELD("Cache-Control"),
  HTTP_FIELD("Origin"),
};

#undef HTTP_FIELD

static_assert(sizeof(kFieldText) / sizeof(kFieldText[0]) == kFieldCount,
              "kFieldText must stay in sync with field_t");

} // namespace

const char* field_name(field_t f) {
  auto i = uint32_t(f);
  return i < kFieldCount ? kFieldText[i].name : "";
}

const char* field_prefix(field_t f, uint32_t& len) {
  auto i = uint32_t(f);
  if (i >= kFieldCount) {
    len = 0;
    return nullptr;
  }
  len = kFieldText[i].prefix_len;
  return kFieldText[i].prefix;
}

// ───────────────────────── status / method ─────────────────────────

// One entry per status we can emit. The full status line is pre-rendered so the
// write path never formats an integer or looks up a reason phrase separately.
#define CORNET_STATUS_MAP(XX)                                        \
  XX(Continue,                    100, "Continue")                 \
  XX(SwitchingProtocols,          101, "Switching Protocols")      \
  XX(Ok,                          200, "OK")                       \
  XX(Created,                     201, "Created")                  \
  XX(Accepted,                    202, "Accepted")                 \
  XX(NoContent,                   204, "No Content")               \
  XX(ResetContent,                205, "Reset Content")            \
  XX(PartialContent,              206, "Partial Content")          \
  XX(MovedPermanently,            301, "Moved Permanently")        \
  XX(Found,                       302, "Found")                    \
  XX(SeeOther,                    303, "See Other")                \
  XX(NotModified,                 304, "Not Modified")             \
  XX(TemporaryRedirect,           307, "Temporary Redirect")       \
  XX(PermanentRedirect,           308, "Permanent Redirect")       \
  XX(BadRequest,                  400, "Bad Request")              \
  XX(Unauthorized,                401, "Unauthorized")             \
  XX(Forbidden,                   403, "Forbidden")                \
  XX(NotFound,                    404, "Not Found")                \
  XX(MethodNotAllowed,            405, "Method Not Allowed")       \
  XX(NotAcceptable,               406, "Not Acceptable")           \
  XX(RequestTimeout,              408, "Request Timeout")          \
  XX(Conflict,                    409, "Conflict")                 \
  XX(Gone,                        410, "Gone")                     \
  XX(LengthRequired,              411, "Length Required")          \
  XX(PreconditionFailed,          412, "Precondition Failed")      \
  XX(ContentTooLarge,             413, "Content Too Large")        \
  XX(UriTooLong,                  414, "URI Too Long")             \
  XX(UnsupportedMediaType,        415, "Unsupported Media Type")   \
  XX(ExpectationFailed,           417, "Expectation Failed")       \
  XX(UnprocessableContent,        422, "Unprocessable Content")    \
  XX(UpgradeRequired,             426, "Upgrade Required")         \
  XX(TooManyRequests,             429, "Too Many Requests")        \
  XX(RequestHeaderFieldsTooLarge, 431, "Request Header Fields Too Large") \
  XX(InternalServerError,         500, "Internal Server Error")    \
  XX(NotImplemented,              501, "Not Implemented")          \
  XX(BadGateway,                  502, "Bad Gateway")              \
  XX(ServiceUnavailable,          503, "Service Unavailable")      \
  XX(GatewayTimeout,              504, "Gateway Timeout")          \
  XX(HttpVersionNotSupported,     505, "HTTP Version Not Supported")

const char* reason_phrase(status_t s) {
  switch (s) {
#define XX(name, code, phrase) case status_t::name: return phrase;
    CORNET_STATUS_MAP(XX)
#undef XX
    default: return "Unknown";
  }
}

const char* status_line(status_t s, uint32_t& len) {
  switch (s) {
#define XX(name, code, phrase)                                    \
    case status_t::name:                                          \
      len = uint32_t(sizeof("HTTP/1.1 " #code " " phrase "\r\n") - 1); \
      return "HTTP/1.1 " #code " " phrase "\r\n";
    CORNET_STATUS_MAP(XX)
#undef XX
    default:
      len = 0;
      return nullptr;
  }
}

#undef CORNET_STATUS_MAP

const char* method_name(method_t m) {
  switch (m) {
    case method_t::Get: return "GET";
    case method_t::Head: return "HEAD";
    case method_t::Post: return "POST";
    case method_t::Put: return "PUT";
    case method_t::Delete: return "DELETE";
    case method_t::Connect: return "CONNECT";
    case method_t::Options: return "OPTIONS";
    case method_t::Trace: return "TRACE";
    case method_t::Patch: return "PATCH";
    default: return "UNKNOWN";
  }
}

method_t method_from_raw(uint8_t raw) {
  switch (raw) {
    case HTTP_DELETE:  return method_t::Delete;
    case HTTP_GET:     return method_t::Get;
    case HTTP_HEAD:    return method_t::Head;
    case HTTP_POST:    return method_t::Post;
    case HTTP_PUT:     return method_t::Put;
    case HTTP_CONNECT: return method_t::Connect;
    case HTTP_OPTIONS: return method_t::Options;
    case HTTP_TRACE:   return method_t::Trace;
    case HTTP_PATCH:   return method_t::Patch;
    default:           return method_t::Unknown;
  }
}

bool field_forbidden_in_trailer(field_t f) {
  switch (f) {
    // framing: already decided when the header section ended
    case field_t::ContentLength:
    case field_t::TransferEncoding:
    case field_t::ContentType:
    // routing and connection control
    case field_t::Host:
    case field_t::Connection:
    case field_t::Upgrade:
    // request modifiers
    case field_t::Expect:
    case field_t::Range:
    case field_t::CacheControl:
    // authentication and state
    case field_t::Authorization:
    case field_t::Cookie:
    case field_t::SetCookie:
    // response control data
    case field_t::Date:
    case field_t::Location:
      return true;
    default:
      return false;
  }
}

// ───────────────────────────── errors ─────────────────────────────

const char* http_error_name(int code) {
  switch (http_error_t(code)) {
    case http_error_t::HeaderTooLarge:     return "header section too large";
    case http_error_t::TooManyHeaders:     return "too many header fields";
    case http_error_t::BodyTooLarge:       return "request body too large";
    case http_error_t::BadContentLength:   return "invalid Content-Length";
    case http_error_t::UnsupportedVersion: return "unsupported HTTP version";
    case http_error_t::OutputOverflow:     return "response exceeded output buffer";
    case http_error_t::InvalidState:       return "response api used out of order";
    case http_error_t::BadUpgrade:         return "invalid protocol upgrade";
    case http_error_t::BadUrl:             return "malformed url";
    case http_error_t::UnsupportedScheme:  return "unsupported url scheme";
    case http_error_t::TooManyRedirects:   return "too many redirects";
    case http_error_t::PoolExhausted:      return "no connection available";
    case http_error_t::ResponseIncomplete: return "peer closed before the response ended";
    case http_error_t::InvalidHeader:      return "CR/LF in header name or value";
    default: break;
  }
  if (code >= 0 && code < kProtocolErrorBase) {
    // pass-through llhttp errno, so a parse failure keeps its exact identity
    return llhttp_errno_name(llhttp_errno_t(code));
  }
  return "unknown http error";
}

status_t status_for_error(error_t err) {
  if (err.domain != error_domain::Http) return status_t::InternalServerError;
  switch (http_error_t(err.code)) {
    case http_error_t::HeaderTooLarge:
    case http_error_t::TooManyHeaders:
      return status_t::RequestHeaderFieldsTooLarge;
    case http_error_t::BodyTooLarge:
      return status_t::ContentTooLarge;
    case http_error_t::UnsupportedVersion:
      return status_t::HttpVersionNotSupported;
    case http_error_t::OutputOverflow:
    case http_error_t::InvalidState:
    case http_error_t::InvalidHeader:
      return status_t::InternalServerError;
    default:
      break;
  }
  switch (llhttp_errno_t(err.code)) {
    case HPE_INVALID_METHOD:
      return status_t::NotImplemented;
    case HPE_INVALID_VERSION:
      return status_t::HttpVersionNotSupported;
    case HPE_INVALID_URL:
      return status_t::UriTooLong;
    default:
      // every other llhttp failure is a malformed request
      return status_t::BadRequest;
  }
}

namespace {

/**
 * @brief register the HTTP renderer with the core at load time.
 *
 * The core links only liburing, so error_t::message() cannot call
 * llhttp_errno_name() itself; it consults this slot instead. Registering from a
 * namespace-scope initializer means any error surfaced after this translation
 * unit is loaded renders precisely.
 */
const bool kResolverRegistered = [] {
  http_message_resolver() = &http_error_name;
  return true;
}();

} // namespace

} // namespace cornet::http
