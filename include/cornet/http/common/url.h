#ifndef CORNET_HTTP_COMMON_URL_H
#define CORNET_HTTP_COMMON_URL_H

#include <cstdint>
#include <string_view>

#include "cornet/base/defines.h"
#include "cornet/http/common/protocol.h"

namespace cornet::http {

/**
 * @brief url scheme. Only the two HTTP schemes are in scope.
 */
enum class scheme_t : uint8_t { Http, Https };

/**
 * @brief the port a scheme implies when the url does not spell one out.
 */
inline uint16_t default_port(scheme_t s) { return s == scheme_t::Https ? 443 : 80; }

inline std::string_view scheme_name(scheme_t s) {
  return s == scheme_t::Https ? std::string_view("https") : std::string_view("http");
}

/**
 * @brief a parsed absolute url, held entirely as views into the caller's bytes.
 *
 * Nothing is copied and nothing is decoded. Percent-escapes are left exactly as
 * they arrived, for the same reason `request_t::path()` leaves them alone: decoding
 * would have to allocate, and the only correct place to decode is where the value
 * is finally used. The caller owns the string the views point into and must keep it
 * alive for as long as the url is used — the client copies the url text into a
 * pooled block precisely so that this holds across a retry or a redirect.
 *
 * Only absolute urls parse. A client that accepted origin-form ("/path") would have
 * to guess the host, and guessing hosts is how requests end up at the wrong server.
 */
class url_t {
 public:
  url_t() = default;

  /**
   * @brief parse an absolute url.
   * @return BadUrl for malformed input, UnsupportedScheme for anything but http(s)
   */
  CORNET_NODISCARD static expected<url_t> parse(std::string_view raw);

  CORNET_NODISCARD scheme_t scheme() const { return scheme_; }

  /**
   * @brief host with no brackets, i.e. what a resolver wants: "::1", not "[::1]".
   */
  CORNET_NODISCARD std::string_view host() const { return host_; }

  /**
   * @brief authority as written, brackets and explicit port included.
   * This is exactly what a Host header needs, so emitting one is a single memcpy.
   */
  CORNET_NODISCARD std::string_view authority() const { return authority_; }

  CORNET_NODISCARD uint16_t port() const { return port_; }

  /**
   * @brief whether the url spelled the port out. Decides whether the Host header
   * carries one: adding ":80" where the peer did not write it changes the header
   * that virtual hosts and caches key on.
   */
  CORNET_NODISCARD bool explicit_port() const { return explicit_port_; }

  /**
   * @brief path as written; empty when the url had none (then the target is "/").
   */
  CORNET_NODISCARD std::string_view path() const { return path_; }

  /**
   * @brief query with the '?' stripped; empty when there was none.
   */
  CORNET_NODISCARD std::string_view query() const { return query_; }

  CORNET_NODISCARD std::string_view userinfo() const { return userinfo_; }
  CORNET_NODISCARD std::string_view raw() const { return raw_; }
  CORNET_NODISCARD bool ipv6_literal() const { return ipv6_; }

  /**
   * @brief the same url, with every view rebased onto different storage holding
   * exactly the same bytes.
   *
   * Exists for the parse cache: the cache holds the one parse per url string,
   * and each request anchors its own pooled copy instead of re-scanning. The
   * caller must ensure storage holds bytes identical to raw() (that is what a
   * memcpy into a pooled lease guarantees).
   */
  CORNET_NODISCARD url_t rebase(std::string_view storage) const;

  /**
   * @brief same scheme, host and effective port — the granularity a connection
   * pool keys on, and the boundary a redirect must not carry credentials across.
   */
  CORNET_NODISCARD bool same_origin(const url_t& other) const;

 private:
  std::string_view raw_{};
  std::string_view authority_{};
  std::string_view host_{};
  std::string_view userinfo_{};
  std::string_view path_{};
  std::string_view query_{};
  scheme_t scheme_{scheme_t::Http};
  uint16_t port_{80};
  bool     explicit_port_{false};
  bool     ipv6_{false};
};

/**
 * @brief resolve a Location value against the url it was returned for, writing an
 * absolute url into caller storage.
 *
 * Handles the three forms that occur in practice: absolute
 * ("http://other/x"), protocol-relative ("//other/x") and origin-relative
 * ("/x"), plus plain relative ("x") merged against the base path. The result is
 * written rather than returned as a view because none of those forms exists as a
 * contiguous run of bytes anywhere.
 *
 * @param out receives the url; not NUL-terminated
 * @return bytes written, or OutputOverflow when it does not fit
 */
CORNET_NODISCARD expected<uint32_t> write_absolute_url(const url_t& base,
                                                      std::string_view location,
                                                      char* out, uint32_t cap);

} // namespace cornet::http

#endif // CORNET_HTTP_COMMON_URL_H
