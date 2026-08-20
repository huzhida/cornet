#include "cornet/http/common/url.h"

#include <cstring>

namespace cornet::http {

namespace {

constexpr auto kNpos = std::string_view::npos;

/**
 * @brief reject anything that cannot legally appear in an authority.
 *
 * Deliberately strict: a space or a control character in a host is how header
 * injection and SSRF tricks get started, and a client has no reason to be lenient
 * about a url its own caller built.
 */
bool valid_host(std::string_view host) {
  if (host.empty()) return false;
  for (char c : host) {
    auto u = static_cast<unsigned char>(c);
    if (u <= 0x20 || u >= 0x7f) return false;
    if (c == '/' || c == '\\' || c == '?' || c == '#' || c == '@') return false;
  }
  return true;
}

expected<uint16_t> parse_port(std::string_view text) {
  if (text.empty() || text.size() > 5) return http_unexpected(http_error_t::BadUrl);
  uint32_t v = 0;
  for (char c : text) {
    if (c < '0' || c > '9') return http_unexpected(http_error_t::BadUrl);
    v = v * 10 + uint32_t(c - '0');
  }
  if (v == 0 || v > 0xffff) return http_unexpected(http_error_t::BadUrl);
  return uint16_t(v);
}

} // namespace

expected<url_t> url_t::parse(std::string_view raw) {
  // Everything in here is spliced verbatim into the request line later, so any
  // control byte is a request-smuggling foot-gun, not a cosmetic difference.
  // (A legal url percent-encodes spaces; seeing one raw is a bug to report.)
  for (char c : raw) {
    if (static_cast<unsigned char>(c) <= 0x20 || c == 0x7f) {
      return http_unexpected(http_error_t::BadUrl);
    }
  }

  url_t u;
  u.raw_ = raw;

  auto sep = raw.find("://");
  if (sep == kNpos || sep == 0) return http_unexpected(http_error_t::BadUrl);

  auto scheme = raw.substr(0, sep);
  if (iequals(scheme, "http")) {
    u.scheme_ = scheme_t::Http;
  } else if (iequals(scheme, "https")) {
    u.scheme_ = scheme_t::Https;
  } else {
    return http_unexpected(http_error_t::UnsupportedScheme);
  }

  auto rest = raw.substr(sep + 3);
  auto split = rest.find_first_of("/?#");
  auto authority = split == kNpos ? rest : rest.substr(0, split);
  auto tail = split == kNpos ? std::string_view{} : rest.substr(split);

  // userinfo: rfind, because a password may itself contain '@'
  if (auto at = authority.rfind('@'); at != kNpos) {
    u.userinfo_ = authority.substr(0, at);
    authority = authority.substr(at + 1);
  }
  if (authority.empty()) return http_unexpected(http_error_t::BadUrl);
  u.authority_ = authority;

  std::string_view port_text;
  bool has_port_sep = false;
  if (authority.front() == '[') {
    auto close = authority.find(']');
    if (close == kNpos) return http_unexpected(http_error_t::BadUrl);
    u.host_ = authority.substr(1, close - 1);
    u.ipv6_ = true;
    auto after = authority.substr(close + 1);
    if (!after.empty()) {
      if (after.front() != ':') return http_unexpected(http_error_t::BadUrl);
      port_text = after.substr(1);
      has_port_sep = true;
    }
  } else if (auto colon = authority.rfind(':'); colon != kNpos) {
    u.host_ = authority.substr(0, colon);
    port_text = authority.substr(colon + 1);
    has_port_sep = true;
  } else {
    u.host_ = authority;
  }

  if (!valid_host(u.host_)) return http_unexpected(http_error_t::BadUrl);

  if (!has_port_sep) {
    u.port_ = default_port(u.scheme_);
  } else {
    // a colon with nothing after it is malformed, not "use the default"
    auto port = parse_port(port_text);
    if (!port) return unexpected(port.error());
    u.port_ = *port;
    u.explicit_port_ = true;
  }

  if (!tail.empty()) {
    // the fragment never goes on the wire
    auto frag = tail.find('#');
    auto pq = frag == kNpos ? tail : tail.substr(0, frag);
    auto q = pq.find('?');
    if (q == kNpos) {
      u.path_ = pq;
    } else {
      u.path_ = pq.substr(0, q);
      u.query_ = pq.substr(q + 1);
    }
  }

  return u;
}

bool url_t::same_origin(const url_t& other) const {
  return scheme_ == other.scheme_ && port_ == other.port_ && iequals(host_, other.host_);
}

url_t url_t::rebase(std::string_view storage) const {
  CORNET_ASSERT(storage.size() == raw_.size(), "rebase onto a different length");
  auto shift = [&](std::string_view v) -> std::string_view {
    if (v.empty()) return {};
    auto off = size_t(v.data() - raw_.data());
    return storage.substr(off, v.size());
  };
  url_t out = *this;
  out.raw_ = storage;
  out.authority_ = shift(authority_);
  out.host_ = shift(host_);
  out.userinfo_ = shift(userinfo_);
  out.path_ = shift(path_);
  out.query_ = shift(query_);
  return out;
}

expected<uint32_t> write_absolute_url(const url_t& base, std::string_view location,
                                     char* out, uint32_t cap) {
  if (location.empty()) return http_unexpected(http_error_t::BadUrl);

  uint32_t n = 0;
  auto put = [&](std::string_view s) -> bool {
    if (n + s.size() > cap) return false;
    std::memcpy(out + n, s.data(), s.size());
    n += uint32_t(s.size());
    return true;
  };

  bool ok = true;
  if (location.find("://") != kNpos) {
    ok = put(location);
  } else if (location.size() >= 2 && location[0] == '/' && location[1] == '/') {
    ok = put(scheme_name(base.scheme())) && put(":") && put(location);
  } else {
    ok = put(scheme_name(base.scheme())) && put("://") && put(base.authority());
    if (ok && location.front() == '/') {
      ok = put(location);
    } else if (ok) {
      // merge against the base path's directory, per RFC 3986 §5.3
      auto dir = base.path();
      auto slash = dir.rfind('/');
      ok = put(slash == kNpos ? std::string_view("/") : dir.substr(0, slash + 1)) &&
           put(location);
    }
  }

  if (!ok) return http_unexpected(http_error_t::OutputOverflow);
  return n;
}

} // namespace cornet::http
