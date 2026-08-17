#include <gtest/gtest.h>

#include "cornet/http/common/url.h"

using namespace cornet;
using namespace cornet::http;

// ─────────────────────────── happy paths ───────────────────────────

TEST(http_url, full_url) {
  auto u = url_t::parse("http://example.com:8080/a/b?x=1&y=2#frag");
  ASSERT_TRUE(u);
  EXPECT_EQ(u->scheme(), scheme_t::Http);
  EXPECT_EQ(u->host(), "example.com");
  EXPECT_EQ(u->authority(), "example.com:8080");
  EXPECT_EQ(u->port(), 8080);
  EXPECT_TRUE(u->explicit_port());
  EXPECT_EQ(u->path(), "/a/b");
  EXPECT_EQ(u->query(), "x=1&y=2");
  EXPECT_FALSE(u->ipv6_literal());
}

TEST(http_url, default_ports) {
  auto http = url_t::parse("http://example.com/");
  ASSERT_TRUE(http);
  EXPECT_EQ(http->port(), 80);
  EXPECT_FALSE(http->explicit_port());

  auto https = url_t::parse("https://example.com/");
  ASSERT_TRUE(https);
  EXPECT_EQ(https->scheme(), scheme_t::Https);
  EXPECT_EQ(https->port(), 443);
}

TEST(http_url, scheme_is_case_insensitive) {
  auto u = url_t::parse("HTTP://example.com/");
  ASSERT_TRUE(u);
  EXPECT_EQ(u->scheme(), scheme_t::Http);
}

// A url with no path at all is legal, and the request target it implies is "/".
// The writer, not the url, supplies that: there is no "/" in these bytes to
// return a view of.
TEST(http_url, missing_path_is_empty_not_slash) {
  auto u = url_t::parse("http://example.com");
  ASSERT_TRUE(u);
  EXPECT_EQ(u->path(), "");
  EXPECT_EQ(u->query(), "");
  EXPECT_EQ(u->authority(), "example.com");
}

TEST(http_url, query_without_path) {
  auto u = url_t::parse("http://example.com?x=1");
  ASSERT_TRUE(u);
  EXPECT_EQ(u->path(), "");
  EXPECT_EQ(u->query(), "x=1");
}

TEST(http_url, empty_query_after_question_mark) {
  auto u = url_t::parse("http://example.com/p?");
  ASSERT_TRUE(u);
  EXPECT_EQ(u->path(), "/p");
  EXPECT_EQ(u->query(), "");
}

TEST(http_url, fragment_is_dropped) {
  auto u = url_t::parse("http://example.com/p#only-a-fragment");
  ASSERT_TRUE(u);
  EXPECT_EQ(u->path(), "/p");
  EXPECT_EQ(u->query(), "");
}

TEST(http_url, ipv6_literal) {
  auto u = url_t::parse("http://[::1]:9000/p");
  ASSERT_TRUE(u);
  EXPECT_TRUE(u->ipv6_literal());
  // the resolver wants the address without brackets ...
  EXPECT_EQ(u->host(), "::1");
  // ... while a Host header wants exactly what was written
  EXPECT_EQ(u->authority(), "[::1]:9000");
  EXPECT_EQ(u->port(), 9000);
}

TEST(http_url, ipv6_literal_without_port) {
  auto u = url_t::parse("http://[2001:db8::1]/");
  ASSERT_TRUE(u);
  EXPECT_EQ(u->host(), "2001:db8::1");
  EXPECT_EQ(u->port(), 80);
  EXPECT_FALSE(u->explicit_port());
}

TEST(http_url, userinfo_is_split_off) {
  auto u = url_t::parse("http://user:pa@ss@example.com/p");
  ASSERT_TRUE(u);
  // rfind on '@': a password may contain one
  EXPECT_EQ(u->userinfo(), "user:pa@ss");
  EXPECT_EQ(u->host(), "example.com");
}

TEST(http_url, views_point_into_the_caller_bytes) {
  std::string raw = "http://example.com/path";
  auto u = url_t::parse(raw);
  ASSERT_TRUE(u);
  EXPECT_EQ(u->host().data(), raw.data() + 7);
  EXPECT_EQ(u->path().data(), raw.data() + 18);
}

// ─────────────────────────── rejections ───────────────────────────

TEST(http_url, relative_url_is_rejected) {
  auto u = url_t::parse("/just/a/path");
  ASSERT_FALSE(u);
  EXPECT_EQ(u.error().code, int(http_error_t::BadUrl));
}

TEST(http_url, unknown_scheme_is_reported_as_such) {
  auto u = url_t::parse("ftp://example.com/f");
  ASSERT_FALSE(u);
  EXPECT_EQ(u.error().code, int(http_error_t::UnsupportedScheme));
}

TEST(http_url, empty_scheme_or_authority_is_rejected) {
  EXPECT_FALSE(url_t::parse("://example.com/"));
  EXPECT_FALSE(url_t::parse("http:///path"));
  EXPECT_FALSE(url_t::parse(""));
}

TEST(http_url, bad_ports_are_rejected) {
  EXPECT_FALSE(url_t::parse("http://example.com:/p"));      // empty
  EXPECT_FALSE(url_t::parse("http://example.com:0/p"));     // zero
  EXPECT_FALSE(url_t::parse("http://example.com:70000/p")); // out of range
  EXPECT_FALSE(url_t::parse("http://example.com:80a/p"));   // not a number
  EXPECT_FALSE(url_t::parse("http://example.com:123456/p"));// too long
}

TEST(http_url, unterminated_ipv6_literal_is_rejected) {
  EXPECT_FALSE(url_t::parse("http://[::1/p"));
  EXPECT_FALSE(url_t::parse("http://[::1]x/p"));
}

// A space or a control character in the authority is how header injection starts,
// so the parser is deliberately strict about it.
TEST(http_url, control_characters_in_host_are_rejected) {
  EXPECT_FALSE(url_t::parse("http://exa mple.com/"));
  EXPECT_FALSE(url_t::parse("http://exa\rmple.com/"));
  EXPECT_FALSE(url_t::parse("http://exa\x7fmple.com/"));
}

TEST(http_url, long_host_is_accepted_as_long_as_it_is_well_formed) {
  std::string raw = "http://" + std::string(4000, 'a') + "/p";
  auto u = url_t::parse(raw);
  ASSERT_TRUE(u);
  EXPECT_EQ(u->host().size(), 4000u);
}

// ─────────────────────────── same_origin ───────────────────────────

TEST(http_url, same_origin_ignores_path_and_case) {
  auto a = url_t::parse("http://Example.com/a");
  auto b = url_t::parse("http://example.com/b?q=1");
  auto c = url_t::parse("http://example.com:8080/a");
  auto d = url_t::parse("https://example.com/a");
  ASSERT_TRUE(a && b && c && d);
  EXPECT_TRUE(a->same_origin(*b));
  EXPECT_FALSE(a->same_origin(*c));
  EXPECT_FALSE(a->same_origin(*d));
}

// An explicit :80 and a default port are the same origin, so a pool must not open
// a second connection for it.
TEST(http_url, explicit_default_port_is_the_same_origin) {
  auto a = url_t::parse("http://example.com/a");
  auto b = url_t::parse("http://example.com:80/a");
  ASSERT_TRUE(a && b);
  EXPECT_TRUE(a->same_origin(*b));
  EXPECT_NE(a->authority(), b->authority());
}

// ─────────────────────── write_absolute_url ───────────────────────

namespace {

std::string resolve(std::string_view base_raw, std::string_view location) {
  auto base = url_t::parse(base_raw);
  EXPECT_TRUE(base);
  char buf[512];
  auto n = write_absolute_url(*base, location, buf, sizeof(buf));
  EXPECT_TRUE(n);
  return n ? std::string(buf, *n) : std::string{};
}

} // namespace

TEST(http_url, redirect_absolute_location) {
  EXPECT_EQ(resolve("http://a.com/x", "http://b.com/y"), "http://b.com/y");
}

TEST(http_url, redirect_protocol_relative_location) {
  EXPECT_EQ(resolve("https://a.com/x", "//b.com/y"), "https://b.com/y");
}

TEST(http_url, redirect_root_relative_location) {
  EXPECT_EQ(resolve("http://a.com:8080/x/y?q=1", "/z"), "http://a.com:8080/z");
}

TEST(http_url, redirect_relative_location_merges_against_the_base_directory) {
  EXPECT_EQ(resolve("http://a.com/x/y", "z"), "http://a.com/x/z");
  EXPECT_EQ(resolve("http://a.com/x/", "z"), "http://a.com/x/z");
  // no path to merge against: the merge base is the root
  EXPECT_EQ(resolve("http://a.com", "z"), "http://a.com/z");
}

TEST(http_url, redirect_location_that_does_not_fit_is_an_error) {
  auto base = url_t::parse("http://a.com/x");
  ASSERT_TRUE(base);
  char buf[8];
  auto n = write_absolute_url(*base, "/a-much-longer-path", buf, sizeof(buf));
  ASSERT_FALSE(n);
  EXPECT_EQ(n.error().code, int(http_error_t::OutputOverflow));
}

TEST(http_url, empty_location_is_an_error) {
  auto base = url_t::parse("http://a.com/x");
  ASSERT_TRUE(base);
  char buf[64];
  EXPECT_FALSE(write_absolute_url(*base, "", buf, sizeof(buf)));
}
