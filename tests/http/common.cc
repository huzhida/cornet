#include <gtest/gtest.h>

#include "cornet/http/common/protocol.h"

using namespace cornet;
using namespace cornet::http;

// ───────────────────── SWAR header name recognition ─────────────────────

TEST(http_common, field_from_name_exact) {
  EXPECT_EQ(field_from_name("Host"), field_t::Host);
  EXPECT_EQ(field_from_name("Content-Length"), field_t::ContentLength);
  EXPECT_EQ(field_from_name("Content-Type"), field_t::ContentType);
  EXPECT_EQ(field_from_name("Transfer-Encoding"), field_t::TransferEncoding);
  EXPECT_EQ(field_from_name("Connection"), field_t::Connection);
  EXPECT_EQ(field_from_name("Accept"), field_t::Accept);
  EXPECT_EQ(field_from_name("Accept-Encoding"), field_t::AcceptEncoding);
  EXPECT_EQ(field_from_name("Accept-Language"), field_t::AcceptLanguage);
  EXPECT_EQ(field_from_name("User-Agent"), field_t::UserAgent);
  EXPECT_EQ(field_from_name("Expect"), field_t::Expect);
  EXPECT_EQ(field_from_name("Upgrade"), field_t::Upgrade);
  EXPECT_EQ(field_from_name("Date"), field_t::Date);
  EXPECT_EQ(field_from_name("Server"), field_t::Server);
  EXPECT_EQ(field_from_name("Location"), field_t::Location);
  EXPECT_EQ(field_from_name("Cookie"), field_t::Cookie);
  EXPECT_EQ(field_from_name("Set-Cookie"), field_t::SetCookie);
  EXPECT_EQ(field_from_name("Authorization"), field_t::Authorization);
  EXPECT_EQ(field_from_name("Referer"), field_t::Referer);
  EXPECT_EQ(field_from_name("Range"), field_t::Range);
  EXPECT_EQ(field_from_name("If-None-Match"), field_t::IfNoneMatch);
  EXPECT_EQ(field_from_name("If-Modified-Since"), field_t::IfModifiedSince);
  EXPECT_EQ(field_from_name("ETag"), field_t::ETag);
  EXPECT_EQ(field_from_name("Cache-Control"), field_t::CacheControl);
  EXPECT_EQ(field_from_name("Origin"), field_t::Origin);
}

TEST(http_common, field_from_name_is_case_insensitive) {
  EXPECT_EQ(field_from_name("host"), field_t::Host);
  EXPECT_EQ(field_from_name("HOST"), field_t::Host);
  EXPECT_EQ(field_from_name("cOnTeNt-LeNgTh"), field_t::ContentLength);
  EXPECT_EQ(field_from_name("TRANSFER-ENCODING"), field_t::TransferEncoding);
}

TEST(http_common, field_from_name_rejects_near_misses) {
  // same length, differing in the middle: the second SWAR word must catch these
  EXPECT_EQ(field_from_name("Content-Lengtx"), field_t::Other);
  EXPECT_EQ(field_from_name("Xontent-Length"), field_t::Other);
  // shared prefix, different tail
  EXPECT_EQ(field_from_name("Content-Typo"), field_t::Other);
  EXPECT_EQ(field_from_name("Accept-Encodinh"), field_t::Other);
  // unknown lengths
  EXPECT_EQ(field_from_name("X"), field_t::Other);
  EXPECT_EQ(field_from_name(""), field_t::Other);
  EXPECT_EQ(field_from_name("X-Request-Id"), field_t::Other);
}

TEST(http_common, field_from_name_distinguishes_same_length_pairs) {
  // Content-Type and Content-Length differ in length, but these three all have 15
  EXPECT_EQ(field_from_name("Accept-Encoding"), field_t::AcceptEncoding);
  EXPECT_EQ(field_from_name("Accept-Language"), field_t::AcceptLanguage);
  EXPECT_EQ(field_from_name("Accept-Charsets"), field_t::Other);
  // both 17
  EXPECT_EQ(field_from_name("Transfer-Encoding"), field_t::TransferEncoding);
  EXPECT_EQ(field_from_name("If-Modified-Since"), field_t::IfModifiedSince);
  // all 13
  EXPECT_EQ(field_from_name("Authorization"), field_t::Authorization);
  EXPECT_EQ(field_from_name("If-None-Match"), field_t::IfNoneMatch);
  EXPECT_EQ(field_from_name("Cache-Control"), field_t::CacheControl);
}

TEST(http_common, field_from_name_does_not_read_past_the_view) {
  // A short name at the very end of an allocation would fault if the lookup read
  // a fixed 8 bytes. ASAN catches it if this regresses.
  std::vector<char> buf{'H', 'o', 's', 't'};
  EXPECT_EQ(field_from_name(std::string_view(buf.data(), buf.size())), field_t::Host);

  std::vector<char> longer{'C', 'a', 'c', 'h', 'e', '-', 'C', 'o', 'n', 't', 'r', 'o', 'l'};
  EXPECT_EQ(field_from_name(std::string_view(longer.data(), longer.size())), field_t::CacheControl);
}

TEST(http_common, iequals) {
  EXPECT_TRUE(iequals("Close", "close"));
  EXPECT_TRUE(iequals("KEEP-ALIVE", "keep-alive"));
  EXPECT_FALSE(iequals("close", "closed"));
  EXPECT_FALSE(iequals("a", "b"));
}

// ─────────────────────────── name / status text ───────────────────────────

TEST(http_common, field_prefix_includes_colon_space) {
  uint32_t len = 0;
  auto* p = field_prefix(field_t::ContentLength, len);
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(std::string_view(p, len), "Content-Length: ");
}

TEST(http_common, status_line_is_prerendered) {
  uint32_t len = 0;
  auto* line = status_line(status_t::NotFound, len);
  ASSERT_NE(line, nullptr);
  EXPECT_EQ(std::string_view(line, len), "HTTP/1.1 404 Not Found\r\n");

  line = status_line(status_t::Ok, len);
  EXPECT_EQ(std::string_view(line, len), "HTTP/1.1 200 OK\r\n");

  line = status_line(status_t::RequestHeaderFieldsTooLarge, len);
  EXPECT_EQ(std::string_view(line, len), "HTTP/1.1 431 Request Header Fields Too Large\r\n");
}

TEST(http_common, status_line_unknown_returns_null) {
  uint32_t len = 0;
  EXPECT_EQ(status_line(status_t(599), len), nullptr);
  EXPECT_EQ(len, 0u);
}

TEST(http_common, reason_phrase) {
  EXPECT_STREQ(reason_phrase(status_t::Ok), "OK");
  EXPECT_STREQ(reason_phrase(status_t::ContentTooLarge), "Content Too Large");
  EXPECT_STREQ(reason_phrase(status_t(599)), "Unknown");
}

TEST(http_common, status_forbids_body_covers_1xx_204_304) {
  EXPECT_TRUE(status_forbids_body(status_t::Continue));
  EXPECT_TRUE(status_forbids_body(status_t::SwitchingProtocols));
  EXPECT_TRUE(status_forbids_body(status_t::NoContent));
  EXPECT_TRUE(status_forbids_body(status_t::NotModified));
  EXPECT_FALSE(status_forbids_body(status_t::Ok));
  EXPECT_FALSE(status_forbids_body(status_t::NotFound));
}

TEST(http_common, method_names_and_body_expectation) {
  EXPECT_STREQ(method_name(method_t::Get), "GET");
  EXPECT_STREQ(method_name(method_t::Patch), "PATCH");
  EXPECT_TRUE(method_expects_body(method_t::Get));
  EXPECT_FALSE(method_expects_body(method_t::Head));
}

// ─────────────────────────────── errors ───────────────────────────────

TEST(http_common, http_domain_errors_render) {
  auto e = http_error(http_error_t::BodyTooLarge);
  EXPECT_EQ(e.domain, error_domain::Http);
  // the module registers its renderer with the core at load time
  EXPECT_STREQ(e.message(), "request body too large");
}

TEST(http_common, llhttp_codes_pass_through_the_same_domain) {
  // a low code is an llhttp errno, rendered by llhttp's own table
  cornet::error_t e{2, error_domain::Http};
  EXPECT_STRNE(e.message(), "unknown http error");
}

TEST(http_common, status_for_error_maps_protocol_failures) {
  EXPECT_EQ(status_for_error(http_error(http_error_t::HeaderTooLarge)),
            status_t::RequestHeaderFieldsTooLarge);
  EXPECT_EQ(status_for_error(http_error(http_error_t::TooManyHeaders)),
            status_t::RequestHeaderFieldsTooLarge);
  EXPECT_EQ(status_for_error(http_error(http_error_t::BodyTooLarge)),
            status_t::ContentTooLarge);
  EXPECT_EQ(status_for_error(http_error(http_error_t::UnsupportedVersion)),
            status_t::HttpVersionNotSupported);
  EXPECT_EQ(status_for_error(http_error(http_error_t::OutputOverflow)),
            status_t::InternalServerError);
  // an llhttp parse failure is a malformed request
  EXPECT_EQ(status_for_error(cornet::error_t{2, error_domain::Http}), status_t::BadRequest);
  // a non-HTTP error is not ours to classify
  EXPECT_EQ(status_for_error(cornet::error_t{EIO, error_domain::System}), status_t::InternalServerError);
}
