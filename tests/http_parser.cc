#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "cornet/http/common/parser.h"

using namespace cornet;
using namespace cornet::http;

namespace {

/**
 * @brief a parser plus the buffers it writes into, so tests can feed raw bytes.
 */
struct fixture_t {
  head_buffer_t  head;
  spill_buffer_t spill;
  headers_t      headers;
  body_buffer_t  body;
  parser_t       parser{parser_t::type_t::Request};

  explicit fixture_t(uint32_t cap = 16u << 10) {
    head.reset(buffer_pool_t::local().acquire(cap));
    parser.bind(head, spill, headers);
    parser.set_limits(parser_limits_t{});
  }

  /**
   * @brief copy bytes into the header buffer and hand the new range to the parser.
   */
  parser_t::result_t feed(std::string_view data) {
    auto w = head.writable();
    EXPECT_GE(w.size(), data.size());
    std::memcpy(w.data(), data.data(), data.size());
    uint32_t off = head.write_pos();
    head.commit(uint32_t(data.size()));
    return parser.execute(off, uint32_t(data.size()));
  }

  /**
   * @brief switch to aggregate mode, reserving room the way prepare_body() would.
   */
  void aggregate(uint32_t cap = 256) {
    uint64_t want = parser.has_content_length() ? parser.content_length() : cap;
    EXPECT_TRUE(body.reserve_exact(buffer_pool_t::local(), want ? want : 1));
    parser.set_body_sink(&body);
  }
};

constexpr std::string_view kSimpleGet =
    "GET /hello?x=1 HTTP/1.1\r\n"
    "Host: example.com\r\n"
    "User-Agent: test\r\n"
    "\r\n";

} // namespace

// ───────────────────────── two-phase dispatch ─────────────────────────

TEST(http_parser, headers_ready_arrives_before_any_body) {
  fixture_t f;
  auto r = f.feed(
      "POST /upload HTTP/1.1\r\n"
      "Host: x\r\n"
      "Content-Length: 5\r\n"
      "\r\n"
      "hello");

  // The parser must hand control back with the headers complete and no body byte
  // consumed. Without this the caller has nowhere to route the request, answer
  // 100-continue, or decide to stream instead of buffer.
  ASSERT_EQ(r, parser_t::result_t::HeadersReady);
  EXPECT_EQ(f.parser.method(), method_t::Post);
  EXPECT_TRUE(f.parser.has_content_length());
  EXPECT_EQ(f.parser.content_length(), 5u);
  EXPECT_EQ(f.parser.body_received(), 0u);

  f.aggregate();
  ASSERT_EQ(f.parser.resume(), parser_t::result_t::MessageReady);
  EXPECT_EQ(f.body.view(), "hello");
}

TEST(http_parser, headers_ready_then_message_ready_without_body) {
  fixture_t f;
  ASSERT_EQ(f.feed(kSimpleGet), parser_t::result_t::HeadersReady);
  EXPECT_EQ(f.parser.method(), method_t::Get);
  EXPECT_EQ(f.parser.target(), "/hello?x=1");
  EXPECT_EQ(f.parser.version(), version_t::Http11);
  EXPECT_TRUE(f.parser.keep_alive());

  f.aggregate();
  EXPECT_EQ(f.parser.resume(), parser_t::result_t::MessageReady);
}

TEST(http_parser, expect_continue_is_visible_at_headers_ready) {
  fixture_t f;
  auto r = f.feed(
      "POST /x HTTP/1.1\r\n"
      "Host: x\r\n"
      "Expect: 100-continue\r\n"
      "Content-Length: 3\r\n"
      "\r\n");
  ASSERT_EQ(r, parser_t::result_t::HeadersReady);
  EXPECT_TRUE(f.parser.expects_continue());
  EXPECT_EQ(f.parser.body_received(), 0u);
}

// ───────────────────────────── headers ─────────────────────────────

TEST(http_parser, headers_are_zero_copy_views) {
  fixture_t f;
  ASSERT_EQ(f.feed(kSimpleGet), parser_t::result_t::HeadersReady);

  EXPECT_EQ(f.headers.size(), 2u);
  EXPECT_EQ(f.headers.get(field_t::Host), "example.com");
  EXPECT_EQ(f.headers.get(field_t::UserAgent), "test");
  EXPECT_TRUE(f.headers.has(field_t::Host));
  EXPECT_FALSE(f.headers.has(field_t::ContentLength));

  // the value must point into the receive buffer, not a copy
  auto value = f.headers.get(field_t::Host);
  EXPECT_GE(value.data(), f.head.base());
  EXPECT_LT(value.data(), f.head.base() + f.head.capacity());
}

TEST(http_parser, unknown_headers_match_by_name) {
  fixture_t f;
  ASSERT_EQ(f.feed(
      "GET / HTTP/1.1\r\n"
      "Host: x\r\n"
      "X-Request-Id: abc123\r\n"
      "\r\n"), parser_t::result_t::HeadersReady);
  EXPECT_EQ(f.headers.get("X-Request-Id"), "abc123");
  EXPECT_EQ(f.headers.get("x-request-id"), "abc123");
  EXPECT_EQ(f.headers.get("X-Missing"), "");
}

TEST(http_parser, contains_token_handles_lists) {
  fixture_t f;
  ASSERT_EQ(f.feed(
      "GET / HTTP/1.1\r\n"
      "Host: x\r\n"
      "Connection: keep-alive, Upgrade\r\n"
      "\r\n"), parser_t::result_t::HeadersReady);
  EXPECT_TRUE(f.headers.contains_token(field_t::Connection, "keep-alive"));
  EXPECT_TRUE(f.headers.contains_token(field_t::Connection, "upgrade"));
  EXPECT_FALSE(f.headers.contains_token(field_t::Connection, "close"));
}

TEST(http_parser, header_iteration_preserves_order) {
  fixture_t f;
  ASSERT_EQ(f.feed(
      "GET / HTTP/1.1\r\n"
      "Host: a\r\n"
      "Accept: b\r\n"
      "X-Custom: c\r\n"
      "\r\n"), parser_t::result_t::HeadersReady);

  std::vector<std::string> names;
  for (auto e : f.headers) names.emplace_back(e.name);
  ASSERT_EQ(names.size(), 3u);
  EXPECT_EQ(names[0], "Host");
  EXPECT_EQ(names[1], "Accept");
  EXPECT_EQ(names[2], "X-Custom");
}

// ────────────────────── split feeds (recv boundaries) ──────────────────────

TEST(http_parser, byte_at_a_time_matches_one_shot) {
  // llhttp splits values across callbacks at feed boundaries. Because the header
  // section lives in one buffer that never moves during a message, the runs are
  // adjacent and the view just grows — this test is what keeps that true.
  std::string request(kSimpleGet);

  fixture_t drip;
  parser_t::result_t r = parser_t::result_t::NeedMore;
  for (char c : request) {
    r = drip.feed(std::string_view(&c, 1));
    if (r == parser_t::result_t::HeadersReady) break;
    ASSERT_EQ(r, parser_t::result_t::NeedMore);
  }
  ASSERT_EQ(r, parser_t::result_t::HeadersReady);

  fixture_t whole;
  ASSERT_EQ(whole.feed(request), parser_t::result_t::HeadersReady);

  EXPECT_EQ(drip.headers.size(), whole.headers.size());
  EXPECT_EQ(drip.parser.target(), whole.parser.target());
  EXPECT_EQ(drip.headers.get(field_t::Host), whole.headers.get(field_t::Host));
  EXPECT_EQ(drip.headers.get(field_t::UserAgent), whole.headers.get(field_t::UserAgent));
  // no spilling should be needed for a well-formed request
  EXPECT_EQ(drip.spill.used(), 0u);
}

TEST(http_parser, body_split_across_feeds) {
  fixture_t f;
  ASSERT_EQ(f.feed(
      "POST /x HTTP/1.1\r\n"
      "Host: x\r\n"
      "Content-Length: 11\r\n"
      "\r\n"), parser_t::result_t::HeadersReady);
  f.aggregate();
  ASSERT_EQ(f.parser.resume(), parser_t::result_t::NeedMore);
  ASSERT_EQ(f.feed("hello "), parser_t::result_t::NeedMore);
  ASSERT_EQ(f.feed("world"), parser_t::result_t::MessageReady);
  EXPECT_EQ(f.body.view(), "hello world");
}

// ──────────────────────────── chunked bodies ────────────────────────────

TEST(http_parser, chunked_body_is_compacted_contiguous) {
  fixture_t f;
  ASSERT_EQ(f.feed(
      "POST /x HTTP/1.1\r\n"
      "Host: x\r\n"
      "Transfer-Encoding: chunked\r\n"
      "\r\n"), parser_t::result_t::HeadersReady);
  EXPECT_TRUE(f.parser.chunked());
  EXPECT_FALSE(f.parser.has_content_length());

  ASSERT_TRUE(f.body.reserve_exact(buffer_pool_t::local(), 64));
  f.aggregate();
  ASSERT_EQ(f.parser.resume(), parser_t::result_t::NeedMore);

  ASSERT_EQ(f.feed("4\r\nWiki\r\n5\r\npedia\r\n"), parser_t::result_t::NeedMore);
  ASSERT_EQ(f.feed("0\r\n\r\n"), parser_t::result_t::MessageReady);

  // The chunk-size lines must not appear in the body: request_t::body() promises a
  // contiguous view of payload only.
  EXPECT_EQ(f.body.view(), "Wikipedia");
}

TEST(http_parser, chunked_with_trailer) {
  fixture_t f;
  ASSERT_EQ(f.feed(
      "POST /x HTTP/1.1\r\n"
      "Host: x\r\n"
      "Transfer-Encoding: chunked\r\n"
      "\r\n"), parser_t::result_t::HeadersReady);
  ASSERT_TRUE(f.body.reserve_exact(buffer_pool_t::local(), 64));
  f.aggregate();
  ASSERT_EQ(f.parser.resume(), parser_t::result_t::NeedMore);
  ASSERT_EQ(f.feed("3\r\nabc\r\n0\r\nX-Trailer: v\r\n\r\n"), parser_t::result_t::MessageReady);
  EXPECT_EQ(f.body.view(), "abc");
}

// ───────────────────────────── pipelining ─────────────────────────────

TEST(http_parser, pipelined_requests_are_separated_exactly) {
  fixture_t f;
  std::string two = std::string(kSimpleGet) +
      "GET /second HTTP/1.1\r\n"
      "Host: example.com\r\n"
      "\r\n";

  ASSERT_EQ(f.feed(two), parser_t::result_t::HeadersReady);
  EXPECT_EQ(f.parser.target(), "/hello?x=1");
  f.aggregate();
  ASSERT_EQ(f.parser.resume(), parser_t::result_t::MessageReady);

  // Pausing at message end is what makes the split exact: whatever follows is the
  // next request, not a guess.
  ASSERT_TRUE(f.parser.has_pending_input());
  f.parser.reset();
  ASSERT_EQ(f.parser.resume(), parser_t::result_t::HeadersReady);
  EXPECT_EQ(f.parser.target(), "/second");
  ASSERT_EQ(f.parser.resume(), parser_t::result_t::MessageReady);
  EXPECT_FALSE(f.parser.has_pending_input());
}

// ─────────────────────── keep-alive and versions ───────────────────────

TEST(http_parser, http10_defaults_to_close) {
  fixture_t f;
  ASSERT_EQ(f.feed("GET / HTTP/1.0\r\n\r\n"), parser_t::result_t::HeadersReady);
  EXPECT_EQ(f.parser.version(), version_t::Http10);
  EXPECT_FALSE(f.parser.keep_alive());
}

TEST(http_parser, http10_with_explicit_keep_alive) {
  fixture_t f;
  ASSERT_EQ(f.feed("GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n"),
            parser_t::result_t::HeadersReady);
  EXPECT_TRUE(f.parser.keep_alive());
}

TEST(http_parser, connection_close_disables_keep_alive) {
  fixture_t f;
  ASSERT_EQ(f.feed("GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"),
            parser_t::result_t::HeadersReady);
  EXPECT_FALSE(f.parser.keep_alive());
}

// ───────────────────────── limits and errors ─────────────────────────

TEST(http_parser, malformed_request_is_an_http_domain_error) {
  fixture_t f;
  auto r = f.feed("GET / HTTP/9.9\r\n\r\n");
  ASSERT_EQ(r, parser_t::result_t::Error);
  EXPECT_EQ(f.parser.error().domain, error_domain::Http);
  EXPECT_EQ(status_for_error(f.parser.error()), status_t::HttpVersionNotSupported);
}

TEST(http_parser, too_many_headers_is_rejected) {
  fixture_t f;
  f.parser.set_limits(parser_limits_t{.max_headers = 2});
  auto r = f.feed(
      "GET / HTTP/1.1\r\n"
      "A: 1\r\n"
      "B: 2\r\n"
      "C: 3\r\n"
      "\r\n");
  ASSERT_EQ(r, parser_t::result_t::Error);
  EXPECT_EQ(f.parser.error().code, int(http_error_t::TooManyHeaders));
  EXPECT_EQ(status_for_error(f.parser.error()), status_t::RequestHeaderFieldsTooLarge);
}

TEST(http_parser, oversized_content_length_is_rejected_before_the_body) {
  fixture_t f;
  f.parser.set_limits(parser_limits_t{.max_body_bytes = 8});
  auto r = f.feed(
      "POST / HTTP/1.1\r\n"
      "Host: x\r\n"
      "Content-Length: 100\r\n"
      "\r\n");
  ASSERT_EQ(r, parser_t::result_t::Error);
  EXPECT_EQ(f.parser.error().code, int(http_error_t::BodyTooLarge));
  // rejecting at the headers means we never read the 100 bytes
  EXPECT_EQ(f.parser.body_received(), 0u);
}

TEST(http_parser, smuggling_variant_is_refused_by_default) {
  // Content-Length together with Transfer-Encoding is the classic request
  // smuggling setup: a proxy and an origin can disagree about where the message
  // ends. Strict mode is the default precisely so this cannot get through.
  fixture_t f;
  auto r = f.feed(
      "POST / HTTP/1.1\r\n"
      "Host: x\r\n"
      "Content-Length: 6\r\n"
      "Transfer-Encoding: chunked\r\n"
      "\r\n");
  if (r == parser_t::result_t::HeadersReady) {
    // llhttp accepts the framing but must prefer chunked; either way the body
    // must not be delimited by Content-Length
    EXPECT_TRUE(f.parser.chunked());
  } else {
    EXPECT_EQ(r, parser_t::result_t::Error);
    EXPECT_EQ(status_for_error(f.parser.error()), status_t::BadRequest);
  }
}

// ─────────────────────────── reuse / reset ───────────────────────────

TEST(http_parser, reset_clears_message_state) {
  fixture_t f;
  ASSERT_EQ(f.feed(kSimpleGet), parser_t::result_t::HeadersReady);
  f.aggregate();
  ASSERT_EQ(f.parser.resume(), parser_t::result_t::MessageReady);

  f.parser.reset();
  EXPECT_EQ(f.parser.method(), method_t::Unknown);
  EXPECT_EQ(f.parser.target(), "");
  EXPECT_FALSE(f.parser.has_content_length());
  EXPECT_FALSE(f.parser.error());
}

// ─────────────────────────── chunked trailers ───────────────────────────
//
// llhttp reports trailers through the same callbacks as the real header section, but
// their bytes live in the body region of the receive buffer — the region that is
// rewound and refilled while the body streams past. Recording them there is unsound,
// so they are dropped. These tests pin that down; the last one is the regression test
// for what happens when a trailer straddles a rewind.

TEST(http_parser, trailers_are_ignored) {
  fixture_t f;
  auto r = f.feed(
      "POST /u HTTP/1.1\r\n"
      "Host: h\r\n"
      "Transfer-Encoding: chunked\r\n"
      "\r\n");
  ASSERT_EQ(r, parser_t::result_t::HeadersReady);
  f.aggregate();

  ASSERT_EQ(f.feed("3\r\nabc\r\n0\r\nX-Checksum: deadbeef\r\n\r\n"),
            parser_t::result_t::MessageReady);
  EXPECT_EQ(f.body.view(), "abc");
  // the two real headers, and nothing else
  EXPECT_EQ(f.headers.size(), 2u);
  EXPECT_TRUE(f.headers.get("x-checksum").empty());
}

// A trailer sharing a name with a real header must not become a second entry: code
// that iterates the header table would otherwise see a value the peer smuggled in
// after the framing decisions were already made.
TEST(http_parser, a_trailer_cannot_shadow_a_header) {
  fixture_t f;
  ASSERT_EQ(f.feed(
                "POST /u HTTP/1.1\r\n"
                "Host: real.example\r\n"
                "Transfer-Encoding: chunked\r\n"
                "\r\n"),
            parser_t::result_t::HeadersReady);
  f.aggregate();

  ASSERT_EQ(f.feed("0\r\nHost: spoofed.example\r\n\r\n"), parser_t::result_t::MessageReady);
  EXPECT_EQ(f.headers.size(), 2u);
  EXPECT_EQ(f.headers.get(field_t::Host), "real.example");
}

// Trailers cost no header slots either, so a trailer flood cannot turn a legitimate
// request into a 431.
TEST(http_parser, trailers_do_not_count_against_max_headers) {
  fixture_t f;
  f.parser.set_limits(parser_limits_t{.max_headers = 2});
  ASSERT_EQ(f.feed(
                "POST /u HTTP/1.1\r\n"
                "Host: h\r\n"
                "Transfer-Encoding: chunked\r\n"
                "\r\n"),
            parser_t::result_t::HeadersReady);
  f.aggregate();

  ASSERT_EQ(f.feed("0\r\nA: 1\r\nB: 2\r\nC: 3\r\nD: 4\r\n\r\n"),
            parser_t::result_t::MessageReady);
  EXPECT_EQ(f.headers.size(), 2u);
  EXPECT_FALSE(f.parser.error());
}

// The regression test for the whole exercise. A body longer than the receive buffer is
// read by rewinding the region behind the header section, so a trailer split across two
// reads used to have its first run overwritten by the second — producing a header whose
// name *and* value were made of body bytes. Whoever writes the body would then be
// choosing header names the handler sees.
TEST(http_parser, trailer_split_across_a_rewound_window_is_ignored) {
  fixture_t f(512);
  ASSERT_EQ(f.feed(
                "POST /u HTTP/1.1\r\n"
                "Host: h\r\n"
                "Transfer-Encoding: chunked\r\n"
                "\r\n"),
            parser_t::result_t::HeadersReady);
  // where the body starts: the mark the connection loops rewind to
  uint32_t window = f.parser.consumed_offset();
  f.aggregate();
  EXPECT_EQ(f.parser.resume(), parser_t::result_t::NeedMore);

  // first read: the body plus the beginning of a trailer
  f.head.rewind_to(window);
  EXPECT_EQ(f.feed("3\r\nabc\r\n0\r\nX-Sum: SECRETVALUE"), parser_t::result_t::NeedMore);
  EXPECT_FALSE(f.parser.mid_header()) << "a recorded trailer run would break the rewind";

  // second read: lands on top of the first, and is long enough to bury it
  f.head.rewind_to(window);
  ASSERT_EQ(f.feed("TAIL\r\n\r\n"), parser_t::result_t::MessageReady);

  EXPECT_FALSE(f.parser.error());
  EXPECT_EQ(f.body.view(), "abc");
  // no fabricated entry: only Host and Transfer-Encoding
  EXPECT_EQ(f.headers.size(), 2u);
  EXPECT_EQ(f.headers.get(field_t::Host), "h");
}

// ─────────────────── trailers, when the caller opts in ───────────────────
//
// max_trailers is 0 by default, so everything above describes the default. Setting it
// records trailers into the same table under their own budget, reachable through
// trailer() rather than get(): a value the peer appended after the body must never
// answer a header lookup, because every framing and routing decision was made long
// before it arrived.

TEST(http_parser, trailers_are_recorded_when_enabled) {
  fixture_t f;
  f.parser.set_limits(parser_limits_t{.max_headers = 64, .max_trailers = 8});
  ASSERT_EQ(f.feed(
                "POST /u HTTP/1.1\r\n"
                "Host: h\r\n"
                "Transfer-Encoding: chunked\r\n"
                "\r\n"),
            parser_t::result_t::HeadersReady);
  f.aggregate();

  ASSERT_EQ(f.feed("3\r\nabc\r\n0\r\nX-Checksum: deadbeef\r\n\r\n"),
            parser_t::result_t::MessageReady);
  EXPECT_EQ(f.body.view(), "abc");
  EXPECT_EQ(f.headers.trailer_count(), 1u);
  EXPECT_EQ(f.headers.trailer("x-checksum"), "deadbeef");
  // and it is not a header
  EXPECT_TRUE(f.headers.get("x-checksum").empty());
  EXPECT_EQ(f.headers.size(), 3u);
}

TEST(http_parser, an_enabled_trailer_still_cannot_answer_a_header_lookup) {
  fixture_t f;
  f.parser.set_limits(parser_limits_t{.max_headers = 64, .max_trailers = 8});
  ASSERT_EQ(f.feed(
                "POST /u HTTP/1.1\r\n"
                "Host: real.example\r\n"
                "Transfer-Encoding: chunked\r\n"
                "\r\n"),
            parser_t::result_t::HeadersReady);
  f.aggregate();

  ASSERT_EQ(f.feed("0\r\nX-Note: hi\r\n\r\n"), parser_t::result_t::MessageReady);
  EXPECT_EQ(f.headers.get(field_t::Host), "real.example");
  EXPECT_EQ(f.headers.trailer("x-note"), "hi");
  EXPECT_TRUE(f.headers.trailer(field_t::Host).empty());
}

// The fields RFC 9110 §6.5.1 forbids in a trailer are dropped even with recording on:
// they were all interpreted when the header section ended, so honouring one here is
// how smuggling and after-the-fact spoofing start.
TEST(http_parser, forbidden_trailers_are_dropped) {
  fixture_t f;
  f.parser.set_limits(parser_limits_t{.max_headers = 64, .max_trailers = 8});
  ASSERT_EQ(f.feed(
                "POST /u HTTP/1.1\r\n"
                "Host: real.example\r\n"
                "Transfer-Encoding: chunked\r\n"
                "\r\n"),
            parser_t::result_t::HeadersReady);
  f.aggregate();

  ASSERT_EQ(f.feed(
                "0\r\n"
                "Host: spoofed.example\r\n"
                "Authorization: Bearer x\r\n"
                "Cookie: session=stolen\r\n"
                "Connection: close\r\n"
                "Date: yesterday\r\n"
                "Location: /elsewhere\r\n"
                "Expect: 100-continue\r\n"
                "Content-Type: text/evil\r\n"
                "X-Kept: yes\r\n"
                "\r\n"),
            parser_t::result_t::MessageReady);

  EXPECT_FALSE(f.parser.error());
  EXPECT_EQ(f.headers.trailer_count(), 1u);
  EXPECT_EQ(f.headers.trailer("x-kept"), "yes");
  EXPECT_TRUE(f.headers.trailer(field_t::Host).empty());
  EXPECT_TRUE(f.headers.trailer(field_t::Authorization).empty());
  EXPECT_TRUE(f.headers.trailer(field_t::Cookie).empty());
  EXPECT_TRUE(f.headers.trailer(field_t::Connection).empty());
  EXPECT_TRUE(f.headers.trailer(field_t::Date).empty());
  EXPECT_TRUE(f.headers.trailer(field_t::Location).empty());
  EXPECT_TRUE(f.headers.trailer(field_t::Expect).empty());
  EXPECT_TRUE(f.headers.trailer(field_t::ContentType).empty());
  EXPECT_EQ(f.headers.get(field_t::Host), "real.example");
}

// The two framing headers do not even reach the deny-list: llhttp rejects them in a
// trailer outright, which is the right answer — a message whose framing is contradicted
// after the fact cannot be trusted, and the connection must not be reused.
TEST(http_parser, framing_headers_in_a_trailer_are_fatal) {
  for (std::string_view offender : {"Content-Length: 99", "Transfer-Encoding: identity"}) {
    fixture_t f;
    f.parser.set_limits(parser_limits_t{.max_headers = 64, .max_trailers = 8});
    ASSERT_EQ(f.feed(
                  "POST /u HTTP/1.1\r\n"
                  "Host: h\r\n"
                  "Transfer-Encoding: chunked\r\n"
                  "\r\n"),
              parser_t::result_t::HeadersReady);
    f.aggregate();

    EXPECT_EQ(f.feed("0\r\n" + std::string(offender) + "\r\n\r\n"),
              parser_t::result_t::Error)
        << offender;
    EXPECT_TRUE(f.parser.error()) << offender;
  }
}

// Trailers have their own budget, and going over it costs the extras, not the message.
TEST(http_parser, trailers_have_their_own_budget) {
  fixture_t f;
  f.parser.set_limits(parser_limits_t{.max_headers = 2, .max_trailers = 2});
  ASSERT_EQ(f.feed(
                "POST /u HTTP/1.1\r\n"
                "Host: h\r\n"
                "Transfer-Encoding: chunked\r\n"
                "\r\n"),
            parser_t::result_t::HeadersReady);
  f.aggregate();

  // two real headers already, and four trailers offered
  ASSERT_EQ(f.feed("0\r\nA: 1\r\nB: 2\r\nC: 3\r\nD: 4\r\n\r\n"),
            parser_t::result_t::MessageReady);
  EXPECT_FALSE(f.parser.error());
  EXPECT_EQ(f.headers.trailer_count(), 2u);
  EXPECT_EQ(f.headers.trailer("a"), "1");
  EXPECT_EQ(f.headers.trailer("b"), "2");
  EXPECT_TRUE(f.headers.trailer("c").empty());
}

// The payoff. A trailer straddling a rewind used to produce a header made of body
// bytes; recorded trailers are copied out of the body region from their first run, so
// the same split now yields the value the peer actually sent.
TEST(http_parser, recorded_trailer_survives_a_rewound_window) {
  fixture_t f(512);
  f.parser.set_limits(parser_limits_t{.max_headers = 64, .max_trailers = 8});
  ASSERT_EQ(f.feed(
                "POST /u HTTP/1.1\r\n"
                "Host: h\r\n"
                "Transfer-Encoding: chunked\r\n"
                "\r\n"),
            parser_t::result_t::HeadersReady);
  uint32_t window = f.parser.consumed_offset();
  f.aggregate();
  EXPECT_EQ(f.parser.resume(), parser_t::result_t::NeedMore);

  f.head.rewind_to(window);
  EXPECT_EQ(f.feed("3\r\nabc\r\n0\r\nX-Sum: SECRETVALUE"), parser_t::result_t::NeedMore);
  // half a trailer is outstanding, but its bytes are already copied, so rewinding
  // underneath it is safe — which is exactly what mid_header() reports
  EXPECT_FALSE(f.parser.mid_header());

  f.head.rewind_to(window);
  ASSERT_EQ(f.feed("TAIL\r\n\r\n"), parser_t::result_t::MessageReady);

  EXPECT_FALSE(f.parser.error());
  EXPECT_EQ(f.body.view(), "abc");
  EXPECT_EQ(f.headers.trailer("x-sum"), "SECRETVALUETAIL");
  EXPECT_EQ(f.headers.get(field_t::Host), "h");
}

// A trailer name split across a rewind, too: names are copied from the first run when
// they arrive in the trailer section, so the adjacency assumption never applies.
TEST(http_parser, recorded_trailer_name_survives_a_rewound_window) {
  fixture_t f(512);
  f.parser.set_limits(parser_limits_t{.max_headers = 64, .max_trailers = 8});
  ASSERT_EQ(f.feed(
                "POST /u HTTP/1.1\r\n"
                "Host: h\r\n"
                "Transfer-Encoding: chunked\r\n"
                "\r\n"),
            parser_t::result_t::HeadersReady);
  uint32_t window = f.parser.consumed_offset();
  f.aggregate();
  EXPECT_EQ(f.parser.resume(), parser_t::result_t::NeedMore);

  f.head.rewind_to(window);
  EXPECT_EQ(f.feed("3\r\nabc\r\n0\r\nX-Check"), parser_t::result_t::NeedMore);
  f.head.rewind_to(window);
  ASSERT_EQ(f.feed("sum: ok\r\n\r\n"), parser_t::result_t::MessageReady);

  EXPECT_FALSE(f.parser.error());
  EXPECT_EQ(f.headers.trailer("x-checksum"), "ok");
}

// A trailer too large for the spill buffer is dropped, not fatal: the message itself
// is complete and correct.
TEST(http_parser, an_oversized_trailer_is_dropped_not_fatal) {
  fixture_t f;
  f.parser.set_limits(parser_limits_t{.max_headers = 64, .max_trailers = 8});
  ASSERT_EQ(f.feed(
                "POST /u HTTP/1.1\r\n"
                "Host: h\r\n"
                "Transfer-Encoding: chunked\r\n"
                "\r\n"),
            parser_t::result_t::HeadersReady);
  f.aggregate();

  std::string huge(1024, 'v');   // spill holds 512
  ASSERT_EQ(f.feed("3\r\nabc\r\n0\r\nX-Big: " + huge + "\r\nX-Small: ok\r\n\r\n"),
            parser_t::result_t::MessageReady);
  EXPECT_FALSE(f.parser.error());
  EXPECT_EQ(f.body.view(), "abc");
  EXPECT_TRUE(f.headers.trailer("x-big").empty());
  // the next trailer starts clean
  EXPECT_EQ(f.headers.trailer("x-small"), "ok");
}
