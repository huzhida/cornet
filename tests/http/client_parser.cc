#include <gtest/gtest.h>

#include <string>

#include "cornet/http/common/parser.h"

using namespace cornet;
using namespace cornet::http;

namespace {

/**
 * @brief a response parser plus the buffers it writes into.
 *
 * Mirrors the request-side fixture in http_parser.cc; the only difference is the
 * parser type and that the caller can declare which method the response answers.
 */
struct fixture_t {
  head_buffer_t  head;
  spill_buffer_t spill;
  headers_t      headers;
  body_buffer_t  body;
  parser_t       parser{parser_t::type_t::Response};

  explicit fixture_t(uint32_t cap = 16u << 10) {
    head.reset(buffer_pool_t::local().acquire(cap));
    parser.bind(head, spill, headers);
    parser.set_limits(parser_limits_t{});
  }

  parser_t::result_t feed(std::string_view data) {
    auto w = head.writable();
    EXPECT_GE(w.size(), data.size());
    std::memcpy(w.data(), data.data(), data.size());
    uint32_t off = head.write_pos();
    head.commit(uint32_t(data.size()));
    return parser.execute(off, uint32_t(data.size()));
  }

  void aggregate(uint32_t fallback = 4096) {
    uint64_t want = parser.has_content_length() ? parser.content_length() : fallback;
    EXPECT_TRUE(body.reserve_exact(buffer_pool_t::local(), want ? want : 1));
    parser.set_body_sink(&body);
  }
};

} // namespace

// ─────────────────────────── the ordinary case ───────────────────────────

TEST(http_client_parser, response_with_content_length) {
  fixture_t f;
  auto r = f.feed(
      "HTTP/1.1 200 OK\r\n"
      "Content-Type: text/plain\r\n"
      "Content-Length: 5\r\n"
      "\r\n"
      "hello");

  ASSERT_EQ(r, parser_t::result_t::HeadersReady);
  EXPECT_EQ(f.parser.status_code(), 200);
  EXPECT_EQ(f.parser.version(), version_t::Http11);
  EXPECT_TRUE(f.parser.keep_alive());
  EXPECT_TRUE(f.parser.has_content_length());
  EXPECT_EQ(f.parser.content_length(), 5u);
  EXPECT_EQ(f.headers.get(field_t::ContentType), "text/plain");

  f.aggregate();
  ASSERT_EQ(f.parser.resume(), parser_t::result_t::MessageReady);
  EXPECT_EQ(f.body.view(), "hello");
}

TEST(http_client_parser, chunked_response_is_aggregated_contiguously) {
  fixture_t f;
  auto r = f.feed(
      "HTTP/1.1 200 OK\r\n"
      "Transfer-Encoding: chunked\r\n"
      "\r\n"
      "5\r\nhello\r\n"
      "5\r\nworld\r\n"
      "0\r\n\r\n");

  ASSERT_EQ(r, parser_t::result_t::HeadersReady);
  EXPECT_TRUE(f.parser.chunked());
  EXPECT_FALSE(f.parser.has_content_length());

  f.aggregate();
  ASSERT_EQ(f.parser.resume(), parser_t::result_t::MessageReady);
  EXPECT_EQ(f.body.view(), "helloworld");
}

// No Content-Length and no chunked encoding: the body runs to end of connection.
// The client discovers that only when the peer half-closes, which is what finish()
// reports.
TEST(http_client_parser, body_delimited_by_connection_close) {
  fixture_t f;
  auto r = f.feed(
      "HTTP/1.1 200 OK\r\n"
      "\r\n"
      "payload");

  ASSERT_EQ(r, parser_t::result_t::HeadersReady);
  EXPECT_FALSE(f.parser.has_content_length());
  EXPECT_FALSE(f.parser.chunked());
  // HTTP/1.1 without framing means the connection cannot be reused
  EXPECT_FALSE(f.parser.keep_alive());

  f.aggregate();
  EXPECT_EQ(f.parser.resume(), parser_t::result_t::NeedMore);
  ASSERT_EQ(f.parser.finish(), parser_t::result_t::MessageReady);
  EXPECT_EQ(f.body.view(), "payload");
}

// ─────────────────── responses that end with their headers ───────────────────
//
// These four are the whole reason parser_t needed a response-side change. If the
// parser kept waiting for a body here, a keep-alive connection would read the next
// response as this one's body and every subsequent exchange would be garbage.

TEST(http_client_parser, no_content_ends_at_the_headers) {
  fixture_t f;
  auto r = f.feed(
      "HTTP/1.1 204 No Content\r\n"
      "Server: x\r\n"
      "\r\n");

  ASSERT_EQ(r, parser_t::result_t::MessageReady);
  EXPECT_EQ(f.parser.status_code(), 204);
  EXPECT_EQ(f.parser.body_received(), 0u);
  EXPECT_TRUE(f.parser.keep_alive());
  EXPECT_EQ(f.headers.get(field_t::Server), "x");
}

TEST(http_client_parser, not_modified_ends_at_the_headers_even_with_content_length) {
  fixture_t f;
  auto r = f.feed(
      "HTTP/1.1 304 Not Modified\r\n"
      "Content-Length: 42\r\n"
      "\r\n");

  ASSERT_EQ(r, parser_t::result_t::MessageReady);
  EXPECT_EQ(f.parser.status_code(), 304);
  EXPECT_EQ(f.parser.body_received(), 0u);
  EXPECT_TRUE(f.parser.keep_alive());
}

TEST(http_client_parser, response_to_head_ends_at_the_headers) {
  fixture_t f;
  f.parser.set_response_to(method_t::Head);

  auto r = f.feed(
      "HTTP/1.1 200 OK\r\n"
      "Content-Length: 42\r\n"
      "\r\n");

  ASSERT_EQ(r, parser_t::result_t::MessageReady);
  EXPECT_EQ(f.parser.content_length(), 42u);
  EXPECT_EQ(f.parser.body_received(), 0u);
  EXPECT_TRUE(f.parser.keep_alive());
}

// Without set_response_to() the same bytes are indistinguishable from a GET
// response whose body has not arrived yet — which is exactly the failure mode the
// call exists to prevent.
TEST(http_client_parser, response_to_head_without_the_hint_waits_for_a_body) {
  fixture_t f;
  auto r = f.feed(
      "HTTP/1.1 200 OK\r\n"
      "Content-Length: 42\r\n"
      "\r\n");
  EXPECT_EQ(r, parser_t::result_t::HeadersReady);
}

TEST(http_client_parser, response_to_head_survives_reset) {
  fixture_t f;
  f.parser.set_response_to(method_t::Head);
  ASSERT_EQ(f.feed("HTTP/1.1 200 OK\r\nContent-Length: 7\r\n\r\n"),
            parser_t::result_t::MessageReady);

  f.parser.reset();
  EXPECT_EQ(f.parser.response_to(), method_t::Head);
  EXPECT_EQ(f.feed("HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\n"),
            parser_t::result_t::MessageReady);
}

// ─────────────────────── interim (1xx) responses ───────────────────────

TEST(http_client_parser, continue_then_final_response) {
  fixture_t f;
  auto r = f.feed(
      "HTTP/1.1 100 Continue\r\n"
      "\r\n"
      "HTTP/1.1 200 OK\r\n"
      "Content-Length: 2\r\n"
      "\r\n"
      "ok");

  // the interim response is a complete message of its own
  ASSERT_EQ(r, parser_t::result_t::MessageReady);
  EXPECT_EQ(f.parser.status_code(), 100);
  EXPECT_TRUE(status_is_informational(status_t(f.parser.status_code())));
  // and the real response is already sitting in the window
  EXPECT_TRUE(f.parser.has_pending_input());

  // what the client does with it: reset, keep parsing the same window
  f.parser.reset();
  ASSERT_EQ(f.parser.resume(), parser_t::result_t::HeadersReady);
  EXPECT_EQ(f.parser.status_code(), 200);

  f.aggregate();
  ASSERT_EQ(f.parser.resume(), parser_t::result_t::MessageReady);
  EXPECT_EQ(f.body.view(), "ok");
}

TEST(http_client_parser, early_hints_carry_headers_and_no_body) {
  fixture_t f;
  auto r = f.feed(
      "HTTP/1.1 103 Early Hints\r\n"
      "Link: </s.css>; rel=preload\r\n"
      "\r\n");

  ASSERT_EQ(r, parser_t::result_t::MessageReady);
  EXPECT_EQ(f.parser.status_code(), 103);
  EXPECT_EQ(f.headers.get("link"), "</s.css>; rel=preload");
  EXPECT_EQ(f.parser.body_received(), 0u);
}

// ─────────────────────────── framing facts ───────────────────────────
//
// Content-Length: 0 still pauses at the headers, exactly like a body-less request
// on the server side: the message only completes on the following resume(), which
// is the one the caller makes after deciding how to take the body.

TEST(http_client_parser, connection_close_clears_keep_alive) {
  fixture_t f;
  ASSERT_EQ(f.feed("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"),
            parser_t::result_t::HeadersReady);
  EXPECT_FALSE(f.parser.keep_alive());
  EXPECT_EQ(f.parser.resume(), parser_t::result_t::MessageReady);
}

TEST(http_client_parser, http10_defaults_to_close) {
  fixture_t f;
  ASSERT_EQ(f.feed("HTTP/1.0 200 OK\r\nContent-Length: 0\r\n\r\n"),
            parser_t::result_t::HeadersReady);
  EXPECT_EQ(f.parser.version(), version_t::Http10);
  EXPECT_FALSE(f.parser.keep_alive());
}

TEST(http_client_parser, zero_length_body_completes_on_the_next_resume) {
  fixture_t f;
  ASSERT_EQ(f.feed("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"),
            parser_t::result_t::HeadersReady);
  f.aggregate(1);
  ASSERT_EQ(f.parser.resume(), parser_t::result_t::MessageReady);
  EXPECT_EQ(f.parser.body_received(), 0u);
  EXPECT_TRUE(f.body.view().empty());
}

// A second response already in the buffer must be visible as leftover input: the
// client treats it as a protocol error, because it never pipelines and therefore
// nothing legitimate can follow.
TEST(http_client_parser, trailing_bytes_are_reported_as_pending_input) {
  fixture_t f;
  ASSERT_EQ(f.feed(
                "HTTP/1.1 204 No Content\r\n\r\n"
                "HTTP/1.1 204 No Content\r\n\r\n"),
            parser_t::result_t::MessageReady);
  EXPECT_TRUE(f.parser.has_pending_input());
}

TEST(http_client_parser, headers_arriving_in_pieces) {
  fixture_t f;
  EXPECT_EQ(f.feed("HTTP/1.1 200 OK\r\nCont"), parser_t::result_t::NeedMore);
  EXPECT_EQ(f.feed("ent-Length: 3\r\n"), parser_t::result_t::NeedMore);
  ASSERT_EQ(f.feed("\r\nabc"), parser_t::result_t::HeadersReady);
  EXPECT_EQ(f.parser.content_length(), 3u);
  f.aggregate();
  ASSERT_EQ(f.parser.resume(), parser_t::result_t::MessageReady);
  EXPECT_EQ(f.body.view(), "abc");
}

// ─────────────────────────── rejections ───────────────────────────

TEST(http_client_parser, garbage_status_line_is_an_error) {
  fixture_t f;
  EXPECT_EQ(f.feed("NOT-HTTP/1.1 200 OK\r\n\r\n"), parser_t::result_t::Error);
  EXPECT_TRUE(f.parser.error());
}

TEST(http_client_parser, oversized_content_length_is_rejected_before_the_body) {
  fixture_t f;
  f.parser.set_limits(parser_limits_t{.max_headers = 64, .max_body_bytes = 16});
  EXPECT_EQ(f.feed("HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n"),
            parser_t::result_t::Error);
  EXPECT_EQ(f.parser.error().code, int(http_error_t::BodyTooLarge));
}

TEST(http_client_parser, too_many_headers_is_rejected) {
  fixture_t f;
  f.parser.set_limits(parser_limits_t{.max_headers = 2});
  std::string raw = "HTTP/1.1 200 OK\r\nA: 1\r\nB: 2\r\nC: 3\r\n\r\n";
  EXPECT_EQ(f.feed(raw), parser_t::result_t::Error);
  EXPECT_EQ(f.parser.error().code, int(http_error_t::TooManyHeaders));
}

// ─────────────────────── streaming (no body sink) ───────────────────────

TEST(http_client_parser, streaming_pauses_on_every_body_run) {
  fixture_t f;
  ASSERT_EQ(f.feed(
                "HTTP/1.1 200 OK\r\n"
                "Transfer-Encoding: chunked\r\n"
                "\r\n"
                "3\r\nabc\r\n"
                "3\r\ndef\r\n"
                "0\r\n\r\n"),
            parser_t::result_t::HeadersReady);

  // no sink bound: each run is published and the parser stops so the reader can
  // consume the bytes before the buffer is touched again
  ASSERT_EQ(f.parser.resume(), parser_t::result_t::BodyPaused);
  EXPECT_EQ(f.parser.pending_body(), "abc");
  f.parser.clear_pending_body();

  ASSERT_EQ(f.parser.resume(), parser_t::result_t::BodyPaused);
  EXPECT_EQ(f.parser.pending_body(), "def");
  f.parser.clear_pending_body();

  EXPECT_EQ(f.parser.resume(), parser_t::result_t::MessageReady);
}

// ─────────────────────── trailers on the response side ───────────────────────

// Same rule as for requests, and for the same reason: a trailer's bytes live in the
// body region, which the read loop rewinds. A malicious server would otherwise get to
// choose a header name and value out of body bytes.
TEST(http_client_parser, response_trailers_are_ignored) {
  fixture_t f;
  ASSERT_EQ(f.feed(
                "HTTP/1.1 200 OK\r\n"
                "Server: x\r\n"
                "Transfer-Encoding: chunked\r\n"
                "\r\n"),
            parser_t::result_t::HeadersReady);
  f.aggregate();

  ASSERT_EQ(f.feed("3\r\nabc\r\n0\r\nServer: spoofed\r\nX-Sum: deadbeef\r\n\r\n"),
            parser_t::result_t::MessageReady);
  EXPECT_EQ(f.body.view(), "abc");
  EXPECT_EQ(f.headers.size(), 2u);
  EXPECT_EQ(f.headers.get(field_t::Server), "x");
  EXPECT_TRUE(f.headers.get("x-sum").empty());
  EXPECT_TRUE(f.parser.keep_alive());
}
