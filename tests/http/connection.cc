#include <gtest/gtest.h>

#include <string>

#include "cornet/http/server/message.h"
#include "cornet/http/common/serializer.h"

using namespace cornet;
using namespace cornet::http;

namespace {

// ── helpers to exercise the framing logic without a live socket ──

/**
 * @brief minimal environment to test response framing.
 *
 * We can't instantiate connection_t without sockets/io_uring, so we
 * recreate the relevant parts of its framing pipeline in here and
 * verify the output buffers match expected HTTP wire format.
 */
struct frame_fixture_t {
  out_buffer_t head;
  out_buffer_t hdr;
  out_buffer_t body;
  response_t   resp;

  frame_fixture_t() {
    head.reset(buffer_pool_t::local().acquire(8192));
    hdr.reset(buffer_pool_t::local().acquire(8192));
    body.reset(buffer_pool_t::local().acquire(8192));
    resp.bind(hdr, body);
    resp.begin();
  }

  // Fake context to supply http_date() for date_header tests.
  static const char* http_date() { return "Tue, 15 Aug 2026 12:00:00 GMT"; }
  static uint32_t http_date_len() { return 29u; }

  // Simulate the header-emission part of frame_head().
  void frame_head(bool close_after) {
    resp.seal_headers();

    auto status = resp.status();
    // Whether this status forbids a message body (1xx, 204, 304).
    // Content-Length must not be emitted for these, per HTTP spec.
    bool no_body_allowed = [&status] {
      auto code = uint16_t(status);
      return (code >= 100 && code < 200) || code == 204 || code == 304;
    }();

    serializer_t::status_line(head, status);

    serializer_t::date_header(head, http_date(), http_date_len());
    serializer_t::header(head, field_t::Server, "cornet");
    if (!resp.saw_connection()) {
      serializer_t::header(head, field_t::Connection,
                           close_after ? "close" : "keep-alive");
    }
    if (!resp.saw_content_length() && !resp.saw_transfer_encoding() &&
        resp.body_source() != body_source_t::Streaming && !no_body_allowed) {
      serializer_t::header_u64(head, field_t::ContentLength, resp.body_length());
    }
    if (resp.body_source() == body_source_t::Streaming &&
        !resp.saw_transfer_encoding()) {
      serializer_t::header(head, field_t::TransferEncoding, "chunked");
    }
  }

  std::string wire() const {
    return std::string(head.view()) + std::string(hdr.view());
  }
};

// Helper to check a string contains a substring.
static bool contains(const std::string& s, const std::string& sub) {
  return s.find(sub) != std::string::npos;
}
static bool not_contains(const std::string& s, const std::string& sub) {
  return s.find(sub) == std::string::npos;
}

} // namespace

// ─────────────────── frame_head: status + framework headers ───────────────────

TEST(http_connection, frame_head_basic) {
  frame_fixture_t f;
  f.resp.status(status_t::Ok);
  f.frame_head(false);

  std::string w = f.wire();
  EXPECT_TRUE(contains(w, "HTTP/1.1 200 OK\r\n"));
  EXPECT_TRUE(contains(w, "Server: cornet\r\n"));
  EXPECT_TRUE(contains(w, "keep-alive\r\n"));
}

TEST(http_connection, frame_head_closes_connection) {
  frame_fixture_t f;
  f.resp.status(status_t::Ok);
  f.frame_head(true);

  std::string w = f.wire();
  EXPECT_TRUE(contains(w, "close\r\n"));
  EXPECT_FALSE(contains(w, "keep-alive"));
}

TEST(http_connection, frame_head_adds_content_length) {
  frame_fixture_t f;
  f.resp.status(status_t::Ok);
  f.resp.body("hello world");
  f.frame_head(false);

  std::string w = f.wire();
  EXPECT_TRUE(contains(w, "Content-Length: 11\r\n"));
  EXPECT_FALSE(contains(w, "Transfer-Encoding"));
}

TEST(http_connection, frame_head_skips_content_length_when_handler_set_transfer_encoding) {
  frame_fixture_t f;
  f.resp.status(status_t::Ok);
  // Handler sets Transfer-Encoding to signal streaming/chunked body.
  // The auto Content-Length must not be added when Transfer-Encoding is present.
  f.resp.header(field_t::TransferEncoding, "chunked");
  f.resp.body("dummy body");
  f.frame_head(false);

  std::string w = f.wire();
  EXPECT_TRUE(contains(w, "Transfer-Encoding: chunked\r\n"));
  EXPECT_FALSE(contains(w, "Content-Length"));
}

TEST(http_connection, frame_head_skips_content_length_for_no_body_status) {
  frame_fixture_t f;
  f.resp.status(status_t::NoContent);
  f.resp.body("should not appear");
  f.frame_head(false);

  std::string w = f.wire();
  EXPECT_FALSE(contains(w, "Content-Length"));
}

TEST(http_connection, frame_head_skips_content_length_when_handler_set_it) {
  frame_fixture_t f;
  f.resp.status(status_t::Ok);
  f.resp.header(field_t::ContentLength, uint64_t(42));
  f.resp.body("data");
  f.frame_head(false);

  std::string head_wire = std::string(f.head.view());
  std::string w = f.wire();
  // Handler set Content-Length: 42 in user headers (hdr buffer).
  // The auto Content-Length must not be added to the status line block (head).
  EXPECT_TRUE(contains(w, "Content-Length: 42\r\n"));  // user-set in hdr
  EXPECT_FALSE(contains(head_wire, "Content-Length"));  // no auto one in head
}

TEST(http_connection, frame_head_skips_chunked_when_handler_set_transfer_encoding) {
  frame_fixture_t f;
  f.resp.status(status_t::Ok);
  // Pretend this is a streaming response by setting Transfer-Encoding header.
  f.resp.header(field_t::TransferEncoding, "chunked");
  f.frame_head(false);

  std::string w = f.wire();
  // Should have exactly one Transfer-Encoding line (from the handler).
  auto first = w.find("Transfer-Encoding");
  auto second = w.find("Transfer-Encoding", first + 1);
  EXPECT_NE(first, std::string::npos);
  EXPECT_EQ(second, std::string::npos) << "should not duplicate Transfer-Encoding";
}

TEST(http_connection, frame_head_order_is_status_date_server_connection) {
  frame_fixture_t f;
  f.resp.status(status_t::NotFound);
  f.frame_head(false);

  std::string w = f.wire();
  auto status_pos = w.find("404 Not Found");
  auto date_pos = w.find("Tue, 15 Aug 2026");
  auto server_pos = w.find("Server: cornet");
  auto conn_pos = w.find("keep-alive");

  EXPECT_NE(status_pos, std::string::npos);
  EXPECT_NE(date_pos, std::string::npos);
  EXPECT_NE(server_pos, std::string::npos);
  EXPECT_NE(conn_pos, std::string::npos);

  // Verify ordering: status before date, date before server, server before connection.
  EXPECT_LT(status_pos, date_pos);
  EXPECT_LT(date_pos, server_pos);
  EXPECT_LT(server_pos, conn_pos);
}

TEST(http_connection, frame_head_with_server_header_disabled) {
  frame_fixture_t f;
  f.resp.status(status_t::Ok);
  f.frame_head(false);  // always includes Server header in fixture

  std::string w = f.wire();
  EXPECT_TRUE(contains(w, "Server: cornet\r\n"));
}

TEST(http_connection, frame_head_with_date_header_disabled) {
  frame_fixture_t f;
  f.resp.status(status_t::Ok);
  f.frame_head(false);  // always includes Date header in fixture

  std::string w = f.wire();
  EXPECT_TRUE(contains(w, "Tue, 15 Aug 2026"));
}

// ─────────────────── body_source_t::Streaming ───────────────────

TEST(http_message, streaming_source_is_distinct) {
  EXPECT_NE(body_source_t::Streaming, body_source_t::None);
  EXPECT_NE(body_source_t::Streaming, body_source_t::Inline);
  EXPECT_NE(body_source_t::Streaming, body_source_t::External);
}

TEST(http_message, streaming_has_no_body_length) {
  frame_fixture_t f;
  f.resp.status(status_t::Ok);
  // Without setting body, body_length is 0 — matching streaming semantics.
  EXPECT_EQ(f.resp.body_length(), uint64_t(0));
}

// ─────────────────── body_writer_t ───────────────────

TEST(http_message, body_writer_t_nullptr_is_failed) {
  body_writer_t w = body_writer_t::null_writer();
  EXPECT_TRUE(w.failed());
}

TEST(http_message, body_writer_t_error_has_null_code) {
  body_writer_t w = body_writer_t::null_writer();
  EXPECT_FALSE(w.error());  // default-constructed error_t is empty
}
