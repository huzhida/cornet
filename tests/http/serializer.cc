#include <gtest/gtest.h>

#include <string>

#include "cornet/http/server/message.h"
#include "cornet/http/common/serializer.h"

using namespace cornet;
using namespace cornet::http;

namespace {

struct out_fixture_t {
  out_buffer_t buf;
  explicit out_fixture_t(uint32_t cap = 4096) {
    buf.reset(buffer_pool_t::local().acquire(cap));
  }
  std::string str() const { return std::string(buf.view()); }
};

} // namespace

// ─────────────────────────── integer writing ───────────────────────────

TEST(http_serializer, write_u64_covers_edges) {
  char out[24];
  auto emit = [&](uint64_t v) {
    auto n = write_u64(out, v);
    return std::string(out, n);
  };
  EXPECT_EQ(emit(0), "0");
  EXPECT_EQ(emit(7), "7");
  EXPECT_EQ(emit(10), "10");
  EXPECT_EQ(emit(99), "99");
  EXPECT_EQ(emit(100), "100");
  EXPECT_EQ(emit(12345), "12345");
  EXPECT_EQ(emit(1000000), "1000000");
  EXPECT_EQ(emit(18446744073709551615ull), "18446744073709551615");
}

TEST(http_serializer, write_chunk_size_is_hex_with_crlf) {
  char out[20];
  auto emit = [&](uint64_t v) {
    auto n = write_chunk_size(out, v);
    return std::string(out, n);
  };
  EXPECT_EQ(emit(0), "0\r\n");
  EXPECT_EQ(emit(4), "4\r\n");
  EXPECT_EQ(emit(500), "1f4\r\n");
  EXPECT_EQ(emit(0xdeadbeef), "deadbeef\r\n");
}

// ─────────────────────────── out_buffer_t ───────────────────────────

TEST(http_serializer, out_buffer_appends) {
  out_fixture_t f;
  f.buf.put("abc");
  f.buf.put_byte('-');
  f.buf.put_u64(42);
  f.buf.put_crlf();
  EXPECT_EQ(f.str(), "abc-42\r\n");
  EXPECT_FALSE(f.buf.failed());
}

TEST(http_serializer, out_buffer_latches_overflow) {
  out_buffer_t buf;
  buf.reset(buffer_pool_t::local().acquire(8));
  std::string big(buf.capacity() + 1, 'x');
  buf.put(big);
  // The failure latches rather than being reported per call: a response is written
  // by a chain of small calls and there is one useful reaction, at the end.
  ASSERT_TRUE(buf.failed());
  EXPECT_EQ(buf.error().code, int(http_error_t::OutputOverflow));

  auto before = buf.size();
  buf.put("ignored");
  EXPECT_EQ(buf.size(), before) << "writes after a failure must be no-ops";
}

TEST(http_serializer, out_buffer_reserve_and_patch) {
  out_fixture_t f;
  f.buf.put("len=");
  auto off = f.buf.reserve(6);
  f.buf.put_crlf();
  f.buf.patch_u64(off, 6, 1234);
  EXPECT_EQ(f.str(), "len=  1234\r\n");
}

// ─────────────────────────── status and headers ───────────────────────────

TEST(http_serializer, status_line_uses_the_static_table) {
  out_fixture_t f;
  serializer_t::status_line(f.buf, status_t::Ok);
  EXPECT_EQ(f.str(), "HTTP/1.1 200 OK\r\n");
}

TEST(http_serializer, status_line_composes_unknown_codes) {
  out_fixture_t f;
  serializer_t::status_line(f.buf, status_t(599));
  EXPECT_EQ(f.str(), "HTTP/1.1 599 Unknown\r\n");
}

TEST(http_serializer, known_header_names_are_prerendered) {
  out_fixture_t f;
  serializer_t::header(f.buf, field_t::ContentType, "text/plain");
  serializer_t::header_u64(f.buf, field_t::ContentLength, 11);
  serializer_t::header(f.buf, "X-Custom", "v");
  serializer_t::end_headers(f.buf);
  EXPECT_EQ(f.str(),
            "Content-Type: text/plain\r\n"
            "Content-Length: 11\r\n"
            "X-Custom: v\r\n"
            "\r\n");
}

TEST(http_serializer, date_header_is_a_memcpy_of_a_cached_string) {
  out_fixture_t f;
  const char* date = "Sun, 06 Nov 1994 08:49:37 GMT";
  serializer_t::date_header(f.buf, date, uint32_t(std::strlen(date)));
  EXPECT_EQ(f.str(), "Date: Sun, 06 Nov 1994 08:49:37 GMT\r\n");
}

TEST(http_serializer, date_header_skips_an_empty_string) {
  out_fixture_t f;
  serializer_t::date_header(f.buf, nullptr, 0);
  EXPECT_TRUE(f.str().empty());
}

// ──────────────────────── response ownership levels ────────────────────────

namespace {

struct resp_fixture_t {
  out_buffer_t hdr;
  out_buffer_t body;
  response_t   resp;

  resp_fixture_t() {
    hdr.reset(buffer_pool_t::local().acquire(4096));
    body.reset(buffer_pool_t::local().acquire(4096));
    resp.bind(hdr, body);
    resp.begin();
  }
  std::string headers() const { return std::string(hdr.view()); }
  std::string body_bytes() const { return std::string(body.view()); }
};

} // namespace

TEST(http_message, body_copies_by_default) {
  resp_fixture_t f;
  std::string local = "transient";
  f.resp.body(local);
  local.clear();
  local.shrink_to_fit();

  // The default has to survive the source going away: responses flush after the
  // handler returns, so a view into a handler local would already be dangling.
  EXPECT_EQ(f.resp.body_source(), body_source_t::Inline);
  EXPECT_EQ(f.resp.body_length(), 9u);
  EXPECT_EQ(f.body_bytes(), "transient");
}

TEST(http_message, body_static_references_without_copying) {
  resp_fixture_t f;
  f.resp.body_static("literal");
  EXPECT_EQ(f.resp.body_source(), body_source_t::External);
  EXPECT_EQ(f.resp.external_body(), "literal");
  EXPECT_TRUE(f.body_bytes().empty()) << "nothing should have been copied";
}

TEST(http_message, body_owned_takes_a_pooled_block) {
  resp_fixture_t f;
  auto lease = buffer_pool_t::local().acquire(64);
  std::memcpy(lease.data(), "owned", 5);
  f.resp.body_owned(std::move(lease), 5);
  EXPECT_EQ(f.resp.body_source(), body_source_t::External);
  EXPECT_EQ(f.resp.external_body(), "owned");

  auto taken = f.resp.take_owned();
  EXPECT_TRUE(taken.valid()) << "the flush must be able to hold the block";
}

TEST(http_message, pin_extends_a_computed_value_past_the_handler) {
  resp_fixture_t f;
  auto& stored = f.resp.pin(std::string("computed in the handler"));
  f.resp.body_static(stored);
  EXPECT_EQ(f.resp.external_body(), "computed in the handler");
  // still valid because the arena lives until the response is released
  EXPECT_EQ(stored.size(), 23u);
}

TEST(http_message, two_bodies_is_refused) {
  resp_fixture_t f;
  f.resp.body("first");
  f.resp.body_static("second");
  // Two bodies would make the framing disagree with the content.
  EXPECT_TRUE(f.resp.failed());
  EXPECT_EQ(f.resp.error().code, int(http_error_t::InvalidState));
}

TEST(http_message, framing_headers_set_by_the_handler_are_noticed) {
  resp_fixture_t f;
  EXPECT_FALSE(f.resp.saw_content_length());
  f.resp.header(field_t::ContentLength, uint64_t(5));
  EXPECT_TRUE(f.resp.saw_content_length());

  resp_fixture_t g;
  // recognised even when spelled by hand in the wrong case
  g.resp.header("content-length", "7");
  EXPECT_TRUE(g.resp.saw_content_length());

  resp_fixture_t h;
  h.resp.header(field_t::Connection, "close");
  EXPECT_TRUE(h.resp.saw_connection());
}

TEST(http_message, header_block_is_sealed_with_a_blank_line) {
  resp_fixture_t f;
  f.resp.header(field_t::ContentType, "text/plain");
  auto before = f.resp.hdr_length();
  f.resp.seal_headers();
  EXPECT_EQ(f.resp.hdr_length(), before + 2);
  EXPECT_EQ(f.headers(), "Content-Type: text/plain\r\n\r\n");
}

TEST(http_message, convenience_helpers_set_content_type) {
  resp_fixture_t f;
  f.resp.text("hi");
  EXPECT_EQ(f.headers(), "Content-Type: text/plain; charset=utf-8\r\n");
  EXPECT_EQ(f.body_bytes(), "hi");

  resp_fixture_t g;
  g.resp.json("{}");
  EXPECT_EQ(g.headers(), "Content-Type: application/json\r\n");

  resp_fixture_t h;
  h.resp.not_found();
  EXPECT_EQ(h.resp.status(), status_t::NotFound);

  resp_fixture_t i;
  i.resp.redirect("/elsewhere");
  EXPECT_EQ(i.resp.status(), status_t::Found);
  EXPECT_EQ(i.headers(), "Location: /elsewhere\r\n");
}

TEST(http_message, begin_resets_per_request_state) {
  resp_fixture_t f;
  f.resp.status(status_t::NotFound).header(field_t::ContentType, "text/plain").body("x");
  f.resp.begin();
  EXPECT_EQ(f.resp.status(), status_t::Ok);
  EXPECT_EQ(f.resp.body_source(), body_source_t::None);
  EXPECT_EQ(f.resp.body_length(), 0u);
  EXPECT_FALSE(f.resp.saw_content_length());
  EXPECT_EQ(f.resp.hdr_length(), 0u);
}

// ─────────────────────────────── query_t ───────────────────────────────

TEST(http_message, query_parses_pairs) {
  query_t q("a=1&b=2&flag&c=");
  EXPECT_EQ(q.get("a"), "1");
  EXPECT_EQ(q.get("b"), "2");
  EXPECT_EQ(q.get("flag"), "");
  EXPECT_EQ(q.get("c"), "");
  EXPECT_EQ(q.get("missing"), "");
}

TEST(http_message, query_iterates) {
  query_t q("x=1&y=2");
  std::vector<std::pair<std::string, std::string>> seen;
  for (auto e : q) seen.emplace_back(std::string(e.key), std::string(e.value));
  ASSERT_EQ(seen.size(), 2u);
  EXPECT_EQ(seen[0].first, "x");
  EXPECT_EQ(seen[0].second, "1");
  EXPECT_EQ(seen[1].first, "y");
  EXPECT_EQ(seen[1].second, "2");
}

TEST(http_message, empty_query) {
  query_t q;
  EXPECT_TRUE(q.empty());
  int n = 0;
  for (auto e : q) { (void)e; ++n; }
  EXPECT_EQ(n, 0);
}
