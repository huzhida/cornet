#include <gtest/gtest.h>

#include <string>

#include "cornet/http/client/connection.h"
#include "cornet/http/client/message.h"

using namespace cornet;
using namespace cornet::http;

namespace {

buffer_pool_t& pool() { return buffer_pool_t::local(); }

out_buffer_t staging(uint32_t cap = 4096) {
  out_buffer_t out;
  out.reset(pool().acquire(cap));
  return out;
}

std::string framed(const client_request_t& req, const client_options_t& opt,
                   bool chunked = false) {
  auto out = staging();
  frame_request_head(out, req, opt, chunked);
  EXPECT_FALSE(out.failed());
  return std::string(out.view());
}

} // namespace

// ─────────────────────────── url ownership ───────────────────────────

// The whole reason the url text is copied: send() outlives the argument, and a retry
// or a redirect needs the url long after the caller's string is gone.
TEST(http_client_message, url_survives_the_callers_string) {
  auto req = [] {
    std::string temporary = "http://example.com:8080/a/b?x=1";
    auto r = client_request_t::make(pool(), method_t::Get, temporary);
    EXPECT_TRUE(r);
    // scribble over the caller's buffer to make a dangling view visible
    temporary.assign(temporary.size(), 'z');
    return std::move(*r);
  }();

  EXPECT_EQ(req.url().host(), "example.com");
  EXPECT_EQ(req.url().port(), 8080);
  EXPECT_EQ(req.url().path(), "/a/b");
  EXPECT_EQ(req.url().query(), "x=1");
}

TEST(http_client_message, bad_url_fails_the_build) {
  auto req = client_request_t::make(pool(), method_t::Get, "not-a-url");
  ASSERT_FALSE(req);
  EXPECT_EQ(req.error().code, int(http_error_t::BadUrl));
}

TEST(http_client_message, request_survives_a_move) {
  auto first = client_request_t::make(pool(), method_t::Post, "http://example.com/p");
  ASSERT_TRUE(first);
  first->header(field_t::ContentType, "text/plain").body("payload");

  client_request_t moved = std::move(*first);
  EXPECT_EQ(moved.url().path(), "/p");
  EXPECT_EQ(moved.body_view(), "payload");
  EXPECT_EQ(moved.staged_headers(), "Content-Type: text/plain\r\n");
}

// ─────────────────────────── header staging ───────────────────────────

TEST(http_client_message, headers_are_staged_verbatim) {
  auto req = client_request_t::make(pool(), method_t::Get, "http://h/");
  ASSERT_TRUE(req);
  req->header(field_t::Accept, "application/json")
      .header("X-Trace", "abc")
      .header(field_t::ContentLength, uint64_t(7));

  EXPECT_EQ(req->staged_headers(),
            "Accept: application/json\r\n"
            "X-Trace: abc\r\n"
            "Content-Length: 7\r\n");
}

// A framing header written by name has to count as well, or the connection would add
// a second one — and two Content-Lengths is a framing error, not a cosmetic one.
TEST(http_client_message, framing_headers_are_noticed_by_name_too) {
  auto req = client_request_t::make(pool(), method_t::Get, "http://h/");
  ASSERT_TRUE(req);
  req->header("host", "other.example").header("CONTENT-LENGTH", "3");
  EXPECT_TRUE(req->saw_host());
  EXPECT_TRUE(req->saw_content_length());
}

// ─────────────────────────── body ownership ───────────────────────────

TEST(http_client_message, body_is_copied) {
  auto req = client_request_t::make(pool(), method_t::Post, "http://h/");
  ASSERT_TRUE(req);
  std::string payload = "hello";
  req->body(payload);

  EXPECT_EQ(req->body_source(), body_source_t::Inline);
  EXPECT_EQ(req->body_length(), 5u);
  EXPECT_EQ(req->body_view(), "hello");
  // copied, not referenced
  EXPECT_NE(req->body_view().data(), payload.data());
}

TEST(http_client_message, static_body_is_referenced) {
  auto req = client_request_t::make(pool(), method_t::Post, "http://h/");
  ASSERT_TRUE(req);
  static constexpr std::string_view kPayload = "never copied";
  req->body_static(kPayload);

  EXPECT_EQ(req->body_source(), body_source_t::External);
  EXPECT_EQ(req->body_view().data(), kPayload.data());
  EXPECT_EQ(req->body_length(), kPayload.size());
}

TEST(http_client_message, owned_body_is_referenced_and_released) {
  auto lease = pool().acquire(64);
  ASSERT_TRUE(lease);
  std::memcpy(lease.data(), "owned", 5);
  const char* data = lease.data();

  {
    auto req = client_request_t::make(pool(), method_t::Post, "http://h/");
    ASSERT_TRUE(req);
    req->body_owned(std::move(lease), 5);
    EXPECT_EQ(req->body_view(), "owned");
    // referenced in place, never copied
    EXPECT_EQ(req->body_view().data(), data);
  }

  // The request went out of scope, so its blocks — the owned body among them — are
  // back on the free list. The request holds several blocks of the same class, so
  // which one is on top is not something to assert on; that it is there is.
  bool recycled = false;
  buffer_lease_t drained[4];
  for (auto& l : drained) {
    l = pool().acquire(64);
    if (l.data() == data) recycled = true;
  }
  EXPECT_TRUE(recycled);
}

TEST(http_client_message, two_bodies_is_api_misuse) {
  auto req = client_request_t::make(pool(), method_t::Post, "http://h/");
  ASSERT_TRUE(req);
  req->body("first").body_static("second");
  EXPECT_TRUE(req->failed());
  EXPECT_EQ(req->error().code, int(http_error_t::InvalidState));
  // the first body is what stands
  EXPECT_EQ(req->body_view(), "first");
}

TEST(http_client_message, empty_body_is_not_a_body) {
  auto req = client_request_t::make(pool(), method_t::Post, "http://h/");
  ASSERT_TRUE(req);
  req->body({});
  EXPECT_EQ(req->body_source(), body_source_t::None);
  EXPECT_EQ(req->body_length(), 0u);
  EXPECT_FALSE(req->failed());
}

// ─────────────────────────── head framing ───────────────────────────

TEST(http_client_message, get_frames_without_content_length) {
  client_options_t opt;
  auto req = client_request_t::make(pool(), method_t::Get, "http://example.com/hello?x=1");
  ASSERT_TRUE(req);

  EXPECT_EQ(framed(*req, opt),
            "GET /hello?x=1 HTTP/1.1\r\n"
            "Host: example.com\r\n"
            "User-Agent: cornet\r\n"
            "Accept: */*\r\n"
            "Connection: keep-alive\r\n");
}

TEST(http_client_message, missing_path_becomes_slash) {
  client_options_t opt;
  opt.send_user_agent = false;
  opt.send_accept = false;
  auto req = client_request_t::make(pool(), method_t::Get, "http://example.com");
  ASSERT_TRUE(req);
  EXPECT_EQ(framed(*req, opt),
            "GET / HTTP/1.1\r\n"
            "Host: example.com\r\n"
            "Connection: keep-alive\r\n");
}

// A query with no path still needs the "/" between them, which is why the request
// line is written from path and query separately rather than from one view.
TEST(http_client_message, query_without_path_still_gets_a_slash) {
  client_options_t opt;
  opt.send_user_agent = false;
  opt.send_accept = false;
  auto req = client_request_t::make(pool(), method_t::Get, "http://example.com?x=1");
  ASSERT_TRUE(req);
  EXPECT_EQ(framed(*req, opt),
            "GET /?x=1 HTTP/1.1\r\n"
            "Host: example.com\r\n"
            "Connection: keep-alive\r\n");
}

// The Host header carries the authority exactly as written: inventing an explicit
// ":80" changes the value virtual hosts and caches key on.
TEST(http_client_message, host_header_keeps_the_authority_verbatim) {
  client_options_t opt;
  opt.send_user_agent = false;
  opt.send_accept = false;

  auto plain = client_request_t::make(pool(), method_t::Get, "http://example.com:80/");
  ASSERT_TRUE(plain);
  EXPECT_NE(framed(*plain, opt).find("Host: example.com:80\r\n"), std::string::npos);

  auto v6 = client_request_t::make(pool(), method_t::Get, "http://[::1]:9000/");
  ASSERT_TRUE(v6);
  EXPECT_NE(framed(*v6, opt).find("Host: [::1]:9000\r\n"), std::string::npos);
}

TEST(http_client_message, post_with_body_gets_content_length) {
  client_options_t opt;
  opt.send_user_agent = false;
  opt.send_accept = false;
  auto req = client_request_t::make(pool(), method_t::Post, "http://h/p");
  ASSERT_TRUE(req);
  req->body("12345");

  EXPECT_EQ(framed(*req, opt),
            "POST /p HTTP/1.1\r\n"
            "Host: h\r\n"
            "Connection: keep-alive\r\n"
            "Content-Length: 5\r\n");
}

// An empty POST body is not the same as an unknown one: without Content-Length: 0 the
// peer cannot tell "no body" from "body not here yet".
TEST(http_client_message, post_without_body_still_says_zero) {
  client_options_t opt;
  opt.send_user_agent = false;
  opt.send_accept = false;
  auto req = client_request_t::make(pool(), method_t::Post, "http://h/p");
  ASSERT_TRUE(req);
  EXPECT_NE(framed(*req, opt).find("Content-Length: 0\r\n"), std::string::npos);
}

TEST(http_client_message, caller_written_framing_headers_are_not_repeated) {
  client_options_t opt;
  auto req = client_request_t::make(pool(), method_t::Post, "http://h/p");
  ASSERT_TRUE(req);
  req->header(field_t::Host, "override.example")
      .header(field_t::UserAgent, "mine/1.0")
      .header(field_t::Accept, "text/plain")
      .header(field_t::Connection, "close")
      .header(field_t::ContentLength, uint64_t(3));

  auto head = framed(*req, opt);
  EXPECT_EQ(head, "POST /p HTTP/1.1\r\n");
  // everything else came from the caller and is staged in the request itself
  EXPECT_EQ(req->staged_headers(),
            "Host: override.example\r\n"
            "User-Agent: mine/1.0\r\n"
            "Accept: text/plain\r\n"
            "Connection: close\r\n"
            "Content-Length: 3\r\n");
}

TEST(http_client_message, chunked_upload_frames_transfer_encoding_instead) {
  client_options_t opt;
  opt.send_user_agent = false;
  opt.send_accept = false;
  auto req = client_request_t::make(pool(), method_t::Post, "http://h/p");
  ASSERT_TRUE(req);

  auto head = framed(*req, opt, /*chunked=*/true);
  EXPECT_NE(head.find("Transfer-Encoding: chunked\r\n"), std::string::npos);
  EXPECT_EQ(head.find("Content-Length"), std::string::npos);
}

TEST(http_client_message, expect_continue_only_with_a_body) {
  client_options_t opt;
  opt.send_user_agent = false;
  opt.send_accept = false;

  auto empty = client_request_t::make(pool(), method_t::Post, "http://h/p");
  ASSERT_TRUE(empty);
  empty->expect_continue();
  EXPECT_EQ(framed(*empty, opt).find("Expect:"), std::string::npos);

  auto with_body = client_request_t::make(pool(), method_t::Post, "http://h/p");
  ASSERT_TRUE(with_body);
  with_body->expect_continue().body("payload");
  EXPECT_NE(framed(*with_body, opt).find("Expect: 100-continue\r\n"), std::string::npos);
}

// ─────────────────────────── inbound_t / response ───────────────────────────

TEST(http_client_message, inbound_node_comes_from_the_pool_and_goes_back) {
  // warm the pool so the first round's allocations do not count
  inbound_t::create(pool(), 16u << 10)->destroy();

  auto allocations = pool().allocations();
  auto* node = inbound_t::create(pool(), 16u << 10);
  ASSERT_NE(node, nullptr);
  EXPECT_TRUE(node->head.attached());
  EXPECT_EQ(node->headers.size(), 0u);
  node->destroy();

  // steady state: the node and its header buffer were both recycled
  EXPECT_EQ(pool().allocations(), allocations);
}

TEST(http_client_message, default_response_is_not_an_answer) {
  client_response_t resp;
  EXPECT_FALSE(resp.valid());
  EXPECT_FALSE(bool(resp));
  // 0 is not a status anyone can mistake for a reply
  EXPECT_EQ(resp.status_code(), 0u);
  EXPECT_FALSE(resp.ok());
  EXPECT_TRUE(resp.body().empty());
  EXPECT_EQ(resp.headers().size(), 0u);
  EXPECT_FALSE(resp.keep_alive());
}
