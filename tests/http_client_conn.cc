#include <gtest/gtest.h>

#include <string>
#include <thread>

#include "http_client_fixture.h"

using namespace cornet;
using namespace cornet::http;
using namespace cornet_test;
using namespace std::chrono_literals;

namespace {

expected<client_request_t> get(conn_env_t& env, const origin_t& origin,
                              std::string_view path = "/x") {
  return client_request_t::make(env.pool, method_t::Get, origin.url(path));
}

} // namespace

// ───────────────────────────── the ordinary case ─────────────────────────────

TEST(http_client_conn, simple_get) {
  std::string seen_request;
  origin_t origin([&seen_request](int fd, int) {
    seen_request = read_until(fd, "\r\n\r\n");
    write_all(fd,
              "HTTP/1.1 200 OK\r\n"
              "Content-Type: text/plain\r\n"
              "Content-Length: 5\r\n"
              "\r\n"
              "hello");
  });

  conn_env_t env;
  std::string body;
  uint16_t status = 0;
  bool reusable = false;

  env.run([&]() -> coro_t<void> {
    auto conn = co_await dial(env, origin.port());
    EXPECT_TRUE(conn);
    if (!conn) co_return;

    auto req = get(env, origin, "/hello?x=1");
    EXPECT_TRUE(req);
    if (!req) co_return;

    auto resp = co_await (*conn)->exchange(*req);
    EXPECT_TRUE(resp);
    if (resp) {
      status = resp->status_code();
      body = std::string(resp->body());
      EXPECT_EQ(resp->header(field_t::ContentType), "text/plain");
      EXPECT_TRUE(resp->ok());
      EXPECT_TRUE(resp->keep_alive());
    }
    reusable = (*conn)->reusable();
    (*conn)->close();
  }());

  EXPECT_EQ(status, 200u);
  EXPECT_EQ(body, "hello");
  EXPECT_TRUE(reusable);

  // the request went out as one writev of framework head + CRLF, nothing else
  EXPECT_NE(seen_request.find("GET /hello?x=1 HTTP/1.1\r\n"), std::string::npos);
  EXPECT_NE(seen_request.find("Host: 127.0.0.1:" + std::to_string(origin.port()) + "\r\n"),
            std::string::npos);
  EXPECT_NE(seen_request.find("Connection: keep-alive\r\n"), std::string::npos);
  EXPECT_TRUE(seen_request.ends_with("\r\n\r\n"));
}

TEST(http_client_conn, post_sends_the_body) {
  std::string seen;
  origin_t origin([&seen](int fd, int) {
    seen = read_with_body(fd, 7);
    write_all(fd, "HTTP/1.1 201 Created\r\nContent-Length: 2\r\n\r\nok");
  });

  conn_env_t env;
  uint16_t status = 0;

  env.run([&]() -> coro_t<void> {
    auto conn = co_await dial(env, origin.port());
    if (!conn) co_return;
    auto req = client_request_t::make(env.pool, method_t::Post,
                                      origin.url("/p"));
    if (!req) co_return;
    req->header(field_t::ContentType, "text/plain").body("payload");

    auto resp = co_await (*conn)->exchange(*req);
    EXPECT_TRUE(resp);
    if (resp) status = resp->status_code();
    (*conn)->close();
  }());

  EXPECT_EQ(status, 201u);
  EXPECT_NE(seen.find("Content-Length: 7\r\n"), std::string::npos);
  EXPECT_TRUE(seen.ends_with("\r\n\r\npayload"));
}

// ───────────────────────────── keep-alive reuse ─────────────────────────────

TEST(http_client_conn, two_exchanges_on_one_connection) {
  origin_t origin([](int fd, int) {
    for (int i = 0; i < 2; ++i) {
      read_until(fd, "\r\n\r\n");
      write_all(fd, "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n");
      write_all(fd, i == 0 ? "a" : "b");
    }
  });

  conn_env_t env;
  std::string first, second;
  uint32_t exchanges = 0;

  env.run([&]() -> coro_t<void> {
    auto conn = co_await dial(env, origin.port());
    if (!conn) co_return;

    for (int i = 0; i < 2; ++i) {
      auto req = get(env, origin);
      if (!req) co_return;
      auto resp = co_await (*conn)->exchange(*req);
      EXPECT_TRUE(resp);
      if (!resp) co_return;
      (i == 0 ? first : second) = std::string(resp->body());
      EXPECT_TRUE((*conn)->reusable());
    }
    exchanges = (*conn)->exchanges();
    (*conn)->close();
  }());

  EXPECT_EQ(first, "a");
  EXPECT_EQ(second, "b");
  EXPECT_EQ(exchanges, 2u);
  EXPECT_EQ(env.metrics.responses, 2u);
}

// A second exchange must not allocate: the buffers of the first response went back to
// the pool when it was destroyed, and the connection takes fresh ones from there.
TEST(http_client_conn, steady_state_does_not_allocate) {
  origin_t origin([](int fd, int) {
    for (int i = 0; i < 3; ++i) {
      read_until(fd, "\r\n\r\n");
      write_all(fd, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi");
    }
  });

  conn_env_t env;
  uint64_t after_first = 0;
  uint64_t after_third = 0;

  env.run([&]() -> coro_t<void> {
    auto conn = co_await dial(env, origin.port());
    if (!conn) co_return;

    for (int i = 0; i < 3; ++i) {
      auto req = get(env, origin);
      if (!req) co_return;
      {
        auto resp = co_await (*conn)->exchange(*req);
        EXPECT_TRUE(resp);
        if (!resp) co_return;
      }   // response destroyed here: every block returns to the pool
      if (i == 0) after_first = env.pool.allocations();
    }
    after_third = env.pool.allocations();
    (*conn)->close();
  }());

  EXPECT_EQ(after_third, after_first);
}

// ─────────────────────── bodies bigger than the buffer ───────────────────────
//
// The receive buffer holds the header section *and* is the window body bytes land in.
// Body bytes are consumed as they arrive, so the window behind the headers is rewound
// and reused; without that a body could never exceed max_header_bytes.

TEST(http_client_conn, body_much_larger_than_the_head_buffer) {
  const size_t kBody = 200u << 10;
  origin_t origin([kBody](int fd, int) {
    read_until(fd, "\r\n\r\n");
    write_all(fd, "HTTP/1.1 200 OK\r\nContent-Length: " + std::to_string(kBody) + "\r\n\r\n");
    write_all(fd, std::string(kBody, 'x'));
  });

  conn_env_t env;
  env.opt.max_header_bytes = 4u << 10;   // deliberately much smaller than the body
  size_t got = 0;
  bool all_x = false;

  env.run([&]() -> coro_t<void> {
    auto conn = co_await dial(env, origin.port());
    if (!conn) co_return;
    auto req = get(env, origin);
    if (!req) co_return;

    auto resp = co_await (*conn)->exchange(*req);
    EXPECT_TRUE(resp);
    if (resp) {
      got = resp->body().size();
      all_x = resp->body().find_first_not_of('x') == std::string_view::npos;
      // the headers are still readable: their offsets live below the rewind mark
      EXPECT_EQ(resp->header(field_t::ContentLength), std::to_string(kBody));
    }
    (*conn)->close();
  }());

  EXPECT_EQ(got, kBody);
  EXPECT_TRUE(all_x);
}

TEST(http_client_conn, chunked_response_of_unknown_length_is_aggregated) {
  origin_t origin([](int fd, int) {
    read_until(fd, "\r\n\r\n");
    write_all(fd,
              "HTTP/1.1 200 OK\r\n"
              "Transfer-Encoding: chunked\r\n"
              "\r\n");
    write_all(fd, "5\r\nhello\r\n");
    write_all(fd, "6\r\n world\r\n");
    write_all(fd, "0\r\n\r\n");
  });

  conn_env_t env;
  std::string body;
  bool chunked = false;

  env.run([&]() -> coro_t<void> {
    auto conn = co_await dial(env, origin.port());
    if (!conn) co_return;
    auto req = get(env, origin);
    if (!req) co_return;
    auto resp = co_await (*conn)->exchange(*req);
    EXPECT_TRUE(resp);
    if (resp) {
      body = std::string(resp->body());
      chunked = resp->chunked();
      EXPECT_TRUE(resp->keep_alive());
    }
    (*conn)->close();
  }());

  EXPECT_EQ(body, "hello world");
  EXPECT_TRUE(chunked);
}

TEST(http_client_conn, headers_and_body_arriving_in_pieces) {
  origin_t origin([](int fd, int) {
    read_until(fd, "\r\n\r\n");
    write_all(fd, "HTTP/1.1 200 ");
    std::this_thread::sleep_for(10ms);
    write_all(fd, "OK\r\nContent-Len");
    std::this_thread::sleep_for(10ms);
    write_all(fd, "gth: 6\r\n\r\nab");
    std::this_thread::sleep_for(10ms);
    write_all(fd, "cd");
    std::this_thread::sleep_for(10ms);
    write_all(fd, "ef");
  });

  conn_env_t env;
  std::string body;

  env.run([&]() -> coro_t<void> {
    auto conn = co_await dial(env, origin.port());
    if (!conn) co_return;
    auto req = get(env, origin);
    if (!req) co_return;
    auto resp = co_await (*conn)->exchange(*req);
    EXPECT_TRUE(resp);
    if (resp) body = std::string(resp->body());
    (*conn)->close();
  }());

  EXPECT_EQ(body, "abcdef");
}

// ─────────────────── responses that end with their headers ───────────────────

TEST(http_client_conn, response_to_head_has_no_body) {
  origin_t origin([](int fd, int) {
    auto head = read_until(fd, "\r\n\r\n");
    EXPECT_TRUE(head.starts_with("HEAD "));
    // Content-Length describes the body a HEAD response does not send. A client that
    // waited for it would read the *next* response as this one's body.
    write_all(fd, "HTTP/1.1 200 OK\r\nContent-Length: 42\r\n\r\n");
    read_until(fd, "\r\n\r\n");
    write_all(fd, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
  });

  conn_env_t env;
  uint64_t announced = 0;
  std::string first_body = "unset";
  std::string second_body;

  env.run([&]() -> coro_t<void> {
    auto conn = co_await dial(env, origin.port());
    if (!conn) co_return;

    auto head_req = client_request_t::make(
        env.pool, method_t::Head, origin.url("/x"));
    if (!head_req) co_return;
    auto resp = co_await (*conn)->exchange(*head_req);
    EXPECT_TRUE(resp);
    if (resp) {
      announced = resp->content_length();
      first_body = std::string(resp->body());
      EXPECT_TRUE(resp->keep_alive());
    }
    EXPECT_TRUE((*conn)->reusable());

    // the connection is still in step, which is the point of the test
    auto req = get(env, origin);
    if (!req) co_return;
    auto second = co_await (*conn)->exchange(*req);
    EXPECT_TRUE(second);
    if (second) second_body = std::string(second->body());
    (*conn)->close();
  }());

  EXPECT_EQ(announced, 42u);
  EXPECT_EQ(first_body, "");
  EXPECT_EQ(second_body, "ok");
}

TEST(http_client_conn, no_content_response_is_reusable) {
  origin_t origin([](int fd, int) {
    read_until(fd, "\r\n\r\n");
    write_all(fd, "HTTP/1.1 204 No Content\r\n\r\n");
  });

  conn_env_t env;
  uint16_t status = 0;
  bool reusable = false;

  env.run([&]() -> coro_t<void> {
    auto conn = co_await dial(env, origin.port());
    if (!conn) co_return;
    auto req = get(env, origin);
    if (!req) co_return;
    auto resp = co_await (*conn)->exchange(*req);
    EXPECT_TRUE(resp);
    if (resp) {
      status = resp->status_code();
      EXPECT_TRUE(resp->body().empty());
    }
    reusable = (*conn)->reusable();
    (*conn)->close();
  }());

  EXPECT_EQ(status, 204u);
  EXPECT_TRUE(reusable);
}

// ─────────────────────── interim and refused responses ───────────────────────

TEST(http_client_conn, continue_then_the_real_response) {
  std::atomic<bool> body_arrived_after_continue{false};
  origin_t origin([&body_arrived_after_continue](int fd, int) {
    auto head = read_until(fd, "\r\n\r\n");
    EXPECT_NE(head.find("Expect: 100-continue\r\n"), std::string::npos);
    // the body must not be here yet: that is the whole point of the header
    EXPECT_TRUE(head.ends_with("\r\n\r\n"));

    write_all(fd, "HTTP/1.1 100 Continue\r\n\r\n");
    char buf[64];
    auto n = ::recv(fd, buf, sizeof(buf), 0);
    body_arrived_after_continue = n == 7 && std::string(buf, 7) == "payload";
    write_all(fd, "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndone");
  });

  conn_env_t env;
  uint16_t status = 0;
  std::string body;

  env.run([&]() -> coro_t<void> {
    auto conn = co_await dial(env, origin.port());
    if (!conn) co_return;
    auto req = client_request_t::make(
        env.pool, method_t::Post, origin.url("/p"));
    if (!req) co_return;
    req->expect_continue().body("payload");

    auto resp = co_await (*conn)->exchange(*req);
    EXPECT_TRUE(resp);
    if (resp) {
      status = resp->status_code();
      body = std::string(resp->body());
    }
    (*conn)->close();
  }());

  EXPECT_TRUE(body_arrived_after_continue.load());
  EXPECT_EQ(status, 200u);
  EXPECT_EQ(body, "done");
}

// The peer refused the body outright. The response is real, but the connection is not
// reusable: we announced a body and never sent it, so the stream is out of step.
TEST(http_client_conn, expectation_refused_before_the_body) {
  origin_t origin([](int fd, int) {
    read_until(fd, "\r\n\r\n");
    write_all(fd, "HTTP/1.1 417 Expectation Failed\r\nContent-Length: 0\r\n\r\n");
  });

  conn_env_t env;
  uint16_t status = 0;
  bool reusable = true;

  env.run([&]() -> coro_t<void> {
    auto conn = co_await dial(env, origin.port());
    if (!conn) co_return;
    auto req = client_request_t::make(
        env.pool, method_t::Post, origin.url("/p"));
    if (!req) co_return;
    req->expect_continue().body("payload");

    auto resp = co_await (*conn)->exchange(*req);
    EXPECT_TRUE(resp);
    if (resp) status = resp->status_code();
    reusable = (*conn)->reusable();
    (*conn)->close();
  }());

  EXPECT_EQ(status, 417u);
  EXPECT_FALSE(reusable);
}

// ─────────────────────────── failure modes ───────────────────────────

// The idle-close race, seen from the client: the peer closed without answering. No
// response byte arrived, so this is exactly the case a retry may cover.
TEST(http_client_conn, peer_closes_without_answering) {
  origin_t origin([](int fd, int) {
    read_until(fd, "\r\n\r\n");
    ::close(::dup(fd));   // keep the script simple; the fd closes on return
  });

  conn_env_t env;
  cornet::error_t err{};
  bool responded = true;

  env.run([&]() -> coro_t<void> {
    auto conn = co_await dial(env, origin.port());
    if (!conn) co_return;
    auto req = get(env, origin);
    if (!req) co_return;
    auto resp = co_await (*conn)->exchange(*req);
    EXPECT_FALSE(resp);
    if (!resp) err = resp.error();
    responded = (*conn)->responded();
    EXPECT_FALSE((*conn)->reusable());
    (*conn)->close();
  }());

  EXPECT_EQ(err.code, int(http_error_t::ResponseIncomplete));
  EXPECT_FALSE(responded);
}

TEST(http_client_conn, truncated_body_is_an_error) {
  origin_t origin([](int fd, int) {
    read_until(fd, "\r\n\r\n");
    write_all(fd, "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nabc");
  });

  conn_env_t env;
  cornet::error_t err{};

  env.run([&]() -> coro_t<void> {
    auto conn = co_await dial(env, origin.port());
    if (!conn) co_return;
    auto req = get(env, origin);
    if (!req) co_return;
    auto resp = co_await (*conn)->exchange(*req);
    EXPECT_FALSE(resp);
    if (!resp) err = resp.error();
    (*conn)->close();
  }());

  EXPECT_EQ(err.code, int(http_error_t::ResponseIncomplete));
}

TEST(http_client_conn, garbage_status_line_is_a_protocol_error) {
  origin_t origin([](int fd, int) {
    read_until(fd, "\r\n\r\n");
    write_all(fd, "definitely not http\r\n\r\n");
  });

  conn_env_t env;
  bool failed = false;

  env.run([&]() -> coro_t<void> {
    auto conn = co_await dial(env, origin.port());
    if (!conn) co_return;
    auto req = get(env, origin);
    if (!req) co_return;
    auto resp = co_await (*conn)->exchange(*req);
    failed = !resp;
    EXPECT_FALSE((*conn)->reusable());
    (*conn)->close();
  }());

  EXPECT_TRUE(failed);
  EXPECT_EQ(env.metrics.protocol_errors, 1u);
}

// We never pipeline, so nothing may follow a finished response. The response itself is
// fine; the connection is not.
TEST(http_client_conn, bytes_after_a_complete_response_poison_the_connection) {
  origin_t origin([](int fd, int) {
    read_until(fd, "\r\n\r\n");
    write_all(fd,
              "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"
              "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nno");
  });

  conn_env_t env;
  std::string body;
  bool reusable = true;

  env.run([&]() -> coro_t<void> {
    auto conn = co_await dial(env, origin.port());
    if (!conn) co_return;
    auto req = get(env, origin);
    if (!req) co_return;
    auto resp = co_await (*conn)->exchange(*req);
    EXPECT_TRUE(resp);
    if (resp) body = std::string(resp->body());
    reusable = (*conn)->reusable();
    (*conn)->close();
  }());

  EXPECT_EQ(body, "ok");
  EXPECT_FALSE(reusable);
}

TEST(http_client_conn, deadline_stops_a_silent_peer) {
  origin_t origin([](int fd, int, hold_gate_t& gate) {
    read_until(fd, "\r\n\r\n");
    gate.wait();   // never answers, so the client's deadline is what ends this
  });

  conn_env_t env;
  cornet::error_t err{};
  bool timed_out = false;

  env.run([&]() -> coro_t<void> {
    auto conn = co_await dial(env, origin.port());
    if (!conn) co_return;
    (*conn)->set_deadline(80ms);
    auto req = get(env, origin);
    if (!req) co_return;

    auto resp = co_await (*conn)->exchange(*req);
    EXPECT_FALSE(resp);
    if (!resp) err = resp.error();
    timed_out = (*conn)->timed_out();
    EXPECT_FALSE((*conn)->reusable());
    (*conn)->close();
  }());

  EXPECT_TRUE(timed_out);
  EXPECT_EQ(err.code, ETIMEDOUT);
  EXPECT_EQ(env.metrics.timeouts, 1u);
}

// ─────────────────────────── streaming download ───────────────────────────

TEST(http_client_conn, streaming_download_yields_runs) {
  origin_t origin([](int fd, int) {
    read_until(fd, "\r\n\r\n");
    write_all(fd, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
    write_all(fd, "3\r\nabc\r\n");
    std::this_thread::sleep_for(5ms);
    write_all(fd, "3\r\ndef\r\n");
    std::this_thread::sleep_for(5ms);
    write_all(fd, "0\r\n\r\n");
  });

  conn_env_t env;
  std::string collected;
  uint32_t runs = 0;
  uint16_t status = 0;
  bool reusable = false;

  env.run([&]() -> coro_t<void> {
    auto conn = co_await dial(env, origin.port());
    if (!conn) co_return;
    auto req = get(env, origin);
    if (!req) co_return;

    auto begun = co_await (*conn)->begin_exchange(*req);
    EXPECT_TRUE(begun);
    if (!begun) co_return;
    status = uint16_t((*conn)->status());

    for (;;) {
      auto run = co_await (*conn)->read_body();
      EXPECT_TRUE(run);
      if (!run) co_return;
      if (run->empty()) break;
      ++runs;
      collected.append(*run);
    }

    reusable = (*conn)->reusable();
    // the response object still comes from the same buffers, with the body left empty
    auto resp = (*conn)->take_response();
    EXPECT_TRUE(resp);
    if (resp) EXPECT_TRUE(resp->body().empty());
    (*conn)->close();
  }());

  EXPECT_EQ(status, 200u);
  EXPECT_EQ(collected, "abcdef");
  EXPECT_GE(runs, 2u);
  EXPECT_TRUE(reusable);
}

TEST(http_client_conn, draining_a_streamed_body_makes_the_connection_reusable) {
  origin_t origin([](int fd, int) {
    read_until(fd, "\r\n\r\n");
    write_all(fd, "HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nabcdef");
    read_until(fd, "\r\n\r\n");
    write_all(fd, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi");
  });

  conn_env_t env;
  std::string second;

  env.run([&]() -> coro_t<void> {
    auto conn = co_await dial(env, origin.port());
    if (!conn) co_return;
    auto req = get(env, origin);
    if (!req) co_return;
    auto begun = co_await (*conn)->begin_exchange(*req);
    if (!begun) co_return;

    // read one run, then give up on the rest
    auto run = co_await (*conn)->read_body();
    EXPECT_TRUE(run);
    EXPECT_FALSE((*conn)->reusable());
    auto drained = co_await (*conn)->drain_body();
    EXPECT_TRUE(drained);
    EXPECT_TRUE((*conn)->reusable());
    auto resp = (*conn)->take_response();
    EXPECT_TRUE(resp);

    auto again = get(env, origin);
    if (!again) co_return;
    auto second_resp = co_await (*conn)->exchange(*again);
    EXPECT_TRUE(second_resp);
    if (second_resp) second = std::string(second_resp->body());
    (*conn)->close();
  }());

  EXPECT_EQ(second, "hi");
}

// ─────────────────────────── chunked upload ───────────────────────────

TEST(http_client_conn, chunked_upload) {
  std::string seen;
  origin_t origin([&seen](int fd, int) {
    seen = read_until(fd, "0\r\n\r\n");
    write_all(fd, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
  });

  conn_env_t env;
  uint16_t status = 0;

  env.run([&]() -> coro_t<void> {
    auto conn = co_await dial(env, origin.port());
    if (!conn) co_return;
    auto req = client_request_t::make(
        env.pool, method_t::Post, origin.url("/upload"));
    if (!req) co_return;
    req->header(field_t::ContentType, "application/octet-stream");

    auto begun = co_await (*conn)->begin_chunked(*req);
    EXPECT_TRUE(begun);
    if (!begun) co_return;

    EXPECT_TRUE(co_await (*conn)->write_chunk("hello"));
    EXPECT_TRUE(co_await (*conn)->write_chunk(" world"));
    EXPECT_TRUE(co_await (*conn)->finish_chunks());

    auto resp = co_await (*conn)->read_response(method_t::Post);
    EXPECT_TRUE(resp);
    if (resp) status = resp->status_code();
    (*conn)->close();
  }());

  EXPECT_EQ(status, 200u);
  EXPECT_NE(seen.find("Transfer-Encoding: chunked\r\n"), std::string::npos);
  EXPECT_EQ(seen.find("Content-Length"), std::string::npos);
  EXPECT_TRUE(seen.ends_with("5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n"));
}

// A body staged in the request and a body streamed in chunks are two different
// requests; asking for both is api misuse, not a merge.
TEST(http_client_conn, chunked_upload_refuses_a_staged_body) {
  origin_t origin([](int fd, int) { read_until(fd, "\r\n\r\n"); });

  conn_env_t env;
  cornet::error_t err{};

  env.run([&]() -> coro_t<void> {
    auto conn = co_await dial(env, origin.port());
    if (!conn) co_return;
    auto req = client_request_t::make(
        env.pool, method_t::Post, origin.url("/p"));
    if (!req) co_return;
    req->body("staged");

    auto begun = co_await (*conn)->begin_chunked(*req);
    EXPECT_FALSE(begun);
    if (!begun) err = begun.error();
    (*conn)->close();
  }());

  EXPECT_EQ(err.code, int(http_error_t::InvalidState));
}

// ─────────────────────────── response trailers ───────────────────────────

// The payoff of copying trailers out of the body region: they are still readable after
// the exchange is over and the connection has gone back — the bytes live in the same
// pooled node as the headers, which the response owns.
TEST(http_client_conn, response_trailers_survive_the_connection) {
  origin_t origin([](int fd, int) {
    read_until(fd, "\r\n\r\n");
    write_all(fd, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
    write_all(fd, "5\r\nhello\r\n");
    write_all(fd, "0\r\nX-Checksum: deadbeef\r\nHost: spoofed\r\n\r\n");
  });

  conn_env_t env;
  env.opt.max_trailers = 4;
  std::string body, checksum, spoofed_host;

  env.run([&]() -> coro_t<void> {
    auto conn = co_await dial(env, origin.port());
    if (!conn) co_return;
    auto req = get(env, origin);
    if (!req) co_return;

    auto resp = co_await (*conn)->exchange(*req);
    EXPECT_TRUE(resp);
    (*conn)->close();          // connection gone; the response owns its buffers
    if (!resp) co_return;

    body = std::string(resp->body());
    checksum = std::string(resp->trailer("x-checksum"));
    // a trailer must not be able to answer a header lookup
    spoofed_host = std::string(resp->header(field_t::Host));
  }());

  EXPECT_EQ(body, "hello");
  EXPECT_EQ(checksum, "deadbeef");
  EXPECT_EQ(spoofed_host, "");
}

// Off by default: nothing is recorded unless the caller asks for it.
TEST(http_client_conn, response_trailers_are_dropped_by_default) {
  origin_t origin([](int fd, int) {
    read_until(fd, "\r\n\r\n");
    write_all(fd, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
    write_all(fd, "5\r\nhello\r\n0\r\nX-Checksum: deadbeef\r\n\r\n");
  });

  conn_env_t env;
  std::string checksum = "unset";

  env.run([&]() -> coro_t<void> {
    auto conn = co_await dial(env, origin.port());
    if (!conn) co_return;
    auto req = get(env, origin);
    if (!req) co_return;
    auto resp = co_await (*conn)->exchange(*req);
    EXPECT_TRUE(resp);
    if (resp) checksum = std::string(resp->trailer("x-checksum"));
    (*conn)->close();
  }());

  EXPECT_EQ(checksum, "");
}
