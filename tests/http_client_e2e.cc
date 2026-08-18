#include <gtest/gtest.h>

#include <string>

#include "cornet/concurrency/combinators.h"
#include "cornet/http/client/client.h"
#include "cornet/http/server/server.h"
#include "cornet/scheduling/context.h"

using namespace cornet;
using namespace cornet::http;
using namespace std::chrono_literals;

namespace {

/**
 * @brief run a client against cornet's own server, both on one context.
 *
 * One context on purpose: it is the configuration a real process uses when it both
 * serves and calls out, and it exercises the two loops sharing a ring. The client
 * coroutine drains the server when it is done, which is what lets run() return.
 *
 * The listener takes whatever port the kernel offers, and `body` is told which one.
 * A fixed port would be flaky: ip_local_port_range covers the range these tests used
 * to hardcode, so any outbound connection in the same run can be holding it.
 */
void run_e2e(const std::function<void(server_t&)>& setup,
             const std::function<coro_t<void>(context_t&, client_t&, uint16_t)>& body) {
  context_t ctx;

  server_options_t sopt;
  sopt.address = "127.0.0.1";
  sopt.port = 0;
  sopt.timer_tick = 20ms;
  server_t server(ctx, sopt);
  setup(server);

  auto listening = server.listen();
  ASSERT_TRUE(listening) << listening.error().message();
  const uint16_t port = server.options().port;

  client_options_t copt;
  copt.timer_tick = 20ms;
  client_t cli(ctx, copt);

  ctx.spawn(server.serve());
  ctx.spawn([&]() -> coro_t<void> {
    co_await body(ctx, cli, port);
    // Close the client first: its pooled connections are what would otherwise keep
    // the server's drain waiting for its own idle timeout.
    cli.close();
    server.drain();
  }());

  ctx.run();
}

std::string url_for(uint16_t port, std::string_view path) {
  return "http://127.0.0.1:" + std::to_string(port) + std::string(path);
}

} // namespace

// ─────────────────────────── the basics ───────────────────────────

TEST(http_client_e2e, get) {
  std::string body;
  uint16_t status = 0;

  run_e2e([](server_t& server) {
            server.get("/hello", [](auto&, response_t& resp) { resp.text("hello cornet"); });
          },
          [&](context_t&, client_t& cli, uint16_t port) -> coro_t<void> {
            auto resp = co_await cli.get(url_for(port, "/hello"));
            EXPECT_TRUE(resp);
            if (!resp) co_return;
            status = resp->status_code();
            body = std::string(resp->body());
            EXPECT_EQ(resp->header(field_t::ContentType), "text/plain; charset=utf-8");
          });

  EXPECT_EQ(status, 200u);
  EXPECT_EQ(body, "hello cornet");
}

// A 404 arrived, parsed, and says what it says. That is not a failure of the request,
// so it comes back through the value channel like any other answer.
TEST(http_client_e2e, not_found_is_a_response_not_an_error) {
  uint16_t status = 0;
  bool ok_flag = true;
  bool valid = false;

  run_e2e([](server_t&) {},
          [&](context_t&, client_t& cli, uint16_t port) -> coro_t<void> {
            auto resp = co_await cli.get(url_for(port, "/nothing-here"));
            valid = bool(resp);
            if (!resp) co_return;
            status = resp->status_code();
            ok_flag = resp->ok();
          });

  EXPECT_TRUE(valid);
  EXPECT_EQ(status, 404u);
  EXPECT_FALSE(ok_flag);
}

TEST(http_client_e2e, post_round_trip) {
  std::string echoed;
  std::string seen_content_type;

  run_e2e([&seen_content_type](server_t& server) {
            server.post("/echo", [&seen_content_type](request_t& req, response_t& resp) {
              seen_content_type = std::string(req.headers().get(field_t::ContentType));
              auto& copy = resp.pin(std::string(req.body()));
              resp.body_static(copy);
            });
          },
          [&](context_t&, client_t& cli, uint16_t port) -> coro_t<void> {
            auto resp = co_await cli.post(url_for(port, "/echo"), "{\"a\":1}", "application/json");
            EXPECT_TRUE(resp);
            if (resp) echoed = std::string(resp->body());
          });

  EXPECT_EQ(echoed, "{\"a\":1}");
  EXPECT_EQ(seen_content_type, "application/json");
}

TEST(http_client_e2e, head_request_has_no_body) {
  uint64_t announced = 0;
  size_t body_size = 99;

  run_e2e([](server_t& server) {
            server.head("/thing", [](auto&, response_t& resp) { resp.text("12345"); });
          },
          [&](context_t&, client_t& cli, uint16_t port) -> coro_t<void> {
            auto resp = co_await cli.head(url_for(port, "/thing"));
            EXPECT_TRUE(resp);
            if (!resp) co_return;
            announced = resp->content_length();
            body_size = resp->body().size();
          });

  EXPECT_EQ(announced, 5u);
  EXPECT_EQ(body_size, 0u);
}

// ─────────────────────────── keep-alive ───────────────────────────

TEST(http_client_e2e, sequential_requests_share_one_connection) {
  uint64_t created = 0;
  uint64_t reused = 0;
  uint32_t answered = 0;

  run_e2e([](server_t& server) {
            server.get("/n", [](auto&, response_t& resp) { resp.text("n"); });
          },
          [&](context_t&, client_t& cli, uint16_t port) -> coro_t<void> {
            for (int i = 0; i < 3; ++i) {
              auto resp = co_await cli.get(url_for(port, "/n"));
              EXPECT_TRUE(resp);
              if (resp && resp->body() == "n") ++answered;
            }
            created = cli.metrics().conn_created;
            reused = cli.metrics().conn_reused;
          });

  EXPECT_EQ(answered, 3u);
  EXPECT_EQ(created, 1u);
  EXPECT_EQ(reused, 2u);
}

TEST(http_client_e2e, concurrent_requests_open_several_connections) {
  uint32_t answered = 0;
  uint64_t created = 0;

  run_e2e([](server_t& server) {
            server.get("/slow", [](auto&, response_t& resp) { resp.text("done"); });
          },
          [&](context_t& ctx, client_t& cli, uint16_t port) -> coro_t<void> {
            uint32_t finished = 0;
            auto task = [&]() -> coro_t<void> {
              auto resp = co_await cli.get(url_for(port, "/slow"));
              EXPECT_TRUE(resp);
              if (resp && resp->body() == "done") ++answered;
              ++finished;
            };
            for (int i = 0; i < 4; ++i) ctx.spawn(task());
            while (finished < 4) co_await sleep(ctx, 2ms);
            created = cli.metrics().conn_created;
          });

  EXPECT_EQ(answered, 4u);
  EXPECT_GT(created, 1u);
}

// ─────────────────────────── bodies ───────────────────────────

TEST(http_client_e2e, chunked_response_is_aggregated) {
  std::string body;

  run_e2e([](server_t& server) {
            server.get("/stream", [](auto&, response_t& resp) -> coro_t<void> {
              auto writer = resp.chunked();
              EXPECT_TRUE(co_await writer.write("abc"));
              EXPECT_TRUE(co_await writer.write("def"));
              EXPECT_TRUE(co_await writer.finish());
            });
          },
          [&](context_t&, client_t& cli, uint16_t port) -> coro_t<void> {
            auto resp = co_await cli.get(url_for(port, "/stream"));
            EXPECT_TRUE(resp);
            if (resp) {
              body = std::string(resp->body());
              EXPECT_TRUE(resp->chunked());
            }
          });

  EXPECT_EQ(body, "abcdef");
}

TEST(http_client_e2e, streaming_download) {
  std::string collected;
  uint16_t status = 0;

  run_e2e([](server_t& server) {
            server.get("/stream", [](auto&, response_t& resp) -> coro_t<void> {
              auto writer = resp.chunked();
              for (int i = 0; i < 4; ++i) {
                EXPECT_TRUE(co_await writer.write("part"));
              }
              EXPECT_TRUE(co_await writer.finish());
            });
          },
          [&](context_t&, client_t& cli, uint16_t port) -> coro_t<void> {
            auto stream = co_await cli.stream(method_t::Get, url_for(port, "/stream"));
            EXPECT_TRUE(stream);
            if (!stream) co_return;
            status = uint16_t(stream->status());

            for (;;) {
              auto run = co_await stream->read();
              EXPECT_TRUE(run);
              if (!run || run->empty()) break;
              collected.append(*run);
            }
            // the head is still available once the body is done
            auto resp = stream->finish();
            EXPECT_TRUE(resp);
          });

  EXPECT_EQ(status, 200u);
  EXPECT_EQ(collected, "partpartpartpart");
}

TEST(http_client_e2e, chunked_upload) {
  std::string reported;

  run_e2e([](server_t& server) {
            auto& route = server.post("/upload", [](request_t& req, response_t& resp)
                                                     -> coro_t<void> {
              size_t total = 0;
              if (auto* reader = req.stream()) {
                for (;;) {
                  auto run = co_await reader->read();
                  if (!run || run->empty()) break;
                  total += run->size();
                }
              } else {
                total = req.body().size();
              }
              auto& text = resp.pin(std::to_string(total));
              resp.body_static(text);
            });
            route.body = body_policy_t::Stream;
          },
          [&](context_t&, client_t& cli, uint16_t port) -> coro_t<void> {
            auto upload = co_await cli.upload(method_t::Post, url_for(port, "/upload"));
            EXPECT_TRUE(upload);
            if (!upload) co_return;

            EXPECT_TRUE(co_await upload->write("hello"));
            EXPECT_TRUE(co_await upload->write(" world"));
            auto resp = co_await upload->finish();
            EXPECT_TRUE(resp);
            if (resp) reported = std::string(resp->body());
          });

  EXPECT_EQ(reported, "11");
}

TEST(http_client_e2e, body_larger_than_the_receive_buffer) {
  static const std::string kPayload(300u << 10, 'p');
  size_t got = 0;
  bool intact = false;

  run_e2e([](server_t& server) {
            server.get("/big", [](auto&, response_t& resp) { resp.body_static(kPayload); });
          },
          [&](context_t&, client_t& cli, uint16_t port) -> coro_t<void> {
            auto resp = co_await cli.get(url_for(port, "/big"));
            EXPECT_TRUE(resp);
            if (!resp) co_return;
            got = resp->body().size();
            intact = resp->body() == kPayload;
          });

  EXPECT_EQ(got, kPayload.size());
  EXPECT_TRUE(intact);
}

// ─────────────────────────── redirects ───────────────────────────

TEST(http_client_e2e, redirects_are_followed_when_asked) {
  std::string body;
  uint64_t redirects = 0;

  run_e2e([](server_t& server) {
            server.get("/from", [](auto&, response_t& resp) { resp.redirect("/to"); });
            server.get("/to", [](auto&, response_t& resp) { resp.text("arrived"); });
          },
          [&](context_t&, client_t& cli, uint16_t port) -> coro_t<void> {
            auto req = cli.request(method_t::Get, url_for(port, "/from"));
            req.follow_redirects(2);
            auto resp = co_await req.send();
            EXPECT_TRUE(resp);
            if (resp) body = std::string(resp->body());
            redirects = cli.metrics().redirects;
          });

  EXPECT_EQ(body, "arrived");
  EXPECT_EQ(redirects, 1u);
}

// Following is opt-in: by default the 3xx is the answer, because whether to follow it
// is the caller's decision to make.
TEST(http_client_e2e, redirects_are_not_followed_by_default) {
  uint16_t status = 0;
  std::string location;

  run_e2e([](server_t& server) {
            server.get("/from", [](auto&, response_t& resp) { resp.redirect("/to"); });
            server.get("/to", [](auto&, response_t& resp) { resp.text("arrived"); });
          },
          [&](context_t&, client_t& cli, uint16_t port) -> coro_t<void> {
            auto resp = co_await cli.get(url_for(port, "/from"));
            EXPECT_TRUE(resp);
            if (!resp) co_return;
            status = resp->status_code();
            location = std::string(resp->header(field_t::Location));
          });

  EXPECT_EQ(status, 302u);
  EXPECT_EQ(location, "/to");
}

// A 303 turns the request into a GET and drops the body, which is what every other
// client does and what servers therefore expect.
TEST(http_client_e2e, see_other_becomes_a_get) {
  std::string method_seen;
  std::string body;

  run_e2e([&method_seen](server_t& server) {
            server.post("/submit", [](request_t&, response_t& resp) {
              resp.status(status_t::SeeOther).header(field_t::Location, "/result");
            });
            server.get("/result", [&method_seen](request_t& req, response_t& resp) {
              method_seen = method_name(req.method());
              resp.text("result");
            });
          },
          [&](context_t&, client_t& cli, uint16_t port) -> coro_t<void> {
            auto req = cli.request(method_t::Post, url_for(port, "/submit"));
            req.body("form=data").follow_redirects(1);
            auto resp = co_await req.send();
            EXPECT_TRUE(resp);
            if (resp) body = std::string(resp->body());
          });

  EXPECT_EQ(method_seen, "GET");
  EXPECT_EQ(body, "result");
}
