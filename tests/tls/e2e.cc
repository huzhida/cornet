/**
 * @brief TLS end-to-end tests: a real cornet https server against client_t.
 *
 * Same structure as http_client_fixture/run_e2e: the server runs on a background
 * thread and publishes an ephemeral port; the client lives in this fixture and
 * reaches the test coroutine by reference. Returning locals borrowed by a coroutine
 * is the classic dangling-lambda bug — the fixture exists precisely so tests
 * cannot write it.
 *
 * Nothing here needs OpenSSL headers; and while the engine tests run on any
 * kernel, these need a live io_uring like every other e2e test in the repo.
 */

#include <gtest/gtest.h>

#include <atomic>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include "cornet/http/client/client.h"
#include "cornet/http/server/server.h"
#include "cornet/net/socket.h"
#include "cornet/scheduling/context.h"

#include "certs.h"

#ifdef CORNET_WITH_TLS

using namespace cornet;
using namespace cornet::http;

namespace {

/**
 * @brief run one https exchange test pair.
 *
 * The server context lives on a background thread; the published pointer stays
 * valid until join so tests can read out metrics afterwards. The client
 * context and the client itself live here on the calling thread: a test body
 * sees them by reference inside a coroutine it returns.
 */
void run_tls_e2e_test(const std::function<void(server_t&)>& setup,
                      const std::function<client_options_t()>& client_opts,
                      const std::function<coro_t<void>(client_t&, uint16_t)>& body,
                      connection_metrics_t* metrics = nullptr
                    ) {
  context_t server_ctx;
  std::atomic<uint16_t> server_port{0};
  std::atomic<bool> server_ready{false};
  std::atomic<bool> server_failed{false};
  std::string listen_error;

  std::thread server_thread([&]() {
    auto tls_ctx = tls::tls_context_t::make_server(tls::tls_server_options_t{
        .cert_pem = std::string(test::tls::kServerCert),
        .key_pem = std::string(test::tls::kServerKey),
    });
    if (!tls_ctx) {
      listen_error = tls_ctx.error().message();
      server_failed.store(true, std::memory_order_release);
      return;
    }

    server_t server(server_ctx, server_options_t{
        .port = 0,
        .address = "127.0.0.1",
        .tls = *tls_ctx,
    });
    setup(server);

    if (auto ok = server.listen(); !ok) {
      listen_error = ok.error().message();
      server_failed.store(true, std::memory_order_release);
      return;
    }
    server_port.store(server.options().port, std::memory_order_relaxed);
    server_ctx.spawn(server.serve());
    server_ready.store(true, std::memory_order_release);
    server_ctx.run();
    if (metrics) *metrics = server.metrics();
  });

  while (!server_ready.load(std::memory_order_acquire) &&
         !server_failed.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }

  if (!server_failed.load(std::memory_order_acquire)) {
    context_t client_ctx;
    client_t client(client_ctx, client_opts());
    client_ctx.spawn(body(client, server_port.load(std::memory_order_relaxed)));
    client_ctx.run();
    server_ctx.stop();
  }
  server_thread.join();
  ASSERT_FALSE(server_failed.load(std::memory_order_relaxed))
      << "setup failed: " << listen_error;
}

/** @brief client options that trust the throwaway CA. */
client_options_t verified_client_opts() {
  client_options_t opt;
  opt.tls_ca_pem = std::string(test::tls::kCaCert);
  return opt;
}

std::string url_for(uint16_t port, std::string_view path) {
  return "https://localhost:" + std::to_string(port) + std::string(path);
}

} // namespace

TEST(tls_e2e, get_over_tls) {
  expected<client_response_t> got = http_unexpected(http_error_t::InvalidState);
  run_tls_e2e_test(
      [](server_t& srv) {
        srv.get("/hello", [](auto&, auto& resp) { resp.text("hello tls"); });
      },
      [] { return verified_client_opts(); },
      [&](client_t& cli, uint16_t port) -> coro_t<void> {
        got = co_await cli.get(url_for(port, "/hello"));
      });

  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(got->status_code(), 200);
  EXPECT_EQ(got->body(), "hello tls");
}
TEST(tls_e2e, post_echo_over_tls) {
  expected<client_response_t> got = http_unexpected(http_error_t::InvalidState);
  std::string big(64u * 1024u, 'z');  // many records both ways
  run_tls_e2e_test(
      [](server_t& srv) {
        srv.post("/echo", [](request_t& req, response_t& resp) -> coro_t<void> {
          // A direct response would exceed body_buffer_bytes (16K), and a
          // single chunk this big exceeds stream_out_ too — the writer takes
          // care of both, going around the staging buffer for the payload.
          // Same rules as plain HTTP; TLS changes none of them.
          auto writer = resp.chunked();
          if (auto ok = co_await writer.write(req.body()); !ok) co_return;
          co_await writer.finish();
        });
      },
      [] { return verified_client_opts(); },
      [&](client_t& cli, uint16_t port) -> coro_t<void> {
        got = co_await cli.post(url_for(port, "/echo"), big, "text/plain");
      });

  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(got->status_code(), 200);
  EXPECT_EQ(got->body(), big);
}

TEST(tls_e2e, keep_alive_reuses_the_tls_connection) {
  client_metrics_t seen;
  expected<client_response_t> first = http_unexpected(http_error_t::InvalidState);
  expected<client_response_t> second = http_unexpected(http_error_t::InvalidState);
  run_tls_e2e_test(
      [](server_t& srv) {
        srv.get("/x", [](auto&, auto& resp) { resp.text("x"); });
      },
      [] { return verified_client_opts(); },
      [&](client_t& cli, uint16_t port) -> coro_t<void> {
        first = co_await cli.get(url_for(port, "/x"));
        second = co_await cli.get(url_for(port, "/x"));
        seen = cli.metrics();
      });

  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(seen.conn_reused, 1u);
}

TEST(tls_e2e, chunked_streaming_over_tls) {
  expected<client_response_t> got = http_unexpected(http_error_t::InvalidState);
  run_tls_e2e_test(
      [](server_t& srv) {
        srv.get("/stream", [](request_t&, response_t& resp) -> coro_t<void> {
          auto writer = resp.chunked();
          for (int i = 0; i < 8; ++i) {
            if (auto ok = co_await writer.write("chunk" + std::to_string(i)); !ok) {
              co_return;
            }
          }
          co_await writer.finish();
        });
      },
      [] { return verified_client_opts(); },
      [&](client_t& cli, uint16_t port) -> coro_t<void> {
        got = co_await cli.get(url_for(port, "/stream"));
      });

  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(got->status_code(), 200);
  EXPECT_EQ(got->body(), "chunk0chunk1chunk2chunk3chunk4chunk5chunk6chunk7");
}
TEST(tls_e2e, wrong_ca_is_refused_with_verify_error) {
  cornet::error_t err{};
  run_tls_e2e_test(
      [](server_t& srv) {
        srv.get("/x", [](auto&, auto& resp) { resp.text("x"); });
      },
      [] {
        client_options_t opt;
        opt.tls_ca_pem = std::string(test::tls::kEvilCaCert);
        return opt;
      },
      [&](client_t& cli, uint16_t port) -> coro_t<void> {
        auto resp = co_await cli.get(url_for(port, "/x"));
        EXPECT_FALSE(resp.has_value());
        if (!resp) err = resp.error();
      });

  EXPECT_EQ(err.domain, error_domain::Tls);
  EXPECT_EQ(err.code, int(tls::tls_error_t::VerifyFailed));
}

// Plain HTTP spoken to a TLS port must fail predictably, and the server must
// shrug it off: the next real TLS client is served like nothing happened.
TEST(tls_e2e, plain_http_to_tls_port_is_rejected) {
  connection_metrics_t metrics;
  expected<client_response_t> plain = http_unexpected(http_error_t::InvalidState);
  expected<client_response_t> after = http_unexpected(http_error_t::InvalidState);

  run_tls_e2e_test(
      [](server_t& srv) {
        srv.get("/x", [](auto&, auto& resp) { resp.text("x"); });
      },
      [] { return verified_client_opts(); },
      [&](client_t& tls_cli, uint16_t port) -> coro_t<void> {
        // one client speaks both schemes: pool keying keeps the plain and TLS
        // connections to the same port strictly apart
        plain = co_await tls_cli.get("http://127.0.0.1:" + std::to_string(port) + "/x");
        after = co_await tls_cli.get(url_for(port, "/x"));
      },
      &metrics);

  EXPECT_FALSE(plain.has_value());
  EXPECT_GE(metrics.tls_handshake_errors, 1u);
  ASSERT_TRUE(after.has_value()) << "the server must survive a garbage handshake";
  EXPECT_EQ(after->body(), "x");
}




TEST(tls_e2e, file_response_over_tls_matches_bytes) {
  expected<client_response_t> got = http_unexpected(http_error_t::InvalidState);
  const std::string content(196u * 1024u, 'z');

  namespace fs = std::filesystem;
  auto path = fs::temp_directory_path() /
              ("cornet_file_test_tls_" + std::to_string(::getpid()) + ".bin");
  {
    FILE* f = std::fopen(path.c_str(), "wb");
    ASSERT_TRUE(f != nullptr);
    std::fwrite(content.data(), 1, content.size(), f);
    std::fclose(f);
  }

  run_tls_e2e_test(
      [&path](server_t& srv) {
        srv.get("/tf", [&path](auto&, response_t& resp) {
          EXPECT_TRUE(resp.local_file(path.string()));
        });
      },
      [] { return verified_client_opts(); },
      [&](client_t& cli, uint16_t port) -> coro_t<void> {
        got = co_await cli.get(url_for(port, "/tf"));
      });

  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(int(got->status_code()), 200);
  EXPECT_EQ(got->body(), content);
  fs::remove(path);
}
#endif // CORNET_WITH_TLS
