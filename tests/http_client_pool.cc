#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <thread>

#include "cornet/concurrency/combinators.h"

#include "http_client_fixture.h"

using namespace cornet;
using namespace cornet::http;
using namespace cornet_test;
using namespace std::chrono_literals;

namespace {

/**
 * @brief a full client — pool, dns cache and timer wheel — on its own context.
 *
 * The client is built on first use rather than up front, so that a test can adjust
 * `opt` first: client_t copies its options, so a tweak after construction would be
 * silently ignored.
 */
struct pool_env_t {
  context_t        ctx;
  client_options_t opt = [] {
    client_options_t o;
    o.timer_tick = 20ms;
    o.idle_timeout = 200ms;
    o.pool_wait_timeout = 300ms;
    // 127.0.0.1 resolves through the executor; caching it keeps the tests from
    // measuring getaddrinfo
    o.dns_cache_ttl = 5s;
    return o;
  }();

  CORNET_NODISCARD client_t& client() {
    if (!client_) client_.emplace(ctx, opt);
    return *client_;
  }

  void run(coro_t<void> task) {
    ctx.spawn(std::move(task));
    ctx.run();
  }

 private:
  std::optional<client_t> client_{};
};

/**
 * @brief answer every request on a connection with a fixed body.
 */
origin_t::script_t answer(std::string body, int per_connection = 1) {
  return [body, per_connection](int fd, int) {
    for (int i = 0; i < per_connection; ++i) {
      auto head = read_until(fd, "\r\n\r\n");
      if (head.find("\r\n\r\n") == std::string::npos) return;
      write_all(fd, "HTTP/1.1 200 OK\r\nContent-Length: " + std::to_string(body.size()) +
                        "\r\n\r\n" + body);
    }
  };
}

} // namespace

// ─────────────────────────────── reuse ───────────────────────────────

TEST(http_client_pool, second_request_reuses_the_connection) {
  origin_t origin(answer("hi", 2));

  pool_env_t env;
  std::string first, second;

  env.run([&]() -> coro_t<void> {
    auto a = co_await env.client().get(origin.url("/a"));
    EXPECT_TRUE(a);
    if (a) first = std::string(a->body());
    auto b = co_await env.client().get(origin.url("/b"));
    EXPECT_TRUE(b);
    if (b) second = std::string(b->body());
  }());

  EXPECT_EQ(first, "hi");
  EXPECT_EQ(second, "hi");
  EXPECT_EQ(env.client().metrics().conn_created, 1u);
  EXPECT_EQ(env.client().metrics().conn_reused, 1u);
  // and it is parked, ready for a third
  EXPECT_EQ(env.client().pool().idle_count(), 1u);
  EXPECT_EQ(env.client().pool().busy_count(), 0u);
}

// The address was looked up once; the second request read it from the cache instead of
// hopping to the executor and back.
TEST(http_client_pool, dns_is_cached_across_requests) {
  origin_t origin(answer("hi", 1), 2);

  // The origin answers once per connection and then closes, so the second request
  // cannot reuse the first connection and has to resolve the host again — from the
  // cache, which is the point.
  pool_env_t env;
  auto& cli = env.client();

  env.run([&]() -> coro_t<void> {
    EXPECT_TRUE(co_await cli.get(origin.url("/a")));
    co_await sleep(env.ctx, 30ms);
    EXPECT_TRUE(co_await cli.get(origin.url("/b")));
  }());

  EXPECT_EQ(cli.metrics().dns_lookups, 1u);
  EXPECT_GE(cli.metrics().dns_cache_hits, 1u);
}

// A response that says Connection: close must not be parked, however healthy the
// socket still looks.
TEST(http_client_pool, connection_close_is_not_pooled) {
  origin_t origin([](int fd, int) {
    read_until(fd, "\r\n\r\n");
    write_all(fd, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nhi");
  });

  pool_env_t env;
  env.run([&]() -> coro_t<void> {
    auto resp = co_await env.client().get(origin.url("/a"));
    EXPECT_TRUE(resp);
    if (resp) EXPECT_FALSE(resp->keep_alive());
  }());

  EXPECT_EQ(env.client().pool().idle_count(), 0u);
  EXPECT_EQ(env.client().pool().total_count(), 0u);
}

// ──────────────────────── the idle-close race ────────────────────────

// The peer closed while the connection sat in the pool. The non-blocking peek on reuse
// catches it, so the request opens a fresh connection instead of failing.
TEST(http_client_pool, a_stale_pooled_connection_is_discarded_before_use) {
  origin_t origin(
      [](int fd, int index) {
        read_until(fd, "\r\n\r\n");
        write_all(fd, "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n");
        write_all(fd, index == 0 ? "a" : "b");
        if (index == 0) return;   // closes on return, while the client parks it
        read_until(fd, "\r\n\r\n");
      },
      2);

  pool_env_t env;
  std::string second;

  env.run([&]() -> coro_t<void> {
    EXPECT_TRUE(co_await env.client().get(origin.url("/a")));
    // give the FIN time to arrive, so the peek has something to see
    co_await sleep(env.ctx, 50ms);
    auto b = co_await env.client().get(origin.url("/b"));
    EXPECT_TRUE(b);
    if (b) second = std::string(b->body());
  }());

  EXPECT_EQ(second, "b");
  EXPECT_EQ(env.client().metrics().stale_discarded, 1u);
  EXPECT_EQ(env.client().metrics().conn_created, 2u);
  EXPECT_EQ(env.client().metrics().retries, 0u);
}

// The same race, but lost: the peer closes only after the request has gone out, so no
// peek could have caught it. No response byte arrived and the method is idempotent, so
// this is exactly the case a retry is for — once.
TEST(http_client_pool, an_idempotent_request_retries_once_when_the_peer_vanishes) {
  origin_t origin(
      [](int fd, int index) {
        if (index == 0) {
          read_until(fd, "\r\n\r\n");
          write_all(fd, "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\na");
          read_until(fd, "\r\n\r\n");   // second request arrives, then we vanish
          return;
        }
        read_until(fd, "\r\n\r\n");
        write_all(fd, "HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\nretry!");
      },
      2);

  pool_env_t env;
  std::string body;

  env.run([&]() -> coro_t<void> {
    EXPECT_TRUE(co_await env.client().get(origin.url("/a")));
    auto b = co_await env.client().get(origin.url("/b"));
    EXPECT_TRUE(b);
    if (b) body = std::string(b->body());
  }());

  EXPECT_EQ(body, "retry!");
  EXPECT_EQ(env.client().metrics().retries, 1u);
  EXPECT_EQ(env.client().metrics().conn_created, 2u);
}

// A POST is not replayed to paper over the same race: the peer may have acted on it
// already, and a duplicate order is worse than an error.
TEST(http_client_pool, a_post_is_not_retried) {
  origin_t origin(
      [](int fd, int index) {
        if (index == 0) {
          read_until(fd, "\r\n\r\n");
          write_all(fd, "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\na");
          read_with_body(fd, 4);   // the POST arrives, then we vanish
          return;
        }
        read_until(fd, "\r\n\r\n");
        write_all(fd, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nno");
      },
      2);

  pool_env_t env;
  bool failed = false;

  env.run([&]() -> coro_t<void> {
    EXPECT_TRUE(co_await env.client().get(origin.url("/a")));
    auto b = co_await env.client().post(origin.url("/b"), "data", "text/plain");
    failed = !b;
  }());

  EXPECT_TRUE(failed);
  EXPECT_EQ(env.client().metrics().retries, 0u);
}

// ...unless the caller says the request is safe to replay.
TEST(http_client_pool, a_post_marked_idempotent_is_retried) {
  origin_t origin(
      [](int fd, int index) {
        if (index == 0) {
          read_until(fd, "\r\n\r\n");
          write_all(fd, "HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\na");
          read_with_body(fd, 4);
          return;
        }
        read_with_body(fd, 4);
        write_all(fd, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
      },
      2);

  pool_env_t env;
  std::string body;

  env.run([&]() -> coro_t<void> {
    EXPECT_TRUE(co_await env.client().get(origin.url("/a")));
    auto req = env.client().request(method_t::Post, origin.url("/b"));
    req.body("data").idempotent(true);
    auto b = co_await req.send();
    EXPECT_TRUE(b);
    if (b) body = std::string(b->body());
  }());

  EXPECT_EQ(body, "ok");
  EXPECT_EQ(env.client().metrics().retries, 1u);
}

// ──────────────────────── capacity and queueing ────────────────────────

TEST(http_client_pool, requests_queue_when_the_host_limit_is_reached) {
  // Two requests, one connection allowed. The origin answers twice on the same
  // connection before closing, so the request that had to queue is handed a connection
  // that is still alive — otherwise this would be a test of the retry path instead.
  origin_t origin([](int fd, int) {
    for (int i = 0; i < 2; ++i) {
      read_until(fd, "\r\n\r\n");
      std::this_thread::sleep_for(20ms);
      write_all(fd, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi");
    }
  });

  pool_env_t env;
  env.opt.max_conns_per_host = 1;
  auto& cli = env.client();

  uint32_t done = 0;

  env.run([&]() -> coro_t<void> {
    uint32_t finished = 0;
    auto task = [&](std::string path) -> coro_t<void> {
      auto r = co_await cli.get(origin.url(path));
      EXPECT_TRUE(r);
      if (r) ++done;
      ++finished;
    };
    env.ctx.spawn(task("/a"));
    env.ctx.spawn(task("/b"));
    while (finished < 2) co_await sleep(env.ctx, 5ms);
  }());

  EXPECT_EQ(done, 2u);
  // one connection served both, and the second request had to wait for it
  EXPECT_EQ(cli.metrics().conn_created, 1u);
  EXPECT_GE(cli.metrics().pool_waits, 1u);
  EXPECT_EQ(cli.metrics().retries, 0u);
}

// Waiting is bounded: a caller that cannot get a connection in time is told so rather
// than blocking forever behind a slow peer.
TEST(http_client_pool, waiting_for_a_connection_times_out) {
  origin_t origin([](int fd, int, hold_gate_t& gate) {
    read_until(fd, "\r\n\r\n");
    write_all(fd, "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nslow");
    gate.wait();   // keep the only connection busy for the whole test
  });

  pool_env_t env;
  env.opt.max_conns_per_host = 1;
  env.opt.pool_wait_timeout = 100ms;
  auto& cli = env.client();

  cornet::error_t err{};

  env.run([&]() -> coro_t<void> {
    // hold the only connection by keeping its body unread
    auto held = co_await cli.stream(method_t::Get, origin.url("/held"));
    EXPECT_TRUE(held);
    if (!held) co_return;

    auto blocked = co_await cli.get(origin.url("/blocked"));
    EXPECT_FALSE(blocked);
    if (!blocked) err = blocked.error();
  }());

  EXPECT_EQ(err.code, int(http_error_t::PoolExhausted));
}

// ──────────────────────── idle expiry ────────────────────────

TEST(http_client_pool, idle_connections_are_reaped) {
  origin_t origin([](int fd, int, hold_gate_t& gate) {
    read_until(fd, "\r\n\r\n");
    write_all(fd, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi");
    // stay open, so only the idle timer can close this
    gate.wait();
  });

  pool_env_t env;
  env.opt.idle_timeout = 60ms;
  auto& cli = env.client();

  uint32_t idle_after_request = 0;
  uint32_t idle_after_wait = 99;

  env.run([&]() -> coro_t<void> {
    EXPECT_TRUE(co_await cli.get(origin.url("/a")));
    idle_after_request = cli.pool().idle_count();
    co_await sleep(env.ctx, 250ms);
    idle_after_wait = cli.pool().idle_count();
  }());

  EXPECT_EQ(idle_after_request, 1u);
  EXPECT_EQ(idle_after_wait, 0u);
}

// ──────────────────────── failure modes ────────────────────────

TEST(http_client_pool, nothing_listening_is_a_connect_error) {
  pool_env_t env;
  cornet::error_t err{};

  env.run([&]() -> coro_t<void> {
    // port 1 on loopback: nothing is there, and binding it needs root
    auto resp = co_await env.client().get("http://127.0.0.1:1/x");
    EXPECT_FALSE(resp);
    if (!resp) err = resp.error();
  }());

  EXPECT_EQ(err.domain, error_domain::System);
  EXPECT_EQ(env.client().metrics().connect_errors, 1u);
  EXPECT_EQ(env.client().pool().total_count(), 0u);
}

// A url this build cannot speak is reported as such, rather than by connecting to 443
// and failing to parse whatever comes back.
TEST(http_client_pool, https_is_reported_as_unsupported) {
  pool_env_t env;
  cornet::error_t err{};

  env.run([&]() -> coro_t<void> {
    auto resp = co_await env.client().get("https://example.com/");
    EXPECT_FALSE(resp);
    if (!resp) err = resp.error();
  }());

  EXPECT_EQ(err.code, int(http_error_t::UnsupportedScheme));
}

TEST(http_client_pool, a_malformed_url_fails_the_send_not_the_builder) {
  pool_env_t env;
  cornet::error_t err{};

  env.run([&]() -> coro_t<void> {
    auto req = env.client().request(method_t::Get, "nonsense");
    EXPECT_TRUE(req.failed());
    auto resp = co_await req.send();
    EXPECT_FALSE(resp);
    if (!resp) err = resp.error();
  }());

  EXPECT_EQ(err.code, int(http_error_t::BadUrl));
}
