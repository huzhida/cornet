#include <gtest/gtest.h>

#include <filesystem>

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <string>
#include <thread>

#include "cornet/concurrency/combinators.h"
#include "cornet/http/server/server.h"
#include "cornet/net/socket.h"
#include "cornet/scheduling/context.h"

using namespace cornet;
using namespace cornet::http;

namespace {

// ─────────────────────── helpers ───────────────────────

/**
 * @brief Extract the body from a raw HTTP response string.
 *
 * For chunked responses the body must be decoded. For Content-Length
 * responses it is a substring.  This is deliberately minimal — we only
 * need what our own routes produce.
 */
static std::string extract_body(std::string_view raw) {
  // Find the blank line that separates headers from body.
  auto sep = raw.find("\r\n\r\n");
  if (sep == std::string_view::npos) return {};

  std::string_view headers(raw.data(), sep);
  std::string_view body(raw.data() + sep + 4);

  // Check for Transfer-Encoding: chunked.
  auto te_pos = headers.find("Transfer-Encoding:");
  if (te_pos != std::string_view::npos &&
      headers.substr(te_pos).find("chunked") != std::string_view::npos) {
    std::string result;
    while (!body.empty()) {
      auto crlf = body.find("\r\n");
      if (crlf == std::string_view::npos) break;
      size_t len = 0;
      try {
        len = std::stoul(std::string(body.substr(0, crlf)), nullptr, 16);
      } catch (...) {
        break;
      }
      if (len == 0) break;
      if (crlf + 2 + len + 2 > body.size()) break;
      result.append(body.data() + crlf + 2, len);
      body = body.substr(crlf + 2 + len + 2); // skip data + \r\n
    }
    return result;
  }

  // Content-Length: take exactly that many bytes.
  auto cl_pos = headers.find("Content-Length:");
  if (cl_pos != std::string_view::npos) {
    auto colon = headers.find(':', cl_pos);
    if (colon != std::string_view::npos) {
      size_t len = 0;
      try {
        len = std::stoul(std::string(headers.substr(colon + 1).substr(0, 10)));
      } catch (...) {
        return std::string(body);
      }
      if (len > 0 && len <= body.size()) {
        return std::string(body.substr(0, len));
      }
    }
  }

  return std::string(body);
}

/**
 * @brief Check that a raw HTTP response contains a substring in its status line.
 */
static bool response_contains_status(std::string_view raw, std::string_view phrase) {
  auto crlf = raw.find("\r\n");
  if (crlf == std::string_view::npos) return false;
  return raw.substr(0, crlf).find(phrase) != std::string_view::npos;
}

/**
 * @brief Base test fixture: sets up a context, starts a server coroutine,
 * returns an atomic port so the client coroutine knows where to connect.
 *
 * Usage:
 *   auto fixture = make_e2e_fixture(ctx);
 *   auto [server, port] = fixture;
 *   ctx.spawn(client(ctx, *port));
 *   ctx.run();
 *   ctx.stop();
 */
struct e2e_fixture_t {
  std::atomic<uint16_t>* port{nullptr};
  std::atomic<bool>* ready{nullptr};
  std::atomic<bool>* done{nullptr};
};

// ─────────────────── run_e2e_test helper ───────────────────

/**
 * @brief Run a single HTTP end-to-end test.
 *
 * Server runs on a background thread; client runs on the calling thread.
 * The client_fn is a coroutine factory that stores its results in captured
 * variables. After the client finishes, the server is drained so the
 * context can wind down, then the thread is joined.
 *
 * The listener takes whatever port the kernel offers and client_fn is told which one.
 * A fixed port would be flaky: ip_local_port_range covers the range these tests used
 * to hardcode, so any outbound connection in the same run can be holding it.
 */
void run_e2e_test(std::function<void(server_t&)> setup,
                  std::function<void(context_t&, uint16_t)> client_fn) {
  // Server context on a background thread
  context_t server_ctx;
  std::atomic<uint16_t> server_port{0};
  std::atomic<bool> server_ready{false};
  std::atomic<bool> server_failed{false};
  std::string listen_error;

  std::thread server_thread([&]() {
    server_t server(server_ctx, server_options_t{
      .port = 0,
      .address = "127.0.0.1",
    });
    setup(server);

    if (auto ok = server.listen(); !ok) {
      // published before the flag, so the waiter below sees a complete message
      listen_error = ok.error().message();
      server_failed.store(true, std::memory_order_release);
      return;
    }
    server_port.store(server.options().port, std::memory_order_relaxed);

    // serve() must be spawned before run() so the accept loop is registered
    // with the context; then run() will keep the context alive.
    server_ctx.spawn(server.serve());
    server_ready.store(true, std::memory_order_release);
    server_ctx.run();
  });

  // Wait until the server is accepting — or gave up, which must also end this loop
  while (!server_ready.load(std::memory_order_acquire)
         && !server_failed.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }

  if (!server_failed.load(std::memory_order_acquire)) {
    // Client context on the calling thread
    context_t client_ctx;
    client_fn(client_ctx, server_port.load(std::memory_order_relaxed));
    client_ctx.run();

    // Stop server: drain closes the listener, which ends accept_loop, which
    // lets user_idle() become true and ctx.run() returns.
    server_ctx.stop();
  }
  server_thread.join();
  ASSERT_FALSE(server_failed.load(std::memory_order_relaxed))
      << "listen failed: " << listen_error;
}

} // namespace


// =========================================================================
//  TEST: Basic GET request and 200 OK response
// =========================================================================

TEST(http_e2e, basic_get) {
  std::string response_body;

  run_e2e_test(
    [&response_body](server_t& server) {
      server.get("/hello", [](auto&, response_t& resp) {
        resp.text("hello cornet");
      });
    },
    [&response_body](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);


        std::string request =
            "GET /hello HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n";

        co_await sock.send(ctx, request.data(), request.size());

        char buf[4096];
        std::string raw;
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw.append(buf, *n);
        }
        response_body = extract_body(raw);
      };
      ctx.spawn(client());
      ctx.run();
    });

  EXPECT_EQ(response_body, "hello cornet");
}

// =========================================================================
//  TEST: POST with body content (aggregated)
// =========================================================================

TEST(http_e2e, post_with_body) {
  std::string response_body;

  run_e2e_test(
    [&response_body](server_t& server) {
      server.post("/echo", [](request_t& req, response_t& resp) {
        resp.text(std::string(req.body()));
      });
    },
    [&response_body](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);


        std::string body = "hello world from client";
        std::string request =
            "POST /echo HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Content-Length: " + std::to_string(body.size()) + "\r\n"
            "Connection: close\r\n"
            "\r\n" + body;

        co_await sock.send(ctx, request.data(), request.size());

        char buf[4096];
        std::string raw;
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw.append(buf, *n);
        }
        response_body = extract_body(raw);
      };
      ctx.spawn(client());
      ctx.run();
    });

  EXPECT_EQ(response_body, "hello world from client");
}

// =========================================================================
//  TEST: Streaming body read (request with chunked transfer encoding)
// =========================================================================

TEST(http_e2e, streaming_body_read) {
  std::string response_body;

  run_e2e_test(
    [&response_body](server_t& server) {
      auto& stream_route = server.route(method_t::Post, "/stream",
        [](request_t& req, response_t& resp) -> coro_t<void> {
          auto* reader = req.stream();
          uint64_t total = 0;
          while (!reader->complete()) {
            auto chunk = co_await reader->read();
            if (!chunk) {
              resp.status(status_t::BadRequest);
              co_return;
            }
            if (chunk->empty()) break;
            total += chunk->size();
          }
          resp.text(std::to_string(total));
        });
      stream_route.body = body_policy_t::Stream;
    },
    [&response_body](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);


        // Send a chunked request body.
        std::string request =
            "POST /stream HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Transfer-Encoding: chunked\r\n"
            "Connection: close\r\n"
            "\r\n"
            "5\r\nhello\r\n"
            "6\r\n world\r\n"
            "0\r\n\r\n";

        co_await sock.send(ctx, request.data(), request.size());

        char buf[4096];
        std::string raw;
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw.append(buf, *n);
        }
        response_body = extract_body(raw);
      };
      ctx.spawn(client());
      ctx.run();
    });

  EXPECT_EQ(response_body, "11");
}

// =========================================================================
//  TEST: Streaming body write (chunked transfer encoding response)
// =========================================================================

TEST(http_e2e, streaming_body_write) {
  std::string response_body;

  run_e2e_test(
    [&response_body](server_t& server) {
      server.get("/stream-out", [](request_t&, response_t& resp) -> coro_t<void> {
        auto w = resp.chunked();
        co_await w.write("chunk1");
        co_await w.write("chunk2");
        co_await w.write("chunk3");
        co_await w.finish();
      });
    },
    [&response_body](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);


        std::string request =
            "GET /stream-out HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n";

        co_await sock.send(ctx, request.data(), request.size());

        char buf[4096];
        std::string raw;
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw.append(buf, *n);
        }
        response_body = extract_body(raw);
      };
      ctx.spawn(client());
      ctx.run();
    });

  EXPECT_EQ(response_body, "chunk1chunk2chunk3");
}

// =========================================================================
//  TEST: 404 Not Found for unmatched routes
// =========================================================================

TEST(http_e2e, not_found) {
  std::string raw_response;

  run_e2e_test(
    [](server_t& server) {
      server.get("/exists", [](auto&, response_t& resp) {
        resp.text("found");
      });
    },
    [&raw_response](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);


        std::string request =
            "GET /nonexistent HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n";

        co_await sock.send(ctx, request.data(), request.size());

        char buf[4096];
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw_response.append(buf, *n);
        }
      };
      ctx.spawn(client());
      ctx.run();
    });

  EXPECT_TRUE(response_contains_status(raw_response, "404"))
      << "expected 404 in response, got: " << raw_response;
}

// =========================================================================
//  TEST: Keep-alive pipelined requests
// =========================================================================

TEST(http_e2e, keep_alive_pipelined) {
  std::string raw_response;

  run_e2e_test(
    [](server_t& server) {
      server.get("/ping", [](auto&, response_t& resp) {
        resp.text("pong");
      });
    },
    [&raw_response](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);


        // Send two pipelined requests on the same connection.
        // Connection: close is used so the client knows when to stop reading.
        std::string request =
            "GET /ping HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n"
            "GET /ping HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "\r\n";

        co_await sock.send(ctx, request.data(), request.size());

        // Read until we see two response bodies.
        char buf[4096];
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw_response.append(buf, *n);
        }
      };
      ctx.spawn(client());
      ctx.run();
    });

  // The pipelined response should contain "pong" (at least once).
  std::string body = extract_body(raw_response);
  EXPECT_TRUE(body.find("pong") != std::string::npos)
      << "expected pong in response, got: " << raw_response;
}

// =========================================================================
//  TEST: Path parameters
// =========================================================================

TEST(http_e2e, path_parameters) {
  std::string response_body;

  run_e2e_test(
    [&response_body](server_t& server) {
      server.get("/users/:id", [](request_t& req, response_t& resp) {
        resp.text("user " + std::string(req.param("id")));
      });
    },
    [&response_body](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);


        std::string request =
            "GET /users/42 HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n";

        co_await sock.send(ctx, request.data(), request.size());

        char buf[4096];
        std::string raw;
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw.append(buf, *n);
        }
        response_body = extract_body(raw);
      };
      ctx.spawn(client());
      ctx.run();
    });

  EXPECT_EQ(response_body, "user 42");
}

// =========================================================================
//  TEST: Query string
// =========================================================================

TEST(http_e2e, query_string) {
  std::string response_body;

  run_e2e_test(
    [&response_body](server_t& server) {
      server.get("/search", [](request_t& req, response_t& resp) {
        auto q = req.query().get("q");
        resp.text("search: " + std::string(q));
      });
    },
    [&response_body](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);


        std::string request =
            "GET /search?q=hello+world HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n";

        co_await sock.send(ctx, request.data(), request.size());

        char buf[4096];
        std::string raw;
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw.append(buf, *n);
        }
        response_body = extract_body(raw);
      };
      ctx.spawn(client());
      ctx.run();
    });

  EXPECT_EQ(response_body, "search: hello+world");
}

// =========================================================================
//  TEST: Content-Length response body matches actual bytes
// =========================================================================

TEST(http_e2e, content_length_matches) {
  std::string raw_response;

  run_e2e_test(
    [&raw_response](server_t& server) {
      server.get("/large", [](auto&, response_t& resp) {
        std::string body(1024, 'A');
        resp.body(body);
      });
    },
    [&raw_response](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);


        std::string request =
            "GET /large HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n";

        co_await sock.send(ctx, request.data(), request.size());

        char buf[4096];
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw_response.append(buf, *n);
        }
      };
      ctx.spawn(client());
      ctx.run();
    });

  std::string body = extract_body(raw_response);
  EXPECT_EQ(body.size(), 1024u);
  EXPECT_EQ(body[0], 'A');
  EXPECT_EQ(body[1023], 'A');
}

// =========================================================================
//  TEST: HEAD request returns headers but no body
// =========================================================================

TEST(http_e2e, head_request_no_body) {
  std::string raw_response;

  run_e2e_test(
    [&raw_response](server_t& server) {
      server.head("/data", [](auto&, response_t& resp) {
        resp.body_static("this is the body that should not appear");
      });
    },
    [&raw_response](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);


        std::string request =
            "HEAD /data HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n";

        co_await sock.send(ctx, request.data(), request.size());

        char buf[4096];
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw_response.append(buf, *n);
        }
      };
      ctx.spawn(client());
      ctx.run();
    });

  EXPECT_TRUE(response_contains_status(raw_response, "200 OK"));
  EXPECT_EQ(extract_body(raw_response), "");
}

// =========================================================================
//  TEST: Streaming body write — many small chunks
// =========================================================================

TEST(http_e2e, streaming_write_many_chunks) {
  std::string response_body;

  run_e2e_test(
    [&response_body](server_t& server) {
      server.get("/stream-many", [](request_t&, response_t& resp) -> coro_t<void> {
        auto w = resp.chunked();
        for (int i = 0; i < 20; ++i) {
          char buf[16];
          int n = snprintf(buf, sizeof(buf), "%d", i);
          co_await w.write(std::string_view(buf, n));
        }
        co_await w.finish();
      });
    },
    [&response_body](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);


        std::string request =
            "GET /stream-many HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n";

        co_await sock.send(ctx, request.data(), request.size());

        char buf[4096];
        std::string raw;
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw.append(buf, *n);
        }
        response_body = extract_body(raw);
      };
      ctx.spawn(client());
      ctx.run();
    });

  EXPECT_EQ(response_body, "012345678910111213141516171819");
}

// =========================================================================
//  TEST: Method not allowed (405)
// =========================================================================

TEST(http_e2e, method_not_allowed) {
  std::string raw_response;

  run_e2e_test(
    [&raw_response](server_t& server) {
      // Register both GET and PUT so the trie has a node with multiple
      // methods. POST to the PUT-only parameterised route triggers 405.
      server.get("/items", [](auto&, response_t& resp) {
        resp.text("list items");
      });
      server.put("/items/:id", [](auto&, response_t& resp) {
        resp.text("update item");
      });
    },
    [&raw_response](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);


        // POST to a PUT-only parameterised route — path matches but method doesn't.
        std::string request =
            "POST /items/42 HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n";

        co_await sock.send(ctx, request.data(), request.size());

        char buf[4096];
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw_response.append(buf, *n);
        }
      };
      ctx.spawn(client());
      ctx.run();
    });

  EXPECT_TRUE(response_contains_status(raw_response, "405"))
      << "expected 405 in response, got: " << raw_response;
}

// =========================================================================
//  TEST: Aggregated body far larger than the receive buffer
//
//  The receive buffer holds the header section *and* is the window body bytes
//  land in. Its size is max_header_bytes, so a body has to be read by rewinding
//  the region behind the headers and refilling it; without that, anything bigger
//  than the buffer fails as 431.
// =========================================================================

TEST(http_e2e, aggregated_body_larger_than_receive_buffer) {
  std::string response_body;
  const size_t kBody = 300u << 10;   // 300K into a 16K receive buffer

  run_e2e_test(
    [](server_t& server) {
      auto& route = server.post("/upload", [](request_t& req, response_t& resp) {
        // The headers must still be readable after the body has streamed past:
        // their offsets live below the rewind mark.
        auto trace = req.headers().get("x-trace");
        auto& text = resp.pin(std::string(trace) + ":" + std::to_string(req.body().size()));
        resp.body_static(text);
      });
      // Pinned rather than left to Auto: Auto streams anything above
      // aggregate_threshold (256K by default), and this test is about the aggregate
      // path — a body that has to be read by rewinding the window, not one the
      // handler pulls itself.
      route.body = body_policy_t::Aggregate;
    },
    [&response_body, kBody](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);

        std::string request =
            "POST /upload HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "X-Trace: abc123\r\n"
            "Content-Length: " + std::to_string(kBody) + "\r\n"
            "Connection: close\r\n"
            "\r\n";
        co_await sock.send(ctx, request.data(), request.size());

        // Deliberately in pieces, so the body spans many reads.
        std::string chunk(32u << 10, 'x');
        size_t sent = 0;
        while (sent < kBody) {
          size_t n = std::min(chunk.size(), kBody - sent);
          auto ok = co_await sock.send(ctx, chunk.data(), n);
          if (!ok) break;
          sent += size_t(*ok);
        }

        char buf[4096];
        std::string raw;
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw.append(buf, *n);
        }
        response_body = extract_body(raw);
      };
      ctx.spawn(client());
      ctx.run();
    });

  EXPECT_EQ(response_body, "abc123:" + std::to_string(kBody));
}

// =========================================================================
//  TEST: Body arriving after the headers, in separate reads
//
//  A round can end mid-body — the body simply has not all arrived yet. Reclaiming
//  the receive buffer at that point would move the bytes every header view of the
//  request in flight points at.
// =========================================================================

TEST(http_e2e, body_split_across_reads_keeps_headers_intact) {
  std::string response_body;

  run_e2e_test(
    [](server_t& server) {
      server.post("/echo", [](request_t& req, response_t& resp) {
        auto trace = req.headers().get("x-trace");
        auto host = req.headers().get(field_t::Host);
        auto& text = resp.pin(std::string(host) + "|" + std::string(trace) + "|" +
                              std::string(req.body()) + "|" + std::string(req.path()));
        resp.body_static(text);
      });
    },
    [&response_body](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);

        std::string head =
            "POST /echo HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "X-Trace: keep-me\r\n"
            "Content-Length: 11\r\n"
            "Connection: close\r\n"
            "\r\n";
        co_await sock.send(ctx, head.data(), head.size());

        // The server sees the headers with no body at all, then two more reads.
        co_await sleep(ctx, std::chrono::milliseconds(5));
        co_await sock.send(ctx, "hello", 5);
        co_await sleep(ctx, std::chrono::milliseconds(5));
        co_await sock.send(ctx, " world", 6);

        char buf[4096];
        std::string raw;
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw.append(buf, *n);
        }
        response_body = extract_body(raw);
      };
      ctx.spawn(client());
      ctx.run();
    });

  EXPECT_EQ(response_body, "127.0.0.1|keep-me|hello world|/echo");
}

// =========================================================================
//  TEST: Streamed body far larger than the receive buffer
// =========================================================================

TEST(http_e2e, streamed_body_larger_than_receive_buffer) {
  std::string response_body;
  const size_t kBody = 300u << 10;

  run_e2e_test(
    [](server_t& server) {
      auto& route = server.route(method_t::Post, "/stream-big",
        [](request_t& req, response_t& resp) -> coro_t<void> {
          auto* reader = req.stream();
          uint64_t total = 0;
          while (!reader->complete()) {
            auto chunk = co_await reader->read();
            if (!chunk) {
              resp.status(status_t::BadRequest);
              co_return;
            }
            if (chunk->empty()) break;
            total += chunk->size();
          }
          // still readable once the whole body has gone past
          auto trace = req.headers().get("x-trace");
          auto& text = resp.pin(std::string(trace) + ":" + std::to_string(total));
          resp.body_static(text);
        });
      route.body = body_policy_t::Stream;
    },
    [&response_body, kBody](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);

        std::string request =
            "POST /stream-big HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "X-Trace: streamed\r\n"
            "Content-Length: " + std::to_string(kBody) + "\r\n"
            "Connection: close\r\n"
            "\r\n";
        co_await sock.send(ctx, request.data(), request.size());

        std::string chunk(16u << 10, 'y');
        size_t sent = 0;
        while (sent < kBody) {
          size_t n = std::min(chunk.size(), kBody - sent);
          auto ok = co_await sock.send(ctx, chunk.data(), n);
          if (!ok) break;
          sent += size_t(*ok);
        }

        char buf[4096];
        std::string raw;
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw.append(buf, *n);
        }
        response_body = extract_body(raw);
      };
      ctx.spawn(client());
      ctx.run();
    });

  EXPECT_EQ(response_body, "streamed:" + std::to_string(kBody));
}

// =========================================================================
//  TEST: a single streaming write bigger than the staging buffer works
// =========================================================================
//
// The staging capacity is an implementation choice, never a writer limit: the
// oversized chunk is framed around a direct write of the caller's memory.

TEST(http_e2e, streaming_write_bigger_than_the_stream_buffer) {
  const std::string big(256u * 1024u, 'b');
  std::string response_body;

  run_e2e_test(
    [&big](server_t& server) {
      server.get("/big-chunk", [&big](request_t&, response_t& resp) -> coro_t<void> {
        auto w = resp.chunked();
        if (auto ok = co_await w.write(big); !ok) co_return;
        co_await w.finish();
      });
    },
    [&response_body](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);

        std::string request =
            "GET /big-chunk HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n";
        co_await sock.send(ctx, request.data(), request.size());

        char buf[8192];
        std::string raw;
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw.append(buf, *n);
        }
        response_body = extract_body(raw);
      };
      ctx.spawn(client());
      ctx.run();
    });

  EXPECT_EQ(response_body, big);
}

// =========================================================================
//  TEST: a streaming route that returns before anything was sent gets a 500
// =========================================================================
//
// Zero wire bytes means the staged response can still be swapped wholesale —
// the client never sees a mysterious EOF where a response was promised.

TEST(http_e2e, streaming_route_that_returns_early_gets_a_500) {
  std::string raw;

  run_e2e_test(
    [](server_t& server) {
      server.get("/bail", [](request_t&, response_t& resp) -> coro_t<void> {
        auto w = resp.chunked();
        (void)w;  // route forgets to write or finish, then returns
        co_return;
      });
    },
    [&raw](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);

        std::string request =
            "GET /bail HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n";
        co_await sock.send(ctx, request.data(), request.size());

        char buf[4096];
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw.append(buf, *n);
        }
      };
      ctx.spawn(client());
      ctx.run();
    });

  EXPECT_TRUE(response_contains_status(raw, "500")) << "raw: " << raw.substr(0, 200);
}

// =========================================================================
//  TEST: a streaming route that quits mid-body truncates deterministically
// =========================================================================
//
// Headers are already out, so no 500 can be sent — and "0\r\n\r\n" would call
// a half message complete. The honest answer is to close after what was
// actually written, and let the peer see the truncation.

TEST(http_e2e, streaming_route_that_quits_mid_body_truncates) {
  std::string raw;

  run_e2e_test(
    [](server_t& server) {
      server.get("/quit", [](request_t&, response_t& resp) -> coro_t<void> {
        auto w = resp.chunked();
        co_await w.write("part1");
        co_await w.write("part2");
        // never finish(): the route just gives up mid-body
        co_return;
      });
    },
    [&raw](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);

        std::string request =
            "GET /quit HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: keep-alive\r\n"
            "\r\n";
        co_await sock.send(ctx, request.data(), request.size());

        char buf[4096];
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;      // must hit EOF, not hang
          raw.append(buf, *n);
        }
      };
      ctx.spawn(client());
      ctx.run();
    });

  EXPECT_NE(raw.find("part1"), std::string::npos) << "raw: " << raw;
  EXPECT_NE(raw.find("part2"), std::string::npos) << "raw: " << raw;
  EXPECT_EQ(raw.find("\r\n0\r\n\r\n"), std::string::npos)
      << "a half message must never be terminated as complete";
}

// =========================================================================
//  TEST: a direct response bigger than body_buffer_bytes works
// =========================================================================
//
// body() spills into the response arena and travels as an external reference;
// nobody should have to pick chunked encoding just to send a big page.

TEST(http_e2e, big_direct_response) {
  const std::string big(256u * 1024u, 'd');
  std::string response_body;

  run_e2e_test(
    [&big](server_t& server) {
      server.get("/big", [&big](auto&, response_t& resp) { resp.text(big); });
    },
    [&response_body](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);

        std::string request =
            "GET /big HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n";
        co_await sock.send(ctx, request.data(), request.size());

        char buf[8192];
        std::string raw;
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw.append(buf, *n);
        }
        response_body = extract_body(raw);
      };
      ctx.spawn(client());
      ctx.run();
    });

  EXPECT_EQ(response_body, big);
}

// =========================================================================
//  TEST: pin()-referenced bodies survive pipelined responses
// =========================================================================
//
// Regression: the pin arena moves into each response's pending slot when the
// response is framed; without that move, the reset of the shared response_t
// for the *next* pipelined request would free the objects while a pending
// entry still referenced them.

TEST(http_e2e, pinned_bodies_survive_pipelining) {
  std::string raw;

  run_e2e_test(
    [](server_t& server) {
      auto handler = [](request_t& req, response_t& resp) {
        auto& s = resp.pin(std::string("pinned:") + std::string(req.path()));
        resp.body_static(s);
      };
      server.get("/a", handler);
      server.get("/b", handler);
    },
    [&raw](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);

        // two responses sharing one writev: the *reason* the arena must move
        std::string request =
            "GET /a HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "\r\n"
            "GET /b HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n";
        co_await sock.send(ctx, request.data(), request.size());

        char buf[4096];
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw.append(buf, *n);
        }
      };
      ctx.spawn(client());
      ctx.run();
    });

  EXPECT_NE(raw.find("pinned:/a"), std::string::npos) << "raw: " << raw;
  EXPECT_NE(raw.find("pinned:/b"), std::string::npos) << "raw: " << raw;
}

// =========================================================================
//  TEST: response file body (resp.file) — plain splice path
// =========================================================================

TEST(http_e2e, file_response_serves_file_bytes_over_http) {
  const std::string content(96u * 1024u, 'f');
  namespace fs = std::filesystem;
  auto path = fs::temp_directory_path() /
              ("cornet_file_test_" + std::to_string(::getpid()) + ".bin");
  {
    FILE* f = std::fopen(path.c_str(), "wb");
    ASSERT_TRUE(f != nullptr);
    std::fwrite(content.data(), 1, content.size(), f);
    std::fclose(f);
  }
  std::string raw;

  run_e2e_test(
    [&path](server_t& server) {
      server.get("/serve", [&path](auto&, response_t& resp) {
        EXPECT_TRUE(resp.local_file(path.string()));
      });
    },
    [&raw, &content](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);

        std::string request =
            "GET /serve HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n";
        co_await sock.send(ctx, request.data(), request.size());

        char buf[8192];
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw.append(buf, *n);
        }
      };
      ctx.spawn(client());
      ctx.run();
    });

  auto cl = raw.find("Content-Length: " + std::to_string(content.size()) + "\r\n");
  EXPECT_NE(cl, std::string::npos) << "missing Content-Length, head: "
                                   << raw.substr(0, 200);

  auto [head, body] = [&raw] {
    auto p = raw.find("\r\n\r\n");
    return std::pair{raw.substr(0, p), raw.substr(p + 4)};
  }();
  EXPECT_EQ(body, content);
  fs::remove(path);
}

TEST(http_e2e, file_response_missing_path_gets_404) {
  std::string status_line;
  std::string raw;

  run_e2e_test(
    [](server_t& server) {
      server.get("/missing", [](auto&, response_t& resp) {
        if (!resp.local_file("/no/such/path/cornet.bin")) return;
      });
    },
    [&status_line, &raw](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);

        std::string request =
            "GET /missing HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n";
        co_await sock.send(ctx, request.data(), request.size());

        char buf[4096];
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw.append(buf, *n);
        }
        status_line = raw.substr(0, raw.find("\r\n"));
      };
      ctx.spawn(client());
      ctx.run();
    });

  EXPECT_TRUE(response_contains_status(raw, "404")) << status_line;
}

TEST(http_e2e, file_response_head_has_headers_but_no_body) {
  const std::string content(1024, 'h');
  namespace fs = std::filesystem;
  auto path = fs::temp_directory_path() /
              ("cornet_file_test_head_" + std::to_string(::getpid()) + ".bin");
  {
    FILE* f = std::fopen(path.c_str(), "wb");
    ASSERT_TRUE(f != nullptr);
    std::fwrite(content.data(), 1, content.size(), f);
    std::fclose(f);
  }
  std::string raw;

  run_e2e_test(
    [&path](server_t& server) {
      server.head("/hf", [&path](auto&, response_t& resp) {
        EXPECT_TRUE(resp.local_file(path.string()));
      });
    },
    [&raw](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);

        std::string request =
            "HEAD /hf HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n";
        co_await sock.send(ctx, request.data(), request.size());

        char buf[4096];
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw.append(buf, *n);
        }
      };
      ctx.spawn(client());
      ctx.run();
    });

  auto eoh = raw.find("\r\n\r\n");
  EXPECT_NE(eoh, std::string::npos);
  EXPECT_TRUE(raw.substr(0, eoh).find("Content-Length: 1024") != std::string::npos) << raw;
  EXPECT_EQ(raw.size(), eoh + 4) << "a HEAD response must carry no body bytes";
  fs::remove(path);
}

TEST(http_e2e, file_response_followed_by_pipelined_response) {
  const std::string content(8192, 'p');
  namespace fs = std::filesystem;
  auto path = fs::temp_directory_path() /
              ("cornet_file_test_pipe_" + std::to_string(::getpid()) + ".bin");
  {
    FILE* f = std::fopen(path.c_str(), "wb");
    ASSERT_TRUE(f != nullptr);
    std::fwrite(content.data(), 1, content.size(), f);
    std::fclose(f);
  }
  std::string raw;

  run_e2e_test(
    [&path](server_t& server) {
      server.get("/f", [&path](auto&, response_t& resp) {
        EXPECT_TRUE(resp.local_file(path.string()));
      });
      server.get("/t", [](auto&, response_t& resp) { resp.text("tail"); });
    },
    [&raw, &content](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);

        // two requests in one send: wire order must hold across a file body
        std::string request =
            "GET /f HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "\r\n"
            "GET /t HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n";
        co_await sock.send(ctx, request.data(), request.size());

        char buf[8192];
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw.append(buf, *n);
        }
      };
      ctx.spawn(client());
      ctx.run();
    });

  EXPECT_TRUE(raw.find(content) != std::string::npos) << "file body missing";
  EXPECT_TRUE(raw.find("tail") != std::string::npos)
      << "the pipelined response after the file body is missing";
  EXPECT_TRUE(raw.find(content) < raw.find("tail"))
      << "wire order violated across the file body";
  fs::remove(path);
}

TEST(http_e2e, file_serves_file_bytes) {
  const std::string content(64u * 1024u, 'a');
  namespace fs = std::filesystem;
  auto path = fs::temp_directory_path() /
              ("cornet_file_test_async_" + std::to_string(::getpid()) + ".bin");
  {
    FILE* f = std::fopen(path.c_str(), "wb");
    ASSERT_TRUE(f != nullptr);
    std::fwrite(content.data(), 1, content.size(), f);
    std::fclose(f);
  }
  std::string raw;

  run_e2e_test(
    [&path](server_t& server) {
      server.get("/af", [&path](auto&, response_t& resp) -> coro_t<void> {
        auto ok = co_await resp.file(path.string());
        EXPECT_TRUE(ok.has_value() && *ok);
        if (!ok || !*ok) co_return;
      });
    },
    [&raw, &content](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);

        std::string request =
            "GET /af HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n";
        co_await sock.send(ctx, request.data(), request.size());

        char buf[8192];
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw.append(buf, *n);
        }
      };
      ctx.spawn(client());
      ctx.run();
    });

  auto p = raw.find("\r\n\r\n");
  EXPECT_NE(p, std::string::npos);
  EXPECT_EQ(raw.substr(p + 4), content);
  fs::remove(path);
}

TEST(http_e2e, file_missing_path_gets_404) {
  std::string status_line;
  std::string raw;

  run_e2e_test(
    [](server_t& server) {
      server.get("/amf", [](auto&, response_t& resp) -> coro_t<void> {
        auto ok = co_await resp.file("/no/such/path/cornet.bin");
        EXPECT_TRUE(ok.has_value() && !*ok);
        co_return;
      });
    },
    [&status_line, &raw](context_t& ctx, uint16_t port) {
      auto client = [&]() -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);

        std::string request =
            "GET /amf HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Connection: close\r\n"
            "\r\n";
        co_await sock.send(ctx, request.data(), request.size());

        char buf[4096];
        while (true) {
          auto n = co_await sock.recv(ctx, buf, sizeof(buf));
          if (!n || *n == 0) break;
          raw.append(buf, *n);
        }
        status_line = raw.substr(0, raw.find("\r\n"));
      };
      ctx.spawn(client());
      ctx.run();
    });

  EXPECT_TRUE(response_contains_status(raw, "404")) << status_line;
}
