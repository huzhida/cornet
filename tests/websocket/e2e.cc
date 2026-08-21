#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <string>
#include <thread>

#include "cornet/http/server/server.h"
#include "cornet/net/socket.h"
#include "cornet/scheduling/context.h"
#include "cornet/websocket.h"

using namespace cornet;
namespace ws = cornet::websocket;

// NOTE: gtest ASSERT_* expands to a bare `return`, which a coroutine cannot
// use — every coroutine below fails with EXPECT_* and then co_returns early.

namespace {

// ─────────────────────── raw wire helpers ───────────────────────

struct raw_frame_t {
  bool        fin{false};
  uint8_t     opcode{0};
  std::string payload{};
};

std::string make_frame(bool fin, uint8_t op, std::string_view payload, bool masked) {
  uint32_t key = 0;
  if (masked) key = 0x37FA213D;
  char hdr[ws::kMaxFrameHeaderLen];
  auto n = ws::write_frame_header(hdr, fin, ws::opcode_t(op), payload.size(), key);
  std::string out(hdr, n);
  std::string body(payload);
  if (masked) ws::apply_mask(key, body.data(), body.size());
  out += body;
  return out;
}

//! accumulate from the socket until one full frame is buffered
coro_t<expected<raw_frame_t>> read_frame(context_t& ctx, tcp::v4::socket_t& sock,
                                         std::string& buf) {
  ws::frame_decoder_t dec(ws::role_t::Client);
  char scratch[8192];
  for (;;) {
    ws::frame_t f;
    auto r = dec.parse(buf, f);
    if (r == ws::frame_decoder_t::result_t::Error) {
      co_return unexpected(dec.error());
    }
    if (r == ws::frame_decoder_t::result_t::Frame &&
        buf.size() >= f.header_len + f.payload_len) {
      raw_frame_t out{f.fin, uint8_t(f.opcode),
                      buf.substr(f.header_len, size_t(f.payload_len))};
      buf.erase(0, f.header_len + size_t(f.payload_len));
      co_return out;
    }
    auto n = co_await sock.recv(ctx, scratch, sizeof(scratch));
    if (!n || *n == 0) co_return unexpected(ECONNRESET);
    buf.append(scratch, size_t(*n));
  }
}

coro_t<expected<std::string>> http_exchange(context_t& ctx, tcp::v4::socket_t& sock,
                                            std::string_view request) {
  auto w = co_await sock.send(ctx, request.data(), request.size());
  if (!w) co_return unexpected(w.error());
  char buf[4096];
  std::string raw;
  for (;;) {
    auto n = co_await sock.recv(ctx, buf, sizeof(buf));
    if (!n || *n == 0) break;
    raw.append(buf, size_t(*n));
    if (raw.find("\r\n\r\n") != std::string::npos) break;
  }
  co_return raw;
}

std::string handshake_request(std::string_view path, std::string_view extra = {}) {
  return "GET " + std::string(path) + " HTTP/1.1\r\n"
         "Host: 127.0.0.1\r\n"
         "Upgrade: websocket\r\n"
         "Connection: Upgrade\r\n"
         "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
         std::string(extra) + "\r\n";
}

constexpr std::string_view kVersionHdr = "Sec-WebSocket-Version: 13\r\n";

/**
 * @brief server thread + raw-socket client, mirroring tests/http/e2e.cc.
 */
void run_ws_test(std::function<void(http::server_t&)> setup,
                 std::function<void(context_t&, uint16_t)> client_fn) {
  context_t server_ctx;
  std::atomic<uint16_t> server_port{0};
  std::atomic<bool> server_ready{false};
  std::atomic<bool> server_failed{false};

  std::thread server_thread([&]() {
    http::server_t server(server_ctx, http::server_options_t{
      .port = 0,
      .address = "127.0.0.1",
    });
    setup(server);
    if (auto ok = server.listen(); !ok) {
      server_failed.store(true, std::memory_order_release);
      return;
    }
    server_port.store(server.options().port, std::memory_order_relaxed);
    server_ctx.spawn(server.serve());
    server_ready.store(true, std::memory_order_release);
    server_ctx.run();
  });

  while (!server_ready.load(std::memory_order_acquire) &&
         !server_failed.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  ASSERT_FALSE(server_failed.load(std::memory_order_acquire));

  {
    context_t client_ctx;
    client_fn(client_ctx, server_port.load(std::memory_order_relaxed));
    client_ctx.run();
  }

  server_ctx.stop();
  server_thread.join();
}

//! connect + handshake, expecting 101; returns the open socket
coro_t<expected<tcp::v4::socket_t>> open_ws(context_t& ctx, uint16_t port,
                                            std::string_view path) {
  tcp::v4::socket_t sock;
  auto c = co_await sock.connect(ctx, "127.0.0.1", port);
  if (!c) co_return unexpected(c.error());
  auto req = handshake_request(path, kVersionHdr);
  auto raw = co_await http_exchange(ctx, sock, req);
  if (!raw) co_return unexpected(raw.error());
  if (raw->find("101 Switching Protocols") == std::string::npos) {
    ADD_FAILURE() << "expected 101, got: " << *raw;
    co_return unexpected(EPROTO);
  }
  co_return std::move(sock);
}

void echo_setup(http::server_t& server) {
  server.websocket("/echo", [](ws::session_t& ws) -> coro_t<void> {
    while (auto msg = co_await ws.recv()) {
      if (msg->opcode == ws::opcode_t::Close) break;
      if (auto ok = co_await ws.send(msg->payload, msg->opcode); !ok) break;
    }
  });
}

} // namespace

// ─────────────────────────── echo: small text ───────────────────────────

TEST(ws_e2e, echo_small_text) {
  run_ws_test(echo_setup, [](context_t& ctx, uint16_t port) {
    ctx.spawn([](context_t& ctx, uint16_t port) -> coro_t<void> {
      auto sock = co_await open_ws(ctx, port, "/echo");
      if (!sock) co_return;

      auto frame = make_frame(true, 0x1, "hello cornet", true /*masked*/);
      co_await sock->send(ctx, frame.data(), frame.size());
      std::string buf;
      auto got = co_await read_frame(ctx, *sock, buf);
      if (!got) { ADD_FAILURE() << "read_frame failed"; co_return; }
      EXPECT_TRUE(got->fin);
      EXPECT_EQ(got->opcode, 0x1);
      EXPECT_EQ(got->payload, "hello cornet");
    }(ctx, port));
  });
}

// ─────────── echo: message larger than the receive window ───────────

TEST(ws_e2e, echo_large_message_runs) {
  run_ws_test(echo_setup, [](context_t& ctx, uint16_t port) {
    ctx.spawn([](context_t& ctx, uint16_t port) -> coro_t<void> {
      auto sock = co_await open_ws(ctx, port, "/echo");
      if (!sock) co_return;

      std::string payload(300u << 10, 'x');  // 300 KiB in one frame
      auto frame = make_frame(true, 0x2, payload, true);
      // trickle it in so the server aggregates from several runs
      for (size_t off = 0; off < frame.size(); off += 4096) {
        size_t n = std::min<size_t>(4096, frame.size() - off);
        auto w = co_await sock->send(ctx, frame.data() + off, n);
        if (!w) { ADD_FAILURE() << "send failed"; co_return; }
      }
      std::string buf;
      auto got = co_await read_frame(ctx, *sock, buf);
      if (!got) { ADD_FAILURE() << "read_frame failed"; co_return; }
      EXPECT_EQ(got->opcode, 0x2);
      EXPECT_EQ(got->payload, payload);
    }(ctx, port));
  });
}

// ─────────────────── echo: fragmented message ───────────────────

TEST(ws_e2e, echo_fragmented) {
  run_ws_test(echo_setup, [](context_t& ctx, uint16_t port) {
    ctx.spawn([](context_t& ctx, uint16_t port) -> coro_t<void> {
      auto sock = co_await open_ws(ctx, port, "/echo");
      if (!sock) co_return;

      // "hello " + "cornet" as two fragments; ping between them must not
      // disturb the reassembly
      auto f1 = make_frame(false, 0x1, "hello ", true);
      auto ping = make_frame(true, 0x9, "mid", true);
      auto f2 = make_frame(true, 0x0, "cornet", true);
      co_await sock->send(ctx, f1.data(), f1.size());
      co_await sock->send(ctx, ping.data(), ping.size());
      co_await sock->send(ctx, f2.data(), f2.size());

      std::string buf;
      // the pong for the interleaved ping arrives first
      auto pong = co_await read_frame(ctx, *sock, buf);
      if (!pong) { ADD_FAILURE() << "pong failed"; co_return; }
      EXPECT_EQ(pong->opcode, 0xA);
      EXPECT_EQ(pong->payload, "mid");

      auto got = co_await read_frame(ctx, *sock, buf);
      if (!got) { ADD_FAILURE() << "read_frame failed"; co_return; }
      EXPECT_EQ(got->opcode, 0x1) << "reassembled message keeps the first frame's opcode";
      EXPECT_EQ(got->payload, "hello cornet");
    }(ctx, port));
  });
}

// ─────────────────── protocol violation: unmasked frame ───────────────────

TEST(ws_e2e, unmasked_frame_gets_protocol_close) {
  run_ws_test(echo_setup, [](context_t& ctx, uint16_t port) {
    ctx.spawn([](context_t& ctx, uint16_t port) -> coro_t<void> {
      auto sock = co_await open_ws(ctx, port, "/echo");
      if (!sock) co_return;

      auto bad = make_frame(true, 0x1, "oops", false /*unmasked: illegal from a client*/);
      co_await sock->send(ctx, bad.data(), bad.size());
      std::string buf;
      auto got = co_await read_frame(ctx, *sock, buf);
      if (!got) { ADD_FAILURE() << "read_frame failed"; co_return; }
      if (got->opcode != 0x8 || got->payload.size() < 2) {
        ADD_FAILURE() << "expected a Close frame with a code";
        co_return;
      }
      uint16_t code = uint16_t(uint8_t(got->payload[0])) << 8 | uint8_t(got->payload[1]);
      EXPECT_EQ(code, 1002);
    }(ctx, port));
  });
}

// ─────────────────── close handshake ───────────────────

TEST(ws_e2e, close_echo) {
  run_ws_test(echo_setup, [](context_t& ctx, uint16_t port) {
    ctx.spawn([](context_t& ctx, uint16_t port) -> coro_t<void> {
      auto sock = co_await open_ws(ctx, port, "/echo");
      if (!sock) co_return;

      char cbody[2] = {'\x03', '\xe8'};  // 1000
      auto closef = make_frame(true, 0x8, std::string_view(cbody, 2), true);
      co_await sock->send(ctx, closef.data(), closef.size());
      std::string buf;
      auto got = co_await read_frame(ctx, *sock, buf);
      if (!got) { ADD_FAILURE() << "read_frame failed"; co_return; }
      EXPECT_EQ(got->opcode, 0x8);
      if (got->payload.size() < 2) { ADD_FAILURE() << "close frame too short"; co_return; }
      EXPECT_EQ(uint16_t(uint8_t(got->payload[0])) << 8 | uint8_t(got->payload[1]),
                1000);
    }(ctx, port));
  });
}

// ─────────────────── handshake rejections ───────────────────

TEST(ws_e2e, missing_version_gets_400) {
  run_ws_test(echo_setup, [](context_t& ctx, uint16_t port) {
    ctx.spawn([](context_t& ctx, uint16_t port) -> coro_t<void> {
      tcp::v4::socket_t sock;
      co_await sock.connect(ctx, "127.0.0.1", port);
      // no Sec-WebSocket-Version header
      auto raw = co_await http_exchange(ctx, sock, handshake_request("/echo"));
      if (!raw) { ADD_FAILURE() << "exchange failed"; co_return; }
      EXPECT_NE(raw->find("400 Bad Request"), std::string::npos) << *raw;
    }(ctx, port));
  });
}

TEST(ws_e2e, plain_get_on_ws_route_gets_426) {
  run_ws_test(echo_setup, [](context_t& ctx, uint16_t port) {
    ctx.spawn([](context_t& ctx, uint16_t port) -> coro_t<void> {
      tcp::v4::socket_t sock;
      co_await sock.connect(ctx, "127.0.0.1", port);
      // an ordinary GET, no upgrade headers: must not reach the ws machinery
      auto raw = co_await http_exchange(
          ctx, sock,
          "GET /echo HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
      if (!raw) { ADD_FAILURE() << "exchange failed"; co_return; }
      EXPECT_NE(raw->find("426"), std::string::npos) << *raw;
    }(ctx, port));
  });
}

TEST(ws_e2e, upgrade_on_plain_route_gets_501) {
  run_ws_test(
    [](http::server_t& server) {
      server.get("/plain", [](auto&, auto& resp) { resp.text("hi"); });
    },
    [](context_t& ctx, uint16_t port) {
      ctx.spawn([](context_t& ctx, uint16_t port) -> coro_t<void> {
        tcp::v4::socket_t sock;
        co_await sock.connect(ctx, "127.0.0.1", port);
        auto raw = co_await http_exchange(ctx, sock, handshake_request("/plain", kVersionHdr));
        if (!raw) { ADD_FAILURE() << "exchange failed"; co_return; }
        EXPECT_NE(raw->find("501 Not Implemented"), std::string::npos) << *raw;
      }(ctx, port));
    });
}

// ─────────────────── accept guard ───────────────────

TEST(ws_e2e, accept_guard_refuses_and_picks_subprotocol) {
  run_ws_test(
    [](http::server_t& server) {
      server
        .websocket("/guarded",
                   [](ws::session_t& ws) -> coro_t<void> {
                     EXPECT_EQ(ws.subprotocol(), "chat");
                     co_return;
                   })
        .accept([](http::request_t& req, ws::accept_info_t& info) {
          if (req.headers().get("x-token") != "s3cret") {
            info.refuse_with = http::status_t::Unauthorized;
            return false;
          }
          info.subprotocol = "chat";
          return true;
        });
    },
    [](context_t& ctx, uint16_t port) {
      ctx.spawn([](context_t& ctx, uint16_t port) -> coro_t<void> {
        // refused without the token
        {
          tcp::v4::socket_t sock;
          co_await sock.connect(ctx, "127.0.0.1", port);
          auto raw = co_await http_exchange(ctx, sock,
                                            handshake_request("/guarded", kVersionHdr));
          if (!raw) { ADD_FAILURE() << "exchange failed"; co_return; }
          EXPECT_NE(raw->find("401 Unauthorized"), std::string::npos) << *raw;
        }
        // accepted with it; the chosen subprotocol is echoed
        {
          tcp::v4::socket_t sock;
          co_await sock.connect(ctx, "127.0.0.1", port);
          std::string extra(kVersionHdr);
          extra += "X-Token: s3cret\r\nSec-WebSocket-Protocol: chat, super\r\n";
          auto raw = co_await http_exchange(ctx, sock, handshake_request("/guarded", extra));
          if (!raw) { ADD_FAILURE() << "exchange failed"; co_return; }
          EXPECT_NE(raw->find("101 Switching Protocols"), std::string::npos) << *raw;
          EXPECT_NE(raw->find("Sec-WebSocket-Protocol: chat"), std::string::npos) << *raw;
        }
      }(ctx, port));
    });
}

// ─────────────────── client round trip ───────────────────

TEST(ws_e2e, client_connect_echo_close) {
  run_ws_test(echo_setup, [](context_t& ctx, uint16_t port) {
    ctx.spawn([](context_t& ctx, uint16_t port) -> coro_t<void> {
      std::string url = "ws://127.0.0.1:" + std::to_string(port) + "/echo";
      auto conn = co_await ws::connect(ctx, url);
      if (!conn) { ADD_FAILURE() << "connect: " << conn.error().message(); co_return; }
      auto& ws = **conn;

      auto sent = co_await ws.send_text("client says hi");
      EXPECT_TRUE(sent);
      auto msg = co_await ws.recv();
      if (!msg) { ADD_FAILURE() << "recv failed"; co_return; }
      EXPECT_TRUE(msg->text());
      EXPECT_EQ(msg->payload, "client says hi");

      auto c = co_await ws.close();
      EXPECT_TRUE(c);
      // the server's echo route breaks on its delivered Close, and both
      // sides wind down; our close was Normal, so the echo is Normal too
      auto done = co_await ws.recv();
      if (!done) { ADD_FAILURE() << "no close message"; co_return; }
      EXPECT_EQ(done->opcode, ws::opcode_t::Close);
      EXPECT_EQ(done->close_code(), ws::close_code_t::Normal);
      co_await ws.finish();
    }(ctx, port));
  });
}
