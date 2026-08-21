/**
 * @file websocket_echo.cc
 * @brief the smallest cornet WebSocket server.
 *
 * Build: cmake --build --preset release --target websocket-echo
 * Run:   ./cmake-build-release/websocket-echo
 *        then, with any RFC 6455 client:  websocat ws://127.0.0.1:8080/echo
 *        (a browser works too: new WebSocket("ws://127.0.0.1:8080/echo"))
 */

#include <cornet/http.h>

using namespace cornet;

int main() {
  context_t ctx;
  http::server_t server(ctx);

  // A websocket route is an http route that answers its upgrade request with
  // a 101 and then speaks frames. The handler drives the session: each recv()
  // yields one complete message (fragmentation is reassembled, Ping is
  // answered in-band already), and a Close message is delivered once before
  // both sides wind down.
  server.websocket("/echo", [](websocket::session_t& ws) -> coro_t<void> {
    while (auto msg = co_await ws.recv()) {
      if (msg->opcode == websocket::opcode_t::Close) {
        break;
      }
      if (auto ok = co_await ws.send(msg->payload, msg->opcode); !ok) {
        break;
      }
    }
    // Returning settles the connection: a Close is sent if one has not been
    // exchanged, the peer's answer is waited out, and only then the transport
    // closes — so nothing the client sent is lost to an early RST.
  });

  // HTTP and WebSocket routes share a port: a plain GET on /echo answers 501
  // (no upgrade attempted), and /hello keeps working as usual.
  server.get("/hello", [](auto&, auto& resp) { resp.text("hello cornet"); });

  if (auto ok = server.listen("0.0.0.0", 8080); !ok) {
    SPDLOG_ERROR("listen failed: {}", ok.error().message());
    return 1;
  }

  // Ctrl-C drains: active sessions are interrupted with ECANCELED, their
  // handlers wind up, and each client receives a 1001 GoingAway.
  ctx.on_signal({SIGINT, SIGTERM}, [&server](int) { server.drain(); });

  SPDLOG_INFO("listening on ws://0.0.0.0:8080/echo");
  ctx.spawn(server.serve());
  ctx.run();
  return 0;
}
