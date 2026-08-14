/**
 * @file http_hello.cc
 * @brief the smallest cornet HTTP server, and the shapes around it.
 *
 * Build: cmake --build --preset release --target http-hello
 * Run:   ./cmake-build-release/http-hello   (then curl http://127.0.0.1:8080/hello)
 */

#include <cornet/http.h>

#include <string>

#include "cornet/concurrency/combinators.h"

using namespace cornet;

int main() {
  context_t ctx;
  http::server_t server(ctx);

  // A handler that does not need to suspend is an ordinary function: no coro_t, no
  // co_return. This is the fast path — no coroutine frame is allocated and the
  // request never goes back through the scheduler.
  server.get("/hello", [](auto&, auto& resp) { resp.text("hello cornet"); });

  // Path parameters are captured into caller-owned slots, so matching allocates
  // nothing. pin() moves a value the handler computed into the response's arena,
  // which is what makes it safe to reference rather than copy: responses are
  // flushed after the handler has already returned.
  server.get("/users/:id", [](http::request_t& req, http::response_t& resp) {
    auto& payload = resp.pin(std::string(R"({"id":")") + std::string(req.param("id")) + R"("})");
    resp.header(http::field_t::ContentType, "application/json");
    resp.body_static(payload);
  });

  // A string literal outlives the connection, so it can be referenced in place.
  server.get("/static", [](auto&, auto& resp) {
    resp.header(http::field_t::ContentType, "text/plain");
    resp.body_static("this response body is never copied\n");
  });

  // Only handlers that actually await something are coroutines.
  server.get("/slow", [&ctx](auto&, http::response_t& resp) -> coro_t<void> {
    auto slept = co_await cornet::sleep(ctx, std::chrono::milliseconds(50));
    (void)slept;
    resp.text("waited 50ms");
  });

  // Query strings are parsed lazily, on request.
  server.get("/echo", [](http::request_t& req, http::response_t& resp) {
    auto value = req.query().get("q");
    resp.text(value.empty() ? "no q" : value);
  });

  if (auto ok = server.listen("0.0.0.0", 8080); !ok) {
    SPDLOG_ERROR("listen failed: {}", ok.error().message());
    return 1;
  }

  // Ctrl-C drains rather than dropping: no new connections, in-flight responses
  // finish, keep-alive connections are told to close.
  ctx.on_signal({SIGINT, SIGTERM}, [&server](int) { server.drain(); });

  SPDLOG_INFO("listening on http://0.0.0.0:8080");
  ctx.spawn(server.serve());
  ctx.run();
  return 0;
}
