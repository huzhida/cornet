/**
 * @file http_client.cc
 * @brief the shapes of the cornet HTTP client.
 *
 * Build: cmake --build --preset release --target http-client
 * Run:   ./cmake-build-release/http-hello &          # something to talk to
 *        ./cmake-build-release/http-client http://127.0.0.1:8080/hello
 */

#include <cornet/http_client.h>

#include <string>

#include "cornet/concurrency/combinators.h"

using namespace cornet;

int main(int argc, char** argv) {
  std::string url = argc > 1 ? argv[1] : "http://127.0.0.1:8080/hello";

  context_t ctx;
  http::client_t cli(ctx);

  ctx.spawn([&]() -> coro_t<void> {
    // ── one line for the common case ──
    auto resp = co_await cli.get(url);
    if (!resp) {
      SPDLOG_ERROR("GET {} failed: {}", url, resp.error().message());
    } else {
      // The response owns its buffers, so body() stays valid for as long as `resp`
      // does — even though the connection went back to the pool already.
      SPDLOG_INFO("GET {} -> {} ({} bytes)", url, resp->status_code(), resp->body().size());
      SPDLOG_INFO("  server: {}", resp->header(http::field_t::Server));
      SPDLOG_INFO("  body:   {}", resp->body());
    }

    // A non-2xx is an answer, not an error: it arrived and it parsed. Only the failures
    // that prevented an answer come back through expected<>.
    auto missing = co_await cli.get(url + "/definitely-not-there");
    if (missing) {
      SPDLOG_INFO("missing path answered {} ({})", missing->status_code(),
                  missing->ok() ? "ok" : "not ok");
    }

    // ── the builder, for everything else ──
    auto req = cli.request(http::method_t::Post, url);
    req.header(http::field_t::ContentType, "application/json")
        .body(R"({"hello":"cornet"})")   // copied, so it is safe to retry
        .timeout(std::chrono::seconds(3))
        .retry(1);
    auto posted = co_await req.send();
    SPDLOG_INFO("POST -> {}", posted ? std::to_string(posted->status_code())
                                     : posted.error().message());

    // ── streaming, when the body should not be buffered whole ──
    auto stream = co_await cli.stream(http::method_t::Get, url);
    if (stream) {
      size_t total = 0;
      for (;;) {
        auto run = co_await stream->read();
        if (!run || run->empty()) break;   // an empty run means the body ended
        total += run->size();
      }
      SPDLOG_INFO("streamed {} -> {} bytes", uint16_t(stream->status()), total);
    }

    // The second request reused the first request's connection, which is what these
    // counters are for.
    const auto& m = cli.metrics();
    SPDLOG_INFO("connections: {} opened, {} reused, {} dns lookups ({} cached)",
                m.conn_created, m.conn_reused, m.dns_lookups, m.dns_cache_hits);

    cli.close();
  }());

  ctx.run();
  return 0;
}
