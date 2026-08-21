#ifndef CORNET_BENCH_HTTP_H
#define CORNET_BENCH_HTTP_H

/**
 * @file http_bench.h
 * @brief the same echo workload spoken over HTTP/1.1, to price the protocol
 * against the raw send/recv row.
 *
 * Two rows, because "HTTP costs more" is not the useful answer — where it goes is:
 *
 *   Cornet/HTTPsrv  http::server_t answering a pre-serialized request from a raw
 *                   socket client that reads back a known number of bytes. The
 *                   client side is deliberately as dumb as the raw bench's, so the
 *                   gap against `Cornet` is what the server pipeline costs:
 *                   parse → route → dispatch → frame → writev.
 *   Cornet/HTTP     the same server plus http::client_t. The gap against
 *                   Cornet/HTTPsrv is what the client stack costs: url parse, pool
 *                   acquire, request framing, response parse.
 *
 * Both echo the same payload and record the same per-exchange latency as the raw
 * row, so the RPS and latency columns line up. Throughput counts payload bytes
 * only; the header bytes every exchange adds are reported separately by
 * print_http_overhead_summary(), because at 64B payloads they are most of the wire.
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "common.h"

#include "cornet/http_client.h"
#include "cornet/http_server.h"
#include "cornet/net/socket.h"
#include "cornet/scheduling/context.h"

#ifdef CORNET_BENCH_TLS
#include "cornet/tls/transport.h"
#include "certs.h"
#endif

namespace bench {

namespace http_echo {

constexpr uint16_t    kPort = 9877;
constexpr const char* kPath = "/echo";
constexpr const char* kUrl  = "http://127.0.0.1:9877/echo";

#ifdef CORNET_BENCH_TLS
constexpr const char* kHttpsUrl = "https://127.0.0.1:9877/echo";

/**
 * @brief TLS material for the bench rows.
 *
 * The server's cert comes from the test PKI (regenerate there if it ever
 * expires; a bench never faces the public internet). The client's context
 * disables verification deliberately: it changes nothing in steady state
 * (verification happens once per connection, and the bench reuses each), and
 * it keeps the measurement from depending on a CA bundle lying around.
 */
inline std::shared_ptr<cornet::tls::tls_context_t> server_tls_ctx() {
  auto cx = cornet::tls::tls_context_t::make_server(cornet::tls::tls_server_options_t{
      .cert_pem = std::string(cornet::test::tls::kServerCert),
      .key_pem = std::string(cornet::test::tls::kServerKey),
  });
  if (!cx) return nullptr;
  return *cx;
}

#endif

/**
 * @brief header bytes each exchange adds on top of the payload, per scenario.
 * Filled by the raw-client row (which has to know the exact response size anyway)
 * and printed by print_http_overhead_summary().
 */
struct wire_overhead_t {
  size_t payload{0};
  size_t request_head{0};
  size_t response_head{0};
};

inline std::map<std::string, wire_overhead_t>& wire_overheads() {
  static std::map<std::string, wire_overhead_t> m;
  return m;
}

/**
 * @brief server options sized so that a buffer limit is never what the numbers
 * measure.
 */
inline cornet::http::server_options_t server_options(const scenario_t& s) {
  cornet::http::server_options_t opt;
  opt.address = "127.0.0.1";
  opt.port = kPort;
  // The echoed body is copied into the response's body buffer, so that buffer has
  // to hold a whole message. Handlers that own their bytes can use body_owned()
  // instead; an echo cannot, see the handler below.
  opt.body_buffer_bytes = std::max<uint32_t>(16u << 10, uint32_t(s.message_size) + (4u << 10));
  opt.max_connections = uint32_t(s.connections) + 16;
  // A bench run never idles, and a wheel expiry mid-run would show up as a failure
  // rather than as a slow request.
  opt.idle_timeout = std::chrono::seconds(30);
  opt.header_timeout = std::chrono::seconds(30);
  opt.body_timeout = std::chrono::seconds(30);
  return opt;
}

/**
 * @brief the echo server on its own thread, with the handshake to start and stop it.
 *
 * Mirrors the raw bench's layout — server on a dedicated thread, client on the main
 * one — so the two rows are measured under the same thread arrangement.
 */
class server_thread_t {
 public:
  server_thread_t(cornet::config_t& config, const scenario_t& scenario, bool use_tls = false)
    : ctx_(&config) {
    auto opt = server_options(scenario);
    thread_ = std::thread([this, opt, use_tls]() mutable {
#ifdef CORNET_BENCH_TLS
      if (use_tls) {
        // built on the server thread: a tls_context_t is per-worker shared-nothing
        auto cx = server_tls_ctx();
        CORNET_ASSERT(cx != nullptr, "bench tls context must build");
        opt.tls = cx;
      }
#else
      (void)use_tls;
#endif
      cornet::http::server_t server(ctx_, opt);

      // A synchronous handler: no coroutine frame, no trip through the scheduler.
      // body() copies, and has to: the request's body buffer is released before the
      // round is flushed, so body_static(req.body()) would hand the kernel a block
      // that is already back in the pool.
      server.post(kPath, [](cornet::http::request_t& req, cornet::http::response_t& resp) {
        resp.body_static(req.body());
      });

      if (auto ok = server.listen(); !ok) {
        fprintf(stderr, "http bench: listen on %u failed: %s\n",
                unsigned(kPort), ok.error().message());
        failed_.store(true, std::memory_order_release);
        stopped_.store(true, std::memory_order_release);
        ready_.store(true, std::memory_order_release);
        return;
      }

      server_ = &server;
      ready_.store(true, std::memory_order_release);
      ctx_.spawn(server.serve());
      ctx_.run();
      stopped_.store(true, std::memory_order_release);
    });

    while (!ready_.load(std::memory_order_acquire)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  ~server_thread_t() { shutdown(); }

  server_thread_t(const server_thread_t&) = delete;
  server_thread_t& operator=(const server_thread_t&) = delete;

  CORNET_NODISCARD bool ok() const { return !failed_.load(std::memory_order_acquire); }

  void shutdown() {
    if (joined_) return;
    joined_ = true;

    // drain() reads and writes the server's own state and its connection list, so it
    // has to run on the server's thread; spawn_remote is the way in.
    if (auto* srv = server_.load(std::memory_order_acquire)) {
      ctx_.spawn_remote([srv]() -> cornet::coro_t<void> {
        srv->drain();
        co_return;
      });
    }

    // After a drain the accept completes with an error, the connections finish, and
    // the context winds itself down — nobody has to call shutdown() on it. Give that
    // a bounded amount of time and stop the context outright if it does not happen,
    // so a wedged connection costs a warning rather than a hung bench.
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (!stopped_.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    if (!stopped_.load(std::memory_order_acquire)) {
      fprintf(stderr, "http bench: server did not drain in 3s, stopping it\n");
      ctx_.stop();
    }
    if (thread_.joinable()) thread_.join();
  }

 private:
  cornet::context_t ctx_;
  std::thread       thread_;
  std::atomic<cornet::http::server_t*> server_{nullptr};
  std::atomic<bool> ready_{false};
  std::atomic<bool> stopped_{false};
  std::atomic<bool> failed_{false};
  bool              joined_{false};
};

/**
 * @brief one request, serialized once and re-sent verbatim every round.
 */
inline std::string build_request(std::string_view body) {
  char head[256];
  int n = snprintf(head, sizeof(head),
                   "POST %s HTTP/1.1\r\n"
                   "Host: 127.0.0.1:%u\r\n"
                   "Content-Type: application/octet-stream\r\n"
                   "Content-Length: %zu\r\n"
                   "\r\n",
                   kPath, unsigned(kPort), body.size());
  std::string out(head, size_t(n < 0 ? 0 : n));
  out.append(body);
  return out;
}

/**
 * @brief learn the exact byte length of the echo response, once, before the clock
 * starts.
 *
 * The raw-client row exists to isolate the server's cost, so its client must not
 * parse anything — it reads a fixed number of bytes and stops. That is legitimate
 * here because the response is byte-identical every round: the only field that
 * changes is Date's value, and IMF-fixdate is always 29 characters.
 *
 * @return total response length in bytes, or 0 if the probe failed
 */
inline size_t probe_response_len(cornet::config_t& config, const std::string& request) {
  using namespace cornet;

  context_t ctx(&config);
  size_t    total = 0;

  auto probe = [&](context_t& c) -> coro_t<void> {
    tcp::v4::socket_t sock;
    if (auto conn = co_await sock.connect(c, "127.0.0.1", kPort); !conn) {
      fprintf(stderr, "http bench: probe connect failed: %s\n", conn.error().message());
      co_return;
    }

    size_t sent = 0;
    while (sent < request.size()) {
      auto n = co_await sock.send(c, request.data() + sent, request.size() - sent);
      if (!n || *n <= 0) co_return;
      sent += size_t(*n);
    }

    std::string buf;
    char        tmp[8192];
    size_t      need = 0;
    constexpr std::string_view kCl = "Content-Length: ";
    while (true) {
      auto n = co_await sock.recv(c, tmp, sizeof(tmp));
      if (!n || *n <= 0) break;
      buf.append(tmp, size_t(*n));
      if (need == 0) {
        auto end = buf.find("\r\n\r\n");
        if (end != std::string::npos) {
          auto cl = buf.find(kCl);
          if (cl == std::string::npos || cl > end) break;   // no length: cannot fix a size
          need = end + 4 + size_t(strtoull(buf.c_str() + cl + kCl.size(), nullptr, 10));
        }
      }
      if (need != 0 && buf.size() >= need) break;
    }
    total = need;
  };

  ctx.spawn(probe(ctx));
  ctx.run();
  return total;
}


} // namespace http_echo

/**
 * @brief an empty row, for when the server could not be brought up. Keeps the table
 * shape intact instead of aborting the whole bench.
 */
inline result_t http_skipped_row(const char* framework, const scenario_t& scenario,
                                 rss_profiler_t& profiler) {
  profiler.stop();
  latency_collector_t empty;
  result_t r = empty.compute(framework, scenario.name, 0, get_vmmhwm_kb());
  empty.fill_profile(r, profiler);
  return r;
}

/**
 * @brief http::server_t against a raw socket client: the server's share of the cost.
 */
inline result_t run_cornet_http_server(const scenario_t& scenario, cornet::config_t& config) {
  using namespace cornet;
  using namespace bench::http_echo;

  rss_profiler_t profiler;
  profiler.start();

  server_thread_t server(config, scenario);

  latency_collector_t collector;
  collector.reserve(scenario.total_messages);
  auto remaining = std::make_shared<std::atomic<int>>(scenario.total_messages);

  std::string payload(size_t(scenario.message_size), 'A');
  std::string request = build_request(payload);

  size_t resp_len = server.ok() ? probe_response_len(config, request) : 0;
  if (resp_len == 0) {
    fprintf(stderr, "http bench: could not size the echo response, skipping row\n");
    server.shutdown();
    return http_skipped_row("Cornet/HTTPsrv", scenario, profiler);
  }
  wire_overheads()[scenario.name] =
    wire_overhead_t{payload.size(), request.size() - payload.size(), resp_len - payload.size()};

  context_t client_ctx(&config);

  auto client_session = [&](context_t& ctx) -> coro_t<void> {
    tcp::v4::socket_t sock;
    if (auto conn = co_await sock.connect(ctx, "127.0.0.1", kPort); !conn) {
      collector.record_failure();
      co_return;
    }

    std::vector<char> recv_buf(resp_len);
    while (true) {
      if ((*remaining)-- <= 0) break;

      auto t0 = std::chrono::steady_clock::now();

      size_t sent = 0;
      while (sent < request.size()) {
        auto n = co_await sock.send(ctx, request.data() + sent, request.size() - sent);
        if (!n || *n <= 0) {
          collector.record_failure();
          co_return;
        }
        sent += size_t(*n);
      }

      size_t got = 0;
      while (got < resp_len) {
        auto n = co_await sock.recv(ctx, recv_buf.data() + got, resp_len - got);
        if (!n || *n <= 0) {
          collector.record_failure();
          co_return;
        }
        got += size_t(*n);
      }

      auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - t0).count();
      collector.record(uint64_t(latency), int(payload.size() * 2));
    }
  };

  size_t hwm_before = get_vmmhwm_kb();
  auto   start = std::chrono::steady_clock::now();
  for (int i = 0; i < scenario.connections; ++i) {
    client_ctx.spawn(client_session(client_ctx));
  }
  client_ctx.run();
  auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

  server.shutdown();
  profiler.stop();

  result_t r = collector.compute("Cornet/HTTPsrv", scenario.name, elapsed, hwm_before);
  collector.fill_profile(r, profiler);
  return r;
}

#ifdef CORNET_BENCH_TLS
/**
 * @brief https::server_t plus http::client_t over TLS: the full https stack.
 */
inline result_t run_cornet_https(const scenario_t& scenario, cornet::config_t& config) {
  using namespace cornet;
  using namespace bench::http_echo;

  rss_profiler_t profiler;
  profiler.start();

  server_thread_t server(config, scenario, true);
  if (!server.ok()) {
    fprintf(stderr, "https bench: server not up, skipping row\n");
    server.shutdown();
    return http_skipped_row("Cornet/HTTPS", scenario, profiler);
  }

  latency_collector_t collector;
  collector.reserve(scenario.total_messages);
  auto remaining = std::make_shared<std::atomic<int>>(scenario.total_messages);

  std::string payload(size_t(scenario.message_size), 'A');

  context_t client_ctx(&config);

  http::client_options_t copt;
  auto conns = uint16_t(std::min(scenario.connections, 65000));
  copt.max_conns_per_host = conns;
  copt.max_idle_per_host = conns;
  copt.max_total_conns = uint32_t(scenario.connections) + 8;
  copt.max_retries = 0;
  copt.total_timeout = std::chrono::seconds(30);
  copt.response_timeout = std::chrono::seconds(30);
  copt.body_timeout = std::chrono::seconds(30);
  // verification adds nothing measurable in steady state (handshakes are pooled)
  // and must not depend on a CA bundle on the box
  copt.tls_verify = false;
  http::client_t cli(client_ctx, copt);

  // Prime every connection before the clock: a TLS handshake is connection
  // setup, not exchange cost, and at 2048 connections lazily built pools start
  // every row with a seconds-long ECDSA blast that queues the first exchanges.
  // Plain rows pay this too (TCP connect), it is just a hundred times cheaper.
  {
    std::atomic<int> primed{0};
    for (int i = 0; i < scenario.connections; ++i) {
      client_ctx.spawn([&]() -> coro_t<void> {
        auto resp = co_await cli.get(kHttpsUrl);
        if (resp) ++primed;
      }());
    }
    client_ctx.run();
    if (primed.load() < scenario.connections) {
      fprintf(stderr, "https bench: primer reached %d/%d connections\n",
              primed.load(), scenario.connections);
    }
  }

  auto client_session = [&](context_t&) -> coro_t<void> {
    while (true) {
      if ((*remaining)-- <= 0) break;

      auto t0 = std::chrono::steady_clock::now();
      auto req = cli.request(http::method_t::Post, kHttpsUrl);
      req.header(http::field_t::ContentType, "application/octet-stream").body_static(payload);
      auto resp = co_await req.send();
      if (!resp || resp->body().size() != payload.size()) {
        collector.record_failure();
        continue;
      }

      auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - t0).count();
      collector.record(uint64_t(latency), int(payload.size() * 2));
    }
  };

  size_t hwm_before = get_vmmhwm_kb();
  auto   start = std::chrono::steady_clock::now();
  for (int i = 0; i < scenario.connections; ++i) {
    client_ctx.spawn(client_session(client_ctx));
  }
  client_ctx.run();
  auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

  cli.close();
  server.shutdown();
  profiler.stop();

  result_t r = collector.compute("Cornet/HTTPS", scenario.name, elapsed, hwm_before);
  collector.fill_profile(r, profiler);
  return r;
}
#endif

/**
 * @brief http::server_t plus http::client_t: what a user of the module pays end to end.
 */
inline result_t run_cornet_http(const scenario_t& scenario, cornet::config_t& config) {
  using namespace cornet;
  using namespace bench::http_echo;

  rss_profiler_t profiler;
  profiler.start();

  server_thread_t server(config, scenario);
  if (!server.ok()) {
    fprintf(stderr, "http bench: server not up, skipping row\n");
    server.shutdown();
    return http_skipped_row("Cornet/HTTP", scenario, profiler);
  }

  latency_collector_t collector;
  collector.reserve(scenario.total_messages);
  auto remaining = std::make_shared<std::atomic<int>>(scenario.total_messages);

  std::string payload(size_t(scenario.message_size), 'A');

  context_t client_ctx(&config);

  http::client_options_t copt;
  // One connection per session, so the pool never becomes the bottleneck being
  // measured — the raw row gets one socket per session too.
  auto conns = uint16_t(std::min(scenario.connections, 65000));
  copt.max_conns_per_host = conns;
  copt.max_idle_per_host = conns;
  copt.max_total_conns = uint32_t(scenario.connections) + 8;
  // A bench should report a failure, not paper over it with a replay.
  copt.max_retries = 0;
  copt.total_timeout = std::chrono::seconds(30);
  copt.response_timeout = std::chrono::seconds(30);
  copt.body_timeout = std::chrono::seconds(30);
  http::client_t cli(client_ctx, copt);

  auto client_session = [&](context_t&) -> coro_t<void> {
    while (true) {
      if ((*remaining)-- <= 0) break;

      auto t0 = std::chrono::steady_clock::now();
      auto req = cli.request(http::method_t::Post, kUrl);
      req.header(http::field_t::ContentType, "application/octet-stream").body_static(payload);
      auto resp = co_await req.send();
      if (!resp || resp->body().size() != payload.size()) {
        collector.record_failure();
        continue;
      }

      auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - t0).count();
      collector.record(uint64_t(latency), int(payload.size() * 2));
    }
  };

  size_t hwm_before = get_vmmhwm_kb();
  auto   start = std::chrono::steady_clock::now();
  for (int i = 0; i < scenario.connections; ++i) {
    client_ctx.spawn(client_session(client_ctx));
  }
  client_ctx.run();
  auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

  // Close the pool first: the server's connections end on their own once the peer
  // is gone, which keeps the drain below short.
  cli.close();
  server.shutdown();
  profiler.stop();

  result_t r = collector.compute("Cornet/HTTP", scenario.name, elapsed, hwm_before);
  collector.fill_profile(r, profiler);
  return r;
}

/**
 * @brief what HTTP costs against raw send/recv, per scenario.
 *
 * Reads the rows the three cornet entries produced and prints the retention rate,
 * so the answer to "how much does the protocol cost here" does not have to be
 * eyeballed off the big table.
 */
inline void print_http_overhead_summary(const std::vector<result_t>& all_results) {
  auto find = [&](const std::string& scenario, const char* framework) -> const result_t* {
    for (auto& r : all_results) {
      if (r.scenario == scenario && r.framework == framework) return &r;
    }
    return nullptr;
  };

  std::vector<std::string> scenarios;
  for (auto& r : all_results) {
    if (std::find(scenarios.begin(), scenarios.end(), r.scenario) == scenarios.end()) {
      scenarios.push_back(r.scenario);
    }
  }

  printf("\n");
  printf("╔══════════════════════════════════════════════════════════════╗\n");
  printf("║          HTTP/1.1 相对原始 send/recv 的开销                  ║\n");
  printf("╚══════════════════════════════════════════════════════════════╝\n");
  printf("\n  Cornet/HTTPsrv = http::server_t + 裸 socket 客户端（只含服务端开销）\n");
  printf("  Cornet/HTTP    = http::server_t + http::client_t（完整栈）\n");
  printf("  RPS 保留率 = HTTP 行 RPS / 原始 send/recv 行 RPS\n");
  printf("  注意: 上面的综合排名把这两行和 Asio 排在了一起，那是不同协议的负载，\n");
  printf("        HTTP 与裸 send/recv 的比较只看这张表。\n\n");

  printf("  %-22s %10s %10s %8s %10s %8s\n",
         "场景", "raw RPS", "srv RPS", "保留率", "full RPS", "保留率");
  printf("  %-22s %10s %10s %8s %10s %8s\n",
         "──────────", "────────", "────────", "──────", "────────", "──────");

  for (auto& scenario : scenarios) {
    const auto* raw = find(scenario, "Cornet");
    const auto* srv = find(scenario, "Cornet/HTTPsrv");
    const auto* full = find(scenario, "Cornet/HTTP");
    if (!raw || raw->rps <= 0) continue;

    printf("  %-22s %10.0f", scenario.substr(0, 22).c_str(), raw->rps);
    if (srv) printf(" %10.0f %7.1f%%", srv->rps, srv->rps / raw->rps * 100.0);
    else     printf(" %10s %8s", "-", "-");
    if (full) printf(" %10.0f %7.1f%%", full->rps, full->rps / raw->rps * 100.0);
    else      printf(" %10s %8s", "-", "-");
    printf("\n");
  }

  printf("\n  %-22s %10s %10s %10s\n", "场景", "raw P99(us)", "srv P99", "full P99");
  printf("  %-22s %10s %10s %10s\n", "──────────", "────────", "────────", "────────");
  for (auto& scenario : scenarios) {
    const auto* raw = find(scenario, "Cornet");
    const auto* srv = find(scenario, "Cornet/HTTPsrv");
    const auto* full = find(scenario, "Cornet/HTTP");
    if (!raw) continue;
    printf("  %-22s %10.0f", scenario.substr(0, 22).c_str(), raw->p99_latency_us);
    if (srv) printf(" %10.0f", srv->p99_latency_us); else printf(" %10s", "-");
    if (full) printf(" %10.0f", full->p99_latency_us); else printf(" %10s", "-");
    printf("\n");
  }

  // Header bytes are the part of the cost that has nothing to do with the
  // implementation: at a 64B payload they are several times the payload itself, and
  // no amount of framework tuning removes them.
  if (!http_echo::wire_overheads().empty()) {
    printf("\n  [每次交换的协议字节] (payload 之外的头部字节，吞吐列未计入)\n");
    printf("  %-22s %10s %10s %10s %10s\n",
           "场景", "payload", "请求头", "响应头", "头/负载");
    printf("  %-22s %10s %10s %10s %10s\n",
           "──────────", "────────", "────────", "────────", "────────");
    for (auto& scenario : scenarios) {
      auto it = http_echo::wire_overheads().find(scenario);
      if (it == http_echo::wire_overheads().end()) continue;
      const auto& w = it->second;
      size_t heads = w.request_head + w.response_head;
      double ratio = w.payload > 0 ? double(heads) / double(w.payload) : 0.0;
      printf("  %-22s %9zuB %9zuB %9zuB %9.2fx\n",
             scenario.substr(0, 22).c_str(), w.payload, w.request_head, w.response_head, ratio);
    }
  }
}

#ifdef CORNET_BENCH_TLS
/**
 * @brief what TLS costs against plain HTTP, per scenario.
 *
 * Pairs the rows that share a client (server-isolated vs full-stack), so the
 * delta is transport-only: record-layer crypto + the copies TLS cannot avoid.
 */
inline void print_tls_overhead_summary(const std::vector<result_t>& all_results) {
  auto find = [&](const std::string& scenario, const char* framework) -> const result_t* {
    for (auto& r : all_results) {
      if (r.scenario == scenario && r.framework == framework) return &r;
    }
    return nullptr;
  };

  std::vector<std::string> scenarios;
  for (auto& r : all_results) {
    if (std::find(scenarios.begin(), scenarios.end(), r.scenario) == scenarios.end()) {
      scenarios.push_back(r.scenario);
    }
  }

  printf("\n");
  printf("╔══════════════════════════════════════════════════════════════╗\n");
  printf("║            TLS 相对明文 HTTP 的开销                          ║\n");
  printf("╚══════════════════════════════════════════════════════════════╝\n");
  printf("\n  保留率 = HTTPS 行 RPS / 对应 HTTP 行 RPS；delta 是纯传输层差异\n");
  printf("  （record layer 加解密 + 一次拷贝；writev 合批在 TLS 下按 record 走）\n\n");

  printf("  %-22s %10s %10s %8s\n", "场景", "HTTP RPS", "HTTPS RPS", "保留率");
  printf("  %-22s %10s %10s %8s\n", "──────────", "────────", "────────", "──────");

  for (auto& scenario : scenarios) {
    const auto* full = find(scenario, "Cornet/HTTP");
    const auto* fulls = find(scenario, "Cornet/HTTPS");
    printf("  %-22s", scenario.substr(0, 22).c_str());
    if (full && fulls && full->rps > 0) {
      printf(" %10.0f %10.0f %7.1f%%", full->rps, fulls->rps, fulls->rps / full->rps * 100.0);
    } else {
      printf(" %10s %10s %8s", "-", "-", "-");
    }
    printf("\n");
  }

  printf("\n  %-22s %10s %10s\n", "场景", "HTTP P99", "HTTPS P99");
  printf("  %-22s %10s %10s\n", "──────────", "────────", "────────");
  for (auto& scenario : scenarios) {
    const auto* full = find(scenario, "Cornet/HTTP");
    const auto* fulls = find(scenario, "Cornet/HTTPS");
    printf("  %-22s", scenario.substr(0, 22).c_str());
    if (full) printf(" %10.0f", full->p99_latency_us); else printf(" %10s", "-");
    if (fulls) printf(" %10.0f", fulls->p99_latency_us); else printf(" %10s", "-");
    printf("\n");
  }

  printf("\n  注: 吞吐列按 payload 计（与明文行同口径）；TLS record 头部开销按记录计\n");
  printf("      (40MB/body 行里 64KB 每 record 约 +0.05%%)，可忽略。\n");
}
#endif

} // namespace bench

#endif // CORNET_BENCH_HTTP_H
