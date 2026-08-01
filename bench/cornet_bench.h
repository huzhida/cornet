#ifndef CORNET_BENCH_CORNET_H
#define CORNET_BENCH_CORNET_H

#include <iostream>

#include "common.h"
#include "cornet/net/socket.h"
#include "cornet/scheduling/context.h"
#include "cornet/scheduling/scheduler.h"

namespace bench {

inline result_t run_cornet(const scenario_t& scenario, cornet::scheduler_type_t sched_type, cornet::config_t& config) {
  using namespace cornet;

  // Server context: created on a dedicated thread
  context_t server_ctx(&config);
  server_ctx.scheduler().set_policy(sched_type);

  std::atomic<bool> server_running{true};
  std::atomic<bool> server_ready{false};
  latency_collector_t collector;
  collector.reserve(scenario.total_messages);

  auto remaining = std::make_shared<std::atomic<int>>(scenario.total_messages);

  auto server_session = [&](context_t& ctx, int sock_fd) -> coro_t<void> {
    tcp::v4::socket_t sock(sock_fd);
    std::vector<char> buf(scenario.message_size + 64);
    while (true) {
      auto n = co_await sock.recv(ctx, buf.data(), buf.size());
      if (!n || *n <= 0) break;
      auto s = co_await sock.send(ctx, buf.data(), *n);
      if (!s || *s <= 0) break;
    }
  };

  auto server_main = [&] (context_t& ctx) -> coro_t<void> {
    tcp::v4::socket_t listener;
    listener.port_reuse(true);
    listener.address_reuse(true);
    listener.listen("127.0.0.1", 9876);
    server_ready.store(true, std::memory_order_release);
    while (server_running.load(std::memory_order_acquire)) {
      auto client_sock = co_await listener.accept(ctx, nullptr, nullptr);
      if (!client_sock) break;
      ctx.spawn(server_session(ctx, *client_sock));
    }
  };

  auto client_session = [&](context_t& ctx, int id) -> coro_t<void> {
    tcp::v4::socket_t sock;
    auto conn = co_await sock.connect(ctx, "127.0.0.1", 9876);
    if (!conn) co_return;

    std::vector<char> send_buf(scenario.message_size, 'A' + (id % 26));
    std::vector<char> recv_buf(scenario.message_size + 64);

    while (true) {
      int cur = (*remaining)--;
      if (cur <= 0) break;

      auto t0 = std::chrono::steady_clock::now();
      auto s = co_await sock.send(ctx, send_buf.data(), send_buf.size());
      if (!s || *s <= 0) break;
      auto r = co_await sock.recv(ctx, recv_buf.data(), scenario.message_size);
      if (!r || *r <= 0) break;

      auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - t0).count();
      collector.record(latency, *s + *r);
    }
  };

  std::thread server_thread([&] {
    server_ctx.spawn(server_main(server_ctx));
    server_ctx.run();
  });

  while (!server_ready.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // Client context: created on the main thread
  context_t client_ctx(&config);
  client_ctx.scheduler().set_policy(sched_type);

  size_t rss_before = get_current_rss_kb();
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < scenario.connections; ++i) {
    client_ctx.spawn(client_session(client_ctx, i));
  }
  client_ctx.run();
  auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

  server_running.store(false, std::memory_order_release);
  server_ctx.stop();
  server_thread.join();

  std::string name = "Cornet/";
  name += scheduler_name(sched_type);
  
  #ifdef CORNET_METRICS
  std::cout << scenario.name << " " << name << " client metrics:" << std::endl;
  client_ctx.metrics().dump();
  std::cout << scenario.name << " " << name << " server metrics:" << std::endl;
  server_ctx.metrics().dump();
  #endif

  return collector.compute(name, scenario.name, elapsed, rss_before);
}

} // namespace bench

#endif // CORNET_BENCH_CORNET_H
