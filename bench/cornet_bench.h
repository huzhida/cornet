#ifndef CORNET_BENCH_CORNET_H
#define CORNET_BENCH_CORNET_H

#include "common.h"
#include "core/socket.h"
#include "core/context.h"
#include "core/combinators.h"

namespace bench {

inline result_t run_cornet(const scenario_t& scenario, cornet::scheduler_type_t sched_type) {
  using namespace cornet;
  std::atomic<bool> server_running{true};
  std::atomic<bool> server_ready{false};
  latency_collector_t collector;
  collector.reserve(scenario.total_messages);

  auto remaining = std::make_shared<std::atomic<int>>(scenario.total_messages);

  auto server_session = [&](int client_fd) -> coro_t<void> {
    auto sock = tcp::v4::socket_t(client_fd);
    std::vector<char> buf(scenario.message_size + 64);
    while (server_running) {
      auto n = co_await sock.recv(buf.data(), buf.size());
      if (!n || *n <= 0) break;
      auto s = co_await sock.send(buf.data(), *n);
      if (!s || *s <= 0) break;
    }
  };

  auto server_main = [&](context_t& ctx) -> coro_t<void> {
    tcp::v4::socket_t listener;
    listener.port_reuse(true);
    listener.address_reuse(true);
    listener.listen("127.0.0.1", 9876);
    server_ready.store(true, std::memory_order_release);
    while (server_running) {
      sockaddr_storage addr{};
      socklen_t len{};
      auto fd = co_await listener.accept((sockaddr*)&addr, &len);
      if (!fd) break;
      ctx.spawn(server_session(*fd));
    }
  };

  auto client_session = [&](int id) -> coro_t<void> {
    tcp::v4::socket_t sock;
    auto conn = co_await sock.connect("127.0.0.1", 9876);
    if (!conn) co_return;

    std::vector<char> send_buf(scenario.message_size, 'A' + (id % 26));
    std::vector<char> recv_buf(scenario.message_size + 64);

    while (true) {
      int cur = (*remaining)--;
      if (cur <= 0) break;

      auto t0 = std::chrono::steady_clock::now();
      auto s = co_await sock.send(send_buf.data(), send_buf.size());
      if (!s || *s <= 0) break;
      auto r = co_await sock.recv(recv_buf.data(), send_buf.size(), MSG_WAITALL);
      if (!r || *r <= 0) break;

      auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - t0).count();
      collector.record(latency, *s + *r);
    }
  };

  std::thread server_thread([&] {
    auto& ctx = context_t::current();
    ctx.set_scheduler_type(sched_type);
    ctx.spawn(server_main(ctx));
    ctx.run();
  });

  while (!server_ready.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  size_t rss_before = get_current_rss_kb();
  auto start = std::chrono::steady_clock::now();
  {
    auto& ctx = context_t::current();
    ctx.set_scheduler_type(sched_type);
    for (int i = 0; i < scenario.connections; ++i) {
      ctx.spawn(client_session(i));
    }
    ctx.run();
  }
  auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

  server_running = false;
  auto sctx = context_t::from_thread(server_thread);
  if (sctx) sctx->stop();
  server_thread.join();

  std::string name = "Cornet/";
  name += cornet::scheduler_name(sched_type);
  return collector.compute(name, scenario.name, elapsed, rss_before);
}

} // namespace bench

#endif // CORNET_BENCH_CORNET_H
