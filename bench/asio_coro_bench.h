#ifndef CORNET_BENCH_ASIO_CORO_H
#define CORNET_BENCH_ASIO_CORO_H

#include "common.h"
#include <asio.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/use_awaitable.hpp>
#include <memory>
#include <thread>

namespace bench {

inline result_t run_asio_coro(const scenario_t& scenario) {
  rss_profiler_t profiler;
  profiler.start();

  std::atomic<bool> server_running{true};
  latency_collector_t collector;
  collector.reserve(scenario.total_messages);
  auto remaining = std::make_shared<std::atomic<int>>(scenario.total_messages);

  asio::io_context server_io;

  auto server_session = [&](asio::ip::tcp::socket sock) -> asio::awaitable<void> {
    std::vector<char> buf(scenario.message_size + 64);
    try {
      while (server_running) {
        auto n = co_await sock.async_read_some(asio::buffer(buf), asio::use_awaitable);
        co_await asio::async_write(sock, asio::buffer(buf.data(), n), asio::use_awaitable);
      }
    } catch (...) {}
  };

  auto server_main = [&]() -> asio::awaitable<void> {
    auto executor = co_await asio::this_coro::executor;
    asio::ip::tcp::acceptor acceptor(executor,
      asio::ip::tcp::endpoint(asio::ip::address::from_string("127.0.0.1"), 9878));
    acceptor.set_option(asio::ip::tcp::acceptor::reuse_address(true));
    while (server_running) {
      auto sock = co_await acceptor.async_accept(asio::use_awaitable);
      asio::co_spawn(executor, server_session(std::move(sock)), asio::detached);
    }
  };

  asio::co_spawn(server_io, server_main(), asio::detached);
  std::thread server_thread([&] { server_io.run(); });
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  asio::io_context client_io;

  auto client_session = [&](int id) -> asio::awaitable<void> {
    auto executor = co_await asio::this_coro::executor;
    asio::ip::tcp::socket sock(executor);
    co_await sock.async_connect(
      asio::ip::tcp::endpoint(asio::ip::address::from_string("127.0.0.1"), 9878),
      asio::use_awaitable);

    std::vector<char> send_buf(scenario.message_size, 'A' + (id % 26));
    std::vector<char> recv_buf(scenario.message_size + 64);

    while (true) {
      int cur = (*remaining)--;
      if (cur <= 0) break;

      auto t0 = std::chrono::steady_clock::now();
      co_await asio::async_write(sock, asio::buffer(send_buf), asio::use_awaitable);
      co_await asio::async_read(sock, asio::buffer(recv_buf.data(), send_buf.size()), asio::use_awaitable);

      auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - t0).count();
      collector.record(latency, send_buf.size() * 2);
    }
  };

  for (int i = 0; i < scenario.connections; ++i) {
    asio::co_spawn(client_io, client_session(i), asio::detached);
  }

  size_t hwm_before = get_vmmhwm_kb();
  auto start = std::chrono::steady_clock::now();
  client_io.run();
  auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

  server_running = false;
  server_io.stop();
  server_thread.join();

  profiler.stop();

  result_t r = collector.compute("Asio/Coroutine", scenario.name, elapsed, hwm_before);
  collector.fill_profile(r, profiler);
  return r;
}

} // namespace bench

#endif // CORNET_BENCH_ASIO_CORO_H
