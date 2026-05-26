#ifndef CORNET_BENCH_ASIO_CB_H
#define CORNET_BENCH_ASIO_CB_H

#include "common.h"
#include <asio.hpp>
#include <memory>
#include <thread>

namespace bench {

inline result_t run_asio_callback(const scenario_t& scenario) {
  std::atomic<bool> server_running{true};
  latency_collector_t collector;
  collector.reserve(scenario.total_messages);
  auto remaining = std::make_shared<std::atomic<int>>(scenario.total_messages);

  struct session : std::enable_shared_from_this<session> {
    asio::ip::tcp::socket socket_;
    std::vector<char> buf_;

    session(asio::ip::tcp::socket sock, int msg_size)
      : socket_(std::move(sock)), buf_(msg_size + 64) {}

    void start() { do_read(); }

    void do_read() {
      auto self = shared_from_this();
      socket_.async_read_some(asio::buffer(buf_),
        [this, self](std::error_code ec, size_t n) {
          if (!ec && n > 0) do_write(n);
        });
    }
    void do_write(size_t n) {
      auto self = shared_from_this();
      asio::async_write(socket_, asio::buffer(buf_.data(), n),
        [this, self](std::error_code ec, size_t) {
          if (!ec) do_read();
        });
    }
  };

  asio::io_context server_io;
  auto acceptor = std::make_shared<asio::ip::tcp::acceptor>(
    server_io, asio::ip::tcp::endpoint(asio::ip::address::from_string("127.0.0.1"), 9877));

  std::function<void()> do_accept = [&]() {
    acceptor->async_accept([&](std::error_code ec, asio::ip::tcp::socket sock) {
      if (!ec) {
        std::make_shared<session>(std::move(sock), scenario.message_size)->start();
      }
      if (server_running) do_accept();
    });
  };
  do_accept();

  std::thread server_thread([&] { server_io.run(); });
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  asio::io_context client_io;
  struct client_session : std::enable_shared_from_this<client_session> {
    asio::ip::tcp::socket socket_;
    std::vector<char> send_buf_;
    std::vector<char> recv_buf_;
    std::shared_ptr<std::atomic<int>> remaining_;
    latency_collector_t& collector_;
    std::chrono::steady_clock::time_point t0_;
    int msg_size_;

    client_session(asio::io_context& io, int id, int msg_size,
                   std::shared_ptr<std::atomic<int>> rem, latency_collector_t& col)
      : socket_(io), send_buf_(msg_size, 'A' + (id % 26)),
        recv_buf_(msg_size + 64), remaining_(rem), collector_(col), msg_size_(msg_size) {}

    void start() {
      auto self = shared_from_this();
      socket_.async_connect(
        asio::ip::tcp::endpoint(asio::ip::address::from_string("127.0.0.1"), 9877),
        [this, self](std::error_code ec) {
          if (!ec) do_send();
        });
    }

    void do_send() {
      int cur = (*remaining_)--;
      if (cur <= 0) return;
      t0_ = std::chrono::steady_clock::now();
      auto self = shared_from_this();
      asio::async_write(socket_, asio::buffer(send_buf_),
        [this, self](std::error_code ec, size_t) {
          if (!ec) do_recv();
        });
    }

    void do_recv() {
      auto self = shared_from_this();
      asio::async_read(socket_, asio::buffer(recv_buf_.data(), msg_size_),
        [this, self](std::error_code ec, size_t n) {
          if (!ec) {
            auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - t0_).count();
            collector_.record(latency, msg_size_ + n);
            do_send();
          }
        });
    }
  };

  for (int i = 0; i < scenario.connections; ++i) {
    std::make_shared<client_session>(client_io, i, scenario.message_size, remaining, collector)->start();
  }

  size_t rss_before = get_current_rss_kb();
  auto start = std::chrono::steady_clock::now();
  client_io.run();
  auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

  server_running = false;
  server_io.stop();
  server_thread.join();

  return collector.compute("Asio/Callback", scenario.name, elapsed, rss_before);
}

} // namespace bench

#endif // CORNET_BENCH_ASIO_CB_H
