#ifndef CORNET_TESTS_HTTP_CLIENT_FIXTURE_H
#define CORNET_TESTS_HTTP_CLIENT_FIXTURE_H

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cornet/http/client/client.h"
#include "cornet/http/client/connection.h"
#include "cornet/net/socket.h"
#include "cornet/scheduling/context.h"

/**
 * @file client_fixture.h
 * @brief a scripted origin server, plus the client-side scaffolding around it.
 *
 * The origin is plain blocking sockets on a thread rather than a cornet server: these
 * tests are about how the client reacts to exact byte sequences — short writes, a peer
 * that closes mid-exchange, trailing garbage — and scripting those is far easier
 * without an event loop in the way. Every socket carries a timeout so that a client
 * bug fails the test instead of hanging it.
 */

namespace cornet_test {

using namespace std::chrono_literals;

inline void set_timeout(int fd, std::chrono::milliseconds ms) {
  timeval tv{};
  tv.tv_sec = time_t(ms.count() / 1000);
  tv.tv_usec = suseconds_t((ms.count() % 1000) * 1000);
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

inline void write_all(int fd, std::string_view data) {
  size_t sent = 0;
  while (sent < data.size()) {
    auto n = ::send(fd, data.data() + sent, data.size() - sent, 0);
    if (n <= 0) return;
    sent += size_t(n);
  }
}

/**
 * @brief read until `marker` has been seen; returns everything that was read.
 */
inline std::string read_until(int fd, std::string_view marker, std::string carry = {}) {
  std::string raw = std::move(carry);
  char buf[4096];
  while (raw.find(marker) == std::string::npos) {
    auto n = ::recv(fd, buf, sizeof(buf), 0);
    if (n <= 0) break;
    raw.append(buf, size_t(n));
  }
  return raw;
}

/**
 * @brief read a head plus at least `len` body bytes behind it.
 */
inline std::string read_with_body(int fd, size_t len) {
  auto raw = read_until(fd, "\r\n\r\n");
  auto sep = raw.find("\r\n\r\n");
  if (sep == std::string::npos) return raw;
  char buf[4096];
  while (raw.size() - (sep + 4) < len) {
    auto n = ::recv(fd, buf, sizeof(buf), 0);
    if (n <= 0) break;
    raw.append(buf, size_t(n));
  }
  return raw;
}

/**
 * @brief lets a script hold a connection open for exactly as long as the test needs.
 *
 * The alternative is sleeping in the script, which costs the full duration every time:
 * the origin's destructor joins the script thread, so a script still sleeping keeps the
 * test alive long after the client side has finished. A script that waits here is
 * released by ~origin_t() instead, just before the join.
 */
class hold_gate_t {
 public:
  void wait() {
    std::unique_lock<std::mutex> lk(mtx_);
    cv_.wait(lk, [this] { return released_; });
  }

  void release() {
    {
      std::lock_guard<std::mutex> lk(mtx_);
      released_ = true;
    }
    cv_.notify_all();
  }

 private:
  std::mutex              mtx_;
  std::condition_variable cv_;
  bool                    released_{false};
};

/**
 * @brief a listening socket on a thread, running a script per accepted connection.
 *
 * The script receives the connection's descriptor and its index, so a test can answer
 * the first connection one way and the second another — which is how the retry and
 * stale-reuse paths are exercised. A script that also takes a hold_gate_t& can park on
 * it to keep its connection open until the test is over.
 */
class origin_t {
 public:
  using script_t = std::function<void(int fd, int index)>;
  using held_script_t = std::function<void(int fd, int index, hold_gate_t&)>;

  explicit origin_t(script_t script, int connections = 1, bool serial = true)
    : origin_t(held_script_t([s = std::move(script)](int fd, int i, hold_gate_t&) { s(fd, i); }),
               connections, serial) {}

  explicit origin_t(held_script_t script, int connections = 1, bool serial = true) {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_GE(listen_fd_, 0);
    int on = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;   // the kernel picks, so tests never collide on a port
    EXPECT_EQ(::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    EXPECT_EQ(::listen(listen_fd_, 8), 0);

    socklen_t len = sizeof(addr);
    ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
    port_ = ntohs(addr.sin_port);
    set_timeout(listen_fd_, 5s);

    thread_ = std::thread([this, script, connections, serial] {
      std::vector<std::thread> workers;
      for (int i = 0; i < connections; ++i) {
        int fd = ::accept(listen_fd_, nullptr, nullptr);
        if (fd < 0) break;
        set_timeout(fd, 5s);
        if (serial) {
          script(fd, i, gate_);
          ::close(fd);
        } else {
          // Concurrent connections need concurrent scripts, or the second client
          // would wait for the first script to finish before being served at all.
          workers.emplace_back([this, script, fd, i] {
            script(fd, i, gate_);
            ::close(fd);
          });
        }
      }
      for (auto& w : workers) w.join();
    });
  }

  ~origin_t() {
    // release before stop(): a script parked on the gate must be able to return
    gate_.release();
    stop();
    if (thread_.joinable()) thread_.join();
    if (listen_fd_ >= 0) ::close(listen_fd_);
  }

  /**
   * @brief stop accepting, waking a thread that is blocked in accept().
   *
   * A test that expects two connections but only makes one would otherwise sit out the
   * full accept timeout in the destructor. shutdown() rather than close() because
   * closing a descriptor another thread is blocked on is a race; on Linux this makes
   * the pending accept() return at once.
   */
  void stop() {
    if (listen_fd_ >= 0) ::shutdown(listen_fd_, SHUT_RDWR);
  }

  origin_t(const origin_t&) = delete;
  origin_t& operator=(const origin_t&) = delete;

  CORNET_NODISCARD uint16_t port() const { return port_; }
  CORNET_NODISCARD std::string url(std::string_view path = "/x") const {
    return "http://127.0.0.1:" + std::to_string(port_) + std::string(path);
  }

 private:
  int         listen_fd_{-1};
  uint16_t    port_{0};
  hold_gate_t gate_;
  std::thread thread_;
};

/**
 * @brief a context plus the pieces a client_connection_t needs on its own.
 */
struct conn_env_t {
  cornet::context_t              ctx;
  cornet::http::client_options_t opt{};
  cornet::http::client_metrics_t metrics{};
  cornet::http::buffer_pool_t&   pool{cornet::http::buffer_pool_t::local()};
  // a short tick, so the timeout tests do not wait half a second. The share is what
  // keeps the wheel alive across the gaps where nothing is armed.
  std::shared_ptr<cornet::timer_wheel_t> wheel_share{ctx.wheel_for(20ms)};
  cornet::timer_wheel_t&         wheel{*wheel_share};

  conn_env_t() {
    opt.timer_tick = 20ms;
  }

  void run(cornet::coro_t<void> task) {
    ctx.spawn(std::move(task));
    ctx.run();
  }
};

/**
 * @brief connect a bare client_connection_t, skipping dns and the pool.
 */
inline cornet::coro_t<cornet::expected<std::unique_ptr<cornet::http::client_connection_t>>>
dial(conn_env_t& env, uint16_t port) {
  cornet::tcp::v4::socket_t sock;
  if (sock.native_fd() < 0) co_return cornet::unexpected(errno);
  auto c = co_await sock.connect(env.ctx, "127.0.0.1", port);
  if (!c) co_return cornet::unexpected(c.error());
  co_return cornet::http::client_connection_t::adopt(env.ctx, std::move(sock), env.opt, env.pool,
                                                     env.wheel_share, env.metrics, "127.0.0.1",
                                                     port);
}

} // namespace cornet_test

#endif // CORNET_TESTS_HTTP_CLIENT_FIXTURE_H
