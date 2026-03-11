#include <iostream>
#include "core/socket.h"

#include <filesystem>

using namespace cornet;

coro_t<int> server(context_t& ctx) {
  SPDLOG_INFO("server start");
  auto s = tcp::v4::socket_t();
  SPDLOG_INFO("server listen");
  s.port_reuse(true);
  s.address_reuse(true);
  bool ok = s.listen("127.0.0.1", 12345);
  SPDLOG_INFO("server accept");
  int fd = co_await s.accept(ctx, 0);
  if (fd < 0) {
    SPDLOG_ERROR("server accept failed with error {}", strerror(errno));
    co_return -1;
  }
  auto c = tcp::v4::socket_t(fd);
  char buff[2048] = {0};
  SPDLOG_INFO("server recv");
  auto n = co_await c.recv(ctx, buff, 2048);
  if (n < 0) {
    SPDLOG_ERROR("server error");
    co_return -1;
  }
  SPDLOG_INFO("server recv: {}", buff);
  co_return 0;
}

coro_t<int> client(context_t& ctx) {
  SPDLOG_INFO("client start");
  auto s = tcp::v4::socket_t();
  SPDLOG_INFO("client connect");
  int ok;
  auto start = std::chrono::steady_clock::now();
  ok = co_await s.connect(ctx, "127.0.0.1", 12345);
  SPDLOG_INFO("connect elapsed: {}", std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count());
  if (ok < 0) {
    SPDLOG_ERROR("failed to connect with error: {}", strerror(-ok));
    co_return -1;
  }
  const char* buff = "hello cornet ~";
  SPDLOG_INFO("client send");
  ok = co_await s.send(ctx, (void*)buff, strlen(buff));
  if (ok < 0) {
    SPDLOG_ERROR("failed to send with error: {}", strerror(-ok));
    co_return -1;
  }
  SPDLOG_INFO("client send: {}", ok);
  co_return 0;
}

int main(int argc, char* argv[]) {
  cornet::config_t::load("conf/default.toml");
  cornet::logging::init();
  auto& ctx = context_t::context();
  std::thread client_thread([] {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    auto& ctx = context_t::context();
    ctx.sched(client(ctx));
    ctx.run();
  });

  ctx.sched(server(ctx));
  ctx.run();
  client_thread.join();
}