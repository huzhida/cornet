#include <iostream>
#include "core/socket.h"

using namespace cornet;

generator_t<int> gen() {
  std::vector<int> nums{1,2,3,4,5};
  for (auto i : nums) {
    co_yield i;
  }
}

coro_t<int> server(context_t& ctx) {
  SPDLOG_INFO("server start");
  auto s = tcp::v4::socket_t();
  SPDLOG_INFO("server listen");
  s.listen("127.0.0.1", 12345);
  SPDLOG_INFO("server accept");
  auto c = co_await s.accept(ctx, 0);
  char buff[2048] = {0};
  SPDLOG_INFO("server recv");
  auto n = co_await c.recv(ctx, buff, 2048);
  if (n < 0) {
    SPDLOG_ERROR("server error");
    co_return -1;
  }
  SPDLOG_INFO("server recv: {}", buff);
  ctx.stop();
  co_return 0;
}

coro_t<int> client(context_t& ctx) {
  SPDLOG_INFO("client start");
  auto s = tcp::v4::socket_t();
  SPDLOG_INFO("client connect");
  int ok;
  ok = co_await s.connect(ctx, "127.0.0.1", 12345);
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
  ctx.stop();
  co_return 0;
}

int main(int argc, char* argv[]) {
  context_t ctx;

  if (argc > 1) {
    auto c = client(ctx);
    ctx.spawn(c);
    ctx.run();
  } else {
    auto c = server(ctx);
    ctx.spawn(c);
    ctx.run();
  }
}