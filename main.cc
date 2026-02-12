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
  auto s = tcp::v4::socket_t("127.0.0.1", 12345);
  SPDLOG_INFO("server listen");
  s.listen();
  sockaddr_in addr{};
  socklen_t len{};
  SPDLOG_INFO("server accept");
  auto fd = co_await s.accept(ctx, &addr, &len, 0);
  auto c = tcp::v4::socket_t(fd, &addr);
  char buff[2048] = {0};
  SPDLOG_INFO("server recv");
  auto n = co_await c.recv(ctx, buff, 2048);
  if (n < 0) {
    SPDLOG_ERROR("server error");
    co_return -1;
  }
  SPDLOG_INFO("server recv: {}", buff);
}

coro_t<int> client(context_t& ctx) {
  auto s = tcp::v4::socket_t();
  sockaddr_in raddr = tcp::v4::to_address("127.0.0.1", 12345);
  int ok = co_await s.connect(ctx, &raddr, sizeof(raddr), 0);
  if (ok < 0) {
    SPDLOG_ERROR("client error");
    perror("client");
    co_return -1;
  }
  const char* buff = "hello cornet ~";
  ok = co_await s.send(ctx, (void*)buff, strlen(buff));
  SPDLOG_INFO("client send: {}", ok);
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