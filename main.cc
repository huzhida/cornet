#include <iostream>
#include "core/socket.h"

#include <filesystem>

#include "core/combinators.h"

using namespace cornet;

coro_t<int> server(context_t& ctx) {
  SPDLOG_INFO("server start");
  auto s = tcp::v4::socket_t();
  SPDLOG_INFO("server listen");
  s.port_reuse(true);
  s.address_reuse(true);
  auto listen_ret = s.listen("127.0.0.1", 12345);
  if (!listen_ret) {
    SPDLOG_ERROR("listen failed: {}", listen_ret.error().message());
    co_return -1;
  }
  SPDLOG_INFO("server accept");
  auto c = co_await s.accept(0);
  if (!c) {
    SPDLOG_ERROR("accept failed: {}", c.error().message());
    co_return -1;
  }
  char buff[2048] = {0};
  SPDLOG_INFO("server recv");
  auto n = co_await c->recv(buff, 2048);
  if (!n) {
    SPDLOG_ERROR("recv failed: {}", n.error().message());
    co_return -1;
  }
  SPDLOG_INFO("server recv: {}", buff);
  co_return 0;
}

coro_t<int> client(context_t& ctx) {
  SPDLOG_INFO("client start");
  auto s = tcp::v4::socket_t();
  SPDLOG_INFO("client connect");
  auto start = std::chrono::steady_clock::now();
  auto conn = co_await s.connect("127.0.0.1", 12345);
  SPDLOG_INFO("connect elapsed: {}", std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count());
  if (!conn) {
    SPDLOG_ERROR("failed to connect: {}", conn.error().message());
    co_return -1;
  }
  const char* buff = "hello cornet ~";
  SPDLOG_INFO("client send");
  auto sent = co_await s.send(buff, strlen(buff));
  if (!sent) {
    SPDLOG_ERROR("failed to send: {}", sent.error().message());
    co_return -1;
  }
  SPDLOG_INFO("client send: {}", *sent);
  co_return 0;
}

coro_t<expected<void>> connect_baidu()
{
  tcp::v4::socket_t s;
  auto ok = co_await s.connect("www.baidu.com", 80, std::chrono::milliseconds(500));

  if (!ok)
  {
    SPDLOG_ERROR("failed to connect: {}", ok.error().message());
    co_return unexpected(ok.error());
  }
  auto sent = co_await s.send("GET / HTTP/1.1\r\nHost: www.baidu.com\r\nConnection: close\r\nUser-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\r\n\r\n",
    strlen("GET / HTTP/1.1\r\nHost: www.baidu.com\r\nConnection: close\r\nUser-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\r\n\r\n"));
  if (!sent)
  {
    SPDLOG_ERROR("failed to send: {}", sent.error().message());
    co_return unexpected(sent.error());
  }
  char buf[4096] = {0};
  expected<int> recieved;
  do {
    recieved = co_await s.recv(buf, sizeof(buf));
    SPDLOG_INFO("{}", buf);
  } while (recieved.value() != 0);
}

int main(int argc, char* argv[]) {
  cornet::config_t::load("conf/default.toml");
  cornet::logging::init();
  auto& ctx = context_t::current();
  std::thread client_thread([] {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    auto& ctx = context_t::current();
    ctx.spawn(client(ctx));
    ctx.run();
  });

  ctx.spawn(server(ctx));
  ctx.run();
  client_thread.join();

  ctx.spawn(connect_baidu());
  ctx.run();
  return 0;
}
