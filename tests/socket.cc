#include "cornet/net/socket.h"
#include "cornet/scheduling/context.h"

#include <gtest/gtest.h>
#include <thread>
#include <asio/buffer.hpp>

using namespace cornet;


class socket : public ::testing::Test {
protected:
  void SetUp() override {
    ctx = new context_t();
  }

  void TearDown() override {
    delete ctx;
  }

  context_t* ctx;
};

TEST_F(socket, to_address_ipv4) {
  sockaddr_storage addr{};
  auto socklen_ = to_address("127.0.0.1", 12345, addr, AF_UNSPEC, SOCK_STREAM, AI_NUMERICHOST);
  socklen_t socklen = socklen_.value();
  EXPECT_EQ(socklen, sizeof(sockaddr_in));
  EXPECT_EQ(addr.ss_family, AF_INET);

  const auto* sin = reinterpret_cast<sockaddr_in*>(&addr);
  EXPECT_EQ(sin->sin_port, htons(12345));
  EXPECT_EQ(sin->sin_addr.s_addr, htonl(INADDR_LOOPBACK));
}

TEST_F(socket, to_address_ipv6) {
  sockaddr_storage addr{};
  auto socklen_ = to_address("::1", 12345, addr, AF_UNSPEC, SOCK_STREAM, AI_NUMERICHOST);
  socklen_t socklen = socklen_.value();
  EXPECT_EQ(socklen, sizeof(sockaddr_in6));
  EXPECT_EQ(addr.ss_family, AF_INET6);

  const auto* sin6 = reinterpret_cast<sockaddr_in6*>(&addr);
  EXPECT_EQ(sin6->sin6_port, htons(12345));
  EXPECT_TRUE(IN6_IS_ADDR_LOOPBACK(&sin6->sin6_addr));
}

TEST_F(socket, to_address_unix_path) {
  sockaddr_storage addr{};
  const std::string path = "/tmp/test.sock";
  auto len_ = to_address(path, addr);
  socklen_t len = len_.value();
  EXPECT_EQ(len, sizeof(sockaddr_un));
  EXPECT_EQ(addr.ss_family, AF_UNIX);

  auto* un = reinterpret_cast<sockaddr_un*>(&addr);
  EXPECT_STREQ(un->sun_path, path.c_str());
}

TEST_F(socket, tcpv4_create_and_close) {
  auto test = [](context_t& ctx) -> coro_t<int> {
    tcp::v4::socket_t sock;
    EXPECT_GT(sock.native_fd(), 0);
    auto ret = co_await sock.close(ctx);
    EXPECT_TRUE(ret.has_value());
    co_return 0;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

TEST_F(socket, tcpv4_listen) {
  auto test = [](context_t& ctx) -> coro_t<void> {
    tcp::v4::socket_t sock;
    sock.address_reuse(true);
    EXPECT_TRUE(sock.listen("127.0.0.1", 0).has_value());
    sockaddr_storage addr{};
    socklen_t len = sizeof(addr);
    EXPECT_EQ(sock.getsockname(reinterpret_cast<sockaddr*>(&addr), &len), 0);

    const auto* sin = reinterpret_cast<sockaddr_in*>(&addr);
    EXPECT_GT(ntohs(sin->sin_port), 0);

    auto _ = co_await sock.close(ctx);
    co_return;
  };
  ctx->spawn(test(*ctx));
  ctx->run();
}

TEST_F(socket, tcpv4_accept_and_connect) {
  auto listener = [](context_t& ctx) -> coro_t<void> {
    tcp::v4::socket_t sock;
    sock.address_reuse(true);
    EXPECT_TRUE(sock.listen("127.0.0.1", 12345).has_value());
    auto client = co_await sock.accept(ctx);
    EXPECT_TRUE(client.has_value());
    co_return;
  };
  auto connector = [](context_t& ctx) -> coro_t<void> {
    tcp::v4::socket_t sock;
    auto ret = co_await sock.connect(ctx, "127.0.0.1", 12345);
    EXPECT_TRUE(ret.has_value());
  };
  ctx->spawn(listener(*ctx));
  ctx->spawn(connector(*ctx));
  ctx->run();
}

TEST_F(socket, tcpv4_send_recv) {
  auto listener = [](context_t& ctx) -> coro_t<void> {
    tcp::v4::socket_t sock;
    sock.address_reuse(true);
    EXPECT_TRUE(sock.listen("127.0.0.1", 12345).has_value());
    auto client = co_await sock.accept(ctx);
    EXPECT_TRUE(client.has_value());
    char buffer[16] = {};
    for (int i = 0; i < 8; ++i) {
      auto received = co_await client->recv(ctx, buffer, sizeof(buffer));
      EXPECT_TRUE(received.has_value());
      EXPECT_EQ(*received, (int)strlen("hello world."));
      EXPECT_STREQ(buffer, "hello world.");
      auto sent = co_await client->send(ctx, buffer, strlen(buffer));
      EXPECT_TRUE(sent.has_value());
      EXPECT_EQ(*sent, (int)strlen("hello world."));
    }
    co_return;
  };
  auto connector = [](context_t& ctx) -> coro_t<void> {
    tcp::v4::socket_t sock;
    auto conn = co_await sock.connect(ctx, "127.0.0.1", 12345);
    EXPECT_TRUE(conn.has_value());
    char buffer[16] = "hello world.";
    for (int i = 0; i < 8; ++i) {
      auto sent = co_await sock.send(ctx, (void*)buffer, strlen("hello world."));
      EXPECT_TRUE(sent.has_value());
      EXPECT_EQ(*sent, (int)strlen("hello world."));
      auto received = co_await sock.recv(ctx, (void*)buffer, sizeof(buffer));
      EXPECT_TRUE(received.has_value());
      EXPECT_EQ(*received, (int)strlen("hello world."));
    }
  };
  ctx->spawn(listener(*ctx));
  ctx->spawn(connector(*ctx));
  ctx->run();
}
TEST_F(socket, tcpv6_send_recv) {
  auto listener = [](context_t& ctx) -> coro_t<void> {
    tcp::v6::socket_t sock;
    sock.address_reuse(true);
    EXPECT_TRUE(sock.listen("::1", 12345).has_value());
    auto client = co_await sock.accept(ctx);
    EXPECT_TRUE(client.has_value());
    char buffer[16] = {};
    for (int i = 0; i < 8; ++i) {
      auto received = co_await client->recv(ctx, buffer, sizeof(buffer));
      EXPECT_TRUE(received.has_value());
      EXPECT_EQ(*received, (int)strlen("hello world."));
      EXPECT_STREQ(buffer, "hello world.");
      auto sent = co_await client->send(ctx, buffer, strlen(buffer));
      EXPECT_TRUE(sent.has_value());
      EXPECT_EQ(*sent, (int)strlen("hello world."));
    }
    co_return;
  };
  auto connector = [](context_t& ctx) -> coro_t<void> {
    tcp::v6::socket_t sock;
    auto conn = co_await sock.connect(ctx, "::1", 12345);
    EXPECT_TRUE(conn.has_value());
    char buffer[16] = "hello world.";
    for (int i = 0; i < 8; ++i) {
      auto sent = co_await sock.send(ctx, (void*)buffer, strlen("hello world."));
      EXPECT_TRUE(sent.has_value());
      EXPECT_EQ(*sent, (int)strlen("hello world."));
      auto received = co_await sock.recv(ctx, (void*)buffer, sizeof(buffer));
      EXPECT_TRUE(received.has_value());
      EXPECT_EQ(*received, (int)strlen("hello world."));
    }
  };
  ctx->spawn(listener(*ctx));
  ctx->spawn(connector(*ctx));
  ctx->run();
}
TEST_F(socket, tcp_local_send_recv) {
  auto listener = [](context_t& ctx) -> coro_t<void> {
    tcp::local::socket_t sock;
    sock.address_reuse(true);
    auto path = "/tmp/cornet.sock";
    EXPECT_TRUE(sock.listen(path).has_value());
    auto client = co_await sock.accept(ctx);
    EXPECT_TRUE(client.has_value());
    char buffer[16] = {};
    for (int i = 0; i < 8; ++i) {
      auto received = co_await client->recv(ctx, buffer, sizeof(buffer));
      EXPECT_TRUE(received.has_value());
      EXPECT_EQ(*received, (int)strlen("hello world."));
      EXPECT_STREQ(buffer, "hello world.");
      auto sent = co_await client->send(ctx, buffer, strlen(buffer));
      EXPECT_TRUE(sent.has_value());
      EXPECT_EQ(*sent, (int)strlen("hello world."));
    }
    co_return;
  };
  auto connector = [](context_t& ctx) -> coro_t<void> {
    tcp::local::socket_t sock;
    auto path = "/tmp/cornet.sock";
    auto conn = co_await sock.connect(ctx, path);
    EXPECT_TRUE(conn.has_value());
    char buffer[16] = "hello world.";
    for (int i = 0; i < 8; ++i) {
      auto sent = co_await sock.send(ctx, (void*)buffer, strlen("hello world."));
      EXPECT_TRUE(sent.has_value());
      EXPECT_EQ(*sent, (int)strlen("hello world."));
      auto received = co_await sock.recv(ctx, (void*)buffer, sizeof(buffer));
      EXPECT_TRUE(received.has_value());
      EXPECT_EQ(*received, (int)strlen("hello world."));
    }
  };
  ctx->spawn(listener(*ctx));
  ctx->spawn(connector(*ctx));
  ctx->run();
}

TEST_F(socket, udpv4_sendto_recvfrom) {
  auto server = [](context_t& ctx) -> coro_t<void> {
    udp::v4::socket_t sock;
    sock.address_reuse(true);
    sock.port_reuse(true);
    EXPECT_TRUE(sock.bind("127.0.0.1", 12345).has_value());
    char buffer[16] = {};
    sockaddr_storage addr{};
    socklen_t len{sizeof(addr)};
    for (int i=0; i<8; ++i) {
      auto received = co_await sock.recvfrom(ctx, buffer, sizeof(buffer), (sockaddr*)&addr, &len);
      EXPECT_TRUE(received.has_value());
      EXPECT_EQ(*received, (int)strlen("hello world."));
      EXPECT_EQ(len, sizeof(sockaddr_in));
      auto sent = co_await sock.sendto(ctx, buffer, strlen(buffer), (sockaddr*)&addr, len);
      EXPECT_TRUE(sent.has_value());
      EXPECT_EQ(*sent, (int)strlen("hello world."));
      len = 0;
    }
  };

  auto client = [](context_t& ctx) -> coro_t<void> {
    udp::v4::socket_t sock;
    sockaddr_storage addr{sizeof(addr)};
    auto socklen_ = to_address("127.0.0.1", 12345, addr, AF_INET, SOCK_DGRAM);
    auto socklen = socklen_.value();
    char buffer[16] = {"hello world."};
    for (int i = 0; i < 8; ++i) {
      auto sent = co_await sock.sendto(ctx, buffer, strlen(buffer), (sockaddr*)&addr, socklen);
      EXPECT_TRUE(sent.has_value());
      EXPECT_EQ(*sent, (int)strlen("hello world."));
      auto received = co_await sock.recvfrom(ctx, buffer, sizeof(buffer), (sockaddr*)&addr, &socklen);
      EXPECT_TRUE(received.has_value());
      EXPECT_EQ(*received, (int)strlen("hello world."));
      EXPECT_EQ(addr.ss_family, AF_INET);
      EXPECT_EQ(((sockaddr_in*)&addr)->sin_port, htons(12345));
    }
  };

  ctx->spawn(server(*ctx));
  ctx->spawn(client(*ctx));
  ctx->run();
}
TEST_F(socket, udpv6_sendto_recvfrom) {
  auto server = [](context_t& ctx) -> coro_t<void> {
    udp::v6::socket_t sock;
    sock.address_reuse(true);
    sock.port_reuse(true);
    EXPECT_TRUE(sock.bind("::1", 12345).has_value());
    char buffer[16] = {};
    sockaddr_storage addr{};
    socklen_t len{sizeof(addr)};
    for (int i=0; i<8; ++i) {
      auto received = co_await sock.recvfrom(ctx, buffer, sizeof(buffer), (sockaddr*)&addr, &len);
      EXPECT_TRUE(received.has_value());
      EXPECT_EQ(*received, (int)strlen("hello world."));
      EXPECT_EQ(len, sizeof(sockaddr_in6));
      auto sent = co_await sock.sendto(ctx, buffer, strlen(buffer), (sockaddr*)&addr, len);
      EXPECT_TRUE(sent.has_value());
      EXPECT_EQ(*sent, (int)strlen("hello world."));
      len = 0;
    }
  };

  auto client = [](context_t& ctx) -> coro_t<void> {
    udp::v6::socket_t sock;
    sockaddr_storage addr{sizeof(addr)};
    auto socklen_ = to_address("::1", 12345, addr, AF_INET6, SOCK_DGRAM);
    auto socklen = socklen_.value();
    char buffer[16] = {"hello world."};
    for (int i = 0; i < 8; ++i) {
      auto sent = co_await sock.sendto(ctx, buffer, strlen(buffer), (sockaddr*)&addr, socklen);
      EXPECT_TRUE(sent.has_value());
      EXPECT_EQ(*sent, (int)strlen("hello world."));
      auto received = co_await sock.recvfrom(ctx, buffer, sizeof(buffer), (sockaddr*)&addr, &socklen);
      EXPECT_TRUE(received.has_value());
      EXPECT_EQ(*received, (int)strlen("hello world."));
      EXPECT_EQ(addr.ss_family, AF_INET6);
      EXPECT_EQ(((sockaddr_in6*)&addr)->sin6_port, htons(12345));
    }
  };

  ctx->spawn(server(*ctx));
  ctx->spawn(client(*ctx));
  ctx->run();
}
TEST_F(socket, udp_local_sendto_recvfrom) {
  auto server = [](context_t& ctx) -> coro_t<void> {
    udp::local::socket_t sock;
    auto path = "/tmp/cornet.sock";
    EXPECT_TRUE(sock.bind(path).has_value());
    char buffer[16] = {};
    sockaddr_storage addr{};
    socklen_t len{sizeof(sockaddr_un)};
    for (int i=0; i<8; ++i) {
      auto received = co_await sock.recvfrom(ctx, buffer, sizeof(buffer), (sockaddr*)&addr, &len);
      EXPECT_TRUE(received.has_value());
      EXPECT_EQ(*received, (int)strlen("hello world."));
      EXPECT_EQ(len, sizeof(sockaddr_un) - 108 + strlen("/tmp/cornet.client.sock") + 1);
      auto sent = co_await sock.sendto(ctx, buffer, strlen(buffer), (sockaddr*)&addr, len);
      EXPECT_TRUE(sent.has_value());
      EXPECT_EQ(*sent, (int)strlen("hello world."));
      len = 0;
    }
  };

  auto client = [](context_t& ctx) -> coro_t<void> {
    udp::local::socket_t sock;
    sockaddr_storage addr{};
    auto path = "/tmp/cornet.sock";
    auto self = "/tmp/cornet.client.sock";
    EXPECT_TRUE(sock.bind(self).has_value());
    auto socklen_ = to_address(path, addr);
    auto socklen = socklen_.value();
    char buffer[16] = {"hello world."};
    for (int i = 0; i < 8; ++i) {
      auto sent = co_await sock.sendto(ctx, buffer, strlen(buffer), (sockaddr*)&addr, socklen);
      EXPECT_TRUE(sent.has_value());
      EXPECT_EQ(*sent, (int)strlen("hello world."));
      auto received = co_await sock.recvfrom(ctx, buffer, sizeof(buffer), (sockaddr*)&addr, &socklen);
      EXPECT_TRUE(received.has_value());
      EXPECT_EQ(*received, (int)strlen("hello world."));
      EXPECT_EQ(addr.ss_family, AF_UNIX);
    }
  };

  ctx->spawn(server(*ctx));
  ctx->spawn(client(*ctx));
  ctx->run();
}
