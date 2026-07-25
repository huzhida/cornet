# Socket API

## 概述

Cornet 提供分层的 Socket 抽象：

```
cornet::socket_t           (基类：fd 管理, recv, send, connect, bind, close)
├── cornet::tcp::socket_t  (TCP：listen, accept)
│   ├── cornet::tcp::v4::socket_t   (IPv4 TCP)
│   ├── cornet::tcp::v6::socket_t   (IPv6 TCP)
│   └── cornet::tcp::local::socket_t (Unix Domain Stream)
└── cornet::udp::socket_t  (UDP：sendto, recvfrom, sendmsg, recvmsg)
    ├── cornet::udp::v4::socket_t   (IPv4 UDP)
    ├── cornet::udp::v6::socket_t   (IPv6 UDP)
    └── cornet::udp::local::socket_t (Unix Domain Datagram)
```

## TCP

### 创建 Socket

```cpp
// IPv4 TCP
tcp::v4::socket_t sock;

// IPv6 TCP
tcp::v6::socket_t sock6;
sock6.v6_only(true);  // 仅 IPv6

// Unix Domain Socket
tcp::local::socket_t unix_sock;
```

### Server 端

```cpp
tcp::v4::socket_t listener;
listener.address_reuse(true);
listener.port_reuse(true);

auto ret = listener.listen("0.0.0.0", 8080);
if (!ret) {
    // ret.error().message() 获取错误信息
}

// accept 返回新的 socket
while (!ctx.is_shutting_down()) {
    auto client = co_await listener.accept(ctx);
    if (!client) continue;
    ctx.spawn(handle(std::move(*client)));
}
```

### Client 端

```cpp
tcp::v4::socket_t sock;

// 连接（自动判断 IP 直连 or DNS 异步解析）
auto ret = co_await sock.connect(ctx, "example.com", 80);
if (!ret) {
    // 连接失败
}
```

### 数据收发

```cpp
char buf[4096];

// 接收
auto n = co_await sock.recv(ctx, buf, sizeof(buf));
if (!n) {
    // n.error() 获取错误
}
int bytes_received = *n;  // 实际接收字节数

// 发送
auto sent = co_await sock.send(ctx, data, len);
if (!sent) {
    // 发送失败
}
int bytes_sent = *sent;
```

### 关闭

```cpp
// 异步关闭（通过 io_uring）
co_await sock.close(ctx);

// 半关闭（shutdown）
co_await sock.shutdown(ctx, SHUT_WR);   // 关闭写端，通知对端 EOF
co_await sock.shutdown(ctx, SHUT_RD);   // 关闭读端

// 析构时自动异步关闭：
// - 同线程：ctx.spawn(async_close(fd))
// - 跨线程：同步 ::close(fd)
```

### 带超时/取消的 connect

```cpp
// 带超时
auto ret = co_await sock.connect(ctx, "example.com", 80);

// 带取消
canceler_t canceler(ctx);
auto ret = co_await sock.connect(ctx, "example.com", 80, canceler);
// 其他协程中调用 canceler.cancel() 可取消连接
```

> **注意**：不带超时/取消参数的 `connect(host, port)` 返回 `ccoro_t<expected<void>>`，
> 支持通过协程级 `with_cancel` / `with_timeout` 自动传播取消。
> 带超时/取消参数的重载返回普通 `coro_t<expected<void>>`。

## UDP

### 基本使用

```cpp
udp::v4::socket_t sock;
sock.bind("0.0.0.0", 9000);

// 接收
sockaddr_storage from_addr{};
socklen_t from_len = sizeof(from_addr);
char buf[65536];
auto n = co_await sock.recvfrom(ctx, buf, sizeof(buf), (sockaddr*)&from_addr, &from_len);

// 发送
auto sent = co_await sock.sendto(ctx, data, len, (sockaddr*)&dest_addr, dest_len);
```

### sendmsg/recvmsg

```cpp
struct msghdr msg{};
struct iovec iov{.iov_base = buf, .iov_len = len};
msg.msg_iov = &iov;
msg.msg_iovlen = 1;

auto n = co_await sock.sendmsg(ctx, &msg, 0);
auto m = co_await sock.recvmsg(ctx, &msg, 0);
```

## DNS 解析

### 自动解析（connect 内置）

```cpp
// IP 地址走快速路径（同步），域名走线程池异步解析
auto ret = co_await sock.connect(ctx, "example.com", 443);
```

### 手动解析（复用地址）

```cpp
// 异步解析
auto resolved = co_await cornet::resolve(ctx, "example.com", 443);
if (!resolved) {
    // resolved.error().message() — 解析错误信息
}

// 用解析结果连接多个 socket
auto ret1 = co_await sock1.connect(ctx, *resolved);
auto ret2 = co_await sock2.connect(ctx, *resolved);
```

## Unix Domain Socket

### TCP 风格

```cpp
tcp::local::socket_t server;
server.listen("/tmp/my.sock");

auto client = co_await server.accept(ctx);

// 客户端
tcp::local::socket_t cli;
auto ret = co_await cli.connect(ctx, "/tmp/my.sock");
```

### UDP 风格

```cpp
udp::local::socket_t sock;
sock.bind("/tmp/dgram.sock");
auto ret = co_await sock.connect(ctx, "/tmp/peer.sock");
```

## 错误处理

所有 IO 操作返回 `expected<T>`：

```cpp
auto n = co_await sock.recv(ctx, buf, len);
if (!n) {
    error_t err = n.error();
    int code = err.code;              // errno 值
    const char* msg = err.message();  // strerror(code)
    error_domain domain = err.domain; // system / resolve / internal
}
```

## Socket 选项

```cpp
sock.address_reuse(true);  // SO_REUSEADDR
sock.port_reuse(true);     // SO_REUSEPORT
sock6.v6_only(true);       // IPV6_V6ONLY (仅 v6 socket)
```

## 注意事项

- Socket 不可拷贝，仅可移动
- 析构时自动关闭 fd（同线程异步，跨线程同步）
- `recv` 返回 0 表示对端关闭连接
- `connect` 是协程（`coro_t<expected<void>>`），需要 `co_await`
- 所有 awaiter 操作必须在拥有 context 的线程上执行
