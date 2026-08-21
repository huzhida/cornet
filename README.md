# Cornet

Cornet 是一个基于 C++20 协程和 Linux io_uring 的高性能异步网络框架。

通过将 io_uring 的零拷贝批量提交能力与 C++20 协程的同步式编程体验相结合，Cornet 在保持代码简洁的同时，提供了超越传统 epoll/回调模型的性能表现。

## 特性

- **C++20 协程原生支持** — `co_await` 驱动的异步 IO，代码如同步般清晰
- **零开销协程分离** — `coro_t<V>` 零额外开销；`cancelable_coro_t<V>`（别名 `ccoro_t<V>`）按需支持自动取消传播
- **CRTP 代码复用** — 两种协程类型通过 `basic_coro_t` 共享公共实现，维护只需改一处
- **io_uring 深度集成** — SQE 批量提交、CQE 批量收割、link timeout、async cancel（5.19+ 单请求取消）
- **自适应调度器** — 动态调整 CPU batch 和 IO wait，自动平衡吞吐与延迟
- **完整 TCP/UDP 抽象** — IPv4、IPv6、Unix Domain Socket 全覆盖
- **异步 DNS 解析** — 域名解析自动卸载到线程池，IP 地址走快速路径
- **并发组合器** — `when_all`、`when_any`、`sleep`、`with_timeout`、`with_cancel`、`semaphore_t`（计数信号量/协程互斥锁）、`singleflight`（并发去重）
- **任务级取消** — `canceler_t` 支持单任务取消和层级取消传播
- **协程级取消与超时** — `with_cancel(ccoro_t)`、`with_timeout(ccoro_t)` 自动传播到内部所有 IO
- **线程池 Executor** — `ctx.async()` 将阻塞操作卸载到工作线程
- **信号处理** — 基于 signalfd + io_uring 的异步信号分发
- **优雅关闭** — drain → timeout → cancel 三阶段关闭流程
- **HTTP/1.1 Server** — 路由（精确/参数/通配）、同步与异步 handler、filter、keep-alive 与 pipelining、body 聚合或流式、chunked 响应、时间轮超时、优雅 drain
- **HTTP/1.1 Client** — 连接池、DNS 缓存、超时分层、幂等重试、重定向、流式上传下载
- **WebSocket（RFC 6455）** — 与 HTTP 同端口复用、路由级注册与守门员、零拷贝消息视图、自动 Ping/Pong 与 Close 握手、分片重组、ws/wss 统一，握手失败按 RFC 精确回绝
- **TLS（OpenSSL 3.x，可选）** — 用户态 record layer + 明文/TLS 统一传输抽象；server/client 共用，`CORNET_WITH_TLS` 开关
- **依赖精简** — 核心仅依赖 liburing、spdlog、toml++、concurrentqueue（vcpkg 管理）；HTTP 模块私有链接 llhttp，TLS 模块私有链接 OpenSSL，公开头文件都不暴露

## 快速开始

### 环境要求

- Linux 5.11+（io_uring 特性支持）
- GCC 11+ 或 Clang 14+（C++20 协程支持）
- liburing 2.0+
- CMake 3.16+
- vcpkg（依赖管理）

### 构建

**一键搭建（推荐）：**

```bash
git clone https://github.com/user/cornet.git && cd cornet
./setup.sh
```

脚本会自动完成：内核版本检查、系统依赖安装（含 liburing）、vcpkg 安装、项目构建。

**手动构建：**

```bash
git clone https://github.com/user/cornet.git && cd cornet

# 确保已安装 vcpkg 并设置 VCPKG_ROOT
cmake --preset release
cmake --build --preset release
```

### 最简示例：TCP Echo Server

```cpp
#include <cornet.h>

using namespace cornet;

coro_t<void> handle_client(tcp::v4::socket_t client, context_t& ctx) {
    char buf[4096];
    while (ctx.is_running()) {
        auto n = co_await client.recv(ctx, buf, sizeof(buf));
        if (!n || *n == 0) break;
        co_await client.send(ctx, buf, *n);
    }
}

coro_t<void> server(context_t& ctx) {
    tcp::v4::socket_t listener;
    listener.port_reuse(true);
    listener.listen("0.0.0.0", 8080);

    while (ctx.is_running()) {
        auto client = co_await listener.accept(ctx);
        if (!client) continue;
        ctx.spawn(handle_client(std::move(*client), ctx));
    }
}

int main() {
    context_t ctx;
    ctx.on_signal({SIGINT, SIGTERM}, [&](int) { ctx.shutdown(); });
    ctx.spawn(server(ctx));
    ctx.run();
}
```

### HTTP Server

```cpp
#include <cornet/http_server.h>

using namespace cornet;

int main() {
    context_t ctx;
    http::server_t server(ctx);

    // 不需要挂起的 handler 就是普通函数：无协程帧、不经过调度器
    server.get("/hello", [](auto&, auto& resp) { resp.text("hello cornet"); });
    server.get("/users/:id", [](http::request_t& req, http::response_t& resp) {
        resp.json(std::string(R"({"id":")") + std::string(req.param("id")) + R"("})");
    });

    if (auto ok = server.listen("0.0.0.0", 8080); !ok) return 1;
    ctx.on_signal({SIGINT, SIGTERM}, [&server](int) { server.drain(); });
    ctx.spawn(server.serve());
    ctx.run();
}
```

详见 [HTTP Server](docs/http_server.md)、[HTTP Client](docs/http_client.md)。

### WebSocket Echo

```cpp
server.websocket("/echo", [](websocket::session_t& ws) -> coro_t<void> {
    while (auto msg = co_await ws.recv()) {
        if (msg->opcode == websocket::opcode_t::Close) break;
        if (auto ok = co_await ws.send(msg->payload, msg->opcode); !ok) break;
    }
});
```

WebSocket 路由与普通路由同端口、同匹配规则，可链 `.accept()` 做握手前守门（Origin/子协议）；客户端用 `co_await websocket::connect(ctx, "ws://...")` 一步到位。详见 [WebSocket](docs/websocket.md)。

### 异步 DNS + 超时连接

```cpp
ccoro_t<expected<void>> connect_with_timeout(context_t& ctx) {
    tcp::v4::socket_t sock;
    // 不带超时参数的 connect：IP 走快速路径，域名走异步 DNS
    auto ret = co_await sock.connect(ctx, "www.example.com", 80);
    if (!ret) {
        co_return unexpected(ret.error());
    }
    co_return {};
}

// 使用 with_timeout 为 IO 操作添加超时
auto result = co_await with_timeout(ctx, connect_with_timeout(ctx), std::chrono::seconds(5));
if (!result && result.error().code == ETIMEDOUT) {
    // 连接超时
}
```

### 协程级超时与取消

```cpp
// 需要自动取消传播的协程，使用 ccoro_t（cancelable_coro_t 的别名）
ccoro_t<expected<int>> fetch_data(context_t& ctx) {
    tcp::v4::socket_t sock;
    auto conn = co_await sock.connect(ctx, "server", 80);   // 自动可取消
    if (!conn) co_return unexpected(conn.error());
    auto n = co_await sock.recv(ctx, buf, 4096);            // 自动可取消
    co_return *n;
}

// 整个协程设定超时，内部所有 IO 自动获得取消能力
auto result = co_await with_timeout(ctx, fetch_data(ctx), std::chrono::seconds(5));
if (!result && result.error().code == ETIMEDOUT) {
    // 协程整体超时，内部 IO 已自动取消
}
```

### 并发等待

```cpp
coro_t<void> fetch_all(context_t& ctx) {
    auto result = co_await when_all(ctx,
        fetch_from(ctx, "service-a", 8001),
        fetch_from(ctx, "service-b", 8002),
        fetch_from(ctx, "service-c", 8003)
    );
    // result.get<0>(), result.get<1>(), result.get<2>()
}
```

### 线程池卸载

```cpp
coro_t<void> heavy_work(context_t& ctx) {
    // 阻塞计算自动在工作线程执行，完成后回到协程
    auto hash = co_await ctx.async([] {
        return compute_sha256(large_data);
    });
}
```

### 多线程 Runtime

```cpp
#include <cornet.h>

coro_t<void> accept_loop(context_t& ctx, runtime_t& rt) {
    tcp::v4::socket_t listener;
    listener.listen("0.0.0.0", 8080);
    while (ctx.is_running()) {
        auto client = co_await listener.accept(ctx);
        if (!client) continue;
        // 通过 rt.spawn() 自动 round-robin 分发
        rt.spawn([&](context_t& target_ctx) -> coro_t<void> {
            co_await handle_client(std::move(*client), target_ctx);
        });
    }
}

int main() {
    runtime_t rt;  // 默认 hardware_concurrency 线程
    rt.start([](size_t idx, context_t& ctx) {
        if (idx == 0) {
            ctx.spawn(accept_loop(ctx, rt));
        }
    });
    rt.join();
}
```

## 配置

通过 TOML 文件配置（可选，框架有合理默认值）：

```toml
[cornet.context.uring]
capacity = 1024

[cornet.context.scheduler]
cpu_batch = 64    # CPU 阶段每周期最大 resume 任务数（默认 64，范围 32–2048）
io_wait = "1ms"   # IO 阶段等待超时（默认 1ms，范围 50us–1ms）

[cornet.logging.stdout]
level = "info"
pattern = "%^%L%$ [%Y-%m-%d %T %t %@] %v"
```

加载配置：

```cpp
// 可选：不加载则使用默认值
cornet::config_t::load("conf/default.toml");
```

## 项目结构

```
cornet/
├── include/
│   ├── base/
│   │   ├── defines.h      # 宏与工具函数
│   │   ├── expected.h     # 轻量级 expected 类型
│   │   ├── metrics.h      # 性能指标
│   │   └── task.h         # 任务基类
│   ├── coroutine/
│   │   ├── coro.h         # 协程包装器 (coro_t / cancelable_coro_t / ccoro_t，CRTP 共享实现)
│   │   └── cancel.h       # 取消器与 cancellable_awaiter
│   ├── io_uring/
│   │   ├── uring.h        # io_uring 封装
│   │   ├── utask.h        # io_uring 任务基类
│   │   ├── awaiters.h     # 通用 awaiter (close/read/write/nop)
│   │   └── io_slot.h      # io_uring user_data 安全管理
│   ├── scheduling/
│   │   ├── context.h      # 事件循环核心
│   │   ├── scheduler.h    # 调度器接口与实现 (ring_queue_t 高效就绪队列)
│   │   ├── executor.h     # 线程池
│   │   └── runtime.h      # 多线程 runtime
│   ├── concurrency/
│   │   ├── combinators.h  # when_all/when_any/sleep/timeout/协程级cancel
│   │   ├── scope.h        # 结构化并发 scope
│   │   ├── semaphore.h    # 计数信号量（协程互斥锁）
│   │   └── singleflight.h # 并发去重（防惊群）
│   ├── net/
│   │   └── socket.h       # TCP/UDP Socket 抽象
│   ├── http/
│   │   ├── common/        # 协议常量、缓冲池、头表、解析器、序列化、时间轮、URL
│   │   ├── server/        # message.h router.h connection.h server.h
│   │   └── client/        # message.h connection.h pool.h client.h
│   ├── http.h             # HTTP 全部（server + client + websocket）
│   ├── http_server.h      # 只要 server（含 websocket 路由）
│   ├── http_client.h      # 只要 client
│   ├── websocket/         # WebSocket：common（帧编解码/握手）、session、client
│   ├── websocket.h        # WebSocket 全部
│   └── utils/
│       ├── config.h       # TOML 配置
│       └── logging.h      # 日志初始化
├── src/                   # 实现文件
├── tests/                 # 单元测试
├── bench/                 # 性能基准测试
├── examples/              # 可运行示例（http_hello / http_client）
├── conf/                  # 配置文件
└── docs/                  # 详细文档
```

## 文档

- [架构概览](docs/architecture.md)
- [Context 与调度器](docs/context.md)
- [协程与错误处理](docs/coroutine.md)
- [Socket API](docs/socket.md)
- [通用 Awaiter](docs/awaiters.md)
- [并发组合器](docs/combinators.md)
- [线程池 Executor](docs/executor.md)
- [配置参考](docs/configuration.md)
- [多线程 Runtime](docs/runtime.md)
- [HTTP Server](docs/http_server.md)
- [HTTP Client](docs/http_client.md)
- [WebSocket](docs/websocket.md)
- [TLS](docs/tls.md)

## 性能

在 Echo 场景下与 Boost.Asio 的对比（单线程）：

| 场景 | Cornet | Asio/Callback |
|------|--------|---------------|
| 小消息高并发 (512conn, 64B) | 127K RPS | 111K RPS |
| 中等消息 (128conn, 1KB) | 122K RPS | 107K RPS |
| 大消息 (32conn, 64KB) | 35K RPS | 35K RPS |
| 极限并发 (2048conn, 128B) | 120K RPS | 109K RPS |

同一套 echo 负载也有 HTTP/1.1 版本，用来量化协议本身的开销：`Cornet`（裸 send/recv）、`Cornet/HTTPsrv`（`http::server_t` + 裸客户端，只含服务端开销）、`Cornet/HTTP`（server + `client_t` 完整栈）三行同口径对比，跑完会打印 RPS 保留率与每次交换的协议字节数。

```bash
cmake --preset release -DCORNET_ENABLE_BENCH=ON
cmake --build --preset release --target bench
./cmake-build-release/bench
```

## License

MIT License
