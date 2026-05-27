# Cornet

Cornet 是一个基于 C++20 协程和 Linux io_uring 的高性能异步网络框架。

通过将 io_uring 的零拷贝批量提交能力与 C++20 协程的同步式编程体验相结合，Cornet 在保持代码简洁的同时，提供了超越传统 epoll/回调模型的性能表现。

## 特性

- **C++20 协程原生支持** — `co_await` 驱动的异步 IO，代码如同步般清晰
- **io_uring 深度集成** — SQE 批量提交、CQE 批量收割、link timeout、multishot 支持
- **多种调度策略** — RoundRobin、Batch、TimeSlice、Adaptive 四种可切换调度器
- **完整 TCP/UDP 抽象** — IPv4、IPv6、Unix Domain Socket 全覆盖
- **异步 DNS 解析** — 域名解析自动卸载到线程池，IP 地址走快速路径
- **并发组合器** — `when_all`、`when_any`、`sleep`、`with_timeout`、`with_cancel`
- **任务级取消** — `canceler_t` 支持单任务取消和层级取消传播
- **线程池 Executor** — `ctx.async()` 将阻塞操作卸载到工作线程
- **信号处理** — 基于 signalfd + io_uring 的异步信号分发
- **优雅关闭** — drain → timeout → cancel 三阶段关闭流程
- **零额外依赖** — 核心仅依赖 liburing，日志/配置可选

## 快速开始

### 环境要求

- Linux 5.11+（io_uring 特性支持）
- GCC 11+ 或 Clang 14+（C++20 协程支持）
- liburing 2.0+
- CMake 3.10+
- vcpkg（依赖管理）

### 构建

```bash
# 克隆项目
git clone https://github.com/user/cornet.git && cd cornet

# 安装 liburing（如果系统未安装）
git clone https://github.com/axboe/liburing.git
cd liburing && ./configure --prefix=/usr/local && make -j$(nproc) && sudo make install && cd ..

# 构建
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

### 最简示例：TCP Echo Server

```cpp
#include "core/socket.h"

using namespace cornet;

coro_t<void> handle_client(tcp::v4::socket_t client) {
    char buf[4096];
    while (true) {
        auto n = co_await client.recv(buf, sizeof(buf));
        if (!n || *n == 0) break;
        auto sent = co_await client.send(buf, *n);
        if (!sent) break;
    }
}

coro_t<void> server() {
    auto& ctx = context_t::current();
    tcp::v4::socket_t listener;
    listener.port_reuse(true);
    listener.listen("0.0.0.0", 8080);

    while (!ctx.is_draining()) {
        auto client = co_await listener.accept();
        if (!client) continue;
        ctx.spawn(handle_client(std::move(*client)));
    }
}

int main() {
    auto& ctx = context_t::current();
    ctx.on_signal({SIGINT, SIGTERM}, [&](int) { ctx.shutdown(); });
    ctx.spawn(server());
    ctx.run();
}
```

### 异步 DNS + 超时连接

```cpp
coro_t<expected<void>> connect_with_timeout() {
    tcp::v4::socket_t sock;
    // connect 自动检测：IP 走快速路径，域名走异步 DNS
    auto ret = co_await with_timeout(sock.connect("example.com", 80), 5s);
    if (!ret) {
        co_return unexpected(ret.error());
    }
    co_return {};
}
```

### 并发等待

```cpp
coro_t<void> fetch_all() {
    auto result = co_await when_all(
        fetch_from("service-a", 8001),
        fetch_from("service-b", 8002),
        fetch_from("service-c", 8003)
    );
    // result.get<0>(), result.get<1>(), result.get<2>()
}
```

### 线程池卸载

```cpp
coro_t<void> heavy_work() {
    auto& ctx = context_t::current();
    // 阻塞计算自动在工作线程执行，完成后回到协程
    auto hash = co_await ctx.async([] {
        return compute_sha256(large_data);
    });
}
```

## 配置

通过 TOML 文件配置（可选，框架有合理默认值）：

```toml
[cornet.context.uring]
capacity = 1024

[cornet.context.scheduler]
name = "Batch"       # RoundRobin | Batch | TimeSlice | Adaptive
batch = 32
cpu_budget = "10ms"  # TimeSlice 专用
io_budget = "100us"  # TimeSlice 专用

[cornet.context.executor]
thread_nr = 4
max_task_nr = 16384

[cornet.logging.stdout]
level = "info"
pattern = "%^%L%$ [%Y-%m-%d %T %t %@] %v"
```

## 项目结构

```
cornet/
├── include/
│   ├── core/
│   │   ├── context.h      # 事件循环核心
│   │   ├── coro.h         # 协程包装器
│   │   ├── utask.h        # io_uring 任务基类
│   │   ├── socket.h       # TCP/UDP Socket 抽象
│   │   ├── combinators.h  # when_all/when_any/sleep/timeout
│   │   ├── awaiters.h     # 通用 awaiter (close/read/write/nop)
│   │   ├── scheduler.h    # 调度器接口与实现
│   │   ├── executor.h     # 线程池
│   │   ├── io_slot.h      # io_uring user_data 安全管理
│   │   └── uring.h        # io_uring 封装
│   └── utils/
│       ├── config.h       # TOML 配置
│       ├── expected.h     # 轻量级 expected 类型
│       ├── metrics.h      # 性能指标
│       └── logging.h      # 日志初始化
├── src/                   # 实现文件
├── tests/                 # 单元测试
├── bench/                 # 性能基准测试
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

## 性能

在 Echo 场景下与 Boost.Asio、libuv 的对比（单线程，AMD EPYC）：

| 场景 | Cornet/Batch | Asio/Callback | libuv |
|------|-------------|---------------|-------|
| 小消息高并发 (512conn, 64B) | 127K RPS | 111K RPS | 27K RPS |
| 中等消息 (128conn, 1KB) | 122K RPS | 107K RPS | 29K RPS |
| 大消息 (32conn, 64KB) | 35K RPS | 35K RPS | 14K RPS |
| 极限并发 (2048conn, 128B) | 120K RPS | 109K RPS | 26K RPS |

## License

MIT License
