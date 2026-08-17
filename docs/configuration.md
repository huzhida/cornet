# 配置参考

## 配置文件格式

Cornet 使用 [TOML](https://toml.io/) 格式配置文件。

### 加载配置

```cpp
// 加载配置文件（可选，不加载则使用默认值）
cornet::config_t::load("conf/default.toml");
```

## 完整配置项

```toml
# ─────────────────────────────────────────
# io_uring 配置
# ─────────────────────────────────────────
[cornet.context.uring]
# SQ/CQ ring 容量（entry 数量，必须为 2 的幂）
# 越大越能容纳更多并发 IO 操作，减少 forced submit
# 默认: 32
capacity = 1024

# ─────────────────────────────────────────
# 调度器配置
# ─────────────────────────────────────────
[cornet.context.scheduler]
# CPU 阶段每周期最大 resume 任务数（默认 64，范围 32–2048）
cpu_batch = 64

# IO 阶段 wait 超时（默认 1ms，范围 50us–1ms）
# 支持单位: ns, us, ms, s, m, h
io_wait = "1ms"

# ─────────────────────────────────────────
# 线程池配置
# ─────────────────────────────────────────
[cornet.context.executor]
# 工作线程数量
# 默认: 1
thread_nr = 4

# 最大排队任务数（超出时 add() 失败）
# 默认: 16384
max_task_nr = 16384

# ─────────────────────────────────────────
# 日志配置
# ─────────────────────────────────────────
[cornet.logging.stdout]
# 日志级别: trace | debug | info | warn | error | critical | off
level = "info"
# spdlog 格式化 pattern
pattern = "%^%L%$ [%Y-%m-%d %T %t %@] %v"

# 文件日志（可选，可配置多个）
# [[cornet.logging.files]]
# path = "cornet.log"
# level = "debug"
# pattern = "%L [%Y-%m-%d %T %t %@] %v"
```

## 配置说明

### io_uring capacity

| 值 | 适用场景 |
|----|---------|
| 32 | 低并发、简单应用 |
| 256 | 通用服务 |
| 1024 | 高并发网络服务 |
| 4096 | 极限并发（C10K+） |

capacity 过小会导致 SQE 队列满时频繁触发 forced submit（额外 syscall）。通过 `metrics.get_sqe_submit_forced` 监控。

### 调度器配置

Cornet 采用自适应调度策略，根据运行时 IO 饱和度和 CPU 压力动态调整 `cpu_batch` 和 `io_wait`。

| 配置项 | 类型 | 默认值 | 范围 | 说明 |
|--------|------|--------|------|------|
| `cpu_batch` | int | 64 | 32–2048 | CPU 阶段每周期最大 resume 任务数 |
| `io_wait` | duration | "1ms" | 50us–1ms | IO 阶段 wait 超时 |

**调整建议：**

- **高并发、低延迟要求**：减小 `cpu_batch`（32–64），缩短 `io_wait`
- **高吞吐、连接数多**：增大 `cpu_batch`（256–1024），适当延长 `io_wait`
- **IO 密集型**：减小 `cpu_batch`，让 IO 收割更及时
- **CPU 密集型**：增大 `cpu_batch`，减少调度开销

### 时间格式

配置中的时间值支持以下单位后缀：

| 后缀 | 含义 |
|------|------|
| `ns`, `nsec` | 纳秒 |
| `us`, `µs`, `usec` | 微秒 |
| `ms`, `msec` | 毫秒 |
| `s`, `sec`, `second` | 秒 |
| `m`, `min`, `minute` | 分钟 |
| `h`, `hr`, `hour` | 小时 |

## 零配置启动

不加载任何配置文件时，框架使用以下默认值：
- uring capacity: 32
- scheduler: 自适应调度（cpu_batch=64, io_wait=1ms）
- executor: 懒初始化，首次 `ctx.async()` 调用时创建
- 日志: 不初始化（需手动调用 `logging::init()`）

```cpp
// 最简启动，无需配置文件
int main() {
    auto& ctx = context_t::current();
    ctx.spawn(my_server());
    ctx.run();
}
```

## HTTP 模块配置

`[cornet.http.server]` 与 `[cornet.http.client]` 两节由 HTTP 模块自己解析（`server_t` / `client_t` 构造时 `load(ctx.config())`），键列表分别见 [HTTP Server](http_server.md#配置) 与 [HTTP Client](http_client.md#配置)。两节的时间类键与这里的时间格式一致，同时也接受整数毫秒。
