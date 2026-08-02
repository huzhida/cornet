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
# 调度器类型: RoundRobin | Batch | TimeSlice | Adaptive
# 默认: RoundRobin
name = "Batch"

# Batch 调度器每周期最大 resume 任务数
# 默认: 32
batch = 32

# TimeSlice 调度器 CPU 阶段时间预算
# 支持单位: ns, us, ms, s, m, h
# 默认: 10ms
cpu_budget = "10ms"

# TimeSlice 调度器 IO 阶段 wait 超时
# 默认: 1ms
io_budget = "100us"

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

### 调度器选择建议

| 调度器 | 适用场景 | 特点 |
|--------|---------|------|
| RoundRobin | IO 密集型、连接数少 | 简单高效，吞吐最大化 |
| Batch | 通用推荐 | 平衡 CPU 和 IO 响应 |
| TimeSlice | 混合负载 | 公平性好 |
| Adaptive | 不确定负载 | 自适应，但有收敛开销 |

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
- scheduler: RoundRobin
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
