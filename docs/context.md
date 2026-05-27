# Context 与调度器

## context_t

`context_t` 是 Cornet 的核心事件循环，每个线程拥有一个实例（thread-local 单例）。

### 获取 Context

```cpp
// 当前线程的 context（自动创建）
auto& ctx = context_t::current();

// 获取其他线程的 context（跨线程通信）
auto* other_ctx = context_t::from_thread(some_thread);
```

### 生命周期管理

```cpp
auto& ctx = context_t::current();

// 注册信号处理
ctx.on_signal({SIGINT, SIGTERM}, [&](int sig) {
    ctx.shutdown();
});

// 启动协程
ctx.spawn(my_coroutine());

// 运行事件循环（阻塞直到所有任务完成或 terminate）
ctx.run();
```

### spawn

```cpp
// fire-and-forget：右值 coro_t 自动 detach，完成后自动销毁协程帧
ctx.spawn(my_coro());

// 保留所有权：左值 coro_t 不 detach，可以后续获取返回值
auto coro = my_coro();
ctx.spawn(coro);
// ... 等 coro.done() 后读 coro.value()
```

### 异步 IO（通用接口）

```cpp
// 通过 lambda 填充任意 io_uring SQE
auto ret = co_await ctx.io([fd, buf, n](io_uring_sqe* sqe) {
    io_uring_prep_read(sqe, fd, buf, n, 0);
});
if (!ret) {
    // 处理错误
}
int bytes_read = *ret;
```

### 线程池卸载

```cpp
// 将阻塞操作放到线程池执行
auto result = co_await ctx.async([] {
    return expensive_computation();
});
```

### 关闭流程

```cpp
// 优雅关闭（等待 5s 后强制取消）
ctx.shutdown(std::chrono::seconds(5));

// 立即停止（取消所有 pending IO）
ctx.stop();
```

状态机：

```
Running ──shutdown()──→ Draining ──timeout──→ Canceling ──all cancelled──→ Terminated
   │                                              ▲
   └──────────────stop()──────────────────────────┘
```

### 指标监控

```cpp
auto& metrics = ctx.metrics();
// ... 运行一段时间后 ...
metrics.dump(stderr);  // 输出性能统计
metrics.reset();       // 重置计数器
```

---

## 调度器

Cornet 提供四种调度策略，可通过配置文件或运行时切换。

### 调度器通用接口

每个调度周期：
1. **CPU 阶段**：从 ready_queue 取出协程并 resume
2. **IO 阶段**：submit pending SQE，收割 CQE，唤醒对应协程

### RoundRobin（轮询）

```toml
[cornet.context.scheduler]
name = "RoundRobin"
```

- 每周期将 ready_queue **全部** drain 完再做 IO
- 最简单，适合 IO 密集型场景
- 缺点：如果 CPU 任务很多，IO 收割会被延迟

### Batch（批量）

```toml
[cornet.context.scheduler]
name = "Batch"
batch = 32
```

- 每周期最多 resume `batch` 个协程，然后立即做 IO
- 平衡了 CPU 和 IO 的响应性
- 推荐作为通用默认选择

### TimeSlice（时间片）

```toml
[cornet.context.scheduler]
name = "TimeSlice"
cpu_budget = "10ms"
io_budget = "100us"
```

- CPU 阶段有时间预算，超时即切换到 IO 阶段
- IO 阶段的 wait 超时也有预算
- 适合混合负载

### Adaptive（自适应）

```toml
[cornet.context.scheduler]
name = "Adaptive"
```

- 动态调整 CPU batch size 和 IO wait timeout
- 根据 IO 饱和度（CQE ready / inflight）作为反馈信号
- 高 IO 压力 → 减少 CPU batch，增加 IO 收割频率
- 低 IO 压力 → 增加 CPU batch，减少系统调用

### 运行时切换

```cpp
ctx.set_scheduler_type(scheduler_type_t::Batch);
```

切换时自动将未完成任务转移到新调度器。
