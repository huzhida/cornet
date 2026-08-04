# Context 与调度器

## context_t

`context_t` 是 Cornet 的核心事件循环，每个线程拥有一个实例。

### 创建 Context

```cpp
// 栈上创建
context_t ctx;

// 带配置创建
cornet::config_t::load("conf/default.toml");
context_t ctx(&cornet::config_t::current());
```

### 生命周期管理

```cpp
context_t ctx;

// 注册信号处理
ctx.on_signal({SIGINT, SIGTERM}, [&](int sig) {
    ctx.shutdown();
});

// 启动协程
ctx.spawn(my_coroutine(ctx));

// 运行事件循环（阻塞直到所有任务完成或 stop() 调用）
ctx.run();
```

### spawn

```cpp
// fire-and-forget：右值 coro_t 自动 detach，完成后自动销毁协程帧
ctx.spawn(my_coro(ctx));

// 保留所有权：左值 coro_t 不 detach，可以后续获取返回值
auto coro = my_coro(ctx);
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

### 跨线程投递（spawn_remote）

```cpp
// 从任意线程向目标 context 投递协程
target_ctx.spawn_remote([data = std::move(data)]() -> coro_t<void> {
    context_t& ctx = context_t::current_from_thread();
    co_await process(ctx, data);
});
```

callable 必须返回 `coro_t<void>`，在目标线程创建并执行协程。详见 [runtime.md](runtime.md)。

### keep_alive 模式

```cpp
// 防止 context 在无用户任务时自动退出（runtime_t 内部使用）
ctx.set_keep_alive(true);
```

启用后 `user_idle()` 始终返回 false，context 不会自动进入 Canceling 状态。

### 关闭流程

```cpp
// 优雅关闭（等待 1s 后强制取消，默认超时）
ctx.shutdown();

// 自定义超时
ctx.shutdown(std::chrono::seconds(5));

// 立即停止（取消所有 pending IO）
ctx.stop();
```

状态机：

```
Running ──shutdown()──→ Draining ──timeout/user_idle──→ Canceling ──spawn cancel──→ Terminated
   │                       │                                ▲
   │                       └────────user_idle()─────────────┘
   └──────────────stop()────────────────────────────────────┘
```

各状态说明：

| 状态 | 说明 |
|------|------|
| Running | 正常运行，接受新连接和任务 |
| Draining | 优雅关闭中，不再接受新连接，等待现有任务完成 |
| Canceling | spawn `cancel_sweep()` 取消所有 inflight IO（含 watcher），sweep 完成前不会重复 spawn |
| Terminated | `run()` 已返回，context 完全干净可复用 |

### idle 语义

| 方法 | 含义 | 用途 |
|------|------|------|
| `user_idle()` | 用户任务全部完成，只剩 persistent watcher | 触发状态切换到 Canceling |
| `idle()` | 真正空闲，`task_nr == 0` | run loop 退出条件 |

run loop 保证：`idle()` 为 true 时所有 IO（含 persistent）已彻底 drain，context 可安全复用。

### shutdown / stop 线程安全

`shutdown()` 和 `stop()` 均使用 `compare_exchange_strong` 保证状态转换的原子性，
多线程并发调用不会导致重复 spawn 或状态回退。

### 状态查询

```cpp
ctx.is_running();      // true 如果仍在 Running 状态
ctx.is_terminated();   // true 如果 run loop 已退出
```

### 指标监控

```cpp
#ifdef CORNET_METRICS
auto& metrics = ctx.metrics();
// ... 运行一段时间后 ...
metrics.dump(stderr);  // 输出性能统计
metrics.reset();       // 重置计数器
#endif
```

---

## 调度器

Cornet 采用自适应调度策略，通过 CPU batch 和 IO wait 的动态调整来平衡吞吐与延迟。
调度策略在构造时通过配置文件确定，不再支持运行时切换。

### 调度周期

每个调度周期依次执行以下步骤：

1. **采集远程任务**：从跨线程投递队列 (`remote_tasks_`) 取出协程句柄并入队
2. **采集线程池任务**：从 `executor_t` 收割已完成任务的协程句柄
3. **CPU 阶段**：从 `ready_queue` 批量 resume 协程（最多 `cpu_batch` 个）
4. **IO 阶段**：submit pending SQE → wait CQE → 唤醒对应协程

调度器使用 EWMA（指数加权移动平均）对 IO 饱和度和 CPU 压力做平滑，
再根据平滑后的信号动态调整 `cpu_batch` 和 `io_wait`。

### 自适应调度（默认）

```toml
[cornet.context.scheduler]
cpu_batch = 64    # CPU 阶段每次批量处理的任务数（默认 64，范围 32–2048）
io_wait = "1ms"   # IO 阶段等待超时（默认 1ms，范围 50us–1ms）
```

调度器根据以下信号动态调整：

**IO 饱和度** — `cqes_ready / (inflight + cqes_ready)`，反映 IO 完成的集中程度：
- 高 IO 饱和度 (>0.65) → 减少 CPU batch，尽快释放 CPU 给 IO 收割
- 低 IO 饱和度 → 允许更大的 batch，降低调度开销

**CPU 压力** — `task_runtime_ns / loop_runtime_ns`，防止协程执行时间过长霸占 event loop：
- CPU 压力过高 (>0.80) → 大幅减少 batch（-15%），让 IO 及时响应
- CPU 压力低 (<0.30) → 适度增加 batch（+10%），提高吞吐量

**空闲检测** — 当 `tasks_resumed == 0 && cqes_ready == 0` 时：
- 使用指数退避增大 `io_wait`（10us → 15us → 22us → ...），减少无意义的系统调用
- busy 时快速缩短 `io_wait`，降低延迟

**就绪队列校正**：
- `ready_queue` 长度超过 `cpu_batch * 4` → 额外 +16 提升 batch
- `ready_queue` 很短且 CPU 空闲 → 微调 -8 避免过度激进

### 配置项

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `cpu_batch` | int | 64 | CPU 阶段每周期最大 resume 任务数（范围 32–2048） |
| `io_wait` | duration | "1ms" | IO 阶段 wait 超时（范围 50us–1ms） |

> **提示**：目前调度器始终采用自适应策略，`name` 配置项已废弃。

### 内部接口

```cpp
// 访问调度器
scheduler_t& scheduler = ctx.scheduler();

// 访问 io_uring
uring_t& uring = ctx.io_uring();

// 访问 io slot 表
io_slot_table_t& slots = ctx.io_slots();

// 访问线程池（通过调度器访问）
executor_t& executor = ctx.executor();
```
