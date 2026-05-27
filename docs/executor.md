# 线程池 Executor

## 概述

`executor_t` 提供线程池能力，用于将阻塞或 CPU 密集型操作从 io_uring 事件循环中卸载。

典型场景：
- DNS 解析（`getaddrinfo`）
- 文件 IO（非 io_uring 方式）
- CPU 密集计算（加密、压缩、序列化）
- 调用阻塞的第三方库

## 使用方式

### ctx.async()

最常用的接口，将 lambda 放到线程池执行，协程自动挂起等待结果：

```cpp
coro_t<void> example() {
    auto& ctx = context_t::current();

    // 返回值自动推导
    auto hash = co_await ctx.async([] {
        return sha256(large_buffer);
    });

    // void 返回
    co_await ctx.async([] {
        sync_write_to_disk(data);
    });
}
```

### 异常处理

线程池中的异常会被捕获，在协程恢复时重新抛出：

```cpp
try {
    auto result = co_await ctx.async([] {
        throw std::runtime_error("oops");
        return 42;
    });
} catch (const std::runtime_error& e) {
    // 在协程中捕获
}
```

## 配置

```toml
[cornet.context.executor]
thread_nr = 4       # 工作线程数
max_task_nr = 16384 # 最大排队任务数
```

Executor 是懒初始化的 — 首次调用 `ctx.async()` 时才创建线程池。

## 工作原理

```
协程线程                          Worker 线程池
    │                                  │
    │ co_await ctx.async(fn)           │
    │                                  │
    ▼                                  │
await_suspend:                         │
  task.fn = fn                         │
  executor.add(&task) ─────────────────→ worker 取出 task
  协程挂起                              │ 执行 task.fn()
    │                                  │ task 推入 completed_queue
    │                                  │
    │ scheduler 轮询 completed_queue ←──┘
    │ 发现完成 → schedule(handle)
    ▼
await_resume:
  return task.result
```

### 关键点

- **提交**：lock-free 的 `BlockingConcurrentQueue`
- **回收**：scheduler 每个 sched 周期调用 `process_async_tasks()` 批量收割
- **线程安全**：任务通过 queue 传递，无共享状态

## 注意事项

- Executor 线程中**不能**使用 `context_t::current()` 或任何协程/awaiter API
- Executor 线程中**不能**操作 socket 或 io_uring
- 任务执行顺序不保证（取决于线程调度）
- 如果 pending queue 满，`add()` 会失败
- `ctx.async()` 的 lambda 必须是值捕获或确保引用在协程恢复前有效
