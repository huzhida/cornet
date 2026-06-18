# 架构概览

## 设计哲学

Cornet 围绕三个核心原则设计：

1. **单线程协作式** — 每个线程拥有独立的 `context_t`，协程在同一线程内协作调度，无锁无竞争
2. **io_uring 原生** — 所有 IO 操作直接映射为 io_uring SQE，享受内核态批量处理的性能优势
3. **零开销抽象** — awaiter 模式无虚函数、无堆分配，编译器可内联优化
4. **按需付费** — `coro_t<V>` 零额外开销；`cancelable_coro_t<V>`（`ccoro_t<V>`）按需引入取消传播，两者通过 CRTP 共享公共实现

## 核心架构

```
┌─────────────────────────────────────────────────────────────┐
│                        用户协程                              │
│   coro_t<T>   ccoro_t<T>   coro_t<T>   ...                 │
│   (零开销)    (自动取消)    (零开销)                          │
└─────────┬───────────┬───────────┬───────────────────────────┘
          │co_await   │co_await   │co_await
          ▼           ▼           ▼
┌─────────────────────────────────────────────────────────────┐
│                     Awaiter 层                               │
│   recv_awaiter  send_awaiter  sleep_awaiter  ...            │
│   (继承自 utask_t，持有 prepare_fn)                          │
└─────────────────────────┬───────────────────────────────────┘
                          │prepare_fn 填充 SQE
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                    context_t (事件循环)                       │
│  ┌──────────┐  ┌──────────────┐  ┌───────────────────────┐ │
│  │Scheduler │  │  io_uring    │  │    io_slot_table_t    │ │
│  │          │  │              │  │                       │ │
│  │ready_queue│  │ SQE ──→ 内核 │  │ [idx|gen] ↔ utask_t* │ │
│  │          │  │ CQE ←── 内核 │  │                       │ │
│  └──────────┘  └──────────────┘  └───────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                    Linux Kernel                              │
│               io_uring (SQ/CQ ring buffers)                 │
└─────────────────────────────────────────────────────────────┘
```

## 数据流

### IO 操作的完整生命周期

```
用户代码: auto n = co_await sock.recv(buf, 4096);
                    │
                    ▼
1. recv_awaiter 构造
   - 记录 fd, buf, nbytes
   - 设置 prepare_fn = io_uring_prep_recv

2. await_suspend(coroutine_handle h)
   - 从 io_slot_table 分配 slot → 得到 user_data
   - 从 uring 获取 SQE
   - 调用 prepare_fn 填充 SQE
   - 设置 sqe->user_data = slot_data
   - 协程挂起

3. Scheduler::sched() 周期
   - flush_io():
     → uring.submit() 提交所有 pending SQE
     → process_async_tasks(): 收割线程池完成的任务
     → 若有用户 IO inflight (!uring.user_idle()):
       peek_cqes() 非阻塞收割
       若无 CQE 且无 ready task: wait_cqes(timeout) 限时等待
     → 若无用户 IO 且无 ready task:
       wait_cqes(timeout) 限时等待（避免空转）
   - 对每个 CQE: process_utask()
     → slot_table.lookup(user_data)
     → task->complete()
     → scheduler.schedule(handle) 推入 ready_queue

4. Scheduler 从 ready_queue 取出 handle 并 resume
   - 协程恢复

5. await_resume()
   - 返回 expected<int>(value) 或 unexpected(errno)
```

### 协程父子关系

```
parent coroutine
    │ co_await child_coro()
    │
    ▼ await_suspend: child.continuation = parent_handle
child coroutine                  return child_handle (symmetric transfer)
    │
    │ ... work ...
    │
    ▼ final_suspend: final_awaiter
        → return continuation (symmetric transfer back to parent)
```

## 线程模型

### 单线程模式（默认）

```
┌────────────────────────────────────┐
│          Main Thread               │
│  context_t (io_uring + scheduler)  │
│  所有协程在此线程调度执行            │
└───────────────┬────────────────────┘
                │ ctx.async(fn)
                ▼
┌────────────────────────────────────┐
│         Thread Pool (executor)     │
│  worker 1: 执行阻塞任务             │
│  worker 2: 执行阻塞任务             │
│  ...                               │
│  完成后推入 completed_queue          │
└────────────────────────────────────┘
                │ scheduler 轮询 completed_queue
                ▼
           协程在 Main Thread 恢复
```

### 多线程模式（runtime_t）

```
┌─────────────────────────────────────────────────────────────┐
│                       runtime_t                              │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │   thread 0   │  │   thread 1   │  │   thread N   │     │
│  │  context_t   │  │  context_t   │  │  context_t   │     │
│  │  io_uring    │  │  io_uring    │  │  io_uring    │     │
│  │  scheduler   │  │  scheduler   │  │  scheduler   │     │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘     │
│         │                  │                  │             │
│         └───── spawn_remote (MPSC queue) ─────┘             │
│                   + eventfd 唤醒                             │
└─────────────────────────────────────────────────────────────┘
```

核心原则：
- **Shared-Nothing**：每个线程独立 context，无共享状态，无锁
- **协程不迁移**：协程从创建到结束在同一线程执行
- **跨线程通信仅通过 `spawn_remote`**：投递 callable 到目标线程执行

## 关键设计决策

### 为什么用 function pointer 而非虚函数？

`utask_t::prepare_fn` 是一个函数指针而非虚方法：
- 避免 vtable 间接跳转
- awaiter 作为栈上对象，无堆分配
- 编译器可以在 lambda 内联时消除间接调用

### 为什么用 io_slot_table？

io_uring 的 user_data 是 64-bit 整数。直接存指针有 use-after-free 风险（CQE 到达时 awaiter 可能已销毁）。io_slot_table 通过 generation 计数器解决：
- alloc 返回 `[index | generation]`
- free 递增 generation
- lookup 时 generation 不匹配 → 识别为 stale CQE，安全丢弃

### 为什么协程初始挂起？

`initial_suspend = suspend_always` 使协程创建后不立即执行，由 scheduler 统一调度。好处：
- spawn 时仅入队，不递归执行
- 调度器可以批量处理多个协程
- 避免深度递归的栈溢出风险

## 错误处理约定

Cornet 采用 **`expected<T>` 为唯一正常错误通道，异常为 bug 安全网** 的原则。

### 错误域

```cpp
enum class error_domain : uint8_t {
  none,       // 无错误
  system,     // errno (POSIX 系统调用错误)
  resolve,    // EAI_* (DNS 解析错误)
  internal,   // 框架内部错误
  exception,  // 协程中抛出的未预期异常
};
```

### 返回类型约定

| 层级 | 返回类型 | 示例 |
|------|----------|------|
| IO awaiter (utask_t 基类) | `expected<int>` | `co_await sock.recv(buf, n)` |
| void 操作 awaiter | `expected<void>` | `co_await sock.close()` / `co_await sleep(1s)` |
| 高层组合 API | `coro_t<expected<T>>` | `co_await sock.connect(host, port)` |
| timeout 包装 | `expected<int>` | `co_await with_timeout(op, 5s)` |

### 使用方式

所有 IO 操作统一用 `if (!result)` 检查错误，**无需 try-catch**：

```cpp
coro_t<void> handle(tcp::socket_t& sock) {
    auto n = co_await sock.recv(buf, 4096);
    if (!n) {
        // n.error().code    → errno 值
        // n.error().domain  → 错误域
        // n.error().message() → 可读描述
        co_return;
    }
    use_data(buf, *n);
}
```

### 异常的角色

异常**不是**正常的错误通道。它仅作为"编程 bug 的安全网"存在：
- `coro_t` 的 `unhandled_exception()` 会捕获异常并存储
- `co_await` 父协程时会 rethrow
- `when_all` / `when_any` / `task_scope` 中的异常被记录日志并转为 `error_domain::exception`

框架保证：**所有公开 API 的 IO 操作不抛异常，错误一律通过 expected 返回。**

### 错误域区分

```cpp
auto result = co_await some_operation();
if (!result) {
    switch (result.error().domain) {
        case error_domain::system:
            // POSIX errno，如 ECONNREFUSED, EPIPE
            break;
        case error_domain::resolve:
            // DNS 解析错误，如 EAI_NONAME
            break;
        case error_domain::exception:
            // 子协程内部 bug（不应出现在正常逻辑中）
            break;
        default: break;
    }
}
```
