# 多线程 Runtime

## runtime_t

`runtime_t` 管理 N 个 worker 线程，每个线程拥有独立的 `context_t`，遵循 **thread-per-core / shared-nothing** 模型。

核心原则：
- 每个线程独立 context，无共享状态，无锁
- 协程从创建到结束在同一线程执行，不跨线程迁移
- 跨线程通信仅通过 `spawn_remote()` 投递

### 基本用法

```cpp
#include <cornet.h>

int main() {
    // 创建 runtime（默认 hardware_concurrency 线程）
    runtime_t rt;

    // 启动所有 worker，init_fn 在各线程执行
    // 注意：init_fn 签名是 (size_t idx, context_t& ctx)，先索引后 context
    rt.start([](size_t idx, context_t& ctx) {
        if (idx == 0) {
            // 线程 0 负责 accept
            ctx.spawn(accept_loop(ctx, rt));
        }
    });

    // 优雅关闭（等待 5s 后强制取消）+ join
    rt.shutdown(std::chrono::seconds(5));
}
```

### 初始化流程

`start()` 使用两阶段同步：

```
Phase 1: 所有线程创建 context 并注册到 contexts_[]
Phase 2: 所有线程执行 init_fn（此时可安全访问任意 context）
         start() 返回，保证所有 context 已就绪
```

`start()` 返回后，所有 `rt.context_at(i)` 均有效，可以安全地进行跨线程协程投递。

### 负载分发

`runtime_t` 提供两种高层 API 来分发任务：

**方式 1：`rt.spawn()` — 自动 round-robin 分发**
```cpp
rt.spawn([&](context_t& ctx) -> coro_t<void> {
    co_await handle_connection(ctx, std::move(conn));
});
```

**方式 2：`rt.submit()` — 带结果返回**
```cpp
auto f = rt.submit([](context_t& ctx) -> coro_t<int> {
    auto data = co_await read_file(ctx, "data.txt");
    co_return process(data);
});
int result = f.get();  // 阻塞直到任务完成
```

### 按索引访问

```cpp
// 获取指定线程的 context
context_t& ctx = rt.context_at(2);  // 第 3 个线程
```

### CPU/阻塞任务分发

```cpp
// 提交到某个 worker 的线程池
auto f = rt.submit_async([] {
    return heavy_computation();
});
int result = f.get();

// fire-and-forget 版本
rt.spawn_async([] {
    background_cleanup();
});
```

### 关闭方式

```cpp
// 优雅关闭：等待现有任务完成，超时后取消 + join（阻塞）
rt.shutdown(std::chrono::seconds(5));

// 强制停止 + join（阻塞）
rt.stop();
rt.join();
```

析构函数自动执行 `stop()` + `join()`，确保无资源泄漏。

---

## spawn_remote

`spawn_remote` 是跨线程协程投递的接口。调用者在任意线程提交一个 **协程工厂**（返回 `coro_t<void>` 的 callable），目标 context 在其 owner 线程执行该 callable 并 spawn 产生的协程。

### 接口签名

```cpp
// context_t 成员方法 — 协程工厂
void spawn_remote(F&& fn);
// F 签名：() -> coro_t<void>

// context_t 成员方法 — 已构造的协程句柄
void spawn_remote(T&& task);
// T: coroutine_handle<> 或 task_t* 或 task_t 派生类
```

### 使用示例

```cpp
// 从任意线程向线程 0 投递协程
rt.context_at(0)->spawn_remote([data = std::move(data)]() -> coro_t<void> {
    context_t& ctx = ...;  // 在协程内通过其他方式获取
    co_await process(ctx, data);
});
```

### 工作原理

```
调用线程                              目标线程 (owner)
    │                                     │
    │  1. enqueue(callable)               │
    │  ─────────────────────→ MPSC Queue  │
    │  2. wakeup (eventfd)                │
    │  ─────────────────────→ io_uring    │
    │                                     │
    │                          3. drain_remote_queue()
    │                             从队列取出 callable
    │                          4. 创建 wrapper 协程
    │                          5. co_await f()
    │                             执行用户协程
```

### 设计细节

**为什么用协程工厂而非直接传递 coroutine？**

协程帧（coroutine frame）绑定线程局部的 `context_t`。如果在调用线程构造协程，其 `ctx` 指针指向调用线程的 context，在目标线程执行会导致数据竞争。协程工厂模式确保协程在目标线程创建，自然绑定正确的 context。

**Lambda 协程生命周期**

直接 `spawn(f())` 有悬垂指针风险：lambda `f` 在 spawn 后被销毁，但协程帧仍持有指向 `f` 的 `this` 指针。框架通过 `detail::make_remote_coro` 解决：

```cpp
namespace detail {
template<typename F>
coro_t<void> make_remote_coro(F f) {
    co_await f();  // f 被 move 进 wrapper 协程帧，安全存活
}
}
```

### 性能特性

| 操作 | 开销 |
|------|------|
| enqueue | 无锁 MPSC（moodycamel::ConcurrentQueue） |
| wakeup | 一次 eventfd write（8 bytes） |
| drain | 批量 dequeue，零系统调用 |

---

## 典型架构：多线程 Echo Server

```cpp
coro_t<void> handle_client(context_t& ctx, tcp::v4::socket_t sock) {
    char buf[4096];
    while (ctx.is_running()) {
        auto n = co_await sock.recv(ctx, buf, sizeof(buf));
        if (!n || *n == 0) break;
        auto w = co_await sock.send(ctx, buf, *n);
        if (!w) break;
    }
}

coro_t<void> accept_loop(context_t& ctx, runtime_t& rt) {
    tcp::v4::socket_t listener;
    listener.listen("0.0.0.0", 8080);
    while (ctx.is_running()) {
        auto client = co_await listener.accept(ctx);
        if (!client) break;

        // rt.spawn() 自动 round-robin 分发到各线程
        rt.spawn([&](context_t& target_ctx) -> coro_t<void> {
            co_await handle_client(target_ctx, std::move(*client));
        });
    }
}

int main() {
    runtime_t rt;
    rt.start([](size_t idx, context_t& ctx) {
        ctx.on_signal({SIGINT, SIGTERM}, [&](int) { rt.shutdown(); });
        if (idx == 0) {
            ctx.spawn(accept_loop(ctx, rt));
        }
    });
    // rt.join() 在析构时自动调用
}
```

---

## keep_alive 模式

`runtime_t` 启动时为每个 context 设置 `keep_alive(true)`，防止 worker 线程在没有用户任务时自动退出。

```cpp
ctx.set_keep_alive(true);   // context 不会因 user_idle() 而退出
ctx.set_keep_alive(false);  // 恢复正常：无任务时自动退出
```

`shutdown()` / `stop()` 时 runtime 会先清除 `keep_alive`，再触发关闭流程。

---

## runtime_t API 速查

| 方法 | 说明 |
|------|------|
| `runtime_t rt(config, n_threads)` | 创建 runtime |
| `rt.start(init_fn, keepalive)` | 启动所有 worker 线程 |
| `rt.shutdown(timeout)` | 优雅关闭 + join（阻塞） |
| `rt.stop()` | 强制停止 |
| `rt.join()` | 等待所有线程完成 |
| `rt.size()` | 线程数 |
| `rt.context_at(i)` | 获取第 i 个线程的 context |
| `rt.spawn(fn)` | 自动 round-robin 分发协程 |
| `rt.submit(fn)` | 提交协程并返回 `task_future_t` |
| `rt.submit_async(fn)` | 提交 CPU 任务并返回 `task_future_t` |
| `rt.spawn_async(fn)` | fire-and-forget CPU 任务 |
