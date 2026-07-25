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
    // 创建 4 线程的 runtime
    runtime_t rt(4);

    // 启动所有 worker，init_fn 在各线程执行
    rt.start([](context_t& ctx, size_t idx) {
        if (idx == 0) {
            // 线程 0 负责 accept
            ctx.spawn(accept_loop(ctx, rt));
        }
    });

    // 优雅关闭（等待 3s 后强制取消）
    rt.shutdown(std::chrono::seconds(3));
    rt.join();
}
```

### 初始化流程

`start()` 使用两阶段 `std::latch` 同步：

```
Phase 1: 所有线程创建 context 并注册到 contexts_[]
         ─── ctx_ready latch ───
Phase 2: 所有线程执行 init_fn（此时可安全访问任意 context）
         ─── init_done latch ───
         start() 返回，保证所有 context 已就绪
```

`start()` 返回后，所有 `rt.context(i)` 均有效，可以安全地进行跨线程协程投递。

### 负载分发

```cpp
// round-robin 选择下一个 context（原子操作，线程安全）
auto& target = rt.next_context();
target.spawn_remote([conn = std::move(conn)]() -> coro_t<void> {
    co_await handle_connection(conn);
});
```

### 按索引访问

```cpp
// 获取指定线程的 context
context_t* ctx = rt.context(2);  // 第 3 个线程
```

### 关闭方式

```cpp
// 优雅关闭：等待现有任务完成，超时后取消
rt.shutdown(std::chrono::seconds(5));
rt.join();

// 强制停止：立即取消所有 pending IO
rt.stop();
rt.join();
```

析构函数自动执行 `stop()` + `join()`，确保无资源泄漏。

---

## spawn_remote

`spawn_remote` 是跨线程协程投递的唯一接口。调用者在任意线程提交一个 **协程工厂**（返回 `coro_t<void>` 的 callable），目标 context 在其 owner 线程执行该 callable 并 spawn 产生的协程。

### 接口签名

```cpp
// context_t 成员方法
template<typename F>
void spawn_remote(F&& fn);
// F 签名：() -> coro_t<void>
```

### 使用示例

```cpp
// 从线程 0 向线程 1 投递协程
rt.context(1)->spawn_remote([data = std::move(data)]() -> coro_t<void> {
    co_await process(data);
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
    │                          4. spawn_remote_runner(f)
    │                             移动 callable 进协程帧
    │                          5. co_await f()
    │                             执行用户协程
```

### 设计细节

**为什么用协程工厂而非直接传递 coroutine？**

协程帧（coroutine frame）绑定线程局部的 `context_t`。如果在调用线程构造协程，其 `ctx` 指针指向调用线程的 context，在目标线程执行会导致数据竞争。协程工厂模式确保协程在目标线程创建，自然绑定正确的 context。

**Lambda 协程生命周期**

直接 `spawn(f())` 有悬垂指针风险：lambda `f` 在 spawn 后被销毁，但协程帧仍持有指向 `f` 的 `this` 指针。框架通过 `detail::spawn_remote_runner(F f)` 解决：

```cpp
namespace detail {
template<typename F>
coro_t<void> spawn_remote_runner(F f) {  // f 按值传入，移动到协程帧
    co_await f();  // 安全：f 存活在本协程帧中
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
coro_t<void> handle_client(tcp::socket_t sock) {
    char buf[4096];
    while (true) {
        auto n = co_await sock.recv(buf, sizeof(buf));
        if (!n || *n == 0) break;
        auto w = co_await sock.send(buf, *n);
        if (!w) break;
    }
}

coro_t<void> accept_loop(context_t& ctx, runtime_t& rt) {
    tcp::acceptor_t acceptor(ctx, "0.0.0.0", 8080);
    while (!ctx.is_shutting_down()) {
        auto sock = co_await acceptor.accept();
        if (!sock) break;

        // round-robin 分发到各线程
        auto& target = rt.next_context();
        target.spawn_remote([s = std::move(*sock)]() mutable -> coro_t<void> {
            co_await handle_client(tcp::socket_t(std::move(s)));
        });
    }
}

int main() {
    runtime_t rt(std::thread::hardware_concurrency());
    rt.start([&](context_t& ctx, size_t idx) {
        ctx.on_signal({SIGINT, SIGTERM}, [&](int) { rt.shutdown(); });
        if (idx == 0) {
            ctx.spawn(accept_loop(ctx, rt));
        }
    });
    rt.join();
}
```

---

## keep_alive 模式

`runtime_t` 启动时会为每个 context 设置 `keep_alive(true)`，防止 worker 线程在没有用户任务时自动退出。

```cpp
ctx.set_keep_alive(true);   // context 不会因 user_idle() 而退出
ctx.set_keep_alive(false);  // 恢复正常：无任务时自动退出
```

`shutdown()` / `stop()` 时 runtime 会先清除 `keep_alive`，再触发关闭流程。
