# 并发组合器

## sleep

挂起协程指定时长，基于 io_uring timeout 实现（非忙等待）。

```cpp
#include "concurrency/combinators.h"
using namespace std::chrono_literals;

co_await cornet::sleep(ctx, 1s);
co_await cornet::sleep(ctx, std::chrono::milliseconds(500));
co_await cornet::sleep(ctx, 100ms);
```

返回 `expected<void>`，正常超时返回成功。

## with_timeout

为任意 IO 操作添加超时限制。基于 io_uring 的 `IOSQE_IO_LINK` + `link_timeout` 实现原子级超时取消。

```cpp
// 5 秒内未收到数据则返回 ETIMEDOUT
auto result = co_await with_timeout(ctx, sock.recv(ctx, buf, 4096), 5s);
if (!result) {
    if (result.error().code == ETIMEDOUT) {
        // 超时
    } else {
        // 其他错误
    }
}
int bytes = *result;
```

### 工作原理

`with_timeout` 通过 io_uring link chain 实现：
1. 主操作 SQE 设置 `IOSQE_IO_LINK` 标志
2. 紧随一个 `link_timeout` SQE
3. 两者通过 `uring.get_sqes(sqes, 2)` 原子获取，保证在同一个 submission batch

如果主操作先完成 → link_timeout 自动取消，返回主操作结果。
如果 timeout 先触发 → 主操作被内核取消，返回 `ETIMEDOUT`。

### 适用范围

`with_timeout` 适用于所有 `utask_t` 派生的 awaiter：

```cpp
co_await with_timeout(ctx, sock.recv(ctx, buf, n), 5s);
co_await with_timeout(ctx, sock.send(ctx, buf, n), 3s);
co_await with_timeout(ctx, sock.accept(ctx), 10s);
co_await with_timeout(ctx, close_awaiter(ctx, fd), 1s);
```

## when_all

并发执行多个协程，等待 **全部** 完成后返回。

```cpp
auto result = co_await when_all(ctx,
    fetch_data_a(),   // coro_t<int>
    fetch_data_b(),   // coro_t<std::string>
    fetch_data_c()    // coro_t<void>
);

// 按索引获取各协程结果
auto& a = result.get<0>();  // expected<int>
auto& b = result.get<1>();  // expected<std::string>
auto& c = result.get<2>();  // expected<void>

if (a) {
    int value = *a;
}
```

### 返回类型

```cpp
when_all_result<Ts...> {
    std::tuple<expected<Ts>...> results;

    template<size_t I> auto& get();        // 按索引访问
    template<size_t I> const auto& get() const;
};
```

### 错误处理

`when_all` 不会因某个子协程失败而取消其他协程。每个结果独立，调用方逐个检查：

```cpp
auto result = co_await when_all(ctx, task1(), task2(), task3());
// 逐个检查结果
if (!result.get<0>()) handle_error(result.get<0>().error());
if (!result.get<1>()) handle_error(result.get<1>().error());
if (!result.get<2>()) handle_error(result.get<2>().error());
```

### 安全性

`when_all` 内部使用 `shared_ptr` 管理状态生命周期。即使 parent 协程被外部取消
（如在 `when_any` 中输掉），子任务仍可安全完成而不会触发 use-after-free。
awaiter 析构时自动置空 continuation，防止已销毁的协程被误调度。

## when_any

并发执行多个协程，**第一个** 完成时立即返回。

```cpp
auto result = co_await when_any(ctx,
    primary_fetch(),    // coro_t<Data>
    fallback_fetch(),   // coro_t<Data>
    timeout_sentinel()  // coro_t<Data>
);

int winner = result.index;          // 第一个完成的索引
auto& data = result.get<0>();       // 仅 winner 对应的 expected 有效
```

### 返回类型

```cpp
when_any_result<Ts...> {
    std::tuple<expected<Ts>...> results;
    int index{-1};  // 第一个完成的协程索引

    template<size_t I> auto& get();
    template<size_t I> const auto& get() const;
};
```

### 注意事项

- 第一个协程完成后，**其他协程仍会继续运行**直到自然结束（结果被安全丢弃）
- 内部使用 `shared_ptr` 管理状态生命周期，无 use-after-free 风险
- awaiter 析构时自动置空 continuation，防止已销毁的协程被误调度
- 适合实现竞争策略（racing）和超时兜底

### 带取消的 when_any

传入 `canceler_t` 可在第一个完成时自动取消剩余协程的 inflight IO：

```cpp
canceler_t canceler(ctx);

// 使用 ccoro_t 使协程内部 IO 自动可取消
ccoro_t<Data> cancellable_fetch(context_t& ctx, const std::string& url) {
    tcp::v4::socket_t sock;
    auto conn = co_await sock.connect(ctx, url, 80);
    if (!conn) co_return Data{};
    auto n = co_await sock.recv(ctx, buf, 4096);
    if (!n) co_return Data{};
    co_return parse(buf, *n);
}

// 手动包装 IO 为可取消
coro_t<Data> manually_cancellable(context_t& ctx) {
    tcp::v4::socket_t sock;
    auto conn = co_await sock.connect(ctx, "server", 80);
    if (!conn) co_return Data{};
    // 用 with_cancel 包裹具体 IO 操作
    auto n = co_await with_cancel(ctx, sock.recv(ctx, buf, 4096), canceler);
    if (!n) co_return Data{};
    co_return parse(buf, *n);
}

// 第一个完成时，canceler 自动触发，取消其他协程的 IO
auto result = co_await when_any(ctx, canceler,
    manually_cancellable(ctx),
    manually_cancellable(ctx)
);
```

当 `canceler.cancel()` 被触发后，其他协程中使用 `with_cancel` 包裹的 IO 操作
会收到 `ECANCELED`，从而快速退出。

### 手动超时模式

```cpp
coro_t<Data> timeout_coro() {
    co_await cornet::sleep(ctx, 5s);
    co_return Data{};  // 哨兵值
}

auto result = co_await when_any(ctx, real_work(), timeout_coro());
if (result.index == 1) {
    // 超时
}
```

## 组合使用示例

```cpp
// 并发请求三个服务，整体超时 10s
coro_t<void> fetch_with_global_timeout() {
    coro_t<Results> work = []() -> coro_t<Results> {
        auto r = co_await when_all(ctx,
            call_service_a(),
            call_service_b(),
            call_service_c()
        );
        co_return Results{r};
    }();

    coro_t<Results> timeout = []() -> coro_t<Results> {
        co_await cornet::sleep(ctx, 10s);
        co_return Results::timeout();
    }();

    auto result = co_await when_any(ctx, std::move(work), std::move(timeout));
    if (result.index == 1) {
        // 全局超时
    }
}
```

## canceler_t（取消器）

支持多任务级取消和层级取消传播。基于 io_uring cancel 实现，取消 inflight 的内核 IO 操作。

### 基本使用

```cpp
canceler_t canceler(ctx);

coro_t<void> handle_client(tcp::socket_t sock, canceler_t& canceler) {
    char buf[4096];
    while (!canceler.is_cancelled()) {
        auto n = co_await with_cancel(ctx, sock.recv(ctx, buf, 4096), canceler);
        if (!n) {
            if (n.error().code == ECANCELED) break;  // 被取消
            break;  // 其他错误
        }
        auto sent = co_await with_cancel(ctx, sock.send(ctx, buf, *n), canceler);
        if (!sent) break;
    }
}

// 启动
ctx.spawn(handle_client(std::move(client), canceler));

// 某时刻取消
canceler.cancel();
```

### 多任务并发取消

一个 canceler 可以同时关联多个 IO 操作（跨多个协程）。取消时所有关联的 inflight IO 都会收到 cancel：

```cpp
canceler_t canceler(ctx);

// 协程 A 和 B 共享同一个 canceler
ctx.spawn(reader(sock1, canceler));
ctx.spawn(writer(sock2, canceler));

// 取消时，sock1 和 sock2 的 IO 同时被 cancel
canceler.cancel();
```

### 层级取消

子 canceler 在父 canceler 取消时自动传播。子 canceler 析构时从父链表 **O(1) 摘除**（双向链表）：

```cpp
canceler_t server_canceler(ctx);

// 每个客户端一个子 canceler
canceler_t client1_canceler(ctx, server_canceler);
canceler_t client2_canceler(ctx, server_canceler);

// 取消所有客户端（迭代式传播，无递归栈溢出风险）
server_canceler.cancel();
// client1_canceler.is_cancelled() == true
// client2_canceler.is_cancelled() == true
```

### 取消时机

| 场景 | 行为 |
|------|------|
| cancel 在 `co_await with_cancel(...)` 之前 | `await_ready` 返回 true，不提交 IO，直接返回 ECANCELED |
| cancel 在 IO inflight 期间 | 发送 `io_uring_prep_cancel` 取消内核操作，CQE 返回 ECANCELED |
| cancel 在操作已完成后 | 无效果，操作正常返回 |

### 重置复用

```cpp
canceler_t canceler(ctx);
canceler.cancel();
// ... 处理完取消逻辑 ...

canceler.reset();  // 可以再次使用
auto n = co_await with_cancel(ctx, sock.recv(ctx, buf, 4096), canceler);  // 正常工作
```

### with_cancel 适用范围

`with_cancel` 可包装所有 `utask_t` 派生的 awaiter：

```cpp
co_await with_cancel(ctx, sock.recv(ctx, buf, n), canceler);
co_await with_cancel(ctx, sock.send(ctx, buf, n), canceler);
co_await with_cancel(ctx, sock.accept(ctx, nullptr, nullptr, 0), canceler);
co_await with_cancel(ctx, cornet::sleep(ctx, 5s), canceler);
co_await with_cancel(ctx, close_awaiter(ctx, fd), canceler);
```

### 协程级 with_cancel

`with_cancel(ctx, ccoro_t<V>, canceler)` 将 canceler 注入到协程的 promise 中，使其内部所有 `co_await utask_t` 操作自动获得取消能力（通过 `await_transform`）：

```cpp
ccoro_t<expected<int>> long_io_task() {
    tcp::v4::socket_t sock;
    auto conn = co_await sock.connect(ctx, "server", 80);  // 自动可取消
    auto n = co_await sock.recv(ctx, buf, 4096);            // 自动可取消
    co_return n;
}

canceler_t canceler(ctx);
auto result = co_await with_cancel(ctx, long_io_task(), canceler);
// canceler.cancel() 会取消 long_io_task 内部正在执行的任意 IO
```

> **注意**：协程级 `with_cancel` 仅适用于 `cancelable_coro_t<V>`（别名 `ccoro_t<V>`），因为只有该类型的 promise 才有 `await_transform` 自动传播取消。普通 `coro_t<V>` 无此能力。

无需在每个 `co_await` 处手动添加 `with_cancel`，canceler 通过 `await_transform` 自动传播。**嵌套的 `ccoro_t` 会自动级联**：外层 canceler 自动注入到内层 `ccoro_t` 的 promise，取消递归传播到任意深度。

```cpp
ccoro_t<expected<void>> inner() {
    co_await sock.recv(ctx, buf, n);  // 自动继承外层 canceler
    co_return {};
}

ccoro_t<expected<void>> outer() {
    co_await inner();  // canceler 自动级联到 inner
    co_return {};
}

canceler_t canceler(ctx);
co_await with_cancel(ctx, outer(), canceler);
// cancel 传播: outer → inner → inner 的所有 IO
```

### 协程级 with_timeout

`with_timeout(ctx, ccoro_t<V>, duration)` 为整个协程设置超时，内部 IO 在超时后自动取消：

```cpp
// 返回 expected<T> 的协程：超时时返回 unexpected(ETIMEDOUT)
ccoro_t<expected<int>> fetch_data() {
    auto conn = co_await sock.connect(ctx, "server", 80);
    auto n = co_await sock.recv(ctx, buf, 4096);
    co_return n;
}

auto result = co_await with_timeout(ctx, fetch_data(), 5s);
if (!result) {
    if (result.error().code == ETIMEDOUT) {
        // 协程整体超时
    }
}
```

> **注意**：协程级 `with_timeout` 仅适用于 `cancelable_coro_t<V>`（别名 `ccoro_t<V>`）。

#### 工作原理

1. 创建一个 `shared_ptr<canceler_t>` 并注入到协程 promise
2. 启动一个定时器协程，到期后调用 `canceler->cancel()`
3. 协程完成后立即取消定时器（避免泄漏）
4. 当 V 是 `expected<T>` 类型时，自动将 ECANCELED 转换为 ETIMEDOUT

#### 返回类型

| 协程返回类型 | with_timeout 返回类型 | 超时行为 |
|---|---|---|
| `ccoro_t<expected<T>>` | `ccoro_t<expected<T>>` | 返回 `unexpected(ETIMEDOUT)` |
| `ccoro_t<expected<void>>` | `ccoro_t<expected<void>>` | 返回 `unexpected(ETIMEDOUT)` |
| `ccoro_t<void>` | `ccoro_t<void>` | 协程被取消，IO 返回 ECANCELED |

推荐协程返回 `expected<T>` 以获得最佳超时体验（ETIMEDOUT 自动展平）。

### 实现细节

- 双向链表管理子 canceler，析构 unlink O(1)
- `cancel_node` 侵入式链表跟踪多个 active IO，link/unlink O(1)
- `cancel()` 使用迭代式 DFS 遍历子树，避免递归栈溢出
- 编译期 `static_assert` 约束：`with_cancel` 只能包装返回 `expected` 的 awaiter

### 注意事项

- `canceler_t` 不可拷贝
- 单线程使用，无原子操作开销
- `cancel()` 仅 prep SQE，不立即 submit（下轮调度 flush_io 时提交）
- 适合配合 `when_any` 或 `task_scope` 使用

## task_scope（结构化并发）

提供 Structured Concurrency 保证：所有通过 scope 启动的子任务在 scope 退出前必然完成或被取消。

### 核心保证

- **无逃逸**：子任务的生命周期不超过 scope
- **异常传播**：任一子任务异常 → 自动 cancel 所有兄弟任务
- **外部取消**：支持通过父 canceler 取消整个 scope
- **阻塞等待**：scope 退出时自动 join 所有子任务

### 基本使用

```cpp
co_await task_scope(ctx, [&](scope_t& scope) -> coro_t<void> {
    scope.spawn(handle_client(client1));
    scope.spawn(handle_client(client2));
    scope.spawn(handle_client(client3));
    co_return;
});
// 到这里，所有 handle_client 必然已经完成
```

### spawn 重载

#### 1. spawn(callable) — 推荐，避免 lambda 生命周期陷阱

```cpp
scope.spawn([&data]() -> coro_t<void> {
    co_await sleep(ctx, 100ms);
    data.process();
});
```

callable 被 move 进协程帧，生命周期安全。**这是推荐的用法。**

#### 2. spawn(coro_t\<T\>) — 传入已构造的协程

```cpp
auto task = make_task(args);  // 返回 coro_t<T>
scope.spawn(std::move(task)); // 结果被丢弃
```

#### 3. spawn(coro_t\<T\>, T& out) — 收集结果

```cpp
int result1, result2;
co_await task_scope(ctx, [&](scope_t& scope) -> coro_t<void> {
    scope.spawn(compute(21), result1);
    scope.spawn(compute(11), result2);
    co_return;
});
// result1, result2 在此安全可用（scope 保证子任务已完成）
```

#### 4. spawn(coro_t\<T\>, expected\<T\>& out) — 结果 + 错误处理

```cpp
expected<int> r1, r2;
co_await task_scope(ctx, [&](scope_t& scope) -> coro_t<void> {
    scope.spawn(may_succeed(), r1);
    scope.spawn(may_fail(), r2);
    co_return;
});
if (r1) { use(*r1); }
if (!r2) { log_error(r2.error()); }
```

### scope 内取消

```cpp
co_await task_scope(ctx, [&](scope_t& scope) -> coro_t<void> {
    scope.spawn([&scope]() -> coro_t<void> {
        auto ret = co_await with_cancel(ctx, long_io(), scope.canceler());
        // ret 可能是 ECANCELED
    });
    scope.spawn([&scope]() -> coro_t<void> {
        co_await cornet::sleep(ctx, 1s);
        scope.cancel();  // 取消 scope 内所有使用 with_cancel 的 IO
    });
    co_return;
});
```

### 外部取消（父 canceler）

```cpp
canceler_t parent(ctx);

// 外部某处触发取消
ctx.spawn([&parent]() -> coro_t<void> {
    co_await cornet::sleep(ctx, 5s);
    parent.cancel();  // 传播到 scope 内部
}());

co_await task_scope(ctx, parent, [&](scope_t& scope) -> coro_t<void> {
    scope.spawn([&scope]() -> coro_t<void> {
        auto ret = co_await with_cancel(ctx, very_long_io(), scope.canceler());
        // parent.cancel() 传播到 scope.canceler()，此 IO 被取消
    });
    co_return;
});
```

### 错误处理

scope 返回 `expected<void>`：

```cpp
auto result = co_await task_scope(ctx, [&](scope_t& scope) -> coro_t<void> {
    scope.spawn(task_that_throws());
    co_return;
});
if (!result) {
    // result.error().domain == error_domain::Exception
    // 某个子任务抛出了异常
}
```

### 注意事项

- **避免临时 lambda 陷阱**：使用 `scope.spawn(lambda)` 而非 `scope.spawn(lambda())`

  ```cpp
  // 正确：lambda 被 move 进协程帧
  scope.spawn([&]() -> coro_t<void> { ... });

  // 危险！临时 lambda 析构后协程帧内 this 悬垂
  scope.spawn([&]() -> coro_t<void> { ... }());
  ```

- scope body 本身也是协程，需要 `co_return`
- `scope.spawn` 只能在 body 内调用（scope 退出后不可再 spawn）
- task_scope 内部使用 `unique_ptr<scope_t>` 管理 scope 生命周期
