# 并发组合器

## sleep

挂起协程指定时长，基于 io_uring timeout 实现（非忙等待）。

```cpp
#include "core/combinators.h"
using namespace std::chrono_literals;

co_await cornet::sleep(1s);
co_await cornet::sleep(std::chrono::milliseconds(500));
co_await cornet::sleep(100ms);
```

返回 `expected<void>`，正常超时返回成功。

## with_timeout

为任意 IO 操作添加超时限制。基于 io_uring 的 `IOSQE_IO_LINK` + `link_timeout` 实现原子级超时取消。

```cpp
// 5 秒内未收到数据则返回 ETIMEDOUT
auto result = co_await with_timeout(sock.recv(buf, 4096), 5s);
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
co_await with_timeout(sock.recv(buf, n), 5s);
co_await with_timeout(sock.send(buf, n), 3s);
co_await with_timeout(sock.accept(), 10s);
co_await with_timeout(close_awaiter(fd), 1s);
```

## when_all

并发执行多个协程，等待 **全部** 完成后返回。

```cpp
auto result = co_await when_all(
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
auto result = co_await when_all(task1(), task2(), task3());
for_each_error(result, [](auto& r) {
    if (!r) handle_error(r.error());
});
```

## when_any

并发执行多个协程，**第一个** 完成时立即返回。

```cpp
auto result = co_await when_any(
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

- 第一个协程完成后，**其他协程仍会继续运行**直到自然结束
- 当前版本不会自动取消未完成的协程
- 适合实现竞争策略（racing）和超时兜底

### 手动超时模式

```cpp
coro_t<Data> timeout_coro() {
    co_await cornet::sleep(5s);
    co_return Data{};  // 哨兵值
}

auto result = co_await when_any(real_work(), timeout_coro());
if (result.index == 1) {
    // 超时
}
```

## 组合使用示例

```cpp
// 并发请求三个服务，整体超时 10s
coro_t<void> fetch_with_global_timeout() {
    coro_t<Results> work = []() -> coro_t<Results> {
        auto r = co_await when_all(
            call_service_a(),
            call_service_b(),
            call_service_c()
        );
        co_return Results{r};
    }();

    coro_t<Results> timeout = []() -> coro_t<Results> {
        co_await cornet::sleep(10s);
        co_return Results::timeout();
    }();

    auto result = co_await when_any(std::move(work), std::move(timeout));
    if (result.index == 1) {
        // 全局超时
    }
}
```

## canceler_t（取消器）

支持单任务级取消和层级取消传播。基于 io_uring cancel 实现，取消 inflight 的内核 IO 操作。

### 基本使用

```cpp
canceler_t canceler;

coro_t<void> handle_client(tcp::socket_t sock, canceler_t& canceler) {
    char buf[4096];
    while (!canceler.is_cancelled()) {
        auto n = co_await with_cancel(sock.recv(buf, 4096), canceler);
        if (!n) {
            if (n.error().code == ECANCELED) break;  // 被取消
            break;  // 其他错误
        }
        auto sent = co_await with_cancel(sock.send(buf, *n), canceler);
        if (!sent) break;
    }
}

// 启动
ctx.spawn(handle_client(std::move(client), canceler));

// 某时刻取消
canceler.cancel();
```

### 层级取消

子 canceler 在父 canceler 取消时自动传播：

```cpp
canceler_t server_canceler;

// 每个客户端一个子 canceler
canceler_t client1_canceler(server_canceler);
canceler_t client2_canceler(server_canceler);

// 取消所有客户端
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
canceler_t canceler;
canceler.cancel();
// ... 处理完取消逻辑 ...

canceler.reset();  // 可以再次使用
auto n = co_await with_cancel(sock.recv(buf, 4096), canceler);  // 正常工作
```

### with_cancel 适用范围

`with_cancel` 可包装所有 `utask_t` 派生的 awaiter：

```cpp
co_await with_cancel(sock.recv(buf, n), canceler);
co_await with_cancel(sock.send(buf, n), canceler);
co_await with_cancel(sock.accept(nullptr, nullptr, 0), canceler);
co_await with_cancel(sleep(5s), canceler);
co_await with_cancel(close_awaiter(fd), canceler);
```

### 注意事项

- `canceler_t` 不可拷贝
- 析构时自动从父 canceler 的子链表摘除
- 单线程使用，无原子操作开销
- `cancel()` 仅 prep SQE，不立即 submit（下轮调度 flush_io 时提交）
- 适合配合 `when_any` 使用：第一个完成后 cancel 其他

