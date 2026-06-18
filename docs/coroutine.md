# 协程与错误处理

## 协程类型体系

Cornet 提供两种协程类型，通过 CRTP（`basic_coro_t`）共享公共实现，未来修改只需改动基类：

| 类型 | 别名 | 开销 | 用途 |
|------|------|------|------|
| `coro_t<V>` | — | 零额外开销 | 通用协程，不需要取消能力 |
| `cancelable_coro_t<V>` | `ccoro_t<V>` | `await_transform` 包装 | 需要自动取消传播的协程 |

### 设计原则

- **零开销原则**：`coro_t<V>` 没有 `await_transform`，`co_await` IO 操作时不附加任何取消逻辑
- **按需付费**：只有 `cancelable_coro_t<V>`（或 `ccoro_t<V>`）才通过 `await_transform` 自动包装 IO
- **代码复用**：两者继承 `basic_coro_t<V, Derived>`，析构、移动、`co_await`、`resume`、`detach` 等操作只有一份实现

### CRTP 架构

```
basic_coro_t<V, Derived>   ← 公共实现（析构、移动、co_await、resume、detach、value）
    ├── coro_t<V>          ← 零开销，plain promise_type
    └── cancelable_coro_t<V> (ccoro_t<V>)  ← 带 await_transform 的 promise_type
```

## coro_t\<V\>

`coro_t<V>` 是 Cornet 的基础协程类型，封装 C++20 coroutine handle 并管理其生命周期。
无 `await_transform`，无 canceler，纯粹的协程包装。

### 定义协程

```cpp
// 返回值协程
coro_t<int> compute() {
    co_return 42;
}

// void 协程
coro_t<void> do_work() {
    co_await cornet::sleep(1s);
    co_return;
}

// 嵌套调用
coro_t<std::string> fetch() {
    auto n = co_await compute();
    co_return std::to_string(n);
}
```

### 启动方式

```cpp
// 方式 1: spawn (fire-and-forget)
ctx.spawn(do_work());  // 右值，自动 detach，完成后自动销毁

// 方式 2: co_await (等待结果)
auto result = co_await compute();  // 父协程等待子协程

// 方式 3: spawn 保留所有权
auto coro = compute();
ctx.spawn(coro);  // 左值，不 detach
// ... 之后可以检查 coro.done() 和 coro.value()
```

### symmetric transfer

`co_await coro_t<T>` 使用 symmetric transfer 优化：
- 父协程挂起时直接跳转到子协程，无栈帧开销
- 子协程完成时直接跳回父协程
- 避免了递归 resume 导致的栈溢出

---

## cancelable_coro_t\<V\> / ccoro_t\<V\>

`cancelable_coro_t<V>`（简写 `ccoro_t<V>`）是带自动取消传播能力的协程类型。其 promise_type 包含 `canceler_t*` 指针和 `await_transform`，使内部所有 `utask_t` 派生的 IO 操作自动获得取消能力。

### 使用场景

需要整个协程被 `with_cancel` 或 `with_timeout` 包装时使用：

```cpp
// 使用 ccoro_t 声明需要取消能力的协程
ccoro_t<expected<int>> long_io_task() {
    tcp::v4::socket_t sock;
    auto conn = co_await sock.connect("server", 80);   // 自动可取消
    auto n = co_await sock.recv(buf, 4096);             // 自动可取消
    co_return n;
}

// 协程级 with_cancel
canceler_t canceler;
auto result = co_await with_cancel(long_io_task(), canceler);

// 协程级 with_timeout
auto result = co_await with_timeout(long_io_task(), 5s);
```

### await_transform 原理

```cpp
// cancelable_coro_t 的 promise_type 内部
canceler_t* canceler_{nullptr};

// 1. utask_t 派生的 IO 操作 → 包装 cancellable_awaiter
template<typename T>
requires std::derived_from<std::decay_t<T>, utask_t>
cancellable_awaiter<std::decay_t<T>> await_transform(T&& op) {
    return {std::forward<T>(op), canceler_};
}

// 2. 子 ccoro_t → 自动级联 canceler（取消传播到子协程）
template<typename T>
requires requires { typename std::decay_t<T>::promise_type::canceler_tag; }
auto await_transform(T&& coro) {
    if (canceler_) coro.native_handle().promise().canceler_ = canceler_;
    return std::move(coro).operator co_await();
}

// 3. 其他 awaitable → 直接透传
template<typename T>
requires (!std::derived_from<std::decay_t<T>, utask_t>
          && !requires { typename std::decay_t<T>::promise_type::canceler_tag; })
T&& await_transform(T&& op) {
    return std::forward<T>(op);
}
```

### 行为

- `canceler_` 被注入后：所有 IO 自动关联取消器，cancel 时自动取消 inflight IO
- `ccoro_t` 内 `co_await` 另一个 `ccoro_t` 时：canceler 自动级联到子协程（无需手动传递）
- 普通 `coro_t` 或 `suspend_always` 等其他 awaitable：正常透传，不受影响

### 自动级联取消

当 `ccoro_t` 嵌套 `ccoro_t` 时，外层的 canceler 自动传播到内层所有 IO：

```cpp
ccoro_t<expected<void>> inner_task() {
    co_await sock.recv(buf, n);   // 自动继承父 canceler
    co_await sock.send(buf, n);   // 同样自动可取消
    co_return {};
}

ccoro_t<expected<void>> outer_task() {
    co_await inner_task();  // canceler 自动注入 inner_task 的 promise
    co_return {};
}

canceler_t canceler;
co_await with_cancel(outer_task(), canceler);
// canceler.cancel() → outer_task 和 inner_task 中的 IO 全部取消
```

级联是递归的：`outer → inner → deeper` 层层传播，无需手动在每层添加 `with_cancel`。

### 与 coro_t 的选择

```cpp
// 不需要取消 → 用 coro_t（零开销）
coro_t<void> simple_handler() {
    co_await sock.recv(buf, n);  // 直接调用，无包装
    co_return;
}

// 需要取消/超时 → 用 ccoro_t
ccoro_t<expected<void>> cancellable_handler() {
    co_await sock.recv(buf, n);  // await_transform 自动包装
    co_return {};
}
```

---

## expected\<T\>

轻量级错误处理类型，零开销（无异常、无堆分配）。

### 基本使用

```cpp
expected<int> divide(int a, int b) {
    if (b == 0) return unexpected(EINVAL);
    return a / b;
}

auto result = divide(10, 2);
if (result) {
    int value = *result;      // 或 result.value()
} else {
    error_t err = result.error();
    // err.code = EINVAL
    // err.message() = "Invalid argument"
}
```

### expected\<void\>

```cpp
expected<void> try_bind() {
    if (::bind(fd, addr, len) < 0) {
        return unexpected(errno);
    }
    return {};  // 成功
}

auto ret = try_bind();
if (!ret) {
    // ret.error().message()
}
```

### error_t

```cpp
struct error_t {
    int code;               // 错误码
    error_domain domain;    // 错误域

    const char* message();  // 人类可读信息
};

enum class error_domain {
    none,       // 无错误
    system,     // errno (strerror)
    resolve,    // EAI_* (gai_strerror)
    internal,   // 框架内部错误
};
```

### unexpected 构造

```cpp
return unexpected(ETIMEDOUT);                     // system domain (默认)
return unexpected(EAI_NONAME, error_domain::resolve);  // DNS 域
return unexpected(ECANCELED, error_domain::internal);  // 内部错误
```

---

## 自动取消传播（await_transform）

> **注意**：此机制仅在 `cancelable_coro_t<V>` / `ccoro_t<V>` 中生效。`coro_t<V>` 没有 `await_transform`，IO 操作不会被包装。

当 `ccoro_t` 通过 `with_cancel` 或 `with_timeout` 包装时，canceler 被注入到 promise，内部所有 IO 自动可取消：

```cpp
ccoro_t<expected<void>> my_handler() {
    auto conn = co_await sock.connect("server", 80);  // 自动可取消
    auto n = co_await sock.recv(buf, 4096);            // 自动可取消
    auto sub = co_await compute_something();           // coro_t，不受影响
    co_return expected<void>{};
}

canceler_t canceler;
co_await with_cancel(my_handler(), canceler);  // 注入 canceler 到 promise
```

---

## IO 操作的返回值约定

| 操作 | 返回类型 | 成功值 | 失败值 |
|------|---------|--------|--------|
| `recv` | `expected<int>` | 接收字节数 | errno |
| `send` | `expected<int>` | 发送字节数 | errno |
| `accept` | `expected<int>` | 新 fd | errno |
| `connect` | `expected<void>` | — | errno |
| `close` | `expected<void>` | — | errno |
| `sleep` | `expected<void>` | — | errno |
| `with_timeout` | `expected<int>` | 操作结果 | ETIMEDOUT 或 errno |
| `resolve` | `expected<resolved_address>` | 地址 | EAI_* |

---

## 异常 vs expected

Cornet 内部使用两种错误传播机制：

| 场景 | 机制 | 原因 |
|------|------|------|
| IO 操作结果 | `expected<T>` | 零开销，无栈展开 |
| 协程内 throw | `std::exception_ptr` | 兼容已有 C++ 代码 |
| ctx.async 线程池异常 | `std::exception_ptr` | 跨线程异常传播 |

```cpp
// IO 错误走 expected
auto n = co_await sock.recv(buf, len);
if (!n) { /* 处理 */ }

// 业务逻辑可以用 throw（协程会捕获并存储）
coro_t<Data> parse(Buffer buf) {
    if (buf.empty()) throw std::invalid_argument("empty buffer");
    co_return parse_impl(buf);
}

// 调用方
try {
    auto data = co_await parse(buf);
} catch (const std::invalid_argument& e) {
    // 在 co_await 处重新抛出
}
```
