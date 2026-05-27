# 协程与错误处理

## coro_t\<T\>

`coro_t<T>` 是 Cornet 的核心协程类型，封装 C++20 coroutine handle 并管理其生命周期。

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
