# 通用 Awaiter

## 概述

`include/core/awaiters.h` 提供不依赖 socket 的通用 io_uring awaiter。

## close_awaiter

异步关闭文件描述符。

```cpp
// 直接使用
co_await close_awaiter(fd);

// 便捷协程（适合 fire-and-forget）
ctx.spawn(async_close(fd));
```

返回 `expected<void>`。

## read_awaiter

异步读文件/设备。

```cpp
char buf[4096];
auto n = co_await read_awaiter(fd, buf, sizeof(buf), /*offset=*/0);
if (n) {
    int bytes_read = *n;
}
```

参数：
- `fd` — 文件描述符
- `buf` — 读缓冲区
- `nbytes` — 最大读取字节数
- `offset` — 文件偏移（默认 0，对 socket 无效）

返回 `expected<int>`（读取字节数）。

## write_awaiter

异步写文件/设备。

```cpp
auto n = co_await write_awaiter(fd, data, len, /*offset=*/0);
if (n) {
    int bytes_written = *n;
}
```

参数同 `read_awaiter`，返回 `expected<int>`（写入字节数）。

## nop_awaiter

io_uring NOP 操作，用于测试或流水线占位。

```cpp
co_await nop_awaiter();
```

返回 `expected<void>`。

## 通用 IO（ctx.io）

对于没有现成 awaiter 的 io_uring 操作，使用 `ctx.io()`：

```cpp
// 任意 io_uring 操作
auto ret = co_await ctx.io([fd, buf, n](io_uring_sqe* sqe) {
    io_uring_prep_readv(sqe, fd, iovs, nr_iovs, 0);
});

// statx
struct statx stx{};
auto ret = co_await ctx.io([fd, &stx](io_uring_sqe* sqe) {
    io_uring_prep_statx(sqe, fd, "", AT_EMPTY_PATH, STATX_SIZE, &stx);
});

// mkdir
auto ret = co_await ctx.io([](io_uring_sqe* sqe) {
    io_uring_prep_mkdirat(sqe, AT_FDCWD, "/tmp/mydir", 0755);
});
```

## 自定义 Awaiter

继承 `utask_t` 创建自定义 awaiter：

```cpp
struct my_awaiter : utask_t {
    int fd_;
    // ... 其他参数

    my_awaiter(int fd, ...) : fd_(fd) {
        this->ctx = &context_t::current();
        this->prepare_fn = [](utask_t* self, io_uring_sqe* sqe) {
            auto* t = static_cast<my_awaiter*>(self);
            // 用 liburing API 填充 sqe
            io_uring_prep_xxx(sqe, t->fd_, ...);
        };
    }

    // 可选：覆盖 await_resume 自定义返回类型
    expected<MyResult> await_resume() const {
        if (value < 0) return unexpected(-value);
        return MyResult{value};
    }
};
```

关键约束：
- 必须设置 `this->ctx`
- 必须设置 `this->prepare_fn`
- `prepare_fn` 中通过 `static_cast` 访问子类成员
- `value` 字段在 CQE 到达后由框架设置为 `cqe->res`
