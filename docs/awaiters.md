# 通用 Awaiter

## 概述

`include/io_uring/awaiters.h` 提供不依赖 socket 的通用 io_uring awaiter。

所有 awaiter 和 `async_*` 便捷函数都接受 `context_t& ctx` 作为第一个参数，用于获取 io_uring 上下文。

## close_awaiter

异步关闭文件描述符。

```cpp
// 协程中使用（等待关闭完成）
co_await close_awaiter(ctx, fd);
```

返回 `expected<void>`。

## async_close

Fire-and-forget 异步关闭。通过 `io_detach` 直接提交 SQE，无需等待 CQE。

```cpp
async_close(ctx, fd);  // 无需 co_await，fire-and-forget
```

## read_awaiter

异步读文件/设备。

```cpp
char buf[4096];
auto n = co_await read_awaiter(ctx, fd, buf, sizeof(buf), /*offset=*/0);
if (n) {
    int bytes_read = *n;
}
```

参数：
- `ctx` — context 实例
- `fd` — 文件描述符
- `buf` — 读缓冲区
- `nbytes` — 最大读取字节数
- `offset` — 文件偏移（默认 0，对 socket 无效）

返回 `expected<int>`（读取字节数）。

## write_awaiter

异步写文件/设备。

```cpp
auto n = co_await write_awaiter(ctx, fd, data, len, /*offset=*/0);
if (n) {
    int bytes_written = *n;
}
```

参数同 `read_awaiter`，返回 `expected<int>`（写入字节数）。

## nop_awaiter

io_uring NOP 操作，用于测试或流水线占位。

```cpp
co_await nop_awaiter(ctx);
```

返回 `expected<void>`。

## 通用 IO（ctx.io）

对于没有现成 awaiter 的 io_uring 操作，使用 `ctx.io()`：

```cpp
// 任意 io_uring 操作
auto ret = co_await ctx.io([fd, buf, n](io_uring_sqe* sqe) {
    io_uring_prep_readv(sqe, fd, iovs, nr_iovs, 0);
});
```

## shutdown_awaiter

异步半关闭 socket。

```cpp
co_await shutdown_awaiter(ctx, fd, SHUT_WR);   // 关闭写端
co_await shutdown_awaiter(ctx, fd, SHUT_RD);   // 关闭读端
co_await shutdown_awaiter(ctx, fd, SHUT_RDWR); // 关闭读写
```

返回 `expected<void>`。

## openat_awaiter

异步打开文件。

```cpp
// 打开已有文件
auto fd = co_await openat_awaiter(ctx, AT_FDCWD, "/path/to/file", O_RDONLY);

// 创建新文件
auto fd = co_await openat_awaiter(ctx, AT_FDCWD, "/tmp/out.txt", O_WRONLY | O_CREAT | O_TRUNC, 0644);
```

参数：
- `ctx` — context 实例
- `dirfd` — 目录 fd（`AT_FDCWD` 表示当前目录）
- `path` — 文件路径
- `flags` — open flags（O_RDONLY, O_WRONLY, O_CREAT 等）
- `mode` — 文件权限（默认 0，仅 O_CREAT 时有效）

返回 `expected<int>`（新 fd）。

## splice_awaiter

零拷贝在两个 fd 之间传输数据（至少一端必须是 pipe）。

```cpp
int pipefd[2];
pipe(pipefd);

// file → pipe
auto n = co_await splice_awaiter(ctx, file_fd, file_offset, pipefd[1], -1, 4096, SPLICE_F_MOVE);

// pipe → socket
co_await splice_awaiter(ctx, pipefd[0], -1, socket_fd, -1, *n, SPLICE_F_MOVE);
```

参数：
- `ctx` — context 实例
- `fd_in` / `fd_out` — 源/目标 fd
- `off_in` / `off_out` — 偏移（-1 表示用 fd 当前偏移，pipe 必须传 -1）
- `nbytes` — 传输字节数
- `flags` — splice flags

返回 `expected<int>`（实际传输字节数）。

## splice_forward

基于 splice 的零拷贝转发，自动管理内部 pipe。

```cpp
// 把文件内容零拷贝发给 socket（等效 sendfile）
auto total = co_await splice_forward(ctx, file_fd, socket_fd);

// 自定义 chunk 大小
auto total = co_await splice_forward(ctx, fd_in, fd_out, 131072);
```

返回 `coro_t<expected<size_t>>`（总传输字节数）。

## poll_add_awaiter

等待 fd 上有事件就绪。

```cpp
// 等待可读
auto mask = co_await poll_add_awaiter(ctx, eventfd, POLLIN);

// 等待可写
auto mask = co_await poll_add_awaiter(ctx, fd, POLLOUT);
```

返回 `expected<int>`（触发的事件 mask）。

## statx_awaiter

异步获取文件元信息。

```cpp
struct statx stx{};
co_await statx_awaiter(ctx, AT_FDCWD, "/path/to/file", 0, STATX_ALL, &stx);
// stx.stx_size, stx.stx_mode 等
```

返回 `expected<void>`。

## unlinkat_awaiter

异步删除文件或目录。

```cpp
// 删除文件
co_await unlinkat_awaiter(ctx, AT_FDCWD, "/tmp/file.txt", 0);

// 删除空目录
co_await unlinkat_awaiter(ctx, AT_FDCWD, "/tmp/dir", AT_REMOVEDIR);
```

返回 `expected<void>`。

## renameat_awaiter

异步重命名/移动文件。

```cpp
co_await renameat_awaiter(ctx, AT_FDCWD, "/tmp/old.txt", AT_FDCWD, "/tmp/new.txt", 0);
```

返回 `expected<void>`。

## mkdirat_awaiter

异步创建目录。

```cpp
co_await mkdirat_awaiter(ctx, AT_FDCWD, "/tmp/newdir", 0755);
```

返回 `expected<void>`。

## fsync_awaiter

异步刷盘。

```cpp
// 完整 fsync
co_await fsync_awaiter(ctx, fd);

// 仅同步数据（不含元数据），等效 fdatasync
co_await fsync_awaiter(ctx, fd, IORING_FSYNC_DATASYNC);
```

返回 `expected<void>`。

## async_* 便捷函数

所有 awaiter 都有对应的 `async_*` 自由函数包装，省去 `AT_FDCWD` 等样板参数。所有便捷函数都以 `context_t& ctx` 作为第一个参数：

```cpp
// 文件操作
auto fd = co_await async_open(ctx, "/path/to/file", O_RDONLY);
auto n  = co_await async_read(ctx, fd, buf, sizeof(buf));
auto w  = co_await async_write(ctx, fd, data, len);
co_await async_fsync(ctx, fd);
async_close(ctx, fd);

// 文件系统
struct statx stx{};
co_await async_statx(ctx, "/path", STATX_ALL, &stx);
co_await async_mkdir(ctx, "/tmp/newdir");
co_await async_rename(ctx, "/tmp/old.txt", "/tmp/new.txt");
co_await async_unlink(ctx, "/tmp/file.txt");

// 网络
co_await async_shutdown(ctx, sock_fd, SHUT_WR);
auto mask = co_await async_poll(ctx, fd, POLLIN);

// 零拷贝
auto n = co_await async_splice(ctx, fd_in, -1, fd_out, -1, 4096);
```

## 自定义 Awaiter

继承 `utask_t` 创建自定义 awaiter：

```cpp
struct my_awaiter : utask_t {
    int fd_;
    // ... 其他参数

    my_awaiter(context_t& ctx, int fd, ...) : fd_(fd) {
        this->ctx = &ctx;
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
