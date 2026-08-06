# HTTP/1.1 模块设计方案

> 目标：在 Cornet 现有协程 + io_uring 框架之上，以 llhttp 为协议解析内核，提供**易用**（5 行起一个 server）且**高性能**（稳态零分配、每请求 ≤1 次 syscall 摊销）的 HTTP 接口。
>
> 范围：HTTP/1.0 & HTTP/1.1 的 Server 与 Client。不含 TLS、HTTP/2、WebSocket（预留扩展点）。

---

## 1. 现有框架能力评估

阅读 `include/cornet/**` 与 `src/**` 后，与 HTTP 层相关的能力分布如下。

### 1.1 可直接复用

| 能力 | 位置 | HTTP 层用法 |
|------|------|-------------|
| `coro_t<V>` / `ccoro_t<V>` | `coroutine/coro.h` | 连接协程用 `coro_t`（零开销）；需要整体超时的请求处理用 `ccoro_t` |
| `utask_t` awaiter 基类（函数指针 prepare_fn，栈上对象，无堆分配） | `io_uring/utask.h` | 所有 HTTP IO 走既有 awaiter，无需新 IO 机制 |
| `expected<T>` + `error_domain` | `base/expected.h` | HTTP 层沿用同一错误通道，仅新增一个 domain |
| `with_timeout(ctx, awaiter, dur)`（`IOSQE_IO_LINK` + `link_timeout`，**零额外 syscall**） | `concurrency/combinators.h` | keep-alive idle 超时、首字节超时的首选实现 |
| `with_timeout(ctx, ccoro_t, dur)` | 同上 | 整个 handler 的兜底超时（代价较高，默认关闭） |
| `canceler_t` 层级取消 | `coroutine/cancel.h` | 优雅关闭时批量中断连接上的 inflight IO |
| `tcp::socket_t`：`accept` / `recv` / `send` / `shutdown` / `close` | `net/socket.h` | 连接读写主路径 |
| `splice_awaiter` / `splice_forward` | `io_uring/awaiters.h` | 静态文件响应零拷贝 |
| `ctx.io(prep_fn)` 泛型 SQE 逃生口、`ctx.io_detach` | `scheduling/context.h` | 表达框架尚未封装的 op（如 `writev`），关闭 fd 不占协程 |
| `ctx.async(fn)` 线程池卸载 | 同上 | 阻塞型 handler（DB、压缩、模板渲染）卸载 |
| `runtime_t` shared-nothing 多线程 | `scheduling/runtime.h` | 每线程一个 listener + 一个 `context_t`，router 只读共享 |
| `context_t` 三阶段优雅关闭（drain → timeout → cancel）与 `user_idle()` 语义 | `scheduling/context.h` | HTTP server 的 graceful shutdown 直接挂靠，不需要自建状态机 |
| `config_t`（TOML）、`CORNET_METRICS` | `utils/config.h`、`base/metrics.h` | HTTP 配置与指标沿用同一体系 |
| `ringbuffer_t`、`buffer_t<T,N>` | `utils/ringbuffer.h` | 缓冲池的底层容器可复用思路（非 SPSC 场景直接用侵入式 freelist） |

### 1.2 需要新增/补齐的框架能力

| 缺口 | 影响 | 处置 |
|------|------|------|
| `tcp::socket_t` 没有公开 `sendmsg`/`writev` 入口（`socket_t::sendmsg_awaiter` 已存在于基类，仅 `udp` 暴露） | 无法一次 syscall 写「头 + 外部 body」两段 iovec，被迫拷贝 body | **P0**：在 `cornet::tcp::socket_t` 暴露 `sendmsg`（1 个转发函数），另加 `writev_awaiter` |
| 没有 `send_all` 语义（短写需要循环） | 每个调用方重复写循环 | **P0**：HTTP 层内部实现 `send_all`，或提升到 `socket_t` |
| `utask_t` 假设「一次提交 = 一个 CQE」（`complete()` 后即完成） | 无法用 multishot recv / multishot accept / provided buffer ring | **P2**：新增 `mutask_t`（处理 `IORING_CQE_F_MORE`）+ `buf_ring_t`；作为可选加速路径，先测后用 |
| `io_uring_queue_init` 只读 `flags` 整数，无 `SUBMIT_ALL` / `COOP_TASKRUN` / `SINGLE_ISSUER` 等命名开关 | 少量可白拿的吞吐 | **P3**：配置层加命名开关，与 HTTP 无强耦合 |
| `error_domain` 无协议错误域 | llhttp 错误只能塞进 `internal` | **P0**：新增 `error_domain::http`，`message()` 走 `llhttp_errno_name` |

上述 P0 项改动极小（约 40 行框架代码），是本方案的前置依赖。

### 1.3 现有约定（必须遵守）

- 命名：类型 `xxx_t`、snake_case、成员 `xxx_`、头文件宏 `CORNET_XXX_H`；2 空格缩进。
- 错误：**公开 API 不抛异常**，一律 `expected<T>`；异常仅作 bug 安全网。
- 头文件放 `include/cornet/<module>/`，实现放 `src/`，CMake 显式列源文件。
- 单线程内无锁、无原子；跨线程只走 `spawn_remote`。

---

## 2. 设计目标

**易用性**

1. Hello World ≤ 5 行有效代码，不暴露 buffer / parser / SQE 等概念。
2. 分三层，逐层下沉：`server_t + router`（应用层）→ `connection_t + parser_t`（协议层）→ awaiter（IO 层）。任何一层都可单独使用。
3. handler 签名同步直观：`coro_t<void>(request_t&, response_t&)`，错误照旧用 `expected` 检查。
4. 与现有 API 同构：`co_await`、`expected`、`ctx.spawn`、`runtime_t`，无新范式。

**性能**（目标值见 §11）

1. 稳态零堆分配：解析、路由、响应组装全程不 `malloc`。
2. 零拷贝解析：header/body 视图直接指向 recv 缓冲。
3. 每请求 syscall 摊销 ≤ 1：pipelining 批量收割 + 批量提交，`recv`/`send` SQE 合并在同一 `io_uring_submit`。
4. 不引入虚函数/`shared_ptr` 到请求热路径（`std::function` 仅出现在注册期）。

**非目标**：TLS、HTTP/2/3、CGI、完整的 URL 规范化。WebSocket 仅保留 upgrade 出口。

---

## 3. 整体架构

```
┌──────────────────────────────────────────────────────────────────┐
│ 应用层        server_t / router_t / filter chain / client_t       │
│               handler: coro_t<void>(request_t&, response_t&)      │
├──────────────────────────────────────────────────────────────────┤
│ 消息层        request_t   response_t   headers_t   body_reader_t   │
│               (全部为 offset 视图，指向 buffer_t，无所有权)         │
├──────────────────────────────────────────────────────────────────┤
│ 协议层        parser_t(llhttp)      serializer_t                  │
│               connection_t：读循环 / pipelining / keep-alive       │
├──────────────────────────────────────────────────────────────────┤
│ 缓冲层        buffer_t（连续可增长，偏移寻址） buffer_pool_t        │
├──────────────────────────────────────────────────────────────────┤
│ Cornet 现有   recv/send/sendmsg/splice awaiter · with_timeout ·    │
│               canceler_t · context_t · runtime_t                  │
└──────────────────────────────────────────────────────────────────┘
```

一次 keep-alive 请求的数据流：

```
recv CQE ──► buffer_t.commit(n)
              │
              ├─► parser_t.execute(新增区间)         llhttp 回调仅记录 (offset,len)
              │     on_header_field/value  → headers_t 追加 header_ref
              │     on_body               → body 区间（或 pause 交给流式读取）
              │     on_message_complete   → 一条 request_t 就绪
              │
              ├─► router.match(method, path) → handler
              ├─► co_await handler(req, resp)        resp 写入 out buffer
              │
              └─► buffer 中还有剩余字节？→ 回到 parser（pipelining，不 flush）
                                        否 → flush()：一次 sendmsg 写出全部响应
```

关键点：**一次 `recv` 内的多个 pipelined 请求共用一次 `flush`**；`submit` 由 scheduler 统一批量执行，因此稳态下 `syscall/请求` 远小于 1（在纯 pipelining 压测中）、约等于 1（在非 pipelining 场景为 1 recv + 1 send 两个 SQE 合并进同一次 submit）。

---

## 4. 目录与构建

```
include/cornet/http/
  common.h       # method_t / status_t / version_t / field_t / error 域
  buffer.h       # buffer_t + buffer_pool_t
  headers.h      # header_ref / headers_t
  message.h      # request_t / response_t / body_reader_t / body_writer_t
  parser.h       # llhttp 封装
  serializer.h   # 状态行/头部编码、date 缓存、整数快速格式化
  connection.h   # 单连接读写循环
  router.h       # 静态表 + radix trie
  server_t.h → server.h
  client.h       # 客户端 + 连接池
include/cornet/http.h        # 聚合头（不并入 cornet.h，保持核心零依赖）
src/http/
  buffer.cc parser.cc serializer.cc connection.cc router.cc server.cc client.cc
tests/http_buffer.cc tests/http_parser.cc tests/http_server.cc tests/http_client.cc
bench/http_bench.h
```

> `src/` 现为平铺布局；HTTP 是首个多文件模块，故新开 `src/http/` 子目录。若坚持平铺，则用 `src/http_*.cc` 前缀。

**构建变更**

```cmake
# vcpkg.json 增加 "llhttp"
option(CORNET_WITH_HTTP "Build the cornet http module" ON)
if(CORNET_WITH_HTTP)
    find_package(llhttp CONFIG REQUIRED)   # vcpkg 静态口：llhttp::llhttp_static
    add_library(cornet_http STATIC src/http/buffer.cc src/http/parser.cc ...)
    add_library(cornet::http ALIAS cornet_http)
    target_link_libraries(cornet_http PUBLIC cornet PRIVATE llhttp::llhttp_static)
endif()
```

独立 target 的理由：README 承诺「核心仅依赖 liburing」。llhttp 只被 `cornet_http` 私有链接，核心用户不受影响；`llhttp.h` 不出现在任何公开头文件中（见 §6.4 的 pimpl 约定）。

---

## 5. 缓冲层：偏移寻址是整个设计的支点

零拷贝解析的难点是 llhttp 回调给的是**指向本次投喂 chunk 的裸指针**，而缓冲区会因扩容而搬移。解法：**所有视图存 `(uint32_t offset, uint32_t len)`，不存指针**，扩容后偏移天然有效。

```cpp
class buffer_t {
 public:
  explicit buffer_t(uint32_t capacity);

  char*        base()  { return data_; }
  uint32_t     readable() const { return w_ - r_; }
  std::span<char> writable(uint32_t hint);   // 保证 ≥hint 可写：先左移整理，不够再扩容
  void         commit(uint32_t n)  { w_ += n; }   // recv 完成
  void         consume(uint32_t n) { r_ += n; if (r_ == w_) r_ = w_ = 0; }

  std::string_view view(uint32_t off, uint32_t len) const { return {data_ + off, len}; }
  uint32_t     offset_of(const char* p) const { return uint32_t(p - data_); }
 private:
  char* data_; uint32_t cap_, r_{0}, w_{0};
};
```

- **扩容策略**：`max(cap*2, need)`，上限 `max_header_bytes`（头部阶段）/ `max_body_bytes`（body 阶段）。超限 → 431 / 413。
- **整理时机**：仅当 `r_ > 0 && 尾部空间不足` 时 `memmove`。整理会使已记录的偏移失效，故**规则：一条消息解析期间禁止整理**（消息边界处才 `consume` + 归零）。头部大小受限，该约束不会导致死锁。
- **`buffer_pool_t`**：每 `context_t` 一个（thread_local / 挂在 context），侵入式 freelist，按 4K/16K/64K 分档；连接进入 idle keep-alive 时归还读缓冲，避免万级空闲连接吃内存。归还即 `consume` 到空，无 memset。

---

## 6. 消息层与协议层

### 6.1 常见头部枚举化

```cpp
enum class field_t : uint8_t {
  host, content_length, content_type, transfer_encoding, connection,
  accept, accept_encoding, user_agent, expect, upgrade, /* ... */ other
};
```

在 `on_header_field` 回调里就用「长度 + 首字符 + 小写化比较」的分支表识别（不做哈希表查找），结果存进 `header_ref::field`。之后 `Content-Length` / `Connection` 之类的判断退化为整数比较；`headers_t` 另存一个 `uint32_t field_bitmap_`，`has(field_t)` 是一次位测试。

### 6.2 headers_t

```cpp
struct header_ref {
  uint32_t name_off, value_off;
  uint16_t name_len, value_len;
  field_t  field;
};

class headers_t {
 public:
  std::string_view get(field_t f) const;              // 位图预判 + 短扫描
  std::string_view get(std::string_view name) const;  // 大小写不敏感
  bool has(field_t f) const;
  // range-for 支持
 private:
  static constexpr size_t kInline = 32;
  header_ref  inline_[kInline];
  uint16_t    size_{0};
  uint32_t    field_bitmap_{0};
  std::vector<header_ref>* overflow_{nullptr};  // >32 个头才分配，罕见
  buffer_t*   buf_;
};
```

内联 32 项（约 512B）覆盖绝大多数真实请求，稳态零分配。

### 6.3 分片处理（llhttp 的必修课）

llhttp 会在跨 `recv` 边界时把一个 header value 拆成多次回调。因为所有数据都投喂进**同一个连续 buffer**，两次回调的区间在 buffer 中天然相邻：

```cpp
// on_header_value(at, len)
uint32_t off = buf_->offset_of(at);
if (cur_.value_len && cur_.value_off + cur_.value_len == off) {
  cur_.value_len += len;              // 相邻 → 直接延长，零拷贝
} else if (cur_.value_len) {
  spill(at, len);                     // 不相邻（obs-fold 等）→ 落到 spill 小缓冲
} else {
  cur_.value_off = off; cur_.value_len = len;
}
```

`spill_` 是每连接一小块（默认 512B）追加缓冲，仅在异常形态下使用，并计入 metrics 以便发现真实流量中的比例。

### 6.4 parser_t

```cpp
class parser_t {                       // 不在头文件暴露 llhttp.h
 public:
  enum class type_t  { request, response };
  enum class result_t { need_more, message_ready, body_paused, upgrade, error };

  explicit parser_t(type_t t);
  void     reset();                                  // 复用同一连接的下一条消息
  result_t execute(buffer_t& buf, uint32_t off, uint32_t len);
  error_t  error() const;                            // error_domain::http
  uint32_t consumed() const;                         // pipelining：本条消息终点

 private:
  alignas(16) unsigned char st_[kLlhttpStateSize];   // llhttp_t 的存储（静态断言校验大小）
  // ...
};
```

设计要点：

- `llhttp_settings_t` **全局唯一、`const`、静态初始化**（每 parser 拷贝一份 settings 是常见的无谓开销）。回调是无捕获静态函数，`data` 指回 `parser_t`。
- **严格模式**：默认关闭所有 `lenient_*`（`LENIENT_HEADERS`、`LENIENT_CHUNKED_LENGTH`、`LENIENT_KEEP_ALIVE`…），从源头挡掉 request smuggling 变体；可通过配置逐项放宽。
- **流式 body**：`on_body` 返回 `HPE_PAUSED`，控制权回到连接协程；消费者读走后 `llhttp_resume` 继续。以此实现「不缓存整个 body」的上传处理。
- **Upgrade**：`on_message_complete` 后若 `llhttp_get_upgrade()`，返回 `result_t::upgrade`，把 socket 与 buffer 剩余字节交给用户回调（WebSocket 的接入点）。
- 头文件不含 `llhttp.h`：用固定大小字节数组 + `static_assert(sizeof(llhttp_t) <= kLlhttpStateSize)`（在 .cc 里断言）避免 pimpl 的堆分配，同时保持依赖私有。

### 6.5 request_t / response_t

```cpp
class request_t {
 public:
  method_t         method() const;
  std::string_view target() const;    // 原始 request-target
  std::string_view path() const;      // 惰性百分号解码，需要时才写入 spill
  const headers_t& headers() const;
  bool             keep_alive() const;
  std::string_view param(std::string_view name) const;  // 路由参数
  query_t          query() const;                       // 惰性解析，range-for

  std::string_view body() const;      // 聚合模式
  body_reader_t&   stream();          // 流式模式：co_await reader.read()
};

class response_t {
 public:
  response_t& status(status_t s);
  response_t& header(field_t f, std::string_view v);      // 预编码名字
  response_t& header(std::string_view n, std::string_view v);
  void body(std::string_view data);                       // 小 body：拷进 out buffer
  void body_ref(std::span<const char> data);              // 大 body：iovec 引用，生命周期需活到 flush
  coro_t<expected<void>> send_file(int fd, uint64_t len); // splice 零拷贝
  body_writer_t chunked();                                // 流式 / SSE
};
```

**易用性糖**（覆盖 90% 场景）：`resp.text("hi")`、`resp.json(sv)`、`resp.not_found()`、`resp.redirect(url)`。

### 6.6 serializer_t：写出路径的性能细节

- 状态行：`HTTP/1.1 200 OK\r\n` 全量静态表，按 status 直接 `memcpy`（不做 `sprintf`）。
- `Date`：每 `context_t` 缓存一份 IMF-fixdate 字符串，秒级刷新（比对 `CLOCK_REALTIME_COARSE`），避免每请求 `gmtime`+格式化。
- `Content-Length`：手写 `u64 → dec` 快速格式化，写进固定 20 字节槽位。
- 头部名：`field_t` 对应的 `"Content-Type: "` 等带冒号空格的字面量直接 `memcpy`。
- 写出：`headers`（out buffer）+ `body_ref`（外部内存）→ 两段 `iovec` 一次 `sendmsg`。多个 pipelined 响应 → 多段 iovec 一次写出（上限 `IOV_MAX`，超出则分批）。
- 短写处理：`send_all` 循环，剩余 iovec 原地推进偏移，不重建。

### 6.7 connection_t

```cpp
class connection_t {
 public:
  connection_t(context_t& ctx, tcp::socket_t sock,
               const server_options_t& opt, buffer_pool_t& pool);
  coro_t<void> run(const router_t& router);   // 连接生命周期 = 该协程生命周期
};
```

主循环（伪码，省略错误分支）：

```cpp
coro_t<void> connection_t::run(const router_t& router) {
  while (ctx_.is_running()) {
    // 1. 读：首字节用 idle 超时，后续用 header 超时（都走 link_timeout，零额外 syscall）
    auto n = co_await with_timeout(ctx_, sock_.recv(ctx_, w.data(), w.size()),
                                   first_ ? opt_.idle_timeout : opt_.header_timeout);
    if (!n || *n == 0) break;                  // 对端关闭 / 超时 / ECANCELED(优雅关闭)
    in_.commit(*n);

    // 2. 解析 + 处理：把本次 recv 里的所有完整请求都消费掉（pipelining）
    while (in_.readable()) {
      auto r = parser_.execute(in_, ...);
      if (r == need_more) break;
      if (r == error)     { write_error(parser_.error()); goto flush_and_close; }
      co_await dispatch(router, req_, resp_);  // 响应写入 out_，此处不 flush
      if (++pipelined_ >= opt_.max_pipelined) break;   // 背压
      if (!req_.keep_alive()) { close_after_flush_ = true; break; }
    }

    // 3. 写：一次 sendmsg 写出本轮全部响应
    if (!co_await flush()) break;
    if (close_after_flush_) break;
    in_.consume_processed();
  }
  co_await graceful_close();   // shutdown(SHUT_WR) → 读到 EOF/超时 → io_detach close
}
```

要点：

- 连接状态**全部在协程帧内**（`in_`/`out_` 是池句柄，`parser_`、`req_`、`resp_` 是成员），无 `shared_ptr`，无引用计数。
- 优雅关闭无需额外机制：`ctx.shutdown()` 进入 Canceling 后取消 inflight recv → `recv` 返回 `ECANCELED` → 循环自然退出。keep-alive 空闲连接因此能被立刻回收。
- `close` 用 `ctx.io_detach`（fire-and-forget），不为关闭再占一个协程帧。
- 半关闭：`shutdown(SHUT_WR)` 后短暂读 drain，避免对端收到 RST 丢响应。

---

## 7. 路由与中间件

```cpp
class router_t {
 public:
  router_t& route(method_t m, std::string_view path, handler_t h);
  router_t& get(std::string_view p, handler_t h);   // post/put/del/patch/head...
  router_t& mount(std::string_view prefix, router_t sub);
  router_t& fallback(handler_t h);

  // 运行期只读：可安全被多线程共享
  match_t match(method_t m, std::string_view path, param_slots_t& out) const;
};
```

- **静态路径**（无参数）：启动时构建 open-addressing flat table，key = `hash(method, path)`，命中 O(1)，无节点跳转。
- **动态路径**：`:name` / `*rest` 走 radix trie，参数以 `(offset,len)` 写入调用方栈上的 `param_slots_t`（固定 8 槽），零分配。
- **构建期完成全部分配**，`match()` 是 `const` 且无副作用 → `runtime_t` 下所有线程共享同一份 `const router_t&`，无锁无拷贝。
- **中间件**：`filter_t = coro_t<void>(request_t&, response_t&, next_t)`，链表在注册期展开成数组。未注册任何 filter 时 `dispatch` 直接调 handler，无额外帧。

---

## 8. Server API（易用性验收）

最简：

```cpp
#include <cornet/http.h>
using namespace cornet;

int main() {
  context_t ctx;
  http::server_t server(ctx);
  server.get("/hello", [](http::request_t&, http::response_t& resp) -> coro_t<void> {
    resp.text("hello cornet");
    co_return;
  });
  server.listen("0.0.0.0", 8080);
  ctx.spawn(server.serve());
  ctx.run();
}
```

多线程（thread-per-core + `SO_REUSEPORT`，router 只读共享）：

```cpp
int main() {
  runtime_t rt;
  http::serve(rt, {.port = 8080}, [](http::router_t& r) {
    r.get("/users/:id", [](auto& req, auto& resp) -> coro_t<void> {
      resp.json(fetch_user(req.param("id")));
      co_return;
    });
  });   // 内部：每线程建 listener、共享 const router、绑定信号做 graceful shutdown
}
```

流式上传 / 下载：

```cpp
server.post("/upload", [](http::request_t& req, http::response_t& resp) -> coro_t<void> {
  auto& body = req.stream();
  uint64_t total = 0;
  while (auto chunk = co_await body.read()) {   // expected<std::string_view>，零拷贝
    if (chunk->empty()) break;
    total += chunk->size();
  }
  resp.text(std::format("{} bytes", total));
});

server.get("/big", [](auto&, http::response_t& resp) -> coro_t<void> {
  auto w = resp.chunked();                      // 自动 Transfer-Encoding: chunked
  for (int i = 0; i < 100; ++i) co_await w.write(make_chunk(i));
  co_await w.finish();
});
```

配置对象：

```cpp
struct server_options_t {
  uint16_t     port              = 8080;
  std::string  address           = "0.0.0.0";
  uint32_t     max_header_bytes  = 16 * 1024;
  uint16_t     max_headers       = 64;
  uint64_t     max_body_bytes    = 8ull << 20;
  uint16_t     max_pipelined     = 32;
  uint32_t     read_buffer_bytes = 16 * 1024;
  uint32_t     write_buffer_bytes= 8  * 1024;
  std::chrono::milliseconds idle_timeout{60'000};
  std::chrono::milliseconds header_timeout{10'000};
  std::chrono::milliseconds body_timeout{30'000};
  std::chrono::milliseconds handler_timeout{0};   // 0 = 关闭（开启需 ccoro_t handler）
  bool tcp_nodelay = true, reuse_port = true, serve_date_header = true;
};
```

同时支持 TOML（与现有配置体系一致）：

```toml
[cornet.http.server]
max_header_bytes = 16384
idle_timeout = "60s"
max_pipelined = 32
```

---

## 9. Client API

```cpp
class client_t {                 // 绑定单个 context，内部无锁
 public:
  explicit client_t(context_t& ctx, client_options_t opt = {});

  ccoro_t<expected<response_view_t>> get(std::string_view url);
  ccoro_t<expected<response_view_t>> request(method_t m, std::string_view url,
                                             headers_view_t hs = {},
                                             std::span<const char> body = {});
};
```

- 返回 `ccoro_t` → 天然支持 `co_await with_timeout(ctx, client.get(url), 3s)`，内部所有 IO 自动可取消（框架已有能力）。
- **连接池**：key = `(host, port)`，per-context 无锁；空闲连接带 idle 定时器，被复用时校验对端是否已关闭（`recv(MSG_PEEK|MSG_DONTWAIT)` 或读到 EOF 即丢弃重连）。
- DNS 直接用现有 `resolve()`（IP 走快路，域名走线程池），并加 per-context 正/负结果缓存（TTL 可配）。
- `response_view_t` 的 body 默认聚合（受 `max_body_bytes` 限制），也可切流式读取。
- 重定向、`Content-Encoding` 解压：默认关闭，作为显式开关（不为未使用的功能付费）。

---

## 10. 错误处理、超时与安全

### 10.1 错误模型

新增域，与现有 `expected` 无缝：

```cpp
enum class error_domain : uint8_t { none, system, resolve, internal, exception, http /* 新增 */ };
// message() 分支：case http: return llhttp_errno_name((llhttp_errno)code);
```

协议错误 → 自动应答并关闭连接：

| 情况 | 响应 |
|------|------|
| llhttp 解析失败 | 400 Bad Request |
| 头部超过 `max_header_bytes` / 数量超限 | 431 Request Header Fields Too Large |
| body 超过 `max_body_bytes` | 413 Content Too Large |
| 未知 method / 版本 | 501 / 505 |
| 无路由匹配 | 404（或 `fallback`） |
| handler 抛异常（bug 安全网） | 500 + `SPDLOG_ERROR` |
| `Expect: 100-continue` | 自动回 `100 Continue` 后再读 body（可配置为 417） |

### 10.2 超时分层（成本从低到高）

1. **idle / header / body 超时**：`with_timeout(ctx, recv_awaiter, dur)` → `IOSQE_IO_LINK` + `link_timeout`，与 recv 同批提交，**零额外 syscall、零额外协程**。这是默认防线。
2. **handler 超时**：`with_timeout(ctx, ccoro_t, dur)` → 会额外 spawn 2 个协程 + 1 个 timer，代价明显，故默认关闭，由业务显式开启。
3. **全局关闭超时**：复用 `ctx.shutdown(timeout)` 的 drain 阶段。

### 10.3 安全默认值

- 关闭全部 llhttp lenient 开关：`Content-Length` 与 `Transfer-Encoding` 同时出现、重复 CL、chunk 长度异常等一律 400（request smuggling 防线）。
- 限制：URL 长度、header 数量与总字节、chunk 扩展长度、pipelining 深度、单连接内存上限；全部可配且有保守默认。
- 语义正确性（易错点，纳入测试）：HEAD 不发 body、204/304 不发 body 且不发 `Content-Length`、HTTP/1.0 默认 `Connection: close`、`Connection: keep-alive` 显式处理、响应同时给出 CL 与 chunked 视为错误。
- 慢速攻击：header 超时 + idle 超时 + 单连接读缓冲上限即可覆盖 slowloris；连接数上限交由 `server_options_t::max_connections`（超限直接 `close`，不进协程）。

---

## 11. 性能设计要点与验收目标

### 11.1 已在设计中固化的优化

| 手段 | 说明 |
|------|------|
| 偏移视图 | header/body 零拷贝，扩容不失效（§5） |
| 每请求零分配 | 内联 32 header + 池化 buffer + 栈上参数槽 + 无 `shared_ptr` |
| 常见头枚举化 | 热路径头判断退化为整数/位比较 |
| Pipelining 批处理 | 一次 recv 消费 N 个请求，一次 sendmsg 写 N 个响应 |
| 双 iovec 写出 | 头部与外部 body 不拷贝合并 |
| 静态表 | 状态行、常见 header 名、`Date` 秒级缓存、手写整数格式化 |
| 静态 `llhttp_settings_t` | 每连接省一次 settings 拷贝与其 cache 占用 |
| 路由 O(1) | 静态路径 flat hash，动态路径 radix，构建期完成分配 |
| fire-and-forget close | `io_detach`，关闭不占协程帧 |
| shared-nothing 多线程 | `SO_REUSEPORT` per-thread listener，router 只读共享，无锁 |

### 11.2 P2 阶段（先测量再落地）

- **multishot recv + `io_uring_buf_ring`**：一次 SQE 供多次接收，内核直接填入 provided buffer，省下每次 recv 的 SQE 与 buffer 绑定；对万级空闲长连接收益最大（每连接不再常驻一个读缓冲）。需要框架 `mutask_t` 支持 `IORING_CQE_F_MORE`。
- **multishot accept**：accept 循环由 N 次 SQE 变 1 次。
- **`IORING_OP_SEND_ZC`**：大响应（≥ 32KB）零拷贝发送；小响应反而更慢，需按大小切换。
- **coroutine frame 池化**：`coro.h` 已 include `<memory_resource>` 但未使用；可为 handler/连接协程的 `promise_type` 提供 `operator new`，从 per-context pool 取帧，消除每请求一次 frame 分配（若 handler 用 `std::function` 返回 `coro_t`，这一次分配是当前设计里唯一的常规分配）。

### 11.3 验收目标（相对指标，避免机器差异）

| 指标 | 目标 |
|------|------|
| HTTP 层相对开销 | `hello world` RPS ≥ 同机 raw echo bench 的 85% |
| 稳态堆分配 | 每请求 0 次 `malloc`（计数器 + ASAN 验证） |
| syscall 摊销 | 非 pipelining ≤ 1 次 `io_uring_enter`/请求；pipelining(16) ≤ 0.15 |
| 解析开销 | 典型 200B 头部 ≤ 1 µs/请求（单核） |
| 每连接稳态内存 | 空闲 keep-alive ≤ 1KB（缓冲已归还池）；活跃 ≤ 32KB |
| 尾延迟 | 10K 并发下 p99 不超过 p50 的 10 倍 |

对比基线：nginx（单 worker）、Boost.Beast、drogon；压测工具 `wrk` / `h2load --h1` / `oha`，含 pipelining 与非 pipelining 两组。

---

## 12. 测试计划

- **单元**：`buffer_t` 扩容/整理/偏移不变量；分片投喂（逐字节喂完整报文，断言结果与整块投喂一致）；chunked 编解码（含 trailer、chunk 扩展）；pipelining；HEAD/204/304 语义；limits 触发；router 匹配（静态/参数/通配/优先级）。
- **模糊**：对 `parser_t` 做 libFuzzer/AFL，语料取 llhttp 与 h1spec 用例；断言「永不越界、永不 UB、错误必被分类」。
- **一致性**：跑 `h1spec` 类符合性用例集；与 curl / nginx 行为对齐若干边界。
- **集成**：`ctx.run()` 内起 server + client 对压，覆盖优雅关闭（关闭中请求不被截断）、超时、连接复用。
- **回归基准**：`bench/http_bench.h` 接入现有 bench target，CI 记录 RPS 与 alloc 计数，防性能回退。

现有测试用 GTest（`CORNET_ENABLE_TESTS`），新增文件挂到 `unit` target 即可。

---

## 13. 里程碑

| 阶段 | 内容 | 交付判据 |
|------|------|----------|
| **M0** | 框架 P0 缺口：`tcp::sendmsg` 暴露、`writev_awaiter`、`error_domain::http` | 单测通过，不影响现有用例 |
| **M1** | `buffer_t` + `parser_t` + `headers_t` + `request/response` + `connection_t` + 最小 `server_t`（keep-alive、pipelining、CL body） | `wrk` 可压通，hello world RPS 达标 |
| **M2** | `router_t` + filter + chunked/流式 body + 全套超时与 limits + 优雅关闭 + HEAD/304 语义 + TOML 配置 | 符合性用例与 fuzz 通过 |
| **M3** | `client_t` + 连接池 + DNS 缓存；`send_file`（splice） | 客户端集成测试通过 |
| **M4** | io_uring 进阶（multishot recv + buf_ring + send_zc）、协程帧池化、性能调优 | 相对开销与内存目标达标 |
| **M5（评估）** | TLS（kTLS / OpenSSL BIO）、WebSocket upgrade、HTTP/2 可行性 | 仅出方案，不承诺 |

---

## 14. 风险与开放问题

1. **`sizeof(llhttp_t)` 内联存储**：升级 llhttp 可能改变结构体大小。用 `static_assert` 在 .cc 里守护，并留 25% 余量；失败即编译期暴露，不会是运行期问题。
2. **偏移不变量的可维护性**：「消息解析期间禁止整理 buffer」是隐式契约。用 debug 断言（记录 `base()`，回调期比对）固化，Release 零成本。
3. **`std::function` handler 的分配**：注册期一次分配可接受，但 handler 返回 `coro_t` 会每请求一次 frame 分配。M1 先接受，M4 用 `promise_type::operator new` + per-context pool 消除。
4. **`ccoro_t` handler 与零开销的取舍**：默认 `coro_t`（无取消），需要 handler 级超时时用 `ccoro_t` 重载。两套签名会带来少量 API 表面积膨胀，倾向用同一模板 `dispatch` 吸收。
5. **multishot + provided buffer 的收益不确定**：在小消息高并发下可能不如现在的 per-connection buffer（buf_ring 耗尽时的退化路径要处理）。列为 P2 并以基准决定去留。
6. **`buffer_pool_t` 归属**：挂 `context_t` 需要改核心；先做成 HTTP 层自持的 `thread_local` 单例，若后续 socket/文件模块也需要，再上提。
