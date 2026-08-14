# HTTP/1.1 模块设计方案

> 目标：在 Cornet 现有协程 + io_uring 框架之上，以 llhttp 为协议解析内核，提供**优雅**（同步 handler 4 行起一个 server）、**易用**（不暴露 buffer / parser / SQE）且**高性能**（热路径零堆分配、SQE 数量不因超时而翻倍、空闲连接不驻留协程帧）的 HTTP 接口。
>
> 范围：HTTP/1.0 & HTTP/1.1 的 Server 与 Client。不含 TLS、HTTP/2、WebSocket（预留扩展点）。

---

## 0. 核心设计决策

后续章节是这七条决策的展开。它们互相咬合，改动其中一条会牵动其余。

| # | 决策 | 解决的问题 | 章节 |
|---|------|-----------|------|
| D1 | **头部 buffer 与 body buffer 分离**：头部固定 16KB 且消息期内永不搬移，body 按 `Content-Length` 精确预分配或强制流式 | 单 buffer 方案下 8MB body 会把连接内存和扩容 memcpy 都放大一个数量级；「解析期禁止整理」的隐式契约作用域过大 | §5 |
| D2 | **两段式 dispatch**：`HeadersReady` 先路由、处理 `Expect`、决定聚合/流式；`MessageReady` 再执行聚合 handler | 单段式（只在 MessageReady 派发）下流式上传永远读不到 body，`100-continue` 无处落脚 | §6.4 §6.7 |
| D3 | **同步 handler 为一等公民**，异步 `coro_t` 为第二重载 | 90% 的 handler 不需要挂起。同步分支零帧分配、零挂起，同时去掉 `co_return;` 噪音 | §7.1 |
| D4 | **per-context 时间轮**承担 idle/header/body 超时，`with_timeout` 只用于 client 一次性请求 | 逐 IO 挂 `link_timeout` 让 SQE/CQE 双倍，10K 连接下打满默认 2048 深度的 SQ | §10.2 |
| D5 | **response 写入按所有权分级**（拷贝 / 静态 / 移交 / pin）+ **错误 latch** | `body_ref` 的「活到 flush」契约在 API 上不可见，是最易出的 UAF；`void` 返回的写入 API 静默吞错 | §6.5 |
| D6 | **`scope_t` 驱动 server 自己的 drain 状态机**，不直接挂靠 `ctx.shutdown()` | `cancel_sweep()` 用 `CANCEL_ANY` 无差别取消，会截断正在发送的响应；listener 的常驻 accept 让 `user_idle()` 永假 | §6.9 §10.4 |
| D7 | **idle keep-alive 连接不驻留协程帧**，靠 multishot recv + `buf_ring` 复活 | 协程帧是一整块，无法部分释放；不做这一步，「空闲连接 ≤ 1KB」在数学上不可能 | §6.8 |

---

## 1. 现有框架能力评估

阅读 `include/cornet/**` 与 `src/**` 后，与 HTTP 层相关的能力分布如下。

### 1.1 可直接复用

| 能力 | 位置 | HTTP 层用法 |
|------|------|-------------|
| `coro_t<V>` / `ccoro_t<V>` | `coroutine/coro.h` | 连接协程用 `coro_t`（零开销）；client 请求用 `ccoro_t`（可整体取消） |
| `utask_t` awaiter 基类（函数指针 `prepare_fn`，栈上对象，无堆分配） | `io_uring/utask.h` | 所有 HTTP IO 走既有 awaiter，无需新 IO 机制 |
| `as_system(op)` 把 op 移出 user work 账 | 同上 | 时间轮的 tick 用它（§10.2）。**listener 的 accept 绝不能用**——见 §10.4 的更正 |
| `expected<T>` + `error_domain` | `base/expected.h` | HTTP 层沿用同一错误通道，仅新增一个 domain |
| `canceler_t` 层级取消 | `coroutine/cancel.h` | **每连接一个**，用于精确取消该连接的 idle recv，而非全局 sweep |
| `scope_t` 结构化并发（保证子任务不逃逸） | `concurrency/scope.h` | server 持有一个 scope，连接协程全部 spawn 进去；drain 时 `co_await scope` 即可确保响应写完 |
| `tcp::socket_t`：`accept` / `recv` / `send` / `shutdown` / `close` | `net/socket.h` | 连接读写主路径 |
| `splice_awaiter` / `splice_forward` | `io_uring/awaiters.h` | 静态文件响应零拷贝 |
| `ctx.io(prep_fn)` 泛型 SQE 逃生口、`ctx.io_detach` | `scheduling/context.h` | 表达框架尚未封装的 op；关闭 fd 不占协程 |
| `ctx.async(fn)` 线程池卸载 | 同上 | 阻塞型 handler（DB、压缩、模板渲染）卸载 |
| `runtime_t` shared-nothing 多线程 | `scheduling/runtime.h` | 每线程一个 listener + 一个 `context_t`，router 只读共享 |
| `config_t`（TOML）、`CORNET_METRICS` | `utils/config.h`、`base/metrics.h` | HTTP 配置与指标沿用同一体系 |

> 注意 `with_timeout(ctx, awaiter, dur)` 被**降级**为非默认手段，原因见 §10.2；`with_timeout(ctx, ccoro_t, dur)` 会额外 spawn 2 协程 + 1 timer，仅供 client 使用。

### 1.2 需要新增/补齐的框架能力（P0）

原方案列了 5 项，评审后修订为 8 项。仍然都是小改动（合计约 150 行框架代码），但 4–8 是**正确性**前提，不是优化项。

| # | 缺口 | 影响 | 处置 |
|---|------|------|------|
| 1 | `tcp::socket_t` 未暴露 `sendmsg`/`writev`（`socket_t::sendmsg_awaiter` 只有 `udp` 暴露） | 无法一次 syscall 写「头 + 外部 body」两段 iovec | 暴露 `sendmsg`；另加**自持 iovec 数组与 msghdr** 的 `writev_awaiter`（参照 `sendto_awaiter` 的内嵌写法，不把生命周期推给调用方） |
| 2 | 没有 `send_all` 语义 | 每个调用方重复写短写循环 | HTTP 层内部实现，剩余 iovec 原地推进偏移 |
| 3 | `error_domain` 无协议错误域，且其枚举项是 snake_case，与 `context_t::state_t` 的 PascalCase 分裂 | llhttp 错误只能塞进 `internal`；两个 enum class 两种风格 | 新增 `error_domain::Http`（`message()` 走 `llhttp_errno_name`），并把既有项统一为 `None`/`System`/`Resolve`/`Internal`/`Exception`（机械重命名，31 处调用点） |
| 4 | **`timeout_awaiter` 把 `-ECANCELED` 一律翻译成 `ETIMEDOUT`**（`combinators.h:94`） | 优雅关闭的 `cancel_sweep` 也产生 `ECANCELED`，套了 `with_timeout` 后无法区分「空闲超时」与「服务器关闭」，metrics 误报且无法走 `Connection: close` 分支 | `await_resume()` 增判 `ctx_->is_running()`；并暴露 `timed_out()` 供调用方自行判断 |
| 5 | **无粗粒度时间源** | `Date` 头与超时判定都需要「当前秒」，每请求一次 `clock_gettime` 是可省的 | run loop 每轮刷新一次，暴露 `ctx.coarse_now()` 与 `ctx.http_date()`（`const char[30]`），HTTP 层只读 |
| 6 | **`io_uring_queue_init` 只读 `flags` 整数**，无 `CQSIZE` / `SUBMIT_ALL` / `COOP_TASKRUN` / `SINGLE_ISSUER` 命名开关 | 默认 `capacity=2048`（`conf/default.toml`）、CQ 默认 2× = 4096。10K 连接目标下 SQ/CQ 都不够 | 配置层加命名开关。**这是 10K 连接能否跑起来的问题，不是「白拿的吞吐」** |
| 7 | **`uring_t::get_sqe()` 在 SQ 满且 submit 后仍失败时 `throw`**（`src/uring.cc`） | 违反「公开 API 不抛异常」；异常被 handler 安全网吞成 500 之后 SQ 依然是满的 | 改返回 `expected<io_uring_sqe*>`，调用方走背压（暂停 accept / 延迟 flush） |
| 8 | **`utask_t` 假设「一次提交 = 一个 CQE」** | 无法用 multishot recv / multishot accept / provided buffer ring | 新增 `mutask_t`（处理 `IORING_CQE_F_MORE`）+ `buf_ring_t`。**从原方案的 P2 提前**：这是 D7 的前提，不是可选加速（§6.8） |

### 1.3 现有约定（必须遵守）

- 命名：类型 `xxx_t`、snake_case、成员 `xxx_`、头文件宏 `CORNET_XXX_H`；2 空格缩进。
- **`enum class` 的枚举项用 PascalCase**（每个单词首字母大写、无下划线），与 `context_t::state_t`（`Running` / `Draining` / `Canceling` / `Terminated`）对齐。类型名本身仍是 `xxx_t`，只有枚举项走 PascalCase——这样 `status_t::NotFound`、`field_t::ContentLength` 与 snake_case 的函数、成员在视觉上自然区分。
  - 例外：`error_domain` 目前是 snake_case，属于既有不一致，随 §1.2 第 3 项一并纠正。
- 错误：**公开 API 不抛异常**，一律 `expected<T>`；异常仅作 bug 安全网。
- 头文件放 `include/cornet/<module>/`，实现放 `src/`，CMake 显式列源文件。
- 单线程内无锁、无原子；跨线程只走 `spawn_remote`。

---

## 2. 设计目标

**优雅**

1. Hello World ≤ 4 行有效代码，且**不出现 `coro_t` / `co_return`**——不需要挂起的 handler 就不该写成协程（D3）。
2. 安全是默认值，零拷贝是显式选择。任何生命周期契约都由类型或命名表达，不靠文档口头约定（D5）。
3. 分三层，逐层下沉：`server_t + router`（应用层）→ `connection_t + parser_t`（协议层）→ awaiter（IO 层）。任何一层都可单独使用。
4. 与现有 API 同构：`co_await`、`expected`、`ctx.spawn`、`runtime_t`、`scope_t`，无新范式。

**性能**

1. 热路径零堆分配：同步 handler 路径全程不 `malloc`（解析、路由、响应组装、flush）。异步 handler 每请求一次帧分配，用 per-context 帧池消除。
2. 零拷贝解析：header 视图直接指向头部 buffer；聚合 body 连续可 `string_view` 化（chunked 请求做一次原地压实，见 §6.6）。
3. **SQE 数量不因超时而翻倍**：超时靠时间轮，不逐 IO 挂 `link_timeout`（D4）。
4. 空闲 keep-alive 连接不持有协程帧与读缓冲（D7）。
5. 不引入虚函数 / `shared_ptr` 到请求热路径（`std::function` 仅出现在注册期）。

**非目标**：TLS、HTTP/2/3、CGI、完整的 URL 规范化。WebSocket 仅保留 upgrade 出口。

---

## 3. 整体架构

```
┌──────────────────────────────────────────────────────────────────┐
│ 应用层     server_t / router_t / filter chain / client_t          │
│            handler: void(request_t&, response_t&)         同步     │
│                   | coro_t<void>(request_t&, response_t&) 异步     │
├──────────────────────────────────────────────────────────────────┤
│ 消息层     request_t   response_t   headers_t   body_reader_t      │
│            header 视图 → head_buf_ ；body → body_buf_ 或流式        │
├──────────────────────────────────────────────────────────────────┤
│ 协议层     parser_t(llhttp)   serializer_t   timer_wheel_t         │
│            connection_t：两段式派发 / pipelining / keep-alive      │
├──────────────────────────────────────────────────────────────────┤
│ 缓冲层     head_buffer_t（定长，消息期不搬移） body_buffer_t（精确）│
│            buffer_pool_t · buf_ring_t（idle 连接共享）             │
├──────────────────────────────────────────────────────────────────┤
│ Cornet     recv/send/writev/splice awaiter · scope_t ·             │
│            canceler_t · context_t · runtime_t · mutask_t           │
└──────────────────────────────────────────────────────────────────┘
```

一次 keep-alive 请求的数据流（两段式派发是关键）：

```
recv CQE ──► head_buf_.commit(n)
              │
              ├─► parser_.execute(新增区间)      llhttp 回调仅记录 (offset,len)
              │
              ├─ HeadersReady ─────────────────────────────────────────┐
              │    ① router.match(method, path) → route               │
              │    ② Expect: 100-continue ? → 立即写 100 并 flush      │
              │    ③ 决定 body 模式：                                  │
              │       route.streaming || CL > aggregate_threshold      │
              │         → 流式：立刻执行 handler，                     │
              │           body_reader_t 内部 recv + llhttp_resume      │
              │       否则 → 聚合：body_buf_ 按 CL 精确分配，继续投喂   │
              │                                                       │
              ├─ MessageReady ──── 聚合模式在此执行 handler ───────────┘
              │    同步 handler：直接调用，不建协程帧
              │    异步 handler：co_await（帧从 per-context 池取）
              │    响应写入 out_，此处不 flush
              │
              ├─► head_buf_ 中还有剩余字节？→ 回到 parser（pipelining）
              │                            否 → 进入 flush
              │
              └─► flush：一次 writev 写出本轮全部响应（多段 iovec）
                        写完后若无待处理数据 → 交给 §6.8 的 idle 路径
```

关键点：**一次 `recv` 内的多个 pipelined 请求共用一次 `writev`**；`submit` 由 scheduler 统一批量执行。因此稳态下 `syscall/请求` 在纯 pipelining 压测中远小于 1，非 pipelining 场景为 1 recv + 1 writev 两个 SQE 合并进同一次 submit。**因为超时不再挂 `link_timeout`，SQE 数就是 2 而不是 4。**

---

## 4. 目录与构建

```
include/cornet/http/
  common.h       # method_t / status_t / version_t / field_t / error 域
  buffer.h       # head_buffer_t + body_buffer_t + buffer_pool_t
  headers.h      # header_ref / headers_t
  message.h      # request_t / response_t / body_reader_t / body_writer_t
  parser.h       # llhttp 封装（含 HeadersReady 状态）
  serializer.h   # 状态行/头部编码、date 引用、整数快速格式化
  timer_wheel.h  # per-context 分层时间轮
  connection.h   # 单连接读写循环
  router.h       # 静态表 + radix trie + 同步/异步 handler 双形态
  server.h       # server_t + drain 状态机
  client.h       # 客户端 + 连接池（RAII response）
include/cornet/http.h        # 聚合头（不并入 cornet.h，保持核心零依赖）
src/http/
  buffer.cc parser.cc serializer.cc timer_wheel.cc
  connection.cc router.cc server.cc client.cc
tests/http_buffer.cc tests/http_parser.cc tests/http_router.cc
tests/http_server.cc tests/http_client.cc tests/http_drain.cc
bench/http_bench.h
```

### 4.1 枚举清单

本模块所有 `enum class` 的枚举项统一 PascalCase（§1.3）。集中列出以便实现时对齐：

```cpp
// common.h
enum class method_t  : uint8_t { Get, Head, Post, Put, Delete, Connect, Options,
                                 Trace, Patch, /* ... */ Unknown };
enum class version_t : uint8_t { Http10, Http11, Unknown };
enum class status_t  : uint16_t {
  Continue = 100, SwitchingProtocols = 101,
  Ok = 200, Created = 201, NoContent = 204, PartialContent = 206,
  MovedPermanently = 301, Found = 302, NotModified = 304,
  BadRequest = 400, Unauthorized = 401, Forbidden = 403, NotFound = 404,
  MethodNotAllowed = 405, RequestTimeout = 408, Conflict = 409,
  ContentTooLarge = 413, UriTooLong = 414, ExpectationFailed = 417,
  RequestHeaderFieldsTooLarge = 431, TooManyRequests = 429,
  InternalServerError = 500, NotImplemented = 501, ServiceUnavailable = 503,
  HttpVersionNotSupported = 505,
};
enum class field_t   : uint8_t { Host, ContentLength, /* ... */ Other };   // §6.1

// buffer.h
enum class body_mode_t : uint8_t { Empty, Exact, Stream };                 // §5.3

// parser.h
enum class parser_t::type_t   { Request, Response };                      // §6.4
enum class parser_t::result_t { NeedMore, HeadersReady, MessageReady,
                                BodyPaused, Upgrade, Error };

// router.h
enum class route_t::kind_t : uint8_t { Sync, Async };                     // §7.1

// server.h
enum class server_t::state_t : uint8_t { Running, Draining, Stopped };    // §10.4
```

`status_t` 显式带数值（既是枚举也是线上码），`Continue = 100` 需注意与 C++ 关键字无冲突——这也是 PascalCase 的附带好处：`Empty`/`Continue`/`Inline` 之类不会撞上 `inline`、`continue` 等关键字，而 snake_case 枚举项会。

> `src/` 现为平铺布局；HTTP 是首个多文件模块，故新开 `src/http/` 子目录。

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

独立 target 的理由：README 承诺「核心仅依赖 liburing」。llhttp 只被 `cornet_http` 私有链接，核心用户不受影响；`llhttp.h` 不出现在任何公开头文件中（见 §6.4 的内联存储约定）。

---

## 5. 缓冲层：头部与 body 分离（D1）

### 5.1 为什么必须分离

零拷贝解析的难点是 llhttp 回调给的是**指向本次投喂 chunk 的裸指针**，而缓冲区会因扩容而搬移。解法是所有视图存 `(uint32_t offset, uint32_t len)` 而非指针，于是产生一条约束：**一条消息解析期间禁止整理/搬移 buffer**。

如果头部和 body 共用一个可增长 buffer，这条约束的代价会失控：

- `max_body_bytes = 8MB` 意味着单连接读缓冲可以涨到 8MB+，与「活跃连接 ≤ 32KB」的目标直接矛盾。
- 翻倍扩容从 16KB 到 8MB 是 9 次 realloc，累计 memcpy ≈ 16MB——为了「零拷贝解析」付出了远超一次拷贝的代价。
- pipelining 时若第 N 个请求跨 recv 边界，因不能整理只能扩容，直到撞上上限才报错。

分离之后，偏移不变量只作用在一个 16KB、生命周期等于一条消息的小对象上，`§14` 里「隐式契约难维护」的风险基本消失。

### 5.2 head_buffer_t

定长，容量 = `max_header_bytes`（默认 16KB），**一条消息解析期间绝不搬移**，消息边界处才 `consume` + 归零。

```cpp
class head_buffer_t {
 public:
  char*           base()                       { return data_; }
  uint32_t        readable() const             { return w_ - r_; }
  std::span<char> writable();                  // 尾部剩余空间；不扩容、不搬移
  void            commit(uint32_t n)           { w_ += n; }
  void            consume(uint32_t n)          { r_ += n; }
  void            reset_if_drained()           { if (r_ == w_) r_ = w_ = 0; }
  void            compact();                   // 仅在消息边界处调用（debug 断言守护）

  std::string_view view(uint32_t off, uint32_t len) const { return {data_ + off, len}; }
  uint32_t         offset_of(const char* p) const { return uint32_t(p - data_); }
 private:
  char* data_; uint32_t cap_, r_{0}, w_{0};
};
```

- 尾部空间不足且**不在解析中** → `compact()`（`memmove` 到头部）。
- 尾部空间不足且**正在解析一条消息** → 该消息头部超过 `max_header_bytes` → 431，关连接。头部有硬上限，所以这不会死锁。
- Debug 下 `parser_t` 在每次回调时断言 `buf.base()` 未变、`r_` 未动，Release 零成本。

### 5.3 body_buffer_t

三种形态，由 §6.7 的 `HeadersReady` 阶段选定：

```cpp
enum class body_mode_t : uint8_t { Empty, Exact, Stream };
```

| 形态 | 条件 | 行为 |
|------|------|------|
| `Empty` | `Content-Length == 0` 或无 body | 不分配 |
| `Exact` | 有 `Content-Length` 且 ≤ `aggregate_threshold`（默认 256KB） | 从池中取一块 ≥ CL 的 buffer，**一次分配，零 realloc**；`body()` 返回连续 `string_view` |
| `Stream` | 无 CL（chunked）或 CL > 阈值 或路由标记 `streaming` | 不聚合。handler 在 `HeadersReady` 时启动，`body_reader_t` 复用一块固定 chunk buffer（默认 32KB） |

`Content-Length > max_body_bytes` → 413，且**在读 body 之前**拒绝（不浪费带宽）。

### 5.4 buffer_pool_t

每 `context_t` 一个（先做成 HTTP 层自持的 `thread_local`，若后续 socket/文件模块也需要再上提到 `context_t`）。侵入式 freelist，按 4K/16K/64K/256K 分档，归还即 `consume` 到空，无 `memset`。

- 连接进入 idle keep-alive 时归还**全部** buffer（head + body），配合 §6.8 让空闲连接接近零内存。
- 超过 256K 的 `Exact` body 直接 `malloc`/`free`，不进池（罕见，避免池被大块污染）。

---

## 6. 消息层与协议层

### 6.1 常见头部枚举化 + SWAR 识别

```cpp
enum class field_t : uint8_t {
  Host, ContentLength, ContentType, TransferEncoding, Connection,
  Accept, AcceptEncoding, UserAgent, Expect, Upgrade, /* ... */ Other
};
```

在 `on_header_field` 回调里识别，但**不用逐字符分支表**——用 SWAR（SIMD Within A Register）：把 header name 前 8 字节读成 `uint64_t`，一次 OR 完成小写化，一次整数比较完成匹配。`Content-Length`（14 字节）= 2 个 u64 比较。相比逐字符 + 首字符分支表，分支预测失败少一个数量级。

```cpp
// 8 字节小写化：ASCII 字母的 0x20 位即大小写位
constexpr uint64_t kLower = 0x2020202020202020ull;

inline uint64_t load8_lower(const char* p) {
  uint64_t v; std::memcpy(&v, p, 8); return v | kLower;
}

// 例：len==14 分支内
static constexpr uint64_t kCL0 = /* "content-" as u64 */;
static constexpr uint64_t kCL1 = /* "-length" 的后 6 字节 */;
if (len == 14 && load8_lower(p) == kCL0 && load8_lower(p + 6) == kCL1)
  return field_t::ContentLength;
```

按 `len` 先 switch（一次跳转表），再在各长度分支内做 1–2 次 u64 比较。识别结果存进 `header_ref::field`；`headers_t` 另存 `uint32_t field_bitmap_`，`has(field_t)` 是一次位测试，`Content-Length` / `Connection` 之类的判断退化为整数比较。

> 长度不足 8 字节的比较不能越界读：header name 位于 `head_buf_` 内部且后面必然还有 `": value\r\n"`，因此读 8 字节安全；但为满足 ASAN，`load8_lower` 在末尾邻近处走 `memcpy(len)` 的慢路径。

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
  head_buffer_t* buf_;
};
```

内联 32 项（约 512B）覆盖绝大多数真实请求，稳态零分配。这 512B 计入连接协程帧——见 §11.3 对帧大小的诚实标注，以及 §6.8 如何让它不出现在空闲连接上。

### 6.3 分片处理（llhttp 的必修课）

llhttp 会在跨 `recv` 边界时把一个 header value 拆成多次回调。因为头部数据全部投喂进**同一个不搬移的 `head_buf_`**，两次回调的区间天然相邻：

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

`spill_` 是每连接一小块（默认 512B）追加缓冲，仅在异常形态下使用，并计入 metrics 以便发现真实流量中的比例。注意 D1 分离之后「相邻」这个性质是**被保证的**而非碰巧成立——头部 buffer 在消息期内绝不搬移。

### 6.4 parser_t

```cpp
class parser_t {                       // 不在头文件暴露 llhttp.h
 public:
  enum class type_t  { Request, Response };
  enum class result_t {
    NeedMore,         // 数据不足，继续 recv
    HeadersReady,     // ★ 新增：头部完整，body 未读。路由/Expect/模式选择在此发生
    MessageReady,     // 整条消息完成
    BodyPaused,       // 流式：on_body 返回 HPE_PAUSED，等消费者读走
    Upgrade,          // 协议升级出口
    Error
  };

  explicit parser_t(type_t t);
  void     reset();                                  // 复用同一连接的下一条消息
  result_t execute(head_buffer_t& buf, uint32_t off, uint32_t len);
  result_t resume();                                 // 流式 body：llhttp_resume 后继续
  void     set_body_sink(body_buffer_t* sink);       // 聚合模式：on_body 直接写入
  error_t  error() const;                            // error_domain::Http
  uint32_t consumed() const;                         // pipelining：本条消息终点

 private:
  alignas(16) unsigned char st_[kLlhttpStateSize];   // llhttp_t 的存储（.cc 内静态断言）
};
```

设计要点：

- **`HeadersReady` 是 D2 的落点**。`on_headers_complete` 回调返回一个特殊值让 `execute` 提前返回，控制权交回 `connection_t` 做路由与模式选择，然后再继续投喂 body。这是原方案缺失的状态，也是流式上传与 `100-continue` 能工作的唯一前提。
- `llhttp_settings_t` **全局唯一、`const`、静态初始化**（每 parser 拷贝一份 settings 是常见的无谓开销）。回调是无捕获静态函数，`data` 指回 `parser_t`。
- **严格模式**：默认关闭所有 `lenient_*`（`LENIENT_HEADERS`、`LENIENT_CHUNKED_LENGTH`、`LENIENT_KEEP_ALIVE`…），从源头挡掉 request smuggling 变体；可通过配置逐项放宽。
- **Upgrade**：`on_message_complete` 后若 `llhttp_get_upgrade()`，返回 `result_t::Upgrade`，把 socket 与 `head_buf_` 剩余字节交给用户回调（WebSocket 的接入点）。
- 头文件不含 `llhttp.h`：用固定大小字节数组 + `static_assert(sizeof(llhttp_t) <= kLlhttpStateSize)`（在 .cc 里断言，留 25% 余量）避免 pimpl 的堆分配，同时保持依赖私有。

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

  std::string_view body() const;      // 聚合模式，保证连续（见 §6.6）
  body_reader_t&   stream();          // 流式模式：co_await reader.read()
};
```

**response_t：写入按所有权分级（D5）**

`body_ref(span)` 那种「数据必须活到 flush」的契约在 API 上完全不可见，而 flush 发生在 handler 返回**之后**（pipelining 批量 flush），所以 handler 里任何局部数据用它都是 UAF。改为四个语义清晰的入口，**安全是默认，零拷贝是显式选择**：

```cpp
class response_t {
 public:
  response_t& status(status_t s);
  response_t& header(field_t f, std::string_view v);      // 预编码名字，memcpy
  response_t& header(std::string_view n, std::string_view v);

  // ── body 写入：四档所有权 ──
  response_t& body(std::string_view data);        // ① 拷进 out buffer。安全默认
  response_t& body_static(std::string_view data); // ② 仅接受静态存储期，零拷贝
  response_t& body_owned(buffer_lease_t lease);   // ③ 移交所有权，flush 后归还池
  template<typename T> T& pin(T&& obj);           // ④ 移进 response 的小 arena，
                                                  //    延寿到 flush 之后，返回引用
  coro_t<expected<void>> send_file(int fd, uint64_t len); // splice 零拷贝
  body_writer_t chunked();                        // 流式 / SSE

  // ── 错误 latch ──
  error_t error() const;      // 任一写入失败后置位，后续写入变 no-op
  bool    failed() const;
};
```

`pin` 让「构造一个临时对象再零拷贝引用它」变得安全，覆盖了原方案示例里那个隐藏的 UAF：

```cpp
// 原方案示例（危险）：临时 string 在本行结束即析构
resp.json(fetch_user(req.param("id")));

// 修正后：要么拷贝（默认安全），要么显式延寿
resp.json(fetch_user(req.param("id")));            // json() → body()，拷贝，安全
resp.body_static(&resp.pin(fetch_user(id))[0]);    // 需要零拷贝时显式 pin
```

**错误 latch 而非逐调用 `expected`**：链式 API 上每行 `if (!r)` 是灾难性的人机工程。`response_t` 内部持一个 `error_t err_`，任何失败（out buffer 超限、`IOV_MAX` 溢出、状态机违规）置位并让后续写入变 no-op；`connection_t::flush()` 检查 `failed()` 后决定回 500 还是照发。错误既不丢失，也不污染 handler 代码——这符合 §1.3「一律 `expected`」的实质要求（错误可观测、不静默）。

**易用性糖**（覆盖 90% 场景）：`resp.text("hi")`、`resp.json(sv)`、`resp.not_found()`、`resp.redirect(url)`。全部走 `body()` 的拷贝路径。

### 6.6 chunked 请求体的连续性

`request_t::body()` 返回 `std::string_view` 的前提是 body 在内存里连续。但 `Transfer-Encoding: chunked` 的 body 被 chunk-size 行和 CRLF 切开，llhttp 的 `on_body` 会给出 N 段不连续区间。

处置：**聚合模式下在 `on_body` 里原地压实**。`body_buffer_t` 维护一个写指针，每段 `on_body` 数据 `memmove` 追加到写指针处。总代价是 O(body) 的一次拷贝且**不需要扩容**（压实只会缩短），换来 `body()` 连续这个不变量。

- chunked 请求无 `Content-Length`，因此默认落到 **stream** 形态；只有路由显式声明 `.aggregate()` 时才走压实聚合，且受 `max_body_bytes` 约束。
- 这条必须进单测：逐字节投喂一个多 chunk 报文，断言 `body()` 与整块投喂结果一致。

### 6.7 connection_t 主循环（两段式派发）

```cpp
class connection_t {
 public:
  connection_t(context_t& ctx, tcp::socket_t sock,
               const server_options_t& opt, buffer_pool_t& pool,
               timer_wheel_t& wheel);
  coro_t<void> run(const router_t& router);   // 连接生命周期 = 该协程生命周期
 private:
  // dispatch / flush / close 均**不是**独立协程：见下方「避免辅助协程」
};
```

```cpp
coro_t<void> connection_t::run(const router_t& router) {
  timer_handle_t timer = wheel_.arm(this, opt_.idle_timeout);   // 时间轮，非 link_timeout

  while (!draining()) {
    // ── 1. 读。无 with_timeout：超时由时间轮驱动，只 1 个 SQE ──
    auto w = head_.writable();
    if (w.empty()) { write_error(status_t::RequestHeaderFieldsTooLarge); break; }
    auto n = co_await sock_.recv(ctx_, w.data(), w.size());
    if (!n || *n == 0) break;              // 对端关闭 / ECANCELED（idle 超时或 drain）
    head_.commit(*n);
    wheel_.rearm(timer, opt_.header_timeout);

    // ── 2. 解析 + 派发（两段式），把本次 recv 里的所有完整请求消费掉 ──
    while (head_.readable()) {
      auto r = parser_.execute(head_, ...);
      if (r == result_t::NeedMore) break;
      if (r == result_t::Error) { write_error(parser_.error()); goto flush_and_close; }

      if (r == result_t::HeadersReady) {
        route_ = router.match(req_.method(), req_.path(), params_);
        if (!prepare_body(route_)) goto flush_and_close;  // 413 / 417 / 100-continue
        if (body_mode_ == body_mode_t::Stream) {
          co_await invoke(route_, req_, resp_);   // 流式：handler 现在就跑
          continue;                               // handler 内部读完 body
        }
        continue;                                 // 聚合：继续投喂到 MessageReady
      }

      if (r == result_t::MessageReady) {
        co_await invoke(route_, req_, resp_);     // 响应写入 out_，此处不 flush
        if (resp_.failed()) { write_error(status_t::InternalServerError); goto flush_and_close; }
        if (!req_.keep_alive() || draining()) { close_after_flush_ = true; break; }
        if (++pipelined_ >= opt_.max_pipelined) break;   // 背压
        parser_.reset(); recycle_message();
      }
    }

    // ── 3. 写：一次 writev 写出本轮全部响应 ──
    if (!co_await flush_iovecs()) break;    // 内联的 send_all 循环，非独立协程
    if (close_after_flush_) break;
    head_.reset_if_drained();
    head_.compact();                        // 安全：此刻不在解析一条消息中

    // ── 4. idle：交给 §6.8，本协程退出，帧被释放 ──
    if (!head_.readable() && opt_.idle_park) {
      wheel_.rearm(timer, opt_.idle_timeout);
      if (park_for_idle()) co_return;       // 成功停泊 → 协程结束
    }
    wheel_.rearm(timer, opt_.idle_timeout);
  }

flush_and_close:
  co_await graceful_close_inline();   // shutdown(SHUT_WR) → 短暂 drain → io_detach close
  wheel_.cancel(timer);
}
```

要点：

- **避免辅助协程。** 原方案伪码里 `co_await dispatch(...)`、`co_await flush()`、`co_await graceful_close()` 各是一个 `coro_t`，也就是**每请求 3+ 次帧分配**——与「每请求 0 次 malloc」直接冲突，且 HALO 帧消除靠不住。这里 `invoke` 对同步 handler 是普通函数调用；`flush_iovecs()` 与 `graceful_close_inline()` 写成返回 awaiter 的成员函数（内部状态放 `connection_t` 成员），或直接把循环体展开在 `run()` 里。
- **超时不占 SQE。** `wheel_.rearm` 是 O(1) 的链表摘挂，全 context 只有一个常驻 `IORING_OP_TIMEOUT`。时间轮到期时对该连接的 `canceler_t` 触发 cancel，recv 返回 `ECANCELED`，循环自然退出。
- **`iovec` 生命周期**：`msghdr` 与 `iovec[]` 是 `connection_t` 成员（不是 flush 的局部量），因此跨 CQE 存活；`send_all` 短写时原地推进偏移，不重建。超过 `IOV_MAX`（1024）分批，且把「分批」计入 metrics。
- **每连接一个 `canceler_t`**，用于精确取消本连接的 idle recv。这替代了原方案依赖的全局 `cancel_sweep`（见 §10.4 为什么必须如此）。
- 状态（`head_`/`body_` 是池句柄，`parser_`、`req_`、`resp_` 是成员）全部在协程帧内，无 `shared_ptr`，无引用计数。
- 半关闭：`shutdown(SHUT_WR)` 后短暂读 drain，避免对端收到 RST 丢响应。`close` 用 `ctx.io_detach`（fire-and-forget），不为关闭再占一个协程帧。
- `Connection: close` 之后 `head_` 里可能还有 pipelined 字节，**必须丢弃且不解析**（安全要求，防走私）。

### 6.8 idle 连接不驻留协程帧（D7）

协程帧是一整块内存，无法部分释放。`connection_t` 的帧含 `headers_t`（512B）+ `spill_`（512B）+ `llhttp_t`（~130B）+ req/resp 状态 ≈ **1.5–2KB**。10K 空闲连接仅帧就是 20MB，「空闲 keep-alive ≤ 1KB」在这个结构下**数学上不可能**。

唯一的正确路径是让 idle 连接彻底不持有帧：

```
请求处理完 + 无 pipelined 数据 + keep-alive
  ↓ park_for_idle()
① 归还 head_/body_ buffer 到池
② 把 fd 注册到 per-context 的 multishot recv（挂 io_uring_buf_ring）
③ 在时间轮上留一个 idle 超时节点
④ 连接协程 co_return → 帧释放
  ↓
此时每连接常驻：fd + parked_conn_t{ int fd; timer_node; uint32_t flags; } ≈ 32B

数据到达（CQE 带 IORING_CQE_F_MORE）
  ↓ 内核已把数据填进 provided buffer
⑤ 从池取 head_buffer_，把 provided buffer 内容搬入（一次 memcpy，≤ 一个 MTU）
⑥ 起一个新的 connection_t::run 协程（帧从 per-context 帧池取）继续处理
```

这解释了为什么 `mutask_t` + `buf_ring_t` 必须从 P2 提前到 M2（§1.2 第 8 项）：**它不是可选加速，而是内存目标的唯一实现手段**。

退化路径必须处理：

- `buf_ring` 耗尽 → CQE 返回 `-ENOBUFS`。此时补充 buffer 并重新 arm，期间该 fd 退回「常驻协程 + 独占 buffer」模式。这条路径要有 metrics，否则线上耗尽会表现为神秘的延迟毛刺。
- multishot recv 被内核终止（无 `IORING_CQE_F_MORE`）→ 重新 arm。
- `opt_.idle_park = false` 时整条路径关闭，退回常驻协程模式（M1 的行为），便于 A/B 对比。

### 6.9 连接注册与 server 的 drain 契约

`server_t` 持有一个 `scope_t`，所有连接协程 `scope.spawn(conn.run())`，所有 parked 连接登记在 `parked_table_`。这给出两个 drain 需要的保证：

- 活跃连接：`co_await scope` 会等到每个连接协程自然退出，因此**正在发送的响应一定写完**。
- parked 连接：遍历 `parked_table_` 逐个 `close`，它们没有帧、没有待写数据，可以立即回收。

具体流程见 §10.4。

---

## 7. 路由与中间件

### 7.1 同步 handler 是一等公民（D3）

原方案统一用 `coro_t<void>(request_t&, response_t&)`，代价是**每请求一次协程帧分配**，而绝大多数 handler（返回静态内容、内存查表、JSON 序列化）根本不需要挂起。同时它强迫用户写 `co_return;` 这行纯噪音——`hello world` 里 5 行有效代码中有 1 行是仪式。

```cpp
using sync_handler_t  = void(*)(request_t&, response_t&);            // 或 std::function
using async_handler_t = coro_t<void>(*)(request_t&, response_t&);

struct route_t {
  enum class kind_t : uint8_t { Sync, Async } kind;
  union { sync_handler_t sync_fn; async_handler_t async_fn; };
  uint8_t flags;      // streaming / aggregate / ...
};
```

注册期用 `if constexpr` 判断可调用对象的返回类型，落到 `kind`；运行期 `invoke` 是一个二分支 switch：

```cpp
// connection_t::invoke —— 注意返回类型是 awaiter 而非 coro_t，同步分支不建帧
auto connection_t::invoke(const route_t& r, request_t& rq, response_t& rs);
//   kind_t::Sync  → 直接调用 r.sync_fn(rq, rs)，返回 already-ready awaiter
//   kind_t::Async → co_await r.async_fn(rq, rs)，帧从 per-context 帧池取
```

收益：同步路径**零帧分配、零挂起、零调度往返**，这是整个方案里单笔最大的性能收益；同时 API 变短：

```cpp
server.get("/hello", [](request_t&, response_t& resp) {   // 无 coro_t，无 co_return
  resp.text("hello cornet");
});
```

### 7.2 匹配结构

```cpp
class router_t {
 public:
  router_t& route(method_t m, std::string_view path, auto&& h);   // 同步/异步自动分派
  router_t& get(std::string_view p, auto&& h);   // post/put/del/patch/head...
  router_t& mount(std::string_view prefix, router_t sub);
  router_t& fallback(auto&& h);

  // 运行期只读：可安全被多线程共享
  match_t match(method_t m, std::string_view path, param_slots_t& out) const;
};
```

- **静态路径**（无参数）：启动时构建 open-addressing flat table，key = `hash(method, path)`，命中 O(1)，无节点跳转。
- **动态路径**：`:name` / `*rest` 走 radix trie，参数值以 `(offset,len)` 写入调用方栈上的 `param_slots_t`（固定 8 槽），零分配。参数**名**表在构建期固化于 trie 节点，`match` 时把名表指针一并返回，`param(name)` 就是 ≤ 8 次短字符串比较（实测 < 20ns），不需要重走 trie。
- **构建期完成全部分配**，`match()` 是 `const` 且无副作用 → `runtime_t` 下所有线程共享同一份 `const router_t&`，无锁无拷贝。

### 7.3 中间件

`filter_t` 同样有同步/异步两形态，链表在注册期展开成数组：

```cpp
using sync_filter_t  = bool(*)(request_t&, response_t&);   // 返回 false = 短路，不再往下
using async_filter_t = coro_t<bool>(*)(request_t&, response_t&);
```

- 未注册任何 filter 时 `invoke` 直接调 handler，无额外帧。
- **全同步 filter 链 + 同步 handler = 整个请求零帧分配**。这是常见形态（鉴权、限流、日志都不需要挂起）。
- 用 `bool` 短路而不是 `next_t` 续延回调：避免每层一个协程帧，也避免了 `next` 被调用两次/零次这类中间件经典 bug。需要「后置处理」的 filter 注册到 `after` 链上（在 flush 前执行）。

---

## 8. Server API

最简（4 行有效代码，无 `coro_t`）：

```cpp
#include <cornet/http.h>
using namespace cornet;

int main() {
  context_t ctx;
  http::server_t server(ctx);
  server.get("/hello", [](auto&, auto& resp) { resp.text("hello cornet"); });
  server.listen("0.0.0.0", 8080);
  ctx.spawn(server.serve());
  ctx.run();
}
```

需要挂起时才写协程：

```cpp
server.get("/users/:id", [](http::request_t& req, http::response_t& resp) -> coro_t<void> {
  auto user = co_await db.query(req.param("id"));
  resp.json(user);
});
```

多线程（thread-per-core + `SO_REUSEPORT`，router 只读共享）：

```cpp
int main() {
  runtime_t rt;
  http::serve(rt, {.port = 8080}, [](http::router_t& r) {
    r.get("/hello", [](auto&, auto& resp) { resp.text("hi"); });
  });   // 内部：每线程建 listener、共享 const router、绑定 SIGTERM/SIGINT 做 graceful drain
}
```

流式上传 / 下载：

```cpp
// 声明 .streaming() 让 HeadersReady 阶段就启动 handler（D2）
server.post("/upload", [](http::request_t& req, http::response_t& resp) -> coro_t<void> {
  auto& body = req.stream();
  uint64_t total = 0;
  while (auto chunk = co_await body.read()) {   // expected<string_view>，零拷贝
    if (chunk->empty()) break;
    total += chunk->size();
  }
  resp.text(std::format("{} bytes", total));
}).streaming();

server.get("/big", [](auto&, http::response_t& resp) -> coro_t<void> {
  auto w = resp.chunked();                      // 自动 Transfer-Encoding: chunked
  for (int i = 0; i < 100; ++i) co_await w.write(make_chunk(i));
  co_await w.finish();
});
```

配置对象：

```cpp
struct server_options_t {
  uint16_t     port                 = 8080;
  std::string  address              = "0.0.0.0";
  uint32_t     max_header_bytes     = 16 * 1024;   // = head_buffer_t 容量（定长）
  uint16_t     max_headers          = 64;
  uint64_t     max_body_bytes       = 8ull << 20;
  uint32_t     aggregate_threshold  = 256 * 1024;  // 超过则强制流式，不聚合
  uint16_t     max_pipelined        = 32;
  uint32_t     max_connections      = 100'000;
  uint32_t     write_buffer_bytes   = 8 * 1024;
  bool         idle_park            = true;        // §6.8：idle 连接不驻留协程
  uint32_t     buf_ring_entries     = 4096;        // provided buffer 数量
  std::chrono::milliseconds idle_timeout{60'000};
  std::chrono::milliseconds header_timeout{10'000};
  std::chrono::milliseconds body_timeout{30'000};
  std::chrono::milliseconds timer_tick{500};       // 时间轮精度
  std::chrono::milliseconds drain_timeout{10'000}; // §10.4
  std::chrono::milliseconds handler_timeout{0};    // 0 = 关闭（开启需 ccoro_t handler）
  bool tcp_nodelay = true, reuse_port = true, serve_date_header = true;
};
```

同时支持 TOML（与现有配置体系一致）：

```toml
[cornet.http.server]
max_header_bytes = 16384
idle_timeout = "60s"
max_pipelined = 32
idle_park = true

# 10K 连接目标必须同时放大 ring（见 §1.2 第 6 项）
[cornet.context.uring]
capacity = 16384
cq_size = 32768
flags = ["SUBMIT_ALL", "COOP_TASKRUN", "SINGLE_ISSUER"]
```

### 8.1 accept 循环

```cpp
coro_t<void> server_t::serve() {
  while (state_ == state_t::Running) {
    // 不加 as_system：常驻 accept 正是「服务器空闲但活着」的依据（§10.4）
    auto sock = co_await listener_.accept(ctx_);
    if (!sock) {
      if (sock.error().code == ECANCELED) break;              // drain
      if (is_fd_exhausted(sock.error().code)) {                // EMFILE/ENFILE
        CORNET_METRICS_ADD(http_metrics.accept_fd_exhausted);
        co_await sleep(ctx_, 50ms);                            // 必须退避，否则 100% CPU 空转
        continue;
      }
      continue;
    }
    if (conns_ >= opt_.max_connections) { ctx_.io_detach(close_sqe(sock->native_fd())); continue; }
    scope_.spawn(make_connection(std::move(*sock)));           // §6.9
  }
}
```

`EMFILE`/`ENFILE` 下不退避就是经典的 100% CPU 空转 bug，必须显式处理并计入 metrics。

---

## 9. Client API

```cpp
class client_t {                 // 绑定单个 context，内部无锁
 public:
  explicit client_t(context_t& ctx, client_options_t opt = {});

  ccoro_t<expected<response_t>> get(std::string_view url);
  ccoro_t<expected<response_t>> request(method_t m, std::string_view url,
                                        headers_view_t hs = {},
                                        std::span<const char> body = {});
};
```

**响应必须持有 buffer 租约，不能是裸视图。** 原方案返回 `response_view_t`——视图指向 client 内部 buffer，而连接一旦归还池、buffer 也归还，视图立刻悬垂。这是不安全的 API。改为 RAII：

```cpp
class response_t {              // client 侧的响应，持有资源
 public:
  status_t         status() const;
  const headers_t& headers() const;
  std::string_view body() const;        // 聚合模式：连续，受 max_body_bytes 限制
  body_reader_t&   stream();            // 流式模式
  ~response_t();                        // 归还 buffer 租约；body 读尽则连接回池，否则关闭
 private:
  buffer_lease_t lease_;
  conn_lease_t   conn_;                 // 决定「读完才复用」的语义
};
```

这同时把「body 未读尽的连接不能复用」这条 HTTP/1.1 硬约束编码进了类型：`~response_t` 检查 body 是否读尽，未尽则关闭连接而非放回池（否则下一个请求会读到上一个响应的残留，是典型的响应错配 bug）。

- 返回 `ccoro_t` → 天然支持 `co_await with_timeout(ctx, client.get(url), 3s)`，内部所有 IO 自动可取消（框架已有能力）。**这是 `with_timeout(ccoro_t)` 的正当用法**：一次性请求，2 协程 + 1 timer 的代价在 client 侧可接受。
- **连接池**：key = `(host, port)`，per-context 无锁；空闲连接挂在同一个时间轮上；被复用时校验对端是否已关闭（`recv(MSG_PEEK|MSG_DONTWAIT)` 或读到 EOF 即丢弃重连）。
- DNS 用现有 `resolve()`（IP 走快路，域名走线程池），加 per-context 正/负结果缓存（TTL 可配）。
- 重定向、`Content-Encoding` 解压：默认关闭，作为显式开关（不为未使用的功能付费）。

---

## 10. 错误处理、超时与安全

### 10.1 错误模型

```cpp
// 既有项一并从 snake_case 改为 PascalCase（§1.2 第 3 项、§1.3）
enum class error_domain : uint8_t { None, System, Resolve, Internal, Exception, Http /* 新增 */ };
// message() 分支：case error_domain::Http: return llhttp_errno_name((llhttp_errno)code);
```

协议错误 → 自动应答并关闭连接：

| 情况 | 响应 |
|------|------|
| llhttp 解析失败 | 400 Bad Request |
| 头部超过 `max_header_bytes`（= `head_buf_` 写满）/ 数量超限 | 431 Request Header Fields Too Large |
| `Content-Length` > `max_body_bytes` | 413 Content Too Large，**在读 body 前拒绝** |
| 未知 method / 版本 | 501 / 505 |
| 无路由匹配 | 404（或 `fallback`） |
| `resp.failed()`（写入 latch 置位） | 500 |
| handler 抛异常（bug 安全网） | 500 + `SPDLOG_ERROR` |
| `Expect: 100-continue` | 在 `HeadersReady` 阶段自动回 `100 Continue` 后再读 body（可配置为 417） |

### 10.2 超时：per-context 时间轮（D4）

原方案默认用 `with_timeout(recv_awaiter, dur)`，声称「零额外 syscall、零额外协程」。syscall 确实为零，但代价被低估了：

- 每次 recv 消耗 **2 个 SQE**（op + `link_timeout`）并产生 **2 个 CQE**（`link_timeout` 无论超时还是被取消都回 CQE，`user_data=nullptr` 被 `process_utask` 丢弃）。
- 默认 `capacity=2048`、CQ 4096。10K 连接下 SQ/CQ 直接打满，而 `get_sqe()` 满时会 `throw`（§1.2 第 7 项）。
- idle 超时是 60s 量级的**低精度**需求，用内核定时器逐 IO arm 是杀鸡用牛刀。

改为分层时间轮：

```cpp
class timer_wheel_t {            // per-context，单线程无锁
 public:
  timer_handle_t arm(void* owner, std::chrono::milliseconds d);   // O(1) 挂链
  void rearm(timer_handle_t h, std::chrono::milliseconds d);      // O(1) 摘挂
  void cancel(timer_handle_t h);                                  // O(1)
  coro_t<void> run();            // 唯一一个常驻 IORING_OP_TIMEOUT，tick = 500ms
};
```

- **全 context 只有 1 个定时器 SQE**，与连接数无关。到期节点触发该连接的 `canceler_t`，recv 返回 `ECANCELED`，连接协程自然退出。这正是 nginx / envoy 的做法。
- `timer_wheel_t::run()` 用 `as_system()` 标记，不计入 user work。
- 精度 500ms 对 idle(60s) / header(10s) / body(30s) 完全够用；需要毫秒级精度的场景才退回 `with_timeout`。

三层超时（成本从低到高）：

1. **idle / header / body 超时**：时间轮。默认防线，**零额外 SQE、零额外协程**。
2. **client 单请求超时**：`with_timeout(ctx, ccoro_t, dur)`。2 协程 + 1 timer，client 侧可接受（§9）。
3. **handler 超时**：同上，代价明显，默认关闭，由业务显式开启。

### 10.3 安全默认值

- 关闭全部 llhttp lenient 开关：`Content-Length` 与 `Transfer-Encoding` 同时出现、重复 CL、chunk 长度异常等一律 400（request smuggling 防线）。
- **`Connection: close` 之后 `head_buf_` 里的残留字节必须丢弃且不解析**（走私防线）。
- 限制：URL 长度、header 数量与总字节、chunk 扩展长度、pipelining 深度、单连接内存上限、`max_connections`；全部可配且有保守默认。
- 语义正确性（易错点，纳入测试）：HEAD 不发 body、204/304 不发 body 且不发 `Content-Length`、HTTP/1.0 默认 `Connection: close`、`Connection: keep-alive` 显式处理、响应同时给出 CL 与 chunked 视为错误。
- 慢速攻击：header 超时 + idle 超时 + `head_buf_` 定长即可覆盖 slowloris（定长 buffer 让「慢慢发一个巨大头部」在 16KB 处硬性终止）。

### 10.4 优雅关闭：server 自建 drain 状态机（D6）

原方案称「HTTP server 的 graceful shutdown 直接挂靠 `ctx.shutdown()`，不需要自建状态机」。代码不支持这个结论：

- `task_tracker_t::user_idle()` 是 `ready_ + cpu_ + user_io_ == 0`。**listener 的 `accept` 是常驻 inflight 的 user io**，所以 Draining 阶段 `user_idle()` 永远不为真，`shutdown(1s)` 必然跑满整个 timeout。
- 超时后 `context_t::cancel_sweep()` 用 `IORING_ASYNC_CANCEL_ANY` **无差别取消所有 inflight op**，包括正在发送响应的 `writev`。也就是说「关闭时截断响应」不是偶发，而是**必然**。

正确流程（全部使用现成能力，不需要新框架机制）：

```
SIGTERM / server.drain()
 ①  state_ = state_t::Draining
 ②  close(listen_fd)            → 不再接新连接；accept 返回 ECANCELED 退出循环
 ③  遍历 parked_table_（§6.8）  → 无帧、无待写数据的 idle 连接直接 close
 ④  对活跃连接：不取消！让它们把当前响应写完。
     下一个响应自动带 Connection: close（draining() 为真 → close_after_flush_）
 ⑤  co_await scope_             → 等所有连接协程自然退出（scope_t 的核心保证）
     若超过 drain_timeout       → 对剩余连接逐个触发其 canceler_t（精确取消，非全局 sweep）
 ⑥  wheel_.stop()
 ⑦  此时 user_idle() 为真 → ctx.shutdown() 立即完成，不再需要靠 timeout 推进
```

关键差别：**取消的粒度是「每连接的 canceler_t」而不是全局 `cancel_sweep`**，所以正在写的响应永远不会被截断。

> **更正（实测）**：本节初版写「`accept` 用 `as_system()` 标记，让 `user_idle()` 能真正变为真」——这是错的，而且是致命的错。
>
> `context_t::run()` 的循环是 `while (!idle())`，循环体末尾一旦发现 `user_idle()` 就 `switch_to(Canceling)`，随后 `cancel_sweep()` 用 `CANCEL_ANY` 把所有 inflight op 一并取消。把 accept 标成 framework io 之后，accept 一挂上去 `user_io_` 就是 0 → `user_idle()` 立刻为真 → 第一个客户端还没连上，context 就把这个 accept 自己取消掉并 Terminated。**服务器会在启动瞬间退出。**
>
> 正确的理解是反过来的：**常驻的 accept 计入 user work 恰恰是「服务器空闲但仍然活着」的唯一依据**，不是缺陷。结束它是 `drain()` 的职责——关掉 listener fd，pending accept 就带错误返回，accept 循环退出；等连接跑完，`user_idle()` 自然为真，context 自己收尾，**根本不需要有人调 `ctx.shutdown()`**。
>
> §10.4 其余部分仍然成立：如果用户确实调了 `ctx.shutdown()`，那个全局 `CANCEL_ANY` sweep 依然会截断正在写的响应，所以顺序必须是「先 drain server，再让 context 收尾」。

`Connection: close` 在 draining 期间自动附加，这是 HTTP 层的正确行为（告知客户端不要复用），原方案未提。

### 10.5 观测

访问日志在热路径上，**不能每请求一次 `spdlog` 格式化**（格式化 + 锁 + 写会成为 RPS 瓶颈）。

- 每请求写一条定长二进制记录（method / status / 路由 id / 字节数 / 耗时 ns / 时间戳）到 per-context ring；后台线程批量取出、格式化、落盘。
- 指标：per-route 的请求数、状态码分布、延迟直方图（HDR 风格分桶，per-context 累加后合并）。
- 新增框架 metrics（验收指标可测量的前提）：`submit_calls`、`sqe_count`、`cqe_count`、`buf_ring_enobufs`、`spill_used`、`iov_batch_split`、`accept_fd_exhausted`。

---

## 11. 性能设计要点与验收目标

### 11.1 已在设计中固化的优化

| 手段 | 说明 |
|------|------|
| 同步 handler 一等公民 | 全同步链路每请求 0 次帧分配、0 次挂起（§7.1，单笔最大收益） |
| 头部/body buffer 分离 | body 零 realloc；偏移不变量收缩到 16KB 定长小对象（§5） |
| 时间轮替代 link_timeout | SQE/CQE 数量减半，全 context 1 个定时器（§10.2） |
| idle 连接不驻留协程 | 空闲连接从 ~2KB 降到 ~32B（§6.8） |
| 偏移视图 | header 零拷贝，头部 buffer 不搬移故视图恒有效 |
| SWAR 头部识别 | 8 字节一次 OR 小写化 + 一次整数比较，替代逐字符分支表（§6.1） |
| 内联 32 header + 池化 buffer + 栈上参数槽 | 稳态零分配，无 `shared_ptr` |
| Pipelining 批处理 | 一次 recv 消费 N 个请求，一次 writev 写 N 个响应 |
| 多段 iovec 写出 | 头部与外部 body 不拷贝合并；msghdr/iovec 是连接成员，生命周期安全 |
| 静态表 | 状态行、常见 header 名全量静态表 `memcpy`；手写 u64→dec |
| `Date` 走 `ctx.http_date()` | run loop tick 刷新，HTTP 层只读 `const char[30]`，零成本（§1.2 第 5 项） |
| 静态 `const llhttp_settings_t` | 每连接省一次 settings 拷贝与其 cache 占用 |
| 路由 O(1) | 静态路径 flat hash，动态路径 radix + 名表指针缓存，构建期完成分配 |
| 无辅助协程 | `dispatch`/`flush`/`close` 内联或 awaiter 化，避免每请求 3+ 次帧分配（§6.7） |
| fire-and-forget close | `io_detach`，关闭不占协程帧 |
| shared-nothing 多线程 | `SO_REUSEPORT` per-thread listener，router 只读共享，无锁 |

### 11.2 后续阶段（先测量再落地）

- **异步 handler 帧池化**：`promise_type::operator new` 从 per-context `std::pmr::unsynchronized_pool_resource` 取帧（`coro.h` 已 include `<memory_resource>` 但未使用，正好落地）。消除异步 handler 的每请求一次帧分配。
- **multishot accept**：accept 循环由 N 次 SQE 变 1 次。
- **`IORING_OP_SEND_ZC`**：大响应（≥ 32KB）零拷贝发送；小响应反而更慢，需按大小切换。
- **响应压缩**（gzip / br）：非目标，但 §7.3 的 after-filter 链是其接入点，设计上不要堵死。

### 11.3 验收目标（修订）

原方案的三个指标互相冲突（「活跃 ≤ 32KB」对不上 8MB body、「空闲 ≤ 1KB」对不上 2KB 协程帧、「0 malloc」对不上 3+ 次辅助协程帧）。修订为可同时成立、且与设计决策一一对应的版本：

| 指标 | 目标 | 由哪条决策保证 |
|------|------|---------------|
| HTTP 层相对开销 | `hello world` RPS ≥ 同机 raw echo bench 的 85% | D3 同步 handler |
| 稳态堆分配（全同步链路） | 每请求 **0** 次 `malloc` | D3 + §6.7 无辅助协程 |
| 稳态堆分配（异步 handler） | M1–M3 每请求 ≤ 1 次；M4 帧池化后 0 次 | §11.2 |
| SQE / 请求（非 pipelining） | **2**（1 recv + 1 writev），不含超时开销 | D4 时间轮 |
| syscall 摊销 | 非 pipelining ≤ 1 次 `io_uring_enter`/请求；pipelining(16) ≤ 0.15 | 批量 submit |
| 解析开销 | 典型 200B 头部 ≤ 1 µs/请求（单核） | SWAR + 偏移视图 |
| 空闲 keep-alive 连接内存 | `idle_park=true`：≤ **64B**（fd + 时间轮节点 + 表项）<br>`idle_park=false`：≤ **4KB**（诚实标注协程帧开销） | D7 |
| 活跃连接内存 | 头部阶段 ≤ 20KB；聚合 body 阶段 ≤ 20KB + CL；流式恒定 ≤ 52KB | D1 |
| 尾延迟 | 10K 并发下 p99 不超过 p50 的 10 倍 | 时间轮 + 无全局 sweep |
| 优雅关闭 | drain 期间**零截断响应**，且 drain 不依赖 timeout 推进 | D6 |

**测量方法必须先落地，否则指标是估算而非验收。**

- malloc 计数：bench 中替换全局 `operator new`/`delete` 做计数（ASAN **不统计分配次数**，原方案此处有误）。
- SQE / syscall：靠 §10.5 新增的 `submit_calls` / `sqe_count` metrics。
- **在 M1 之前先跑一遍 raw echo 基线**，所有相对指标以该基线为分母。

对比基线：nginx（单 worker）、Boost.Beast、drogon；压测工具 `wrk` / `h2load --h1` / `oha`，含 pipelining 与非 pipelining 两组。

---

## 12. 测试计划

**单元**

- `head_buffer_t`：定长写满 → 431；`compact()` 只在消息边界发生（debug 断言）；偏移视图在消息期内恒有效。
- `body_buffer_t`：三形态选择逻辑；exact 形态零 realloc（hook `realloc` 计数）。
- **分片投喂**：逐字节喂完整报文，断言结果与整块投喂完全一致（headers、body、`consumed()`）。
- **chunked 请求体压实**：多 chunk + trailer + chunk 扩展，断言 `body()` 连续且内容正确（§6.6）。
- **两段式派发**：`HeadersReady` 早于 body 到达；`Expect: 100-continue` 在读 body 前应答；流式 handler 能读到跨多个 recv 的 body（§6.4）。
- SWAR 头部识别：全部 `field_t` 的大小写混合 / 前缀相同（`Content-Length` vs `Content-Type`）用例；末尾邻近的越界读检查（ASAN）。
- pipelining；`Connection: close` 后残留字节被丢弃；HEAD/204/304 语义；limits 触发。
- router：静态 / 参数 / 通配 / 优先级；`param(name)` 正确性；同步与异步 handler 混用。
- `timer_wheel_t`：O(1) 摘挂、精度、大量 rearm 下无泄漏。

**模糊**：对 `parser_t` 做 libFuzzer/AFL，语料取 llhttp 与 h1spec 用例；断言「永不越界、永不 UB、错误必被分类」。

**一致性**：跑 `h1spec` 类符合性用例集；与 curl / nginx 行为对齐若干边界。

**集成**

- `ctx.run()` 内起 server + client 对压，覆盖超时、连接复用、流式上传下载。
- **`tests/http_drain.cc` 是独立且必须的**：drain 期间持续压测，断言（a）零截断响应、（b）drain 在远小于 `drain_timeout` 的时间内完成、（c）parked 连接被立即回收、（d）响应带 `Connection: close`。这条覆盖 D6，是原方案「列为测试项但设计上无机制保证」的部分。
- `buf_ring` 耗尽的退化路径：人为把 `buf_ring_entries` 调到 8，断言功能正确且 `buf_ring_enobufs` 计数上升。
- client 侧 `~response_t`：body 未读尽时连接**不得**回池（否则响应错配），用两次连续请求断言。

**回归基准**：`bench/http_bench.h` 接入现有 bench target，CI 记录 RPS、alloc 计数、SQE/请求，防性能回退。

现有测试用 GTest（`CORNET_ENABLE_TESTS`），新增文件挂到 `unit` target 即可。

---

## 13. 里程碑

状态列反映仓库现状。

| 阶段 | 状态 | 内容 | 交付判据 |
|------|------|------|----------|
| **M0** | **已落地（7/8）** | 框架 P0：`sendmsg`/`recvmsg`/`writev_awaiter`、`send_all`、`error_domain::Http`、`timeout_awaiter` 语义修正 + `timed_out()`、`ctx.coarse_now()`/`http_date()`、uring 命名开关 + `cq_size`、`get_sqe()` 去异常。**未做**：`mutask_t` + `buf_ring_t`（随 D7 一起推到 M2）；raw echo 基线未跑（本机无 io_uring） | 单测通过，不影响现有用例 |
| **M1** | **已落地** | `head_buffer_t`/`body_buffer_t`/`buffer_pool_t`（D1）+ `parser_t`（`HeadersReady` 两段式）+ `headers_t`(SWAR) + `request/response`（四档所有权 + `pin` + error latch，D5）+ `connection_t` 两段式主循环（D2，无辅助协程）+ `timer_wheel_t`（D4）+ 同步/异步双 handler（D3）+ `router_t` + `server_t` + `scope_t` drain（D6 提前） | 89 个 HTTP 单测全绿；release/debug 均编译通过；`wrk` 压测与 RPS/malloc/SQE 指标待有 io_uring 的机器 |
| **M2** | 未开始 | filter 链的集成测试 + chunked 请求端到端 + 流式 body 端到端 + **`idle_park` / multishot recv / buf_ring**（D7）+ HEAD/304 语义测试 + `http_drain` 测试 + fuzz | 符合性用例与 fuzz 通过；`http_drain` 全绿；10K 空闲连接内存 ≤ 64B/连接 |
| **M3** | 未开始 | `client_t` + RAII `response_t` + 连接池 + DNS 缓存；`send_file`（splice）；chunked 响应 / `body_writer_t`；访问日志二进制 ring + per-route 直方图（§10.5） | 客户端集成测试通过；日志开启后 RPS 下降 < 3% |
| **M4** | 未开始 | 协程帧池化、multishot accept、`send_zc`、性能调优 | 异步 handler 也达到 0 malloc；相对开销与内存目标全部达标 |
| **M5（评估）** | 未开始 | TLS（kTLS / OpenSSL BIO）、WebSocket upgrade、响应压缩、HTTP/2 可行性 | 仅出方案，不承诺 |

与原方案的排期差异：`mutask_t` + `buf_ring` 从 M4 提前到 M2（它是内存目标的唯一手段，不是可选加速）；同步 handler、两段式派发、buffer 分离、时间轮进入 M1（它们决定 API 形态，越晚改代价越大）；drain 状态机作为正确性问题从 M2 提前到 M1 一并落地；`router_t` 也提前到 M1（`connection_t` 的派发路径离不开它）。

### 13.1 已落地代码索引

```
include/cornet/http.h              聚合头
include/cornet/http/common.h       枚举 + SWAR field 识别 + 静态状态行表 + 错误域
include/cornet/http/buffer.h       head/body/spill buffer + 池 + RAII 租约
include/cornet/http/headers.h      内联 32 项 + 字段位图
include/cornet/http/parser.h       llhttp 封装（llhttp.h 不泄漏到头文件）
include/cornet/http/serializer.h   out_buffer_t（error latch）+ 整数快速格式化
include/cornet/http/message.h      request_t / response_t / query_t / body_reader_t
include/cornet/http/router.h       同步/异步双 handler + flat table + radix trie
include/cornet/http/timer_wheel.h  per-context 时间轮
include/cornet/http/connection.h   server_options_t + 两段式连接循环
include/cornet/http/server.h       server_t + drain 状态机 + serve(runtime)
src/http/*.cc                      对应实现，10 个编译单元
examples/http_hello.cc             默认构建，公开 API 的编译期回归
tests/http_{common,buffer,parser,router,serializer}.cc   89 个单测
```

框架侧改动：`base/expected.h`（`Http` 域 + 渲染器钩子）、`base/defines.h`（`CORNET_ASSERT`）、`utils/clock.h`（新增）、`scheduling/context.h`（时钟 + `io_detach` 返回 `expected`）、`io_uring/uring.h`（`get_sqe` 返回 `expected` + 命名 flags）、`io_uring/utask.h`（`await_suspend` 返回 `bool` + `fail()`）、`coroutine/cancel.h`（提交失败与取消区分）、`concurrency/combinators.h`（`timeout_awaiter` 语义）、`net/socket.h`（`writev`/`sendmsg`/`recvmsg`）。

---

## 14. 风险与开放问题

1. **`sizeof(llhttp_t)` 内联存储**：升级 llhttp 可能改变结构体大小。用 `static_assert` 在 .cc 里守护，并留 25% 余量；失败即编译期暴露，不会是运行期问题。
2. **偏移不变量的可维护性**：D1 已把「消息期禁止搬移」的作用域从「可增长到 8MB 的混合 buffer」缩小到「16KB 定长头部 buffer」，风险大幅下降。仍用 debug 断言（回调期比对 `base()` 与 `r_`）固化，Release 零成本。
3. **`idle_park` 的收益不确定**：multishot + provided buffer 在小消息高并发下可能不如常驻 buffer；`buf_ring` 耗尽的退化路径有额外分支。因此 `idle_park` 做成**运行期开关**，M2 用 A/B 基准决定默认值。内存目标只在开启时承诺。
4. **同步 handler 阻塞事件循环**：同步 handler 里不小心做了阻塞调用（同步 DB、文件读）会卡住整个 context。缓解：debug 构建下用 `ctx.coarse_now()` 测量 handler 耗时，超过阈值 `SPDLOG_WARN` 并给出「改用异步 handler 或 `ctx.async()`」的提示。这是 D3 的必要配套。
5. **`response_t::pin` 的 arena 大小**：默认多大？过小则退化为堆分配，过大则浪费。倾向 512B 内联 + 溢出走池，并用 metrics 观察真实分布。
6. **SWAR 的越界读与 ASAN**：`load8_lower` 在 buffer 末尾邻近处必须走 `memcpy(len)` 慢路径，否则 ASAN 报错。需确认这个分支不会污染热路径的分支预测（用 `__builtin_expect` 标注冷路径）。
7. **`buffer_pool_t` 归属**：先做成 HTTP 层自持的 `thread_local`；若后续 socket/文件模块也需要，再上提到 `context_t`（需改核心）。
8. **filter 用 `bool` 短路而非 `next_t`**：牺牲了「在 next 前后都做事」的能力（需要拆成 before/after 两条链）。这是为零帧分配做的取舍；若实践中 after 链不够用，再评估引入协程 filter 的代价。
