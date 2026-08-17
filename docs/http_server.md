# HTTP Server

## 概述

`http::server_t` 是绑定在单个 `context_t` 上的 HTTP/1.1 服务器：监听套接字、路由表、时间轮、缓冲池、选项都由它持有，一条连接就是一个协程加几块池化缓冲。

```
        server_t                 门面：选项 + 路由 + 监听 + 时间轮 + scope
           │  get()/post()/route()/filter()/fallback()  → router_t
           │  listen() → serve() → accept 循环
           ▼
       router_t                  精确路径哈希表 + 参数路径 radix trie，match() 只读
           │
           ▼
     connection_t                一条连接：recv → parse → dispatch → writev → 循环
           │
           ├── request_t / response_t     handler 看到的两个门面（视图，零拷贝）
           └── body_reader_t / body_writer_t   流式读 body / chunked 写 body
                     │
                     ▼
   tcp::socket_t + parser_t(Request) + buffer_pool_t + timer_wheel_t
```

与 client 共享 `include/cornet/http/common/`（协议常量、缓冲、头表、解析器、序列化、时间轮），server 只新增「答」这一侧：

```
include/cornet/http/
  common/   protocol.h buffer.h headers.h parser.h serializer.h timer_wheel.h trace.h url.h
  server/   message.h router.h connection.h server.h
include/cornet/http_server.h    # 只要 server 的伞头
include/cornet/http.h           # 全部
```

三条原则：

- **一个 server 属于一个 context，不跨线程共享**。多线程用法是每个 worker 自己一个 `server_t` + `SO_REUSEPORT` 监听，全程 shared-nothing、无锁。
- **不需要挂起的 handler 就不是协程**。同步 handler 直接调用：没有协程帧、不经过调度器。这不是便利包装，而是快路径。
- **关闭是 server 自己的事，不是 `ctx.shutdown()` 的事**。见「优雅关闭」。

## 快速开始

```cpp
#include <cornet/http_server.h>

using namespace cornet;

int main() {
  context_t ctx;
  http::server_t server(ctx);

  server.get("/hello", [](auto&, auto& resp) { resp.text("hello cornet"); });

  if (auto ok = server.listen("0.0.0.0", 8080); !ok) {
    SPDLOG_ERROR("listen failed: {}", ok.error().message());
    return 1;
  }
  ctx.on_signal({SIGINT, SIGTERM}, [&server](int) { server.drain(); });

  ctx.spawn(server.serve());
  ctx.run();
}
```

`listen()` 同步打开监听套接字并返回 `expected<void>`；`serve()` 是 accept 循环加时间轮 tick，必须 spawn 到 context 里。不带参数的 `listen()` 用 `server_options_t` 里的 `address`/`port`。

可运行示例：`examples/http_hello.cc`（`cmake --build --preset release --target http-hello`）。

## 路由

```cpp
server.get("/users/:id", handler);
server.post("/items", handler);
server.route(http::method_t::Patch, "/items/:id", handler);   // 任意方法
server.del(...); server.put(...); server.head(...); server.options(...);
```

| 形式 | 匹配 | 捕获 |
|---|---|---|
| `/a/b` | 精确 | — |
| `/users/:id` | 一个路径段 | `req.param("id")` |
| `/files/*rest` | 该位置之后的全部路径 | `req.param("rest")` 含斜杠 |

- 优先级：**字面量 > `:param` > `*wildcard`**，匹配失败会回溯，所以 `/a/:x/c` 与 `/a/b/d` 共存时不会互相遮蔽。
- 路径先规范化：前导斜杠补齐、尾部斜杠去掉、空段折叠，所以 `/a/b`、`/a/b/`、`/a//b` 是同一条路由。
- 匹配大小写敏感，百分号转义**不解码**（解码要分配内存，而且只有用到值的地方知道该怎么解）。
- 参数槽位上限 8 个（`param_slots_t::kMax`），存在调用方栈上，匹配过程零分配。
- 精确路径进哈希表（一次探测），只有含 `:`/`*` 的路径进 trie。同一 (method, path) 注册两次会 warn 并替换。

### handler 的两种形态

返回类型决定形态，写法都是普通 lambda：

```cpp
// 同步：不分配协程帧、不挂起、不经过调度器
server.get("/sync", [](http::request_t& req, http::response_t& resp) {
  resp.text("done");
});

// 异步：真的要 co_await 什么才用
server.get("/async", [&ctx](auto&, http::response_t& resp) -> coro_t<void> {
  auto row = co_await query_db(ctx);
  resp.json(row);
});
```

返回 `void` → 同步，返回 `coro_t<void>` → 异步，其他返回类型 `static_assert` 失败。

### fallback 与 filter

```cpp
server.fallback([](auto& req, auto& resp) {            // 路径完全没匹配上时
  resp.status(http::status_t::NotFound).text("nope");
});

server.filter([](http::request_t& req, http::response_t& resp) {  // bool 或 coro_t<bool>
  if (req.headers().get(http::field_t::Authorization).empty()) {
    resp.status(http::status_t::Unauthorized);
    return false;    // 短路：已写的响应照发，handler 不执行
  }
  return true;
});
```

filter 是**全局**的，按注册顺序在 handler 之前执行（聚合与流式两条路径都执行）。没有 fallback 时未匹配的请求得到 404；**方法不匹配（路径存在但该方法没注册）走 405，不进 fallback**。

路由注册返回 `route_t&`，用来设置该路由的 body 策略：

```cpp
auto& r = server.post("/upload", handler);
r.body = http::body_policy_t::Stream;
```

路由表在开始 serve 之后不应再改：`match()` 是 const 且无副作用，多个 worker 才能各自持有一份而不需要锁。

## 读请求

```cpp
req.method();            // method_t
req.version();           // version_t
req.target();            // 原始 request-target，含 query
req.path();              // 去掉 query 的路径（转义保持原样）
req.query();             // query_t，懒解析
req.headers();           // const headers_t&
req.param("id");         // 路由参数
req.body();              // 聚合后的 body；流式路由为空
req.stream();            // body_reader_t*；聚合路由为 nullptr
req.keep_alive();
req.has_content_length(); req.content_length(); req.chunked();
```

所有 `string_view` 指向连接自己的缓冲，**有效期到 handler 返回为止**。要活得更久就自己拷走（或者 `resp.pin()`）。

头部查询：

```cpp
req.headers().get(http::field_t::ContentType);     // 位图短路，一次整数比较
req.headers().get("x-request-id");                 // 任意名字，大小写不敏感
req.headers().has(http::field_t::Expect);
req.headers().contains_token(http::field_t::Connection, "close");
req.headers().trailer("x-checksum");               // 需 max_trailers > 0
for (auto [name, value, field] : req.headers()) { ... }
```

trailer 与 header 是**两个命名空间**：`get()` 永远不会返回对端在 body 之后追加的值。默认 `max_trailers = 0`（丢弃 trailer），原因见 [HTTP Client](http_client.md) 的「chunked trailer」一节，两端同一套逻辑。

query 懒解析、不解码：

```cpp
auto q = req.query();
q.raw();            // 原始串
q.get("page");      // 第一个同名值
for (auto [k, v] : q) { ... }
```

## 写响应

```cpp
resp.status(http::status_t::Created)
    .header(http::field_t::ContentType, "application/json")
    .header("X-Trace-Id", trace_id)
    .header(http::field_t::ContentLength, 42u)     // 数值直接手写十进制，不 format
    .body(payload);
```

（自己写 `Content-Length` / `Connection` / `Transfer-Encoding` / `Date` 会让框架不再补那一条，见下面「框架头与错误 latch」。）

便捷方法：`text()` / `json()` / `html()`（各自补 Content-Type）、`not_found()`、`redirect(location, status)`。

### body 的四种所有权

响应是在 handler **返回之后**才 flush 的（多个 pipelined 响应共享一次 writev），所以「这些字节归谁、能活多久」不能隐式处理：

| 方法 | 语义 |
|---|---|
| `body(sv)` | 拷进输出缓冲。任何生命期都安全，默认选择 |
| `body_static(sv)` | 引用静态存储 / 映射文件 / 字符串字面量，零拷贝 |
| `body_owned(lease, len)` | 交出一块池化 block，响应写完后释放 |
| `pin(T&&)` | 把对象搬进响应 arena，返回稳定引用 |

`pin()` 是「handler 现算出来的值也想零拷贝引用」的解法：

```cpp
auto& s = resp.pin(render_json(user));   // 值搬进 arena，活到响应写完
resp.body_static(s);
```

指向 handler 局部变量的 `body_static()` 是悬垂引用——内核读到它的时候局部变量已经没了。

一次响应**只能设一次 body**（`text()` / `json()` / `html()` / `not_found()` 内部都走 `body()`）。设第二次会 latch `InvalidState`，连接改发 500：两个 body 会让 framing 与内容互相矛盾，宁可回绝也不发一条含义不明的响应。

### 框架头与错误 latch

状态行、`Date`、`Server`、`Connection`、`Content-Length` / `Transfer-Encoding` 由连接在 framing 阶段补齐，**handler 自己写过的不会被写第二遍**（重复的 `Content-Length` 是框架错误，不是外观问题）。1xx / 204 / 304 与 `HEAD` 的响应不带 body。

错误是 latch 的，不是每次调用返回：

```cpp
resp.failed();    // 缓冲装不下等
resp.error();
```

一次响应由一串小调用写成，而「装不下」只有一种合理反应——连接把它变成 500——所以逐个检查只会加噪音。handler 返回后连接检查一次 `failed()`，成立就改发 `500` 并关闭连接。

## 请求 body：聚合还是流式

在头部解析完、还没读过一个 body 字节的时候决定：

| 策略 | 行为 |
|---|---|
| `Auto`（默认） | 有 `Content-Length` 且 ≤ `aggregate_threshold`（默认 256K）就聚合，否则流式 |
| `Aggregate` | 总是聚合，上限 `max_body_bytes`（默认 8M） |
| `Stream` | 总是流式，headers 解析完 handler 立刻执行 |

没有 `Content-Length` 时（chunked）大小要到最后一块才知道，所以 `Auto` 选流式——聚合就等于无上限缓冲。`Content-Length > max_body_bytes` 直接 413，不读 body。

`Auto` 意味着同一个 handler 两种都可能遇到：`req.stream()` 非空就是流式，此时 `req.body()` 是空的。只想写一种的路由就把 `route.body` 显式定下来。

聚合路由里 `req.body()` 是**连续**的一整块，chunked 请求也一样。流式路由用 reader：

```cpp
auto& route = server.post("/upload", [](http::request_t& req, http::response_t& resp) -> coro_t<void> {
  auto* reader = req.stream();
  uint64_t total = 0;
  for (;;) {
    auto run = co_await reader->read();   // 视图，有效期到下一次 read()
    if (!run) { resp.status(http::status_t::BadRequest); co_return; }
    if (run->empty()) break;              // 空视图 = body 结束
    total += run->size();
  }
  resp.text(std::to_string(total));
});
route.body = http::body_policy_t::Stream;
```

handler 提前返回没读完 body 也不会让连接错位：keep-alive 复用前连接会自己 `reader.drain()` 把剩下的丢掉。流式读的窗口是 `stream_window_bytes`（默认 32K），body 可以远大于任何缓冲——见「实现要点：窗口回绕」。要读 body 就得 `co_await`，所以流式路由的 handler 必然是异步形态。

## 流式响应（chunked）

```cpp
server.get("/events", [](auto&, http::response_t& resp) -> coro_t<void> {
  auto w = resp.chunked();               // 必须在任何 body 方法之前调用
  for (auto& part : parts) {
    if (auto ok = co_await w.write(part); !ok) co_return;
  }
  co_await w.finish();                   // 收尾的 0 长度 chunk
});
```

`chunked()` 会暂存状态行与头部，**第一次 `write()` 把 head + 头部 + 第一块 chunk 合成一次 writev** 发出，之后每次 `write()` 只发 chunk。`Transfer-Encoding: chunked` 自动补齐（除非 handler 自己写过）。忘记 `finish()` 就等于给了对端一个不完整的消息。

## keep-alive 与 pipelining

一轮读里能解析出多少个完整请求，就顺序执行多少个 handler，然后**一次 writev 把这一批响应全发出去**（上限 `max_pipelined`，默认 32）。partial write 就地推进 iovec 重发，不重建列表。

连接在这些情况下答完就关（响应带 `Connection: close`）：请求自己说 close、HTTP/1.0 未声明 keep-alive、`drain()` 已经打过招呼、或者状态码属于「keep-alive 活不下来」那一类（400 / 408 / 413 / 414 / 431 / 500 / 501 / 503 / 505）。协议错误一律关连接。

正常关闭走 half-close + 短暂读（`SHUT_WR` 后最多读 4 次、200ms），避免直接 close 让对端看到 RST 而丢掉已经写出去的响应。

## 超时

三个 deadline 全部走**每 context 一个**的时间轮（`timer_tick` 默认 500ms），不用 per-op link_timeout：后者会让 SQE/CQE 翻倍，而无论多少连接，整个 context 只需要一个 timeout SQE。

| 选项 | 覆盖阶段 |
|---|---|
| `idle_timeout`（60s） | 两个请求之间的空闲 |
| `header_timeout`（10s） | 开始读到头部读完 |
| `body_timeout`（30s） | body 的两次数据之间 |

超时只取消**这条连接**的在途读操作（`canceler_t`），不做 context 级 sweep——sweep 会连别人正在写的响应一起收走。超时的连接直接关闭，不发 `408`，也不做 half-close 收尾。

## 优雅关闭

```cpp
server.drain();   // 停止 accept，把在途的答完
server.stop();    // 立刻停，连接直接取消
server.state();   // Running / Draining / Stopped
server.connections();
```

`drain()` 可以在 `ctx.on_signal()` 回调里直接调用。顺序是：

1. `shutdown()` + `close()` 监听套接字 → 在途的 accept 以错误完成 → accept 循环退出；
2. 逐个连接 `request_close()`：**只取消它的读**，已经排进写队列的响应照样发出去；
3. `serve()` 在 scope 上 join，等每条连接真正结束；
4. 停时间轮，`state = Stopped`。

**为什么关闭不能交给 `ctx.shutdown()`**：监听套接字上永远挂着一个 accept，它算 user work，所以 context 的 drain 阶段永远看不到 `user_idle()`，只能干等满超时；而 context 的取消 sweep 用 `IORING_ASYNC_CANCEL_ANY`，会把在途的写一起收走——一个写到一半的响应就被截断了。反过来，`drain()` 之后连接自然收敛，`user_idle()` 自己变真，context 会自己退出，**不需要再调 `ctx.shutdown()`**。

监听 accept 故意**不**标成 `as_system()`：server 空闲时挂着的那个 accept，正是应该让 context 活着的东西。

## 多线程

```cpp
#include <cornet/http_server.h>

int main() {
  runtime_t rt;                       // 默认 hardware_concurrency 个线程
  http::server_options_t opt;
  opt.port = 8080;

  http::serve(rt, opt, [](http::router_t& r) {   // 每个 worker 各调用一次
    r.get("/hello", [](auto&, auto& resp) { resp.text("hello"); });
  });
}
```

每个 worker 自己 `SO_REUSEPORT` 监听同一端口、自己一个 `server_t` 和路由表，什么都不共享，所以没有锁、没有跨线程分发。`serve()` 内部已经注册了 SIGINT/SIGTERM → `drain()`，并在末尾 `rt.join()`。

要更细的控制（比如只让 0 号线程 accept）就自己 `rt.start()` 并在回调里建 `server_t`，`http::serve()` 只是最常用那种排布的封装。

## 配置

```toml
[cornet.http.server]
address = "0.0.0.0"
port = 8080

max_header_bytes = 16384        # 头部缓冲；装不下就是 431
max_headers = 64
max_trailers = 0                # chunked trailer 记录条数；0 = 丢弃（默认）
max_body_bytes = 8388608        # 聚合上限，超出即 413
aggregate_threshold = 262144    # 超过这个就转流式
stream_window_bytes = 32768     # 流式读窗口
max_pipelined = 32              # 一轮最多答多少个 pipelined 请求
max_connections = 100000

tcp_nodelay = true
reuse_port = true
serve_date_header = true
serve_server_header = true
lenient_headers = false         # 每个 lenient_* 都会放回一种请求走私变体
lenient_chunked_length = false
lenient_keep_alive = false

idle_timeout = "60s"
header_timeout = "10s"
body_timeout = "30s"
timer_tick = "500ms"
drain_timeout = "10s"
```

`server_t` 构造时自动 `load(ctx.config())`；也可以直接给结构体赋值：

```cpp
http::server_options_t opt;
opt.port = 9000;
opt.idle_timeout = std::chrono::seconds(15);
http::server_t server(ctx, opt);
```

时间类配置同时接受字符串（`"1500ms"`、`"3s"`）和整数毫秒。三个输出暂存容量（`head_buffer_bytes` 4K / `hdr_buffer_bytes` 8K / `body_buffer_bytes` 16K）只能在结构体上改，没有对应的 TOML 键。

一条连接从池里拿 5 块缓冲：接收（`max_header_bytes`）、head、用户头、body、流式 chunk，默认合计约 60K，全部来自 `buffer_pool_t::local()`，稳态不向 allocator 要内存。

## 指标

```cpp
const auto& m = server.metrics();
```

| 字段 | 含义 |
|---|---|
| `requests` / `responses` | 收到的请求 / 成型的响应 |
| `pipelined_batches` | 一次 flush 里带了多个响应的次数 |
| `writev_calls` / `writev_partial` | gather-write 次数 / 短写次数 |
| `iov_batch_split` | iovec 数量顶到上限、剩下的挪到下一次 writev |
| `spill_used` | 用到 spill 缓冲（折行头部 / trailer）的连接数 |
| `protocol_errors` | 以错误状态码回绝的请求 |
| `timeouts` | 被时间轮打断的连接 |

配合 `buffer_pool_t::local().allocations()/hits()` 就能确认稳态是否真的零 malloc。

## 错误与状态码

handler 之外的失败由连接直接回绝，回绝一律关闭连接：

| 情况 | 状态码 |
|---|---|
| 头部超 `max_header_bytes` / 超 `max_headers` | 431 |
| `Content-Length` 超 `max_body_bytes` | 413 |
| 方法不认识 | 501 |
| 版本不是 1.0/1.1 | 505 |
| URL 非法 | 414 |
| 其他解析失败 | 400 |
| 路径匹配上但方法没注册 | 405 |
| 路径没匹配上且没有 fallback | 404（不关连接） |
| 输出缓冲溢出 / `resp.failed()` | 500 |
| `Upgrade` 请求 | 501（未实现升级） |

错误体是状态码的 reason phrase 纯文本。HTTP 域错误码（`http_error_t`，`error_domain::Http`）到状态码的映射在 `status_for_error()`。

调试某个请求卡在哪个阶段（accept / recv / parse / route / handler / frame / write）时打开 trace：

```bash
cmake --preset debug -DCORNET_HTTP_TRACE=ON
cmake --build --preset debug --target http-hello
```

它以 INFO 级打日志，所以既不用改 `SPDLOG_ACTIVE_LEVEL` 也不用调运行时级别；关闭时编译成空，参数都不求值。

## 实现要点

### 同步 handler 是快路径，不是糖

返回协程的 handler 每个请求都要付一次帧分配，即使它从不挂起——而大多数 handler 从不挂起：读内存、拼响应、返回。把两种形态分开，让这批 handler 跑在零分配、零挂起、不经过调度器的路上，顺带也省掉了「协程唯一 API」强加给每个平凡 handler 的那句 `co_return;`。同样的算术也决定了连接内部的分工：framing（`frame_head` / `frame_response`）、iovec 构建与推进都是普通函数，只有真的要碰 socket 的 `fill` / `flush` / `read_body_chunk` 才是协程——每个 `co_await` 辅助协程都是一次帧分配，一个请求三次就把同步 handler 省下来的还回去了。

### 三段暂存 + writev

状态行与框架头只有在 body 长度已知（handler 返回后）才能写，而用户头是 handler 执行期间写的。所以输出分三块 append-only 缓冲——`head_out_` / `hdr_out_` / `body_out_`——由 writev 把片段缝起来，既不用改写也不用拷贝就得到正确的线序。pipelined 的一批响应就是把每个响应的三段依次挂进同一组 iovec。

### 两阶段循环

路由、`Expect: 100-continue`、聚合还是流式，全部在 `HeadersReady` 决定，一个 body 字节都还没消费；只有聚合请求才等到 `MessageReady`。在 `MessageReady` 才 dispatch 的单阶段循环根本表达不了流式上传，也没有地方回答 100-continue。

### body 可以比接收缓冲大：窗口回绕

接收缓冲既装头部也是 body 落地窗口。头部视图是 (offset,len)，所以缓冲**不能** compact；但 body 字节是「到达即消费」的，于是头部区之后那段可以反复重用：

- 头部解析完时记下 `body_window_ = parser_.consumed_offset()`；
- 还要更多 body 且窗口已解析干净时 `in_.rewind_to(body_window_)` 再 recv；
- 所有头部 offset 都在 `body_window_` 之下，纹丝不动。

回绕点上有 `CORNET_ASSERT(!parser_.mid_header(), ...)` 守着「没有任何指向回绕区的半成品视图」。`in_body_` 正是「缓冲里有一条正在解析的消息」这个条件，也正是不能 compact 的时刻：一轮可以在 body 中间结束（body 还没到齐），这时候回收缓冲会毁掉在途请求的头部。

client 侧用的是同一套机制，两端 body 读取路径行为一致。

### 时间轮而不是 link_timeout

见「超时」。arm/re-arm/cancel 都是指针交换、零分配——keep-alive 连接每个请求都要重置空闲定时器。`timer_node_t` 内嵌在 `connection_t` 里；tick 协程标成 framework io，所以它自己不会拖住 context 的 drain。

### 每请求预算

稳态 0 次 malloc、1–2 次 `recv`、1 次 `writev`（pipelined 时是一批共用一次）、0 次 `clock_gettime`（`Date` 头用 `ctx.http_date()`，每轮事件循环只更新一次）。

## 性能：与裸 send/recv 的对比

`bench` target 里有一组 HTTP echo 场景，和裸 `send`/`recv` 的 echo 跑在同一套负载、同一套统计口径下，用来回答「上了 HTTP 之后掉多少」：

```bash
cmake --preset release -DCORNET_ENABLE_BENCH=ON
cmake --build --preset release --target bench
./cmake-build-release/bench
```

三行 cornet 的结果：

| 行 | 组成 | 与上一行的差值意味着 |
|---|---|---|
| `Cornet` | 裸 socket 两端 echo | 基线 |
| `Cornet/HTTPsrv` | `http::server_t` + 裸 socket 客户端 | 服务端 parse → route → dispatch → frame → writev 的代价 |
| `Cornet/HTTP` | `http::server_t` + `http::client_t` | 客户端栈的代价：URL 解析、取连接、请求组帧、响应解析 |

`Cornet/HTTPsrv` 的客户端故意保持"笨"：请求提前序列化好、每轮原样重发，响应按**固定字节数**读回（开跑前先探一次真实长度）。这样它和基线那行的客户端一样廉价，两行的差值才只包含服务端。之所以可以固定长度，是因为 echo 响应逐字节相同——唯一变化的 `Date` 值，IMF-fixdate 恒为 29 字符。

跑完在最后会额外打一张表：每个场景的 RPS 保留率、P99 对比，以及**每次交换的协议字节数**。后者要单独看：64B payload 的场景里请求头加响应头是负载的好几倍，这部分开销跟实现无关，任何框架都省不掉。吞吐列（MB/s）只统计 payload，方便三行直接比。

服务端 handler 就是 `resp.body(req.body())` —— 一次拷贝，而且必须拷：请求的 body 缓冲在本轮 flush 之前就还给池了，`body_static(req.body())` 会指向一块已经回收的内存。想零拷贝回显得走 `body_owned()` 自己接管一块 block。

内存列也值得一起看：一条连接固定占 5 块池化缓冲（见「配置」），其中流式 chunk 那块即使从不用 chunked 也会分配，所以高并发场景下 HTTP 的 RSS 明显高于裸 socket。

## 测试

```bash
cmake --preset debug
cmake --build --preset debug --target unit
./cmake-build-debug/unit --gtest_filter='http_*'
```

| 文件 | 覆盖 |
|---|---|
| `tests/http_common.cc` | SWAR 头名识别、reason phrase、状态/方法表、`status_for_error()` 映射 |
| `tests/http_buffer.cc` | 缓冲池分级与复用、lease 移动、head 偏移量在写入后依然有效、compact/reserve 约束、spill 溢出 |
| `tests/http_parser.cc` | 请求端解析：两阶段状态、零拷贝头视图、逐字节喂与整块等价、chunked、pipelined 切分、keep-alive 判定、超限、走私变体、trailer 的记录/丢弃/配额/回绕存活 |
| `tests/http_serializer.cc` | 字节级输出（状态行、头名预渲染、数值、chunk-size、Date）与 `response_t`：三种 body 所有权、`pin()`、两个 body 被拒、框架头识别、`query_t` |
| `tests/http_router.cc` | 精确匹配、规范化、参数与通配、优先级与回溯、405 与 fallback、handler 形态推导、参数槽上限 |
| `tests/http_connection.cc` | framing：框架头顺序、不重复写 Content-Length / Transfer-Encoding、无 body 状态、流式 body source |
| `tests/http_e2e.cc` | 打真 socket：GET/POST/404/405/HEAD、路径参数、query、pipelining、流式读写、超接收缓冲的聚合与流式 body、跨读边界的头部完整性 |

除 `tests/http_e2e.cc` 外都不依赖 io_uring，任何机器都能跑（`http_connection.cc` 是把 framing 那段流水线单独搭出来验证字节输出，不需要 socket）。

## 限制与未实现

- **TLS**：没有。要 https 就放在反向代理后面。
- **HTTP/2、WebSocket、协议升级**：`Upgrade` 请求一律 501——宁可回绝，也不留一个两边理解不一致的连接状态。
- **`Expect: 100-continue` 不是真的握手**：`100 Continue` 会在读 body 之前排进输出队列，但输出是一轮结束才 flush 的，所以严格等待 100 的客户端会等到自己的超时再发 body。有 body 的请求本来就能正常处理，只是省不掉那次等待。
- **流式响应总是宣称 keep-alive**：`chunked()` 暂存头部时 body 长度还未知、也还不知道要不要关，所以写的是 `Connection: keep-alive`；请求要求 close 时连接仍然会关，只是那一行头对不上。
- **`drain_timeout` 已解析但当前未使用**：`drain()` 会一直等到所有连接自己结束。需要硬上限就自己在外面套 `with_timeout` 或改调 `stop()`。
- **`max_connections` 满了直接 close**，不回 503——回绝前不想再为它花一个协程帧。
- **不解码百分号转义**，`path()`、`param()`、`query()` 都是原始字节；解码要分配内存，且只有用到值的地方知道正确的解法。
- **路由参数上限 8 个**，超出的段照样匹配但不记录。
- **没有内置的 cookie 解析、静态文件服务、压缩、CORS**：都属于 handler 层，可以按需自己写。
- **每条连接的流式 chunk 缓冲是无条件分配的**：从不写 chunked 的连接也占着 `body_buffer_bytes` 那一块，高并发下这是一笔可见的常驻内存。

## 注意事项

- `server_t` 不可跨线程共享，一个 context 一个；`runtime_t` 下每个 worker 各建一个。
- 开始 serve 之后不要再改路由表：`match()` 的无锁前提是路由在这之后不再变。
- `request_t` / `response_t` 上所有 `string_view` 的有效期到 handler 返回；要留住就 `pin()` 或自己拷。
- `resp.chunked()` 之后不要再调 `body*()`，那是两种互斥的 body 来源。
- 还有连接在跑的时候销毁 `server_t` 是调用方错误：先 `drain()`（或 `stop()`）并让 `serve()` 结束。
- server 与 client 同进程时各有一个时间轮，暂不合并（合并会让核心依赖 http 模块的类型）。
