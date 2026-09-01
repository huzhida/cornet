# HTTP Client

## 概述

`http::client_t` 是绑定在单个 `context_t` 上的 HTTP/1.1 客户端：连接池、DNS 缓存、选项都由它持有（时间轮借用 context 的），一次请求就是几块池化缓冲加一个协程。

```
        client_t                 门面：选项 + 连接池 + DNS 缓存（+ context 的时间轮）
           │  request() / get() / post() / stream() / upload()
           ▼
     client_request_t            出向请求：暂存 head/hdr/body，send() 拿响应
           │
           ▼
      client_pool_t              按 origin(host,port) 复用 keep-alive 连接
           │  acquire() / release()
           ▼
   client_connection_t           一条线：writev 请求 → recv/parse 响应
           │
           ▼
  tcp::socket_t + parser_t(Response) + buffer_pool_t + timer_wheel_t
```

与 server 共享 `include/cornet/http/common/`（协议常量、缓冲、头表、解析器、序列化、时间轮），client 只新增「方向相反」的那部分：

```
include/cornet/http/
  common/   protocol.h buffer.h headers.h parser.h serializer.h timer_wheel.h trace.h url.h
  server/   message.h router.h connection.h server.h
  client/   message.h connection.h pool.h client.h
include/cornet/http_client.h    # 只要 client 的伞头
include/cornet/http_server.h    # 只要 server 的伞头
include/cornet/http.h           # 全部
```

三条原则：

- **一个 client 属于一个 context，不跨线程共享**。多线程用法是每个 worker 自己 `client_t cli(ctx)`。
- **一条连接同时只有一个请求**，不做 pipelining。并发靠多连接（`client_pool_t`）。
- **非 2xx 不是错误**：它到了、解析了，怎么处理是调用方的事。`expected<>` 只承载「没能拿到答案」的失败：DNS、connect、超时、协议错误、缓冲超限。

## 快速开始

```cpp
#include <cornet/http_client.h>

context_t ctx;
http::client_t cli(ctx);

ctx.spawn([&]() -> coro_t<void> {
  auto resp = co_await cli.get("http://127.0.0.1:8080/hello");
  if (!resp) {
    SPDLOG_ERROR("request failed: {}", resp.error().message());
    co_return;
  }
  // 响应自持缓冲，body() 在 resp 活着期间一直有效——即使连接已经回池
  SPDLOG_INFO("{} {}", resp->status_code(), resp->body());
}());

ctx.run();
```

可运行示例：`examples/http_client.cc`（`cmake --build --preset release --target http-client`）。

## 便捷方法

```cpp
co_await cli.get(url);
co_await cli.head(url);
co_await cli.del(url);
co_await cli.post(url, body, "application/json");   // content_type 可省
co_await cli.put(url, body, "text/plain");
```

全部返回 `coro_t<expected<client_response_t>>`。

## 请求构建

```cpp
auto req = cli.request(http::method_t::Post, "http://api.internal/v1/items");
req.header(http::field_t::ContentType, "application/json")
   .header("X-Trace-Id", trace_id)
   .body(payload)                        // 拷进池化缓冲，可重放
   .timeout(std::chrono::seconds(3))     // 整个 send() 的 deadline
   .retry(1)
   .follow_redirects(2);
auto resp = co_await req.send();
```

URL 非法不抛异常，也不需要在这里检查：错误 latch 在请求里，由 `send()` 报出来。

### header

| 方法 | 说明 |
|---|---|
| `header(field_t, string_view)` | 已知字段名，一次 memcpy |
| `header(string_view, string_view)` | 任意字段名 |
| `header(field_t, uint64_t)` | 数值，手写十进制转换，不 format |

用户头按写入顺序原样暂存。框架头（Host / User-Agent / Accept / Connection / Content-Length / Transfer-Encoding / Expect）由连接在发送时补齐，**已经自己写过的不会被重复写一遍**——重复的 Content-Length 是框架错误，不是外观问题。

### body 所有权

| 方法 | 语义 |
|---|---|
| `body(sv)` | 拷进池化缓冲。默认选择：安全，且可重放（重试要靠它） |
| `body_static(sv)` | 引用静态存储 / 映射文件，零拷贝 |
| `body_owned(lease, len)` | 交出一块池化 block，写完释放 |
| `json(sv)` / `text(sv)` | `body()` + 对应 Content-Type |

`POST`/`PUT`/`PATCH` 即使 body 为空也会带 `Content-Length: 0`——否则对端无法区分「没有 body」和「body 还没到」。`GET` 不带。

### 单请求覆盖项

| 方法 | 覆盖的选项 |
|---|---|
| `timeout(ms)` | `total_timeout` |
| `retry(n)` | `max_retries` |
| `idempotent(bool)` | 方法自带的幂等性判断 |
| `follow_redirects(n)` | `max_redirects` |
| `expect_continue(bool)` | 发 `Expect: 100-continue`（仅在有 body 时生效） |

## 响应

```cpp
resp->status();            // status_t
resp->status_code();       // uint16_t；无效响应是 0
resp->ok();                // 2xx
resp->version();
resp->headers();           // const headers_t&
resp->header(http::field_t::ContentType);
resp->header("x-request-id");
resp->body();              // 聚合后的 body；流式响应里为空
resp->content_length();  resp->has_content_length();  resp->chunked();
resp->keep_alive();
resp->trailer("x-checksum");        // 需 max_trailers > 0，且 body 读完后才完整
```

`client_response_t` **持有它被解析出来的那几块缓冲**，所以：

- 所有 `string_view` 的有效期就是这个对象的生命期，可以跨 `co_await` 持有、可以移动；
- 只可移动不可拷贝（两个所有者就会双份归还池）；
- 析构（或 `reset()`）时缓冲整块还给 `buffer_pool_t`。

## 流式下载

body 不想整块缓冲时用：

```cpp
auto st = co_await cli.stream(http::method_t::Get, url);
if (!st) co_return;

SPDLOG_INFO("status={}", uint16_t(st->status()));
for (;;) {
  auto run = co_await st->read();     // 视图，有效期到下一次 read()
  if (!run || run->empty()) break;    // 空视图 = body 结束
  sink(*run);
}
auto resp = st->finish();             // 头部变成 client_response_t，连接回池
```

`client_stream_t` 在 body 读完之前**借用**着连接，`status()/headers()/read()` 都转发给它。提前析构（没读完就走）时连接会被丢弃而不是回池——想留住它就先 `co_await st->drain()`。

## 流式上传

```cpp
auto up = co_await cli.upload(http::method_t::Post, url);   // Transfer-Encoding: chunked
if (!up) co_return;
for (auto& part : parts) {
  if (auto ok = co_await up->write(part); !ok) break;        // part 被引用，不拷贝
}
auto resp = co_await up->finish();                           // 收尾 chunk + 读响应
```

流式上传天生不可重试（字节已经出去了），所以 `upload()` 没有重试循环。请求不能同时带暂存 body 和流式 body，那是两个不同的请求（返回 `InvalidState`）。

## 连接池

- key 是 origin `(host, port)`；`http://h/` 与 `http://h:80/` 是同一个 origin。
- 每 origin：空闲 LRU 链 + 活跃列表。空闲超过 `idle_timeout` 由时间轮回收。
- 活跃数达到 `max_conns_per_host` 时，请求按到达顺序排队，超过 `pool_wait_timeout` 返回 `PoolExhausted`。
- **不做主动心跳**。复用前做一次非阻塞 `MSG_PEEK`：返回 0（对端已 FIN）或有可读数据（流已错位）都判定不可用，直接换新连接。把连接交给排队者之前同样 peek 一次。
- 响应说 `Connection: close`、协议出错、body 没读完、超时——任一条成立就不回池。

```cpp
cli.pool().idle_count();
cli.pool().busy_count();
cli.pool().total_count();
cli.pool().clear();        // 关掉所有连接
```

## 超时

全部走共享时间轮，**不用 per-op link_timeout**：后者会让 SQE/CQE 数量翻倍，而一个 tick SQE 就够整个 context 用，无论多少连接、多少 client。

| 选项 | 覆盖阶段 |
|---|---|
| `connect_timeout` | DNS + connect |
| `write_timeout` | 请求写完 |
| `response_timeout` | 收到完整响应头 |
| `body_timeout` | 两次 body 数据之间 |
| `total_timeout` | 整个 `send()`（含重试与重定向），`.timeout()` 可覆盖 |
| `idle_timeout` | 连接在池中空闲存活 |
| `pool_wait_timeout` | 排队等连接 |

每个阶段实际用的是 `min(阶段超时, 剩余总 deadline)`。超时只取消本连接的在途操作（`canceler_t`），不做 context 级 sweep，所以不会波及别的连接。超时错误是 `error_domain::System` + `ETIMEDOUT`。

## 重试

keep-alive 池必然存在「取出的连接其实已被对端关掉」的竞态。自动重试**只在三条同时成立**时发生：

1. 请求幂等（GET/HEAD/PUT/DELETE/OPTIONS/TRACE，或显式 `.idempotent(true)`）；
2. 还没读到响应的任何一个字节；
3. 这条连接是从池里复用来的（新建连接失败不重试，直接报错）。

默认上限 1 次，换一条新连接。超时不重试。流式上传永不重试。body 在池化缓冲里，所以重试就是再 writev 一次，没有额外拷贝。

## 重定向

默认**不跟随**：3xx 就是答案，跟不跟由调用方决定。`.follow_redirects(n)` 或 `max_redirects` 打开后：

- 301 / 302 / 303 → 改成 GET 并丢掉 body；307 / 308 → 保持方法与 body；
- `Location` 支持绝对、协议相对（`//host/x`）、根相对（`/x`）和普通相对（按 RFC 3986 §5.3 与 base 路径合并）；
- 跨 origin 且请求写过 `Authorization` 时**不跟随**，把 3xx 交回调用方——用户头是原始字节，写进去就没法撤销某一条，宁可不跟随也不能把凭据带过去；
- 次数用尽、没有 `Location`、`Location` 解析失败：都是把 3xx 原样返回，不算错误。

## DNS

复用 `cornet::resolve()`（数字 IP 同步返回，仅域名走 getaddrinfo 线程池），在其上加一层 per-client 缓存：

- 数字 IP 字面量在缓存之前就短路——没有值得缓存的东西，也不计入 `dns_lookups`；
- 按 **host** 缓存（地址与端口无关），出手时把端口补进副本，所以查表可以直接用 `string_view`，不用拼 key；
- TTL `dns_cache_ttl`，容量 `dns_cache_entries`，满了先清过期项；
- 命中就省掉一次线程池往返——这是每请求延迟里最容易被忽视的一块。

```cpp
cli.dns().clear();
cli.dns().size();
```

## 配置

```toml
[cornet.http.client]
max_header_bytes = 16384        # 响应头缓冲；装不下即报错
max_headers = 64
max_trailers = 0                # chunked trailer 记录条数；0 = 丢弃（默认）
max_body_bytes = 8388608
aggregate_threshold = 262144    # 长度未知 body 的首次预留上限
head_buffer_bytes = 4096        # 请求行 + 框架头
hdr_buffer_bytes = 4096         # 用户头
chunk_buffer_bytes = 4096       # chunk-size 行

max_conns_per_host = 8
max_idle_per_host = 4
max_total_conns = 1024

max_retries = 1
max_redirects = 0
tcp_nodelay = true
send_user_agent = true
send_accept = true
user_agent = "cornet"
lenient_headers = false         # 每个 lenient_* 都会放回一种响应拆分变体
lenient_chunked_length = false
lenient_keep_alive = false

dns_cache_entries = 256
dns_cache_ttl = "30s"

connect_timeout = "5s"
write_timeout = "10s"
response_timeout = "10s"
body_timeout = "30s"
total_timeout = "60s"
idle_timeout = "60s"
pool_wait_timeout = "5s"
timer_tick = "500ms"
```

`client_t` 构造时自动 `load(ctx.config())`；也可以直接给结构体赋值：

```cpp
http::client_options_t opt;
opt.max_conns_per_host = 32;
opt.total_timeout = std::chrono::seconds(10);
http::client_t cli(ctx, opt);
```

时间类配置同时接受字符串（`"1500ms"`、`"3s"`）和整数毫秒。

## 指标

```cpp
const auto& m = cli.metrics();
```

| 字段 | 含义 |
|---|---|
| `requests` / `responses` | 发出的请求 / 收全的响应 |
| `conn_created` / `conn_reused` / `conn_closed` | 新建 / 复用 / 关闭的连接 |
| `connect_errors` | DNS 或 connect 失败 |
| `stale_discarded` | 复用前 peek 发现已失效而丢弃的连接 |
| `retries` / `redirects` | 自动重试 / 跟随重定向的次数 |
| `timeouts` / `protocol_errors` | 超时 / 协议错误 |
| `dns_lookups` / `dns_cache_hits` | 真查 / 命中缓存 |
| `writev_calls` / `writev_partial` | gather-write 次数 / 短写次数 |
| `pool_waits` | 因连接数达上限而排队的次数 |

配合 `buffer_pool_t::local().allocations()/hits()` 就能确认稳态是否真的零 malloc。

## 错误处理

```cpp
auto resp = co_await cli.get(url);
if (!resp) {
  auto err = resp.error();
  // err.domain:  System(errno) / Resolve(EAI_*) / Http(协议层)
  // err.message() 可直接打印
}
```

client 侧新增的 HTTP 域错误码（`http_error_t`，均在 `error_domain::Http`）：

| 码 | 含义 |
|---|---|
| `BadUrl` | URL 无法解析（含只给相对路径） |
| `UnsupportedScheme` | 本构建不支持的 scheme（`https`） |
| `PoolExhausted` | 排队等连接超时 |
| `ResponseIncomplete` | 对端在响应结束前关闭 |
| `InvalidState` | API 用错顺序，例如同时给暂存 body 和流式 body |

复用公共层的还有 `HeaderTooLarge`、`BodyTooLarge`、`OutputOverflow`、`TooManyHeaders`，以及 llhttp 原样透传的解析错误码。

## 低层接口

需要精确控制连接绑定（会话粘连、探活、协议升级），或者要对着手写字节流做断言时，可以绕开池直接用 `client_connection_t`：

```cpp
// 自己 connect
auto conn = co_await http::client_connection_t::open(ctx, opt, pool, wheel, metrics, host, port);
auto resp = co_await (*conn)->exchange(req);

// 或者接管一个已经连上的 socket
auto adopted = http::client_connection_t::adopt(ctx, std::move(sock), opt, pool, wheel,
                                                metrics, host, port);
```

关键方法：`exchange()`（写请求 + 读完整响应）、`begin_exchange()` + `read_body()` / `drain_body()` + `take_response()`（流式）、`begin_chunked()` + `write_chunk()` + `finish_chunks()` + `read_response()`（分块上传）、`reusable()` / `responded()` / `alive_hint()`（池与重试判断用）、`set_deadline()` / `abort()` / `close()`。

## 实现要点

### 响应缓冲怎么活过连接归池

`co_await send()` 之后连接立刻回池，而调用方还要读 body——但解析出来的每个视图都指向解析时用的缓冲。做法是：**把一次接收所需的全部状态打包成一个池化节点，解析一开始就落在最终归属地**。

```cpp
struct inbound_t {          // 由 buffer_pool_t 分配 + placement new
  head_buffer_t  head;      // 状态行、头部，以及它们之后的 body 窗口
  spill_buffer_t spill;     // 解析器留不住成视图的值
  headers_t      headers;   // 指向上面两块的偏移量条目
  body_buffer_t  body;      // 聚合 body
  status_t status; version_t version; uint64_t content_length; bool keep_alive; ...
};
```

消息完成时把节点指针交给 `client_response_t`，连接为下一个请求取一个新节点。没有 move、没有 rebind、没有拷贝，头部 offset 天然安全，稳态也不向 allocator 要内存。

这条成立的前提是「响应读完后接收缓冲里不该有剩余字节」：一条连接同时只有一个请求，所以剩余字节只能是对端多发的东西。检测到就把连接标成不可复用（响应本身仍然交付）。

### body 可以比接收缓冲大：窗口回绕

接收缓冲既装头部，也是 body 落地的窗口。头部视图是 (offset,len)，所以缓冲**不能** compact；但 body 字节是「到达即消费」的（拷进聚合缓冲，或作为视图交给流式 reader），于是头部区之后那段可以反复重用：

- 头部解析完时记下 `body_window_ = parser_.consumed_offset()`；
- 每次要更多 body 且窗口已解析干净时，`head.rewind_to(body_window_)` 再 recv，然后 `parser_.execute(body_window_, n)`；
- 所有头部 offset 都在 `body_window_` 之下，纹丝不动。

没有它，任何超过 `max_header_bytes`（默认 16K）的 body 都会以 `HeaderTooLarge` 失败。

### 一次请求一次 writev

`[请求行+框架头][用户头][CRLF][body]` 四段 iovec 一次 gather-write。空行单独成段，是因为用户头暂存在请求里、框架头在连接里，没有第三处能放它而不引入拷贝或改写请求（改写会让重试时多出一行）。body 是 static / owned 时全程零拷贝。

### chunked trailer：默认丢弃，可显式开启

trailer 走的是与真正头部同一组 llhttp 回调，但它的字节位于 body 区间——也就是会被回绕覆盖的那段。若原地记成偏移量，跨两次读的 trailer 前半段会被后半段盖掉，最终生成一条**名字和值都由 body 字节拼出来**的 header，等于把 header 注入原语交给了写 body 的那一方。

处理办法分两层：

**默认（`max_trailers = 0`）不记录。** trailer 到达时每个框架与路由决策都已经做完了，把它默认混进 header 表等于让每个 handler 都可能读到对端事后追加的值；而真正想用 trailer 的代码总是显式去找的。所以默认丢弃，且不占 `max_headers` 配额。

**开启（`max_trailers > 0`）时从第一段起就拷进 spill 缓冲**，不在 body 区间留任何视图，于是回绕覆盖也无所谓，跨读边界的相邻性假设整个不需要了（名字也一样，所以 trailer 的名字可以 spill）。开启后：

- **独立命名空间**：`headers.trailer(f)` / `resp->trailer(name)` 才能读到，`get()` 永远只扫非 trailer 条目——事后追加的值不可能回答一次 header 查询，`contains_token()` 因此也不受影响；
- **独立配额**：`max_trailers` 与 `max_headers` 互不侵占，trailer 洪水顶不出 431；
- **禁用字段一律丢弃**（无论开关状态）：`field_forbidden_in_trailer()` 按 RFC 9110 §6.5.1 拦掉 Host / Connection / Upgrade / Expect / Range / Cache-Control / Authorization / Cookie / Set-Cookie / Date / Location / Content-Type；`Content-Length` 和 `Transfer-Encoding` 更早——llhttp 自己就把它们判成协议错误（`HPE_INVALID_CONTENT_LENGTH` / `HPE_INVALID_TRANSFER_ENCODING`），消息直接失败、连接不复用，这是对的：框架被事后否认的消息不可信；
- **超限或装不下就丢那一条**，不是把整条消息判死：trailer 是 best effort，对端在这上面草率不该让一个本来完好的消息失败；
- **生命期**：trailer 与 header 同住一个 `inbound_t` 节点，所以 client 侧连接回池之后 `resp->trailer(...)` 依然有效；server 侧与折行 header 值同寿命，handler 期间有效。流式路由要注意 trailer 只在 body 读完之后才完整。

回绕点上另有一条 debug 断言 `CORNET_ASSERT(!parser_.mid_header(), ...)`，`mid_header()` 的定义是「半累积**且仍引用活缓冲**」——已拷进 spill 的半个 trailer 不算，所以断言守的正好是「没有任何指向回绕区的半成品视图」这条真不变量。

### 响应端解析

`parser_t` 是与 server 共用的 llhttp 包装，响应端额外处理三件事：

- **1xx**：`100 Continue`、`103 Early Hints` 在真正的响应之前到达，读到就 `reset()` 继续读，不上报调用方；
- **无 body 的响应**：1xx / 204 / 304，以及**对 HEAD 的响应**（靠 `parser_.set_response_to(m)` 告知）。这类消息在头部处就结束，否则解析器会等一个永远不来的 body，keep-alive 连接随后会把下一个响应当成这一个的 body；
- **读到关闭为止**的响应（既无 Content-Length 也非 chunked）：对端 FIN 就是 body 结束，同时连接不可复用。

### 挂在池上的请求要算 user work

协程挂在「不是 io 的东西」上（连接池槽位）时，tracker 里既没有 SQE 也没有 ready handle，`ctx.run()` 会判定应用已跑完直接返回，优雅退出也会提前 drain。等待者持一个 `context_t::work_token_t`（RAII，活着期间计入 user work），这是唯一能证明「这个请求还在」的东西。

### 每请求预算

0 次 malloc、1 次 `writev`、1–2 次 `recv`、0 次 `clock_gettime`（deadline 用 `ctx.coarse_now_ns()`，每轮事件循环只读一次时钟）。

## 测试

```bash
cmake --preset debug
cmake --build --preset debug --target unit
./cmake-build-debug/unit --gtest_filter='http_url.*:http_client*'
```

| 文件 | 覆盖 |
|---|---|
| `tests/http/url.cc` | URL 解析边界：无 scheme、IPv6 字面量、空 path、只有 query、非法端口、`same_origin`、Location 解析 |
| `tests/http/client_parser.cc` | 响应端解析：1xx、对 HEAD 的响应、204/304、chunked、读到关闭为止、非法状态行、超限 |
| `tests/http/client_message.cc` | URL 拷贝与移动、三种 body 所有权、框架头不重复、`inbound_t` 归池 |
| `tests/http/client_conn.cc` | 单连接闭环：分片到达、200K body、HEAD、100-continue、截断、多余字节、超时、流式收发 |
| `tests/http/client_pool.cc` | 复用、DNS 缓存、陈旧连接丢弃、重试恰好一次、非幂等不重试、排队与等待超时、空闲回收 |
| `tests/http/client_e2e.cc` | 打自家 `http::server_t`（同一个 context）：GET/POST/HEAD/404/chunked 上传/流式下载/并发/大 body/重定向 |

`tests/http/client_fixture.h` 是三个 io 用例共用的脚手架：跑在线程上的脚本化 origin（阻塞 socket，可按连接序号分别应答，所有 socket 带超时，客户端有 bug 时是测试失败而不是测试挂住），外加 `conn_env_t` 与 `dial()`。

前三个文件不依赖 io_uring，任何机器都能跑；后三个需要内核支持 io_uring。

## 限制与未实现

- **TLS**：`https` URL 直接返回 `UnsupportedScheme`，不会连上 443 再去解析看不懂的东西。
- **HTTP/2、pipelining**：都不做。pipelining 只在响应耗时均匀时才有收益，却让队头阻塞和重试语义都难以推理；并发靠多连接。
- **Cookie jar、自动解压（gzip）、代理**：未实现。
- **bench**：`bench` target 里的 `Cornet/HTTP` 行是 `client_t` + `server_t` 的完整栈 echo，和裸 send/recv 同口径对比（见 [HTTP Server](http_server.md#性能与裸-sendrecv-的对比)）；client 单独的压测项（并发连接、池行为）还没做。
- `TooManyRedirects` 错误码已定义但当前用不到：次数用尽时返回 3xx 响应本身，而不是报错。
- 手写过 `Host` 头的请求跟随跨 origin 重定向时，那个 `Host` 不会被改写（用户头是原始字节）。

## 注意事项

- `client_t` 不可跨线程共享，一个 context 一个；`runtime_t` 下每个 worker 各建一个。
- 请求还在飞的时候销毁 `client_t` 是调用方错误：`close()` 与析构会关掉池里所有连接。
- `client_response_t` / `client_stream_t` / `client_upload_t` 都只可移动。`client_stream_t` 与 `client_upload_t` 持有连接期间会占用池里一个名额。
- 时间轮由 context 按 tick 分发（`ctx.wheel_for(timer_tick)` 返回一份 `shared_ptr`）：同 context 上的 server、多个 client 只要 tick 相同就用同一个轮子，一个 tick SQE。空闲时两级都不留：没有 armed 节点时 runner 直接退出（下次 `arm()` 再拉起来），而 registry 只持 weak，最后一个持有者走了轮子本身（4KB 槽位）也一起回收。启停不用调用方管，client 只在 `close()` 里摘掉自己的节点。
- 窗口回绕在 server 侧也已用上（`src/http/server/connection.cc`），两端的 body 读取路径行为一致。

## URL 解析缓存（parse cache）

`client_t` 内置一个 128 槽直映射解析缓存：同一 URL 字符串的 `url_t::parse` 只做一次，
第二次直接命中（视图重锚到请求自己的池化租约上，生命周期与逐条 Scan 完全等价，
互斥/悬空不存在）。指标在 `client_metrics_t::url_cache_hits/_misses`。

命中 = 同一客户端、URL 字符串**逐字符相等**；不同 URL 即插入 miss+覆盖，
保持 O(1) 无锁，代价只在首次 miss。线上命中一旦形成稳态就是单次 memcpy + 一次哈希。

