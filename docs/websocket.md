# WebSocket

Cornet 的 WebSocket 是 HTTP 模块的一部分（RFC 6455）：服务端复用同一条 HTTP 连接完成 Upgrade 握手，握手成功后整条连接切换为帧协议。客户端是一站式的 `websocket::connect()`。

- **同端口复用** — WebSocket 路由与普通 HTTP 路由共享监听端口；没有 Upgrade 头的 GET 走原来的 HTTP 路径
- **零拷贝接收** — 未分片且落在接收窗口内的消息直接以视图交付，不经过聚合缓冲
- **掩码纪律** — 服务端拒绝未掩码的客户端帧、客户端拒绝掩码的服务端帧（RFC 6455 §5.1），结构性违规一律 1002 关闭
- **控制帧自动化** — Ping 自动回 Pong（跨分片交错也不干扰重组）、Close 自动应答并只交付一次
- **分片重组** — continuation 帧聚合交付，受 `max_message_bytes` 上限约束（超限 1009）
- **ws:// 与 wss:// 统一** — 会话跑在 `tls::transport_t` 上，与 HTTP 完全同款的明文/TLS 传输抽象
- **优雅关闭** — 半双工 Close 握手（发送 → 等回应 → 半关 → 短引流），服务端 drain 时以 1001 GoingAway 收尾
- **无扩展** — 不协商 permessage-deflate 等任何扩展；RSV 位非 0 即协议错误

命名空间 `cornet::websocket`，伞头 `<cornet/websocket.h>`（`<cornet/http.h>` 已包含）。

## 服务端

```cpp
#include <cornet/http.h>

using namespace cornet;

int main() {
  context_t ctx;
  http::server_t server(ctx);

  server.websocket("/echo", [](websocket::session_t& ws) -> coro_t<void> {
    while (auto msg = co_await ws.recv()) {
      if (msg->opcode == websocket::opcode_t::Close) break;
      if (auto ok = co_await ws.send(msg->payload, msg->opcode); !ok) break;
    }
  });

  if (auto ok = server.listen("0.0.0.0", 8080); !ok) return 1;
  ctx.on_signal({SIGINT, SIGTERM}, [&server](int) { server.drain(); });
  ctx.spawn(server.serve());
  ctx.run();
}
```

`websocket(path, handler)` 按 GET 方法注册路由；路径匹配（精确 / `:param` / `*wildcard`）与普通路由完全一致。handler 恒为协程——WebSocket 处理器注定要挂起在 IO 上，没有同步形态。

### 握手流程

一个 GET Upgrade 请求到达时，连接依次检查：

1. 路由匹配到 `kind == WebSocket` 的条目，否则回 **501 Not Implemented**；**不带 Upgrade 头的普通 GET** 命中 ws 路由则回 **426 Upgrade Required**；
2. RFC 6455 §4.2.1 校验：**400 Bad Request**（缺少 `Sec-WebSocket-Version: 13`、key 畸形、Upgrade 值不是 `websocket`、方法不是 GET）；
3. **HTTP filters** 照常执行——鉴权过滤器对 WebSocket 路由同样有效，拒绝时按照滤器写出的响应回复；
4. 可选的路由级守门员（见下）；
5. 回 101（预组常量头块，`Sec-WebSocket-Accept` 现算现发），已管道化的先前响应先行 flush；
6. 连接持有的 HTTP 缓冲释放还给池，传输对象与握手后剩余字节移交给 session，handler 开始驱动。

### 守门员 accept()

```cpp
server.websocket("/chat", handler).accept([](http::request_t& req, websocket::accept_info_t& info) {
  if (req.headers().get("origin") != "https://example.com") {
    info.refuse_with = http::status_t::Forbidden;
    return false;
  }
  info.subprotocol = "chat";   // 必须来自客户端的 offer 列表
  return true;
});
```

守门员是同步的，在 101 写出之前运行：返回 false 即以 `refuse_with`（默认 403）拒绝。需要 IO 的鉴权请放到 filter 里。

### session_t

```cpp
struct message_t {
  opcode_t opcode;            // Text / Binary / Close（Ping/Pong 不上浮）
  std::string_view payload;   // 视图指入会话缓冲，下一次 recv() 前有效
  close_code_t close_code() const;      // Close 消息专用
  std::string_view close_reason() const;
};

class session_t {
 public:
  coro_t<expected<message_t>> recv();
  coro_t<expected<void>> send(std::string_view payload, opcode_t op = opcode_t::Text);
  coro_t<expected<void>> send_text(std::string_view);
  coro_t<expected<void>> send_binary(std::string_view);
  coro_t<expected<void>> ping(std::string_view payload = {});
  coro_t<expected<void>> close(close_code_t = close_code_t::Normal, std::string_view reason = {});
  coro_t<void> finish();
  void request_close();                    // drain 入口：recv 立即 ECANCELED
  bool is_open() const;
  std::string_view subprotocol() const;    // 握手选定的子协议，可为空
};
```

- `recv()` 返回的错误：传输层 errno；`websocket_error_t`（对端协议违规，违规时已自动按对应码关闭）；`ETIMEDOUT`（空闲超时）；`ECANCELED`（drain）；`Closed`（Close 交付之后再调用）。
- 收到 Close：若是先收方会自动回显（同码同 reason），并向 handler 交付一次 Close 消息；handler 应当循环退出或调 `close()`。
- `send()` 服务端零拷贝（载荷独立 iovec）；客户端需要掩码变换，载荷复制进会话暂存缓冲。
- 一个 coroutine 驱动读、另一个驱动写是支持的形态；两个并发读者不支持（帧会交错）。

### drain 集成

`server_t::drain()` 会经由连接转发到活跃 session 的 `request_close()`：handler 的 `recv()` 以 `ECANCELED` 返回，`finish()` 以 **1001 GoingAway** 完成关闭握手——与普通响应"读可取消、写跑完"的纪律一致。

## 客户端

```cpp
coro_t<void> chat(context_t& ctx) {
  auto conn = co_await websocket::connect(ctx, "ws://localhost:8080/chat");
  if (!conn) co_return;
  auto& ws = **conn;

  co_await ws.send_text("hello");
  auto msg = co_await ws.recv();
  co_await ws.close();
  co_await ws.finish();     // 等对端的 Close 回应，然后关闭传输
}
```

```cpp
struct client_options_t {
  std::shared_ptr<tls::tls_context_t> tls{};   // wss:// 必填（make_client 构造）
  std::chrono::milliseconds handshake_timeout{10000};  // 覆盖 connect+TLS+Upgrade 总预算
  std::vector<std::string> subprotocols{};             // offer 列表，按偏好序
  session_options_t session{};
};
```

握手校验（RFC 6455 §4.1/§4.2.2）：状态非 101、`Upgrade`/`Connection` 缺失、`Sec-WebSocket-Accept` 哈希不匹配、服务端选择了未提供的扩展或子协议，一律 `HandshakeFailed`。错误通道另有：系统错误（connect/TLS）、`http_error_t::BadUrl/UnsupportedScheme`。

## 配置

```toml
[cornet.http.server.ws]
max_message_bytes = 16777216   # 聚合消息上限，超限以 1009 关闭（默认 16MiB）
idle_timeout = "0"             # 无帧流量多久后断开；"0" 关闭（默认）
close_timeout = "2s"           # finish() 等对端 Close 的预算
recv_buffer_bytes = 65536      # 接收窗口；更大的帧跨窗口聚合，纯内存预算
```

## 协议纪律（实现侧要点）

- **帧结构强制**：RSV=0、opcode 合法、控制帧 `FIN=1` 且载荷 ≤125、扩展长度取最简形（126→>125、127→>65535、MSB=0）——违反即以 1002 关闭。
- **分片规则**：`Continue` 必须有开启中的消息、消息中途不得插入新数据帧——同样 1002。
- **Close 码校验**：1004/1005/1006/1015 及 <1000 的码不得上线（收到视为 1002）。
- **UTF-8 不验证**：text 帧内容不检查（多数高性能实现的同款取舍；需要则在应用层验证）。
- **读写纪律**与 HTTP 连接相同：超时/取消只作用于读，写永不取消（截断的帧比超时更糟）；定时全部挂在共享 `timer_wheel_t` 上，一个连接一个内嵌 `timer_node_t`。

## 限制与未实现

- 发送侧不支持显式分片（`send()` 总是单帧 FIN=1）；接收侧分片完整重组。
- 无 permessage-deflate 等扩展（RSV 拒绝）。
- 无自动心跳定时器：`ping()` 提供手动 API，`idle_timeout` 提供断路器。
- text 帧不做 UTF-8 验证。
