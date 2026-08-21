# TLS

Cornet 的 TLS 是一个插入在 socket 与协议层之间的**传输层**：加解密发生在用户态
（OpenSSL 3.x + 内存 BIO），io_uring 只负责搬运密文。协议栈（HTTP server/client）
面对的是统一签名的字节流接口，对明文/TLS 无感知。

```cpp
// server
auto cx = tls::tls_context_t::make_server(tls::tls_server_options_t{
    .cert_file = "cert.pem", .key_file = "key.pem",
});
http::server_t server(ctx, http::server_options_t{.port = 8443, .tls = *cx});

// client
http::client_t client(ctx);   // https:// url 自动走 TLS，校验默认开启
```

构建：`-DCORNET_WITH_TLS=ON`（默认），需要 OpenSSL ≥ 3.0。依赖走系统包
（`apt install libssl-dev` / `dnf install openssl-devel`，setup.sh 已包含）而非
vcpkg manifest——离线构建环境里 manifest 没有可下载的来源。非标准路径的 OpenSSL
用 `-DOPENSSL_ROOT_DIR=...` 或 `OPENSSL_SSL_LIBRARY`/`OPENSSL_CRYPTO_LIBRARY`/
`OPENSSL_INCLUDE_DIR` 显式指定；也可以 `vcpkg install openssl` 走 classic 模式。
OpenSSL 私有链接进 `cornet_tls`，公开头文件不出现任何 OpenSSL 类型——与 llhttp
的隔离策略一致。

## 分层

| 层 | 文件 | 职责 |
|---|---|---|
| `tls_context_t` | `tls/context.h` | SSL_CTX 封装：证书/密钥/CA/ALPN，pimpl |
| `tls_engine_t` | `tls/engine.h` | **纯状态机**：SSL + 两个内存 BIO，不碰 socket |
| `tls_stream_t` | `tls/stream.h` | 协程泵：在 socket 与 engine 之间搬密文 |
| `transport_t` | `tls/transport.h` | 明文/TLS 统一的 `ccoro_t<expected<size_t>>` 接口 |

`tls_engine_t` 与 socket 的分离是故意的：引擎可以在两个引擎对象之间手工交换
密文完成握手/数据/shutdown 的全量测试（`tests/tls/engine.cc`），不需要 io_uring、
不需要 socket、甚至不需要支持 io_uring 的内核。

## 关键语义

- **取消与超时零成本复用**：stream 的全部操作是 `ccoro_t` 协程，叶子 IO 仍是
  socket awaiter。调用点用现成的 `with_cancel` / `with_timeout` 包住，层级取消
  自动传进握手/读写的内部挂起点，不存在任何 TLS 专属取消代码。
- **EOF 语义对齐 TCP**：`close_notify` 在 `recv()` 里映射为干净的 0 返回；
  没有 close_notify 的直接断连是 `tls_error_t::UnexpectedEof`。
- **半关闭对齐 TCP**：`shutdown_write()` 只发 close_notify，不等对端；对端的
  close_notify 作为后续 `recv()` 返回 0 到达——drain 逻辑与
  SHUT_WR-then-drain 完全同构。
- **握手先做，HTTP 后说**：server 在 accept 后、构造 `connection_t` 前握手，
  预算 `server_options_t::handshake_timeout`（默认 10s）。失败的握手记录到
  `connection_metrics_t::tls_handshake_errors` 后弃连接，不影响后续连接。
- **writev 的对价**：明文走原生 writev（服务端 pipelining 聚批不动）；TLS 下
  明文聚合进 16KiB 记录暂存、逐 record `SSL_write`，一次拷贝加之 splice 零拷贝
  失效——这是软件 record layer 的固有代价。
- **IO 步长自适应**：泵的收发缓冲从 16KiB（单 record 上限）起步，对端证明
  会用更大窗口（满拉）即升到 64KiB——小消息连接零成本，大消息连接
  io_uring op 数为 1/4。测量出来的数据：large_msg 场景 srv 保留率
  20.7%→27.2%、66% 以上；极限并发不为此付出每连接 64KiB 的内存。
- **`resp.local_file()` 在 TLS 下**：文件正文经 64KiB `pread` → record 层直写（无暂存上限）；
  明文 `transport` 下走 page-cache → pipe → socket 真零拷贝。安全语义不变，
  代价回到"软件 record layer 必然要抄一次"的固有水位。

## 客户端

- **https 即插即用**：`https://` 由 url 层解析（默认 443），`client_t` 在首次
  https 请求时按 `client_options_t` 的 tls_* 旋钮惰性构建默认 `tls_context_t`——
  纯 http 客户端永远不为校验路径付一分钱的惰性默认构造。
- **校验默认开启**（`tls_verify{true}`）：CA 来源 `tls_ca_pem` > `tls_ca_file`/
  `tls_ca_dir` > 系统默认 verify paths。证书 CN/SAN 校验走 OpenSSL 的
  `SSL_set1_host`，SNI 默认取 url host，`tls_server_name` 可覆盖。
- **连接池按 (host, port, scheme) 分键**：同一端口的明文与 TLS 连接是两个世界，
  永远不会互相发放（pool key = `port<<1 | https`）。
- **池化连接的活性探测 vs TLS 1.3 NewSessionTicket**：复用前的非阻塞
  `MSG_PEEK` 看到 ticket 密文会判为"流去同步"而丢弃连接——不是"偶尔浪费",
  在 2048 连接级会演变成**大规模重复握手**（profile 指纹：OpenSSL 锁、
  malloc 风暴、pool detach、page-fault 洪涌）。
  **处置**：`tls_server_options_t::num_tickets` 默认 0（cornet 不做会话恢复，
  ticket 只添乱）。面向**外部**服务端时 ticket 由对方决定，复用前的
  SSL 感知消化（engine 吃掉纯握手 record）是后续项——在此之前对端发
  ticket 时空闲连接复用率会下降，行为仍然安全。
- **池内关闭不做 close_notify 仪式**（析构/超时路径上无法 co_await），与业界
  keep-alive 连接静默淘汰的做法一致；不做会话恢复所以无恢复风险。

## 限制（有意）

- TLS 1.2 起步，TLS 1.3 优先；0-RTT/early data 不支持（HTTP/1.1 客户端
  本就用不到，且会模糊所见即所得的语义，不值得为边际收益引入）。
- ALPN 只配置 `http/1.1`；h2 不在本期范围。
- 内存 CA/证书装载面向测试与嵌入场景（链式证书请用文件路径）。
- 证书/私钥路径**不可经 TOML 配置加载**（秘密不属于配置文件），全部通过
  `tls_server_options_t` 编程传入；超时等旋钮可配置（见 `conf/default.toml`）。
- 会话票据旋转、mTLS 自动重载、OCSP stapling：后续项。
