#ifndef CORNET_BENCH_LIBUV_H
#define CORNET_BENCH_LIBUV_H

#include "common.h"
#include <uv.h>
#include <thread>
#include <vector>
#include <cstring>

namespace bench {

namespace detail_uv {

struct server_ctx {
  uv_loop_t* loop;
  int msg_size;
  std::atomic<bool>* running;
};

struct conn_ctx {
  uv_tcp_t handle;
  std::vector<char> buf;
  int msg_size;
};

inline void alloc_cb(uv_handle_t* handle, size_t suggested, uv_buf_t* buf) {
  auto* ctx = (conn_ctx*)handle->data;
  buf->base = ctx->buf.data();
  buf->len = ctx->buf.size();
}

inline void write_cb(uv_write_t* req, int status) {
  delete req;
}

inline void read_cb(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf) {
  if (nread <= 0) {
    uv_close((uv_handle_t*)stream, [](uv_handle_t* h) {
      delete (conn_ctx*)h->data;
    });
    return;
  }
  auto* req = new uv_write_t;
  uv_buf_t wbuf = uv_buf_init(buf->base, nread);
  uv_write(req, stream, &wbuf, 1, write_cb);
}

inline void on_connection(uv_stream_t* server, int status) {
  if (status < 0) return;
  auto* sctx = (server_ctx*)server->data;

  auto* conn = new conn_ctx;
  conn->buf.resize(sctx->msg_size + 64);
  conn->msg_size = sctx->msg_size;
  uv_tcp_init(sctx->loop, &conn->handle);
  conn->handle.data = conn;

  if (uv_accept(server, (uv_stream_t*)&conn->handle) == 0) {
    uv_read_start((uv_stream_t*)&conn->handle, alloc_cb, read_cb);
  } else {
    uv_close((uv_handle_t*)&conn->handle, [](uv_handle_t* h) { delete (conn_ctx*)h->data; });
  }
}

struct client_ctx {
  uv_tcp_t handle;
  uv_connect_t connect_req;
  std::vector<char> send_buf;
  std::vector<char> recv_buf;
  int msg_size;
  int received;
  std::shared_ptr<std::atomic<int>> remaining;
  latency_collector_t* collector;
  std::chrono::steady_clock::time_point t0;
};

inline void client_read_cb(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf);
inline void client_write_cb(uv_write_t* req, int status);

inline void client_do_send(client_ctx* ctx) {
  int cur = (*ctx->remaining)--;
  if (cur <= 0) {
    uv_close((uv_handle_t*)&ctx->handle, [](uv_handle_t* h) { delete (client_ctx*)h->data; });
    return;
  }
  ctx->t0 = std::chrono::steady_clock::now();
  ctx->received = 0;
  auto* req = new uv_write_t;
  req->data = ctx;
  uv_buf_t wbuf = uv_buf_init(ctx->send_buf.data(), ctx->msg_size);
  uv_write(req, (uv_stream_t*)&ctx->handle, &wbuf, 1, client_write_cb);
}

inline void client_write_cb(uv_write_t* req, int status) {
  delete req;
}

inline void client_alloc_cb(uv_handle_t* handle, size_t suggested, uv_buf_t* buf) {
  auto* ctx = (client_ctx*)handle->data;
  buf->base = ctx->recv_buf.data() + ctx->received;
  buf->len = ctx->msg_size - ctx->received;
}

inline void client_read_cb(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf) {
  auto* ctx = (client_ctx*)stream->data;
  if (nread <= 0) {
    uv_close((uv_handle_t*)stream, [](uv_handle_t* h) { delete (client_ctx*)h->data; });
    return;
  }
  ctx->received += nread;
  if (ctx->received >= ctx->msg_size) {
    auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now() - ctx->t0).count();
    ctx->collector->record(latency, ctx->msg_size * 2);
    client_do_send(ctx);
  }
}

inline void on_connect(uv_connect_t* req, int status) {
  auto* ctx = (client_ctx*)req->data;
  if (status < 0) {
    uv_close((uv_handle_t*)&ctx->handle, [](uv_handle_t* h) { delete (client_ctx*)h->data; });
    return;
  }
  uv_read_start((uv_stream_t*)&ctx->handle, client_alloc_cb, client_read_cb);
  client_do_send(ctx);
}

} // namespace detail_uv

inline result_t run_libuv(const scenario_t& scenario) {
  std::atomic<bool> server_running{true};
  latency_collector_t collector;
  collector.reserve(scenario.total_messages);
  auto remaining = std::make_shared<std::atomic<int>>(scenario.total_messages);

  std::thread server_thread([&] {
    uv_loop_t loop;
    uv_loop_init(&loop);

    uv_tcp_t server;
    uv_tcp_init(&loop, &server);

    detail_uv::server_ctx sctx{&loop, scenario.message_size, &server_running};
    server.data = &sctx;

    struct sockaddr_in addr;
    uv_ip4_addr("127.0.0.1", 9879, &addr);
    uv_tcp_bind(&server, (const struct sockaddr*)&addr, 0);
    uv_listen((uv_stream_t*)&server, 2048, detail_uv::on_connection);

    while (server_running && uv_loop_alive(&loop)) {
      uv_run(&loop, UV_RUN_NOWAIT);
    }
    uv_close((uv_handle_t*)&server, nullptr);
    uv_run(&loop, UV_RUN_DEFAULT);
    uv_loop_close(&loop);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  uv_loop_t client_loop;
  uv_loop_init(&client_loop);

  struct sockaddr_in addr;
  uv_ip4_addr("127.0.0.1", 9879, &addr);

  for (int i = 0; i < scenario.connections; ++i) {
    auto* ctx = new detail_uv::client_ctx;
    ctx->send_buf.resize(scenario.message_size, 'A' + (i % 26));
    ctx->recv_buf.resize(scenario.message_size + 64);
    ctx->msg_size = scenario.message_size;
    ctx->received = 0;
    ctx->remaining = remaining;
    ctx->collector = &collector;

    uv_tcp_init(&client_loop, &ctx->handle);
    ctx->handle.data = ctx;
    ctx->connect_req.data = ctx;
    uv_tcp_connect(&ctx->connect_req, &ctx->handle, (const struct sockaddr*)&addr, detail_uv::on_connect);
  }

  size_t rss_before = get_current_rss_kb();
  auto start = std::chrono::steady_clock::now();
  uv_run(&client_loop, UV_RUN_DEFAULT);
  auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

  uv_loop_close(&client_loop);
  server_running = false;
  server_thread.join();

  return collector.compute("Libuv", scenario.name, elapsed, rss_before);
}

} // namespace bench

#endif // CORNET_BENCH_LIBUV_H
