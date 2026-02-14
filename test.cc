#include <benchmark/benchmark.h>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <future>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>

// ==================== 1. 你的Cornet框架 ====================
#include "core/socket.h"  // 你的socket头文件
// 假设你的命名空间是cornet，如果不是请调整
// 不使用using，显式使用cornet::前缀

// ==================== 2. Boost.Asio ====================
#include <asio.hpp>
// 不使用using，显式使用asio::前缀

// ==================== 3. libuv ====================
#include <uv.h>

// ==================== 4. 原始epoll ====================
#include <sys/epoll.h>

// ==================== 全局配置 ====================
constexpr int PORT = 1234;
constexpr int MESSAGE_SIZE = 64;
constexpr int LARGE_MESSAGE_SIZE = 4096;
const char* TEST_MESSAGE = "Hello, this is a benchmark test message!";

// ==================== 辅助函数 ====================
void set_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// ==================== Benchmark 基类 ====================
class NetworkBenchmark : public benchmark::Fixture {
 public:
  void SetUp(const ::benchmark::State& state) override {
    server_running = true;
    server_thread = std::thread([this]() { run_server(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  void TearDown(const ::benchmark::State& state) override {
    server_running = false;
    stop_server();
    if (server_thread.joinable()) {
      server_thread.join();
    }
  }

 protected:
  virtual void run_server() = 0;
  virtual void stop_server() = 0;
  virtual void run_client(int num_requests) = 0;

  std::thread server_thread;
  std::atomic<bool> server_running{true};
};

// ==================== 1. Cornet Benchmark ====================
class CornetBenchmark : public NetworkBenchmark {
 public:
  void run_server() override {
    // 使用你的cornet框架
    cornet::context_t& ctx = cornet::context_t::context();

    // server协程
    auto server_task = [&ctx, this]() -> cornet::coro_t<void> {
      cornet::tcp::v4::socket_t listener;
      listener.port_reuse(true);
      listener.address_reuse(true);
      if (!listener.listen("0.0.0.0", PORT)) {
        co_return;
      }

      while (server_running) {
        int client_fd = co_await listener.accept(ctx, 0);
        if (client_fd < 0) {
          if (-client_fd == ECANCELED) {
            co_return;
          }
          SPDLOG_ERROR("accept failed {} {}", client_fd, strerror(-client_fd));
          continue;
        }

        // 为每个client创建处理协程
        auto client_task = [&ctx, client_fd, this]() -> cornet::coro_t<void> {
          cornet::tcp::v4::socket_t client(client_fd);
          char buffer[LARGE_MESSAGE_SIZE];

          while (server_running) {
            auto n = co_await client.recv(ctx, buffer, sizeof(buffer));
            if (n <= 0) {
              if (n < 0) {
                SPDLOG_ERROR("recv failed {}, {} {} {}",strerror(errno), errno, n, strerror(-n));
              }
              break;
            }
            n = co_await client.send(ctx, buffer, n);
            if (n <= 0) {
              SPDLOG_ERROR("send failed {}, {} {} {}",strerror(errno), errno, n, strerror(-n));
              break;
            }
          }
          co_return;
        };

        ctx.sched(client_task());
      }
      co_return;
    };

    ctx.sched(server_task());
    ctx.run();
  }

  void stop_server() override {
    auto& ctx = cornet::context_t::from_thread(server_thread);
    ctx.sched(ctx.stop());
  }

  void run_client(int num_requests) override {
    cornet::context_t& ctx = cornet::context_t::context();
    std::atomic<int> completed{0};

    auto client_task = [&ctx, &completed, num_requests, this]() -> cornet::coro_t<void> {
      cornet::tcp::v4::socket_t client;

      int ok = co_await client.connect(ctx, "127.0.0.1", PORT);
      if (ok < 0) {
        SPDLOG_ERROR("failed");
        co_return;
      }

      char send_buf[MESSAGE_SIZE];
      char recv_buf[MESSAGE_SIZE];
      std::memset(send_buf, 'a', sizeof(send_buf));

      for (int i = 0; i < num_requests; i++) {
        auto sent = co_await client.send(ctx, send_buf, sizeof(send_buf));
        if (sent <= 0) {
          SPDLOG_ERROR("client send failed {}", strerror(errno));
          break;
        }

        auto received = co_await client.recv(ctx, recv_buf, sizeof(recv_buf));
        if (received <= 0) {
          SPDLOG_ERROR("client recv failed {}", strerror(errno));
          break;
        }
        completed++;
      }
      co_return;
    };

    ctx.sched(client_task());
    ctx.run();
  }
};

// ==================== 2. Boost.Asio Benchmark ====================
class AsioBenchmark : public NetworkBenchmark {
 public:
  void run_server() override {
    asio::io_context server_io;
    asio::ip::tcp::acceptor acceptor(
        server_io,
        asio::ip::tcp::endpoint(asio::ip::tcp::v4(), PORT),
        true
    );

    std::function<void()> do_accept = [&]() {
      acceptor.async_accept(
          [&](asio::error_code ec, asio::ip::tcp::socket socket) {
            if (!ec && server_running) {
              handle_client(std::move(socket));
            }
            if (server_running) {
              do_accept();
            }
          });
    };

    do_accept();
    server_io.run();
  }

  void handle_client(asio::ip::tcp::socket socket) {
    auto buffer = std::make_shared<std::array<char, LARGE_MESSAGE_SIZE>>();
    auto socket_ptr = std::make_shared<asio::ip::tcp::socket>(std::move(socket));

    std::function<void()> do_read = [=]() {
      socket_ptr->async_read_some(
          asio::buffer(*buffer),
          [=](asio::error_code ec, size_t length) {
            if (!ec && server_running) {
              asio::async_write(
                  *socket_ptr,
                  asio::buffer(buffer->data(), length),
                  [=](asio::error_code ec, size_t) {
                    if (!ec) {
                      do_read();
                    }
                  });
            }
          });
    };

    do_read();
  }

  void stop_server() override {
    server_running = false;
  }

  void run_client(int num_requests) override {
    asio::io_context client_io;
    asio::ip::tcp::socket socket(client_io);
    std::atomic<int> completed{0};

    socket.async_connect(
        asio::ip::tcp::endpoint(
            asio::ip::address::from_string("127.0.0.1"), PORT),
        [&](asio::error_code ec) {
          if (!ec) {
            auto send_buf = std::make_shared<std::array<char, MESSAGE_SIZE>>();
            auto recv_buf = std::make_shared<std::array<char, MESSAGE_SIZE>>();

            std::function<void()> do_request = [&, send_buf, recv_buf]() {
              if (completed >= num_requests) return;

              asio::async_write(
                  socket,
                  asio::buffer(*send_buf),
                  [&, send_buf, recv_buf](asio::error_code ec, size_t) {
                    if (!ec) {
                      socket.async_read_some(
                          asio::buffer(*recv_buf),
                          [&, send_buf, recv_buf](asio::error_code ec, size_t) {
                            if (!ec) {
                              completed++;
                              do_request();
                            }
                          });
                    }
                  });
            };

            do_request();
          }
        });

    client_io.run();
  }
};

// ==================== 3. libuv Benchmark ====================
class LibuvBenchmark : public NetworkBenchmark {
  struct ClientContext {
    uv_tcp_t handle;
    LibuvBenchmark* self;
    char buffer[LARGE_MESSAGE_SIZE];
  };

 public:
  void run_server() override {
    loop = uv_default_loop();
    server.data = this;

    uv_tcp_init(loop, &server);

    struct sockaddr_in addr;
    uv_ip4_addr("0.0.0.0", PORT, &addr);
    uv_tcp_bind(&server, (const struct sockaddr*)&addr, 0);

    uv_listen((uv_stream_t*)&server, 128,
              [](uv_stream_t* server, int status) {
                auto* self = static_cast<LibuvBenchmark*>(server->data);

                auto* client = new uv_tcp_t;
                uv_tcp_init(self->loop, client);

                if (uv_accept(server, (uv_stream_t*)client) == 0) {
                  auto* ctx = new ClientContext;
                  ctx->handle = *client;
                  ctx->self = self;
                  client->data = ctx;

                  uv_read_start((uv_stream_t*)client,
                                [](uv_handle_t*, size_t suggested_size, uv_buf_t* buf) {
                                  buf->base = new char[suggested_size];
                                  buf->len = suggested_size;
                                },
                                [](uv_stream_t* client, ssize_t nread, const uv_buf_t* buf) {
                                  auto* ctx = static_cast<ClientContext*>(client->data);
                                  if (nread > 0 && ctx->self->server_running) {
                                    uv_buf_t write_buf = uv_buf_init(buf->base, nread);
                                    uv_write_t* write_req = new uv_write_t;
                                    write_req->data = buf->base;
                                    uv_write(write_req, client, &write_buf, 1,
                                             [](uv_write_t* req, int status) {
                                               delete[] static_cast<char*>(req->data);
                                               delete req;
                                             });
                                  } else {
                                    delete[] buf->base;
                                    uv_close((uv_handle_t*)client,
                                             [](uv_handle_t* handle) {
                                               delete static_cast<ClientContext*>(handle->data);
                                               delete (uv_tcp_t*)handle;
                                             });
                                  }
                                });
                } else {
                  delete client;
                }
              });

    uv_run(loop, UV_RUN_DEFAULT);
  }

  void stop_server() override {
    uv_stop(loop);
  }

  void run_client(int num_requests) override {
    loop = uv_default_loop();
    client_req.data = this;
    client_handle.data = this;

    uv_tcp_init(loop, &client_handle);

    struct sockaddr_in dest;
    uv_ip4_addr("127.0.0.1", PORT, &dest);

    uv_tcp_connect(&client_req, &client_handle, (const struct sockaddr*)&dest,
                   [](uv_connect_t* req, int status) {
                     auto* self = static_cast<LibuvBenchmark*>(req->data);
                     if (status == 0) {
                       self->start_benchmark();
                     }
                   });

    uv_run(loop, UV_RUN_DEFAULT);
  }

  void start_benchmark() {
    completed = 0;
    send_buf = new char[MESSAGE_SIZE];
    recv_buf = new char[MESSAGE_SIZE];
    std::memset(send_buf, 'a', MESSAGE_SIZE);

    start_time = std::chrono::steady_clock::now();
    send_next();
  }

  void send_next() {
    uv_buf_t buf = uv_buf_init(send_buf, MESSAGE_SIZE);
    write_req.data = this;

    uv_write(&write_req, (uv_stream_t*)&client_handle, &buf, 1,
             [](uv_write_t* req, int status) {
               auto* self = static_cast<LibuvBenchmark*>(req->data);
               if (status == 0) {
                 self->start_read();
               }
             });
  }

  void start_read() {
    uv_read_start((uv_stream_t*)&client_handle,
                  [](uv_handle_t*, size_t suggested_size, uv_buf_t* buf) {
                    buf->base = new char[suggested_size];
                    buf->len = suggested_size;
                  },
                  [](uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf) {
                    auto* self = static_cast<LibuvBenchmark*>(stream->data);
                    if (nread > 0) {
                      delete[] buf->base;
                      self->completed++;
                      if (self->completed < self->num_requests) {
                        self->send_next();
                      } else {
                        self->end_time = std::chrono::steady_clock::now();
                        uv_stop(self->loop);
                      }
                    } else {
                      delete[] buf->base;
                    }
                  });
  }

 private:
  uv_loop_t* loop;
  uv_tcp_t server;
  uv_tcp_t client_handle;
  uv_connect_t client_req;
  uv_write_t write_req;
  std::atomic<int> completed{0};
  int num_requests;
  char* send_buf;
  char* recv_buf;
  std::chrono::steady_clock::time_point start_time, end_time;
};

// ==================== 4. Epoll Benchmark ====================
class EpollBenchmark : public NetworkBenchmark {
  static constexpr int MAX_EVENTS = 1024;
  static constexpr int BUFFER_SIZE = LARGE_MESSAGE_SIZE;

  struct Connection {
    int fd;
    char buffer[BUFFER_SIZE];
    size_t to_send;
    size_t sent;
    bool writing;
  };

 public:
  void run_server() override {
    int listen_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);

    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PORT);

    bind(listen_fd, (sockaddr*)&addr, sizeof(addr));
    listen(listen_fd, 128);
    int reuse = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, (void*)&reuse, sizeof(reuse));
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, (void*)&reuse, sizeof(reuse));

    int epoll_fd = epoll_create1(0);

    epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = listen_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev);

    epoll_event events[MAX_EVENTS];
    std::unordered_map<int, Connection*> connections;

    while (server_running) {
      int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, 100);

      for (int i = 0; i < nfds; i++) {
        if (events[i].data.fd == listen_fd) {
          // 新连接
          while (true) {
            sockaddr_in client_addr;
            socklen_t client_len = sizeof(client_addr);
            int client_fd = accept4(listen_fd, (sockaddr*)&client_addr,
                                    &client_len, SOCK_NONBLOCK);
            if (client_fd <= 0) break;

            auto* conn = new Connection{client_fd, "", 0, 0, false};
            connections[client_fd] = conn;

            ev.events = EPOLLIN | EPOLLET;
            ev.data.ptr = conn;
            epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);
          }
        } else {
          auto* conn = static_cast<Connection*>(events[i].data.ptr);

          if (events[i].events & EPOLLIN) {
            // 读事件
            ssize_t n = read(conn->fd, conn->buffer, BUFFER_SIZE);
            if (n > 0) {
              // 准备写回
              conn->to_send = n;
              conn->sent = 0;
              conn->writing = true;

              ev.events = EPOLLOUT | EPOLLET;
              ev.data.ptr = conn;
              epoll_ctl(epoll_fd, EPOLL_CTL_MOD, conn->fd, &ev);
            } else if (n == 0) {
              // 连接关闭
              close(conn->fd);
              connections.erase(conn->fd);
              delete conn;
            }
          }

          if (events[i].events & EPOLLOUT) {
            // 写事件
            if (conn->writing) {
              ssize_t n = write(conn->fd,
                                conn->buffer + conn->sent,
                                conn->to_send - conn->sent);
              if (n > 0) {
                conn->sent += n;
                if (conn->sent >= conn->to_send) {
                  // 写完，切回读模式
                  conn->writing = false;
                  ev.events = EPOLLIN | EPOLLET;
                  ev.data.ptr = conn;
                  epoll_ctl(epoll_fd, EPOLL_CTL_MOD, conn->fd, &ev);
                }
              }
            }
          }

          if (events[i].events & (EPOLLERR | EPOLLHUP)) {
            close(conn->fd);
            connections.erase(conn->fd);
            delete conn;
          }
        }
      }
    }

    close(listen_fd);
    close(epoll_fd);
    for (auto& [fd, conn] : connections) {
      close(fd);
      delete conn;
    }
  }

  void stop_server() override {
    server_running = false;
  }

  void run_client(int num_requests) override {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);

    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    // 连接
    int epoll_fd = epoll_create1(0);
    epoll_event ev;
    ev.events = EPOLLOUT;
    ev.data.fd = fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev);

    connect(fd, (sockaddr*)&addr, sizeof(addr));

    epoll_event events[1];
    int completed = 0;
    char send_buf[MESSAGE_SIZE];
    char recv_buf[MESSAGE_SIZE];
    std::memset(send_buf, 'a', sizeof(send_buf));

    bool writing = false;
    size_t sent = 0;
    size_t to_send = 0;
    bool reading = false;
    size_t received = 0;

    while (completed < num_requests && server_running) {
      int nfds = epoll_wait(epoll_fd, events, 1, -1);

      for (int i = 0; i < nfds; i++) {
        if (events[i].events & EPOLLOUT) {
          if (!writing && completed < num_requests) {
            // 发送新请求
            to_send = MESSAGE_SIZE;
            sent = 0;
            writing = true;
          }

          if (writing) {
            ssize_t n = write(fd, send_buf + sent, to_send - sent);
            if (n > 0) {
              sent += n;
              if (sent >= to_send) {
                writing = false;
                reading = true;
                received = 0;

                ev.events = EPOLLIN;
                epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
              }
            }
          }
        }

        if (events[i].events & EPOLLIN) {
          if (reading) {
            ssize_t n = read(fd, recv_buf + received, MESSAGE_SIZE - received);
            if (n > 0) {
              received += n;
              if (received >= MESSAGE_SIZE) {
                reading = false;
                completed++;

                ev.events = EPOLLOUT;
                epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
              }
            }
          }
        }
      }
    }

    close(fd);
    close(epoll_fd);
  }
};

// ==================== Benchmark 测试用例 ====================

// 1. 小消息吞吐量测试 (64 bytes)
BENCHMARK_DEFINE_F(CornetBenchmark, SmallMsgThroughput)(benchmark::State& state) {
  int num_requests = state.range(0);

  for (auto _ : state) {
    run_client(num_requests);
  }

  state.SetItemsProcessed(state.iterations() * num_requests);
}
BENCHMARK_REGISTER_F(CornetBenchmark, SmallMsgThroughput)
->Arg(1000)->Arg(10000)->Arg(100000)->Unit(benchmark::kMicrosecond);

//BENCHMARK_DEFINE_F(AsioBenchmark, SmallMsgThroughput)(benchmark::State& state) {
//  int num_requests = state.range(0);
//
//  for (auto _ : state) {
//    run_client(num_requests);
//  }
//
//  state.SetItemsProcessed(state.iterations() * num_requests);
//}
//BENCHMARK_REGISTER_F(AsioBenchmark, SmallMsgThroughput)
//->Arg(1000)->Arg(10000)->Arg(100000)->Unit(benchmark::kMicrosecond);

//BENCHMARK_DEFINE_F(LibuvBenchmark, SmallMsgThroughput)(benchmark::State& state) {
//  int num_requests = state.range(0);
//
//  for (auto _ : state) {
//    run_client(num_requests);
//  }
//
//  state.SetItemsProcessed(state.iterations() * num_requests);
//}
//BENCHMARK_REGISTER_F(LibuvBenchmark, SmallMsgThroughput)
//->Arg(1000)->Arg(10000)->Arg(100000)->Unit(benchmark::kMicrosecond);

//BENCHMARK_DEFINE_F(EpollBenchmark, SmallMsgThroughput)(benchmark::State& state) {
//  int num_requests = state.range(0);
//
//  for (auto _ : state) {
//    run_client(num_requests);
//  }
//
//  state.SetItemsProcessed(state.iterations() * num_requests);
//}
//BENCHMARK_REGISTER_F(EpollBenchmark, SmallMsgThroughput)
//->Arg(1000)->Arg(10000)->Arg(100000)->Unit(benchmark::kMicrosecond);

// 2. 延迟测试
BENCHMARK_DEFINE_F(CornetBenchmark, Latency)(benchmark::State& state) {
  for (auto _ : state) {
    run_client(1);
  }
}
BENCHMARK_REGISTER_F(CornetBenchmark, Latency)->Unit(benchmark::kMicrosecond);

//BENCHMARK_DEFINE_F(AsioBenchmark, Latency)(benchmark::State& state) {
//  for (auto _ : state) {
//    run_client(1);
//  }
//}
//BENCHMARK_REGISTER_F(AsioBenchmark, Latency)->Unit(benchmark::kMicrosecond);
//
//BENCHMARK_DEFINE_F(LibuvBenchmark, Latency)(benchmark::State& state) {
//  for (auto _ : state) {
//    run_client(1);
//  }
//}
//BENCHMARK_REGISTER_F(LibuvBenchmark, Latency)->Unit(benchmark::kMicrosecond);
//
//BENCHMARK_DEFINE_F(EpollBenchmark, Latency)(benchmark::State& state) {
//  for (auto _ : state) {
//    run_client(1);
//  }
//}
//BENCHMARK_REGISTER_F(EpollBenchmark, Latency)->Unit(benchmark::kMicrosecond);

// 3. 大消息吞吐量 (4KB)
BENCHMARK_DEFINE_F(CornetBenchmark, LargeMsgThroughput)(benchmark::State& state) {
  int num_requests = state.range(0);

  for (auto _ : state) {
    run_client(num_requests);
  }

  state.SetBytesProcessed(state.iterations() * num_requests * LARGE_MESSAGE_SIZE);
}
BENCHMARK_REGISTER_F(CornetBenchmark, LargeMsgThroughput)
->Arg(100)->Arg(1000)->Arg(10000)->Unit(benchmark::kMicrosecond);

//BENCHMARK_DEFINE_F(AsioBenchmark, LargeMsgThroughput)(benchmark::State& state) {
//  int num_requests = state.range(0);
//
//  for (auto _ : state) {
//    run_client(num_requests);
//  }
//
//  state.SetBytesProcessed(state.iterations() * num_requests * LARGE_MESSAGE_SIZE);
//}
//BENCHMARK_REGISTER_F(AsioBenchmark, LargeMsgThroughput)
//->Arg(100)->Arg(1000)->Arg(10000)->Unit(benchmark::kMicrosecond);

// ==================== 主函数 ====================
int main(int argc, char** argv) {
  ::benchmark::Initialize(&argc, argv);

  std::cout << "========================================\n";
  std::cout << "Network Library Benchmark\n";
  std::cout << "Message Size: " << MESSAGE_SIZE << " bytes (small)\n";
  std::cout << "Message Size: " << LARGE_MESSAGE_SIZE << " bytes (large)\n";
  std::cout << "Port: " << PORT << "\n";
  std::cout << "========================================\n\n";

  if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }

  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();

  return 0;
}