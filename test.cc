// cornet_vs_asio_benchmark.cpp (修复缓冲区溢出版本)
#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <cstring>
#include <iomanip>
#include <functional>
#include <asio.hpp>

// cornet 头文件
#include <numeric>

#include "core/socket.h"
#include "core/context.h"
#include "core/tools.h"

using namespace cornet;

// ==================== 基准测试配置 ====================
struct BenchmarkConfig {
  BenchmarkConfig() {
    if (auto bench_config = config_t::get()["cornet"]["benchmark"]) {
      num_connections = bench_config["num_connections"].value_or(1024);
      message_size = bench_config["message_size"].value_or(1024);
      total_messages = bench_config["total_messages"].value_or(10000);
    }
  }
  int num_connections = 1024;  // 并发连接数
  int message_size = 1024;     // 消息大小（字节）
  int total_messages = 100000; // 总消息数
  bool echo_mode = true;       // 回声模式（客户端发送，服务器回声）

  void print() const {
    std::cout << "====================================\n";
    std::cout << "基准测试配置:\n";
    std::cout << "  并发连接数: " << num_connections << "\n";
    std::cout << "  消息大小: " << message_size << " bytes\n";
    std::cout << "  总消息数: " << total_messages << "\n";
    std::cout << "  模式: " << (echo_mode ? "回声测试" : "吞吐测试") << "\n";
    std::cout << "====================================\n\n";
  }
};

// ==================== 测试结果 ====================
struct BenchmarkResult {
  std::string library_name;
  double duration_seconds;    // 测试持续时间
  double requests_per_second; // 每秒请求数
  double throughput_mbps;     // 吞吐量（MB/s）
  double avg_latency_us;      // 平均延迟（微秒）
  double min_latency_us;      // 最小延迟
  double max_latency_us;      // 最大延迟
  double p95_latency_us;      // 95%延迟
  double p99_latency_us;      // 99%延迟
};

// ==================== 性能监控器 ====================
class PerfMonitor {
private:
  std::chrono::time_point<std::chrono::steady_clock> start_time;
  std::chrono::time_point<std::chrono::steady_clock> end_time;
  std::atomic<long long> total_bytes_sent{0};
  std::atomic<long long> total_bytes_recv{0};
  std::atomic<long long> total_messages{0};
  std::vector<long long> latencies;
  std::mutex latency_mutex;

public:
  void start() {
    start_time = std::chrono::steady_clock::now();
    total_bytes_sent = 0;
    total_bytes_recv = 0;
    total_messages = 0;
    latencies.clear();
  }

  void stop() {
    end_time = std::chrono::steady_clock::now();
  }

  void record_message(int sent_bytes, int recv_bytes, long long latency_us) {
    total_bytes_sent += sent_bytes;
    total_bytes_recv += recv_bytes;
    total_messages++;
    std::lock_guard<std::mutex> lock(latency_mutex);
    latencies.push_back(latency_us);
  }

  BenchmarkResult get_result(const std::string& name) {
    BenchmarkResult result;
    result.library_name = name;

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
      end_time - start_time).count();
    result.duration_seconds = duration / 1000000.0;

    result.requests_per_second = (total_messages * 1000000.0) / duration;

    double total_bytes = total_bytes_sent + total_bytes_recv;
    result.throughput_mbps = (total_bytes / 1000000.0) / result.duration_seconds; // MB/s

    if (!latencies.empty()) {
      std::sort(latencies.begin(), latencies.end());
      double sum = std::accumulate(latencies.begin(), latencies.end(), 0.0);
      result.avg_latency_us = sum / latencies.size();
      result.min_latency_us = latencies.front();
      result.max_latency_us = latencies.back();
      result.p95_latency_us = latencies[static_cast<size_t>(latencies.size() * 0.95)];
      result.p99_latency_us = latencies[static_cast<size_t>(latencies.size() * 0.99)];
    }

    return result;
  }
};

// ==================== Cornet 测试 ====================
class CornetBenchmark {
private:
  std::shared_ptr<PerfMonitor> monitor;
  std::atomic<bool> server_running{true};
  std::atomic<int> active_connections{0};
  scheduler_type_t type;
public:
  CornetBenchmark(scheduler_type_t type) {
    this->type = type;
    monitor = std::make_shared<PerfMonitor>();
  }

  // 服务器协程 - 处理单个客户端连接
  coro_t<void> server_session(context_t& ctx, const BenchmarkConfig& config, int client_fd) {
    active_connections++;
    auto socket = tcp::v4::socket_t(client_fd);
    // 使用动态分配的缓冲区，避免栈溢出
    auto buffer = std::make_shared<std::vector<char> >(4096);

    while (server_running) {

      chain_builder builder(ctx);
      auto ra = socket.recv(ctx, buffer->data(), config.message_size);
      auto wa = socket.send(ctx, buffer->data(), config.message_size);
      co_await builder.with_link(ra).chain(wa);

      auto r = ra.value;
      auto w = wa.value;
      if (r <=0 || w <=0) {
        break;
      }
//      auto n = co_await socket.recv(ctx, buffer->data(), buffer->size());
//      if (n <= 0)
//        break;
//      auto send_n = co_await socket.send(ctx, buffer->data(), n);
//      if (send_n <= 0)
//        break;
    }

    active_connections--;
    co_return;
  }

  // 服务器主协程
  coro_t<void> server_main(context_t& ctx, const BenchmarkConfig& config) {
    auto listener = tcp::v4::socket_t();
    if (!listener.listen("127.0.0.1", "12345")) {
      std::cerr << "服务器监听失败\n";
      co_return;
    }

    while (server_running) {
      sockaddr_storage addr{};
      socklen_t len;
      int client_fd = co_await listener.accept(ctx, (sockaddr*)&addr, &len);
      auto s = co_await listener.accept(ctx);
      if (client_fd < 0)
        break;

      // 为每个客户端创建一个会话协程
      ctx.sched(server_session(ctx, config, client_fd));
    }

    co_return;
  }

  // 客户端协程
  coro_t<void> client_session(context_t& ctx, int id, const BenchmarkConfig& config,
                              std::shared_ptr<std::atomic<int> > remaining_msgs) {
    auto socket = std::make_shared<tcp::v4::socket_t>();

    // 连接到服务器
    int ok = co_await socket->connect(ctx, "127.0.0.1", "12345");
    if (ok < 0) {
      co_return;
    }

    // 准备消息 - 使用shared_ptr管理缓冲区
    auto send_buf = std::make_shared<std::string>(config.message_size, 'a' + (id % 26));
    auto recv_buf = std::make_shared<std::vector<char> >(config.message_size + 1024);

    // 持续发送消息直到达到总数
    while (true) {
      int current = (*remaining_msgs)--;
      if (current <= 0)
        break;

      auto send_start = std::chrono::steady_clock::now();

      auto builder = chain_builder(ctx);
      auto s = socket->send(ctx, send_buf->data(), send_buf->size(), MSG_WAITALL);
      auto r =socket->recv(ctx, recv_buf->data(), send_buf->size(), MSG_WAITALL);
      co_await builder.with_link(s).chain(r);

      if (s.value <= 0 || r.value <= 0) {
        break;
      }
//      // 发送消息
//      int sent = co_await socket->send(ctx, send_buf->data(), send_buf->size());
//      if (sent <= 0)
//        break;
//
//      // 接收回声
//      int received = 0;
//      int need_recv = send_buf->size();
//      while (received < need_recv) {
//        int n = co_await socket->recv(ctx,
//                                      recv_buf->data() + received,
//                                      recv_buf->size() - received);
//        if (n <= 0)
//          break;
//        received += n;
//      }

      if (r.value == send_buf->size()) {
        auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - send_start).count();

        monitor->record_message(s.value, r.value, latency);
      }
    }

    co_return;
  }

  BenchmarkResult run(const BenchmarkConfig& config) {
    monitor->start();
    server_running = true;

    // 启动服务器
    std::thread server_thread([this, &config] {
      auto& ctx = context_t::context();
      ctx.set_scheduler_type(type);
      auto server = server_main(ctx, config);
      ctx.sched(server);
      ctx.run();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    auto& ctx = context_t::context();
    ctx.set_scheduler_type(type);
    auto remaining_msgs = std::make_shared<std::atomic<int> >(config.total_messages);
    std::vector<coro_t<void> > clients;
    for (int i = 0; i < config.num_connections; i++) {
      clients.push_back(std::move(client_session(ctx, i, config, remaining_msgs)));
      ctx.sched(clients.back());
    }
    ctx.run();

    auto sctx = context_t::from_thread(server_thread);
    if (sctx != nullptr) {
      sctx->stop();
    }

    if (server_thread.joinable()) {
      server_thread.join();
    }

    monitor->stop();
    return monitor->get_result("Cornet");
  }
};

// ==================== ASIO 测试 ====================
class AsioBenchmark {
private:
  asio::io_context io_context;
  std::shared_ptr<PerfMonitor> monitor;
  std::atomic<bool> server_running{true};

  // 服务器类，确保所有资源正确管理
  class AsioServer : public std::enable_shared_from_this<AsioServer> {
  private:
    asio::ip::tcp::acceptor acceptor_;
    std::shared_ptr<PerfMonitor> monitor_;
    std::atomic<bool>& server_running_;

  public:
    AsioServer(asio::io_context& io_context,
               std::shared_ptr<PerfMonitor> monitor,
               std::atomic<bool>& server_running)
      : acceptor_(io_context, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), 8889))
        , monitor_(monitor)
        , server_running_(server_running) {
    }

    void start() {
      do_accept();
    }

  private:
    void do_accept() {
      auto self = shared_from_this();
      acceptor_.async_accept(
        [this, self](asio::error_code ec, asio::ip::tcp::socket socket) {
          if (!ec && server_running_) {
            // 为每个连接创建会话
            std::make_shared<AsioSession>(std::move(socket), monitor_)->start();
          }
          if (server_running_) {
            do_accept();
          }
        });
    }

    // 会话类
    class AsioSession : public std::enable_shared_from_this<AsioSession> {
    private:
      asio::ip::tcp::socket socket_;
      std::shared_ptr<std::vector<char> > buffer_; // 使用shared_ptr管理缓冲区
      std::shared_ptr<PerfMonitor> monitor_;

    public:
      AsioSession(asio::ip::tcp::socket socket, std::shared_ptr<PerfMonitor> monitor)
        : socket_(std::move(socket))
          , buffer_(std::make_shared<std::vector<char> >(4096))
          , monitor_(monitor) {}

      void start() {
        do_read();
      }

    private:
      void do_read() {
        auto self = shared_from_this();
        socket_.async_read_some(asio::buffer(*buffer_),
                                [this, self](asio::error_code ec, std::size_t length) {
                                  if (!ec) {
                                    do_write(length);
                                  }
                                });
      }

      void do_write(std::size_t length) {
        auto self = shared_from_this();
        asio::async_write(socket_, asio::buffer(*buffer_, length),
                          [this, self](asio::error_code ec, std::size_t) {
                            if (!ec) {
                              do_read();
                            }
                          });
      }
    };
  };

  // 客户端会话类
  class ClientSession : public std::enable_shared_from_this<ClientSession> {
  private:
    std::shared_ptr<asio::ip::tcp::socket> socket_;
    std::shared_ptr<std::string> send_buf_;
    std::shared_ptr<std::vector<char> > recv_buf_;
    std::shared_ptr<PerfMonitor> monitor_;
    std::shared_ptr<std::atomic<int> > remaining_msgs_;
    const BenchmarkConfig& config_;

  public:
    ClientSession(std::shared_ptr<asio::ip::tcp::socket> socket,
                  std::shared_ptr<std::string> send_buf,
                  std::shared_ptr<std::vector<char> > recv_buf,
                  std::shared_ptr<PerfMonitor> monitor,
                  std::shared_ptr<std::atomic<int> > remaining_msgs,
                  const BenchmarkConfig& config)
      : socket_(socket)
        , send_buf_(send_buf)
        , recv_buf_(recv_buf)
        , monitor_(monitor)
        , remaining_msgs_(remaining_msgs)
        , config_(config) {}

    void start() {
      send_receive();
    }

  private:
    void send_receive() {
      int current = (*remaining_msgs_)--;
      if (current <= 0)
        return;

      auto self = shared_from_this();
      auto send_start = std::chrono::steady_clock::now();

      asio::async_write(*socket_, asio::buffer(*send_buf_),
                        [this, self, send_start](asio::error_code ec, std::size_t sent) {
                          if (!ec) {
                            asio::async_read(*socket_,
                                             asio::buffer(*recv_buf_, send_buf_->size()),
                                             asio::transfer_exactly(send_buf_->size()),
                                             [this, self, send_start, sent](asio::error_code ec, std::size_t received) {
                                               if (!ec && received == send_buf_->size()) {
                                                 auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
                                                   std::chrono::steady_clock::now() - send_start).count();

                                                 monitor_->record_message(static_cast<int>(sent),
                                                                          static_cast<int>(received),
                                                                          latency);

                                                 // 继续下一个消息
                                                 send_receive();
                                               }
                                             });
                          }
                        });
    }
  };

public:
  AsioBenchmark() {
    monitor = std::make_shared<PerfMonitor>();
  }

  BenchmarkResult run(const BenchmarkConfig& config) {
    std::cout << "启动 ASIO 测试...\n";

    monitor->start();
    server_running = true;

    // 创建并启动服务器（使用shared_ptr确保生命周期）
    auto server = std::make_shared<AsioServer>(io_context, monitor, server_running);
    server->start();

    // 在单独线程中运行 io_context
    std::thread io_thread([this]() {
      io_context.run();
    });

    // 等待服务器启动
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 启动客户端
    std::vector<std::thread> client_threads;
    auto remaining_msgs = std::make_shared<std::atomic<int> >(config.total_messages);

    int hardware_threads = std::thread::hardware_concurrency();
    int clients_per_thread = config.num_connections / hardware_threads;
    if (clients_per_thread == 0)
      clients_per_thread = 1;

    for (int t = 0; t < hardware_threads; t++) {
      client_threads.emplace_back([this, t, clients_per_thread, config, remaining_msgs]() {
        run_clients(t, clients_per_thread, config, remaining_msgs);
      });
    }

    // 等待客户端完成
    for (auto& thread : client_threads) {
      thread.join();
    }

    // 给一点时间让所有异步操作完成
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 停止服务器
    server_running = false;
    io_context.stop();
    io_thread.join();

    monitor->stop();
    return monitor->get_result("ASIO");
  }

private:
  void run_clients(int thread_id, int num_clients, const BenchmarkConfig& config,
                   std::shared_ptr<std::atomic<int> > remaining_msgs) {
    asio::io_context client_io;

    std::vector<std::shared_ptr<asio::ip::tcp::socket> > sockets;

    for (int i = 0; i < num_clients; i++) {
      auto socket = std::make_shared<asio::ip::tcp::socket>(client_io);

      asio::error_code ec;
      socket->connect(
        asio::ip::tcp::endpoint(
          asio::ip::address::from_string("127.0.0.1"), 8889),
        ec);

      if (!ec) {
        sockets.push_back(socket);

        // 为每个连接创建独立的缓冲区
        auto send_buf = std::make_shared<std::string>(config.message_size, 'x');
        auto recv_buf = std::make_shared<std::vector<char> >(config.message_size + 1024);

        auto session = std::make_shared<ClientSession>(
          socket, send_buf, recv_buf, monitor, remaining_msgs, config);
        session->start();
      }
    }

    client_io.run();
  }
};

std::string format_cell(double value, double asio_value, int width) {
  std::ostringstream ss;
  using namespace std;
  ss << fixed << setprecision(2) << value << " / " << setprecision(1) << (value / asio_value * 100) << "%";

  auto result = ss.str();
  int padding = width - static_cast<int>(result.length());
  if (padding > 0) {
    result += std::string(padding, ' ');
  }
  return result;
}

// ==================== 主测试程序 ====================
int main(int argc, char* argv[]) {
  try {
    config_t::load("conf/default.toml");
    logging::init();

    BenchmarkConfig config;
    config.print();

    std::unordered_map<std::string, BenchmarkResult> results;

    {
      CornetBenchmark cornet_bench(scheduler_type_t::RoundRobin);
      std::cout << "Cornet RoundRobin Warming..." << std::endl;
      auto result = cornet_bench.run(config);
    }
    {
      CornetBenchmark cornet_bench(scheduler_type_t::TimeSlice);
      std::cout << "Cornet TimeSlice Warming..." << std::endl;
      auto result = cornet_bench.run(config);
    }
    {
      CornetBenchmark cornet_bench(scheduler_type_t::Batch);
      std::cout << "Cornet Batch Warming..." << std::endl;
      auto result = cornet_bench.run(config);
    }

    // 测试 Cornet
    {
      CornetBenchmark cornet_bench(scheduler_type_t::Batch);
      std::cout << "Cornet Batch Running..." << std::endl;
      auto result = cornet_bench.run(config);
      results["Batch"] = result;
    }

    {
      CornetBenchmark cornet_bench(scheduler_type_t::RoundRobin);
      std::cout << "Cornet RoundRobin Running..." << std::endl;
      auto result = cornet_bench.run(config);
      results["RoundRobin"] = result;
    }

    {
      CornetBenchmark cornet_bench(scheduler_type_t::TimeSlice);
      std::cout << "Cornet TimeSlice Running..." << std::endl;
      auto result = cornet_bench.run(config);
      results["TimeSlice"] = result;
    }

    // 测试 ASIO
    auto asio_result = BenchmarkResult();
    {
      AsioBenchmark asio_bench;
      std::cout << "Asio Running..." << std::endl;
      asio_result = asio_bench.run(config);
      results["Asio"] = asio_result;
    }
  using namespace std;
    cout << "\n\n ==================== 性能对比 ========================\n";
    cout << left << setw(24) << fixed << "metrics"
    << left << setw(24) << fixed << "RPS(/asio)"
    << left << setw(24) << fixed << "throughout(MB/s)(/asio)"
    << left << setw(24) << fixed << "latency(us)(/asio)"
    << left << setw(24) << fixed << "P95 latency(us)(/asio)" << endl;
    std::cout << std::right << "------------------------------------------------------------------------------\n";
    for (const auto& result : results) {
      std::cout << std::left << std::setw(24) << fixed << result.first
      << left << setw(24) << format_cell(result.second.requests_per_second, asio_result.requests_per_second, 24)
      << left << setw(24) << format_cell(result.second.throughput_mbps, asio_result.throughput_mbps, 24)
      << left << setw(24) << format_cell(result.second.avg_latency_us, asio_result.avg_latency_us, 24)
      << left << setw(24) << format_cell(result.second.p95_latency_us, asio_result.p95_latency_us, 24)
      << endl;
    }

  } catch (const std::exception& e) {
    std::cerr << "错误: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}