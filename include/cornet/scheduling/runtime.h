#ifndef CORNET_RUNTIME_H
#define CORNET_RUNTIME_H

#include <thread>
#include <vector>
#include <functional>
#include <atomic>
#include <mutex>

#include "cornet/scheduling/context.h"

namespace cornet {

/**
 * @brief multi-threaded runtime that manages N worker threads, each with its own context_t.
 * Thread-per-core / shared-nothing model. Coroutines never migrate between threads.
 * Cross-thread communication is via spawn_remote() only.
 *
 * Usage:
 *   runtime_t rt(4);
 *   rt.start([](size_t idx, context_t& ctx) {
 *       // per-thread initialization (e.g., spawn listeners)
 *   });
 *   rt.join();  // blocks until all threads exit
 */
class runtime_t {
public:
  /**
   * @brief construct runtime with specified thread count and create all context_t instances.
   * @param thread_nr number of worker threads (default: hardware_concurrency)
   */
  explicit runtime_t(size_t thread_nr = std::thread::hardware_concurrency());

  ~runtime_t();

  runtime_t(const runtime_t&) = delete;
  runtime_t& operator=(const runtime_t&) = delete;
  runtime_t(runtime_t&&) = delete;
  runtime_t& operator=(runtime_t&&) = delete;

  /**
   * @brief start all worker threads. Each thread calls run().
   * Blocks until all threads are ready to run.
   * @param init_fn per-thread initialization function called with (thread_index, context_t&).
   *                The context for each thread is created by the runtime,
   *                so it is passed directly to the init_fn.
   */
  void start(std::function<void(size_t, context_t&)> init_fn);

  /**
   * @brief initiate graceful shutdown on all contexts.
   * Does not block; call join() to wait for threads to finish.
   * @param timeout per-context shutdown timeout
   */
  void shutdown(std::chrono::nanoseconds timeout = std::chrono::seconds(5));

  /**
   * @brief forcefully stop all contexts.
   */
  void stop();

  /**
   * @brief wait for all worker threads to finish (blocks caller).
   */
  void join();


  /**
   * @brief number of worker threads.
   */
  size_t size() const { return thread_nr_; }

  /**
   * @brief get the context_t for a specific worker thread.
   * Must be called after start() and before shutdown().
   * @param idx thread index (0 <= idx < size())
   * @return context_t& reference to the context
   */
  CORNET_NODISCARD context_t& context_at(size_t idx) const {
    if (idx >= contexts_.size()) throw std::out_of_range("context_at: idx out of range");
    return *contexts_[idx];
  }

private:
  size_t thread_nr_;
  std::atomic<size_t> next_index_{0};
  std::vector<std::thread> workers_;
  std::vector<std::unique_ptr<context_t>> contexts_;
  std::atomic<bool> stopped_{false};
  mutable std::mutex mutex_;
};

} // namespace cornet

#endif // CORNET_RUNTIME_H
