#ifndef CORNET_RUNTIME_H
#define CORNET_RUNTIME_H

#include "context.h"
#include <thread>
#include <vector>
#include <functional>
#include <atomic>

namespace cornet {

/**
 * @brief multi-threaded runtime that manages N worker threads, each with its own context_t.
 * Thread-per-core / shared-nothing model. Coroutines never migrate between threads.
 * Cross-thread communication is via spawn_remote() only.
 *
 * Usage:
 *   runtime_t rt(4);
 *   rt.start([](context_t& ctx, size_t idx) {
 *       // per-thread initialization (e.g., spawn listeners)
 *   });
 *   rt.join();  // blocks until all threads exit
 */
class runtime_t {
public:
  /**
   * @brief construct runtime with specified thread count.
   * @param thread_nr number of worker threads (default: hardware_concurrency)
   */
  explicit runtime_t(size_t thread_nr = std::thread::hardware_concurrency());

  ~runtime_t();

  runtime_t(const runtime_t&) = delete;
  runtime_t& operator=(const runtime_t&) = delete;
  runtime_t(runtime_t&&) = delete;
  runtime_t& operator=(runtime_t&&) = delete;

  /**
   * @brief start all worker threads. Each thread initializes its context and calls run().
   * Blocks until all contexts are initialized (but does NOT wait for run() to finish).
   * @param init_fn per-thread initialization function called with (context_t&, thread_index)
   *               before ctx.run(). Used to spawn initial coroutines.
   */
  void start(std::function<void(context_t&, size_t)> init_fn);

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
   * @brief get context by index.
   * @param index thread/context index [0, thread_nr)
   * @return pointer to context (nullptr if index out of range)
   */
  context_t* context(size_t index) const;

  /**
   * @brief round-robin select next context for load distribution.
   * Thread-safe.
   * @return reference to selected context
   */
  context_t& next_context();

  /**
   * @brief number of worker threads.
   */
  size_t size() const { return thread_nr_; }

private:
  size_t thread_nr_;
  std::atomic<size_t> next_index_{0};
  std::vector<std::thread> workers_;
  std::vector<context_t*> contexts_;
  std::atomic<bool> stopped_{false};
};

} // namespace cornet

#endif // CORNET_RUNTIME_H
