#ifndef CORNET_EXECUTOR_H
#define CORNET_EXECUTOR_H

#include <moodycamel/concurrentqueue.h>
#include <vector>

#ifdef BLOCK_SIZE
#undef BLOCK_SIZE
#endif
#include <concurrentqueue/moodycamel/blockingconcurrentqueue.h>

#include "cornet/scheduling/task_tracker.h"
#include "cornet/utils/config.h"

namespace cornet {
struct atask_t;

/**
 * @brief thread pool executor for offloading blocking/CPU-intensive work.
 * Tasks are submitted via add(), executed on worker threads, and
 * collected back via get_completed() on the owner thread.
 */
class executor_t {
 public:
  using block_queue_t = moodycamel::BlockingConcurrentQueue<atask_t*>;
  using queue_t = moodycamel::ConcurrentQueue<atask_t*>;

  /**
   * @brief construct executor (threads started lazily on first add()).
   * Reads thread_nr and max_task_nr from config.
   * @param tracker work tracker
   * @param config configuration pointer (may be nullptr)
   */
  executor_t(task_tracker_t& tracker, config_t* config);

  ~executor_t();

  /**
   * @brief ensure worker threads are running (called before add).
   * Threads are started lazily on the first task submission.
   */
  void ensure_workers();

  /**
   * @brief submit a task to be executed on a worker thread
   * @param t task to execute
   * @return true if enqueued, false if queue is full
   */
  bool add(atask_t* t);

  /**
   * @brief collect completed tasks back to the owner thread
   * @param tasks output array for completed task pointers
   * @return number of tasks dequeued
   */
  size_t get_completed(std::array<atask_t*, 32>& tasks);

  /**
   * @brief gracefully stop all workers and drain remaining tasks
   */
  void terminate();

 private:
  static void worker(executor_t* p_executor);

  std::atomic<bool> terminated{false};
  // maximum pending task capacity
  const size_t max_task_nr;
  // pending tasks waiting for worker threads
  block_queue_t pending_tasks;
  // completed tasks waiting for owner thread to collect
  queue_t completed_tasks;
  // worker thread pool
  std::vector<std::thread> workers;
  // number of worker threads (used for lazy start)
  int thread_nr_;
  // work tracker (owned by context_t, set after construction)
  task_tracker_t& tracker_;
};

}

#endif //CORNET_EXECUTOR_H
