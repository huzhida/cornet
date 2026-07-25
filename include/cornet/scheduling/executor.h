#ifndef CORNET_EXECUTOR_H
#define CORNET_EXECUTOR_H

#ifdef BLOCK_SIZE
#undef BLOCK_SIZE
#endif
#include <concurrentqueue/moodycamel/blockingconcurrentqueue.h>
#include <vector>

namespace cornet {
struct atask_t;

/**
 * @brief thread pool executor for offloading blocking/CPU-intensive work.
 * Tasks are submitted via add(), executed on worker threads, and
 * collected back via get_completed() on the owner thread.
 */
class executor_t {
 public:
  using queue_t = moodycamel::BlockingConcurrentQueue<atask_t*>;

  /**
   * @brief construct executor with worker threads
   * @param thread_nr number of worker threads
   * @param max_task_nr maximum pending task capacity
   */
  explicit executor_t(int thread_nr, size_t max_task_nr = 16384);

  ~executor_t();

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

  /**
   * @brief whether all submitted tasks have completed
   * @return true if no tasks are in-flight
   */
  bool idle() const;

 private:
  static void worker(executor_t* p_executor);

  std::atomic<bool> terminated{false};
  // pending tasks waiting for worker threads
  queue_t pending_tasks;
  // completed tasks waiting for owner thread to collect
  queue_t completed_tasks;
  // worker thread pool
  std::vector<std::thread> workers;
  // maximum pending task capacity
  const size_t max_task_nr;
  // number of tasks currently in-flight (pending + executing)
  std::atomic<size_t> running_task_nr{0};
};

}

#endif //CORNET_EXECUTOR_H
