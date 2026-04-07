#ifndef CORNET_EXECUTOR_H
#define CORNET_EXECUTOR_H

#ifdef BLOCK_SIZE
#undef BLOCK_SIZE
#endif
#include <concurrentqueue/moodycamel/blockingconcurrentqueue.h>

namespace cornet {
struct atask_t;

class executor_t {
 public:
  using queue_t = moodycamel::BlockingConcurrentQueue<atask_t*>;

  explicit executor_t(int thread_nr, size_t max_task_nr = 16384);
  ~executor_t();

  bool add(atask_t* t);
  size_t get_completed(std::array<atask_t*, 32>& tasks);
  void terminate();

  static void worker(executor_t* p_executor);
 private:
  std::atomic<bool> terminated{false};
  queue_t pending_tasks;
  queue_t completed_tasks;
  std::vector<std::thread> workers;
  const size_t max_task_nr;
};

}

#endif //CORNET_EXECUTOR_H
