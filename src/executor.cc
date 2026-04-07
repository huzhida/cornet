#include "core/executor.h"
#include "core/atask.h"

namespace cornet {
executor_t::executor_t(int thread_nr, size_t max_task_nr) : max_task_nr(max_task_nr) {
  for (int i=0; i < thread_nr; ++i) {
    workers.emplace_back(worker, this);
  }
}

executor_t::~executor_t() {
  terminate();
}

bool executor_t::add(atask_t* t) {
  if (pending_tasks.size_approx() > max_task_nr) return false;
  return pending_tasks.try_enqueue(t);
}
size_t executor_t::get_completed(std::array<atask_t*, 32>& tasks) {
  return completed_tasks.try_dequeue_bulk(tasks.begin(), 32);
}
void executor_t::terminate() {
  if (terminated) return;
  terminated = true;
  for (auto& w : workers) {
    if (w.joinable()) {
      w.join();
    }
  }
}

void executor_t::worker(executor_t* p_executor) {
  auto& executor = *p_executor;
  std::array<atask_t*, 8> tasks;
  while(!executor.terminated) {
    auto dequeued = executor.pending_tasks.wait_dequeue_bulk_timed(tasks.begin(), 4, std::chrono::milliseconds(10));
    if (dequeued == 0) {
      std::this_thread::yield();
      continue;
    }
    for (int idx = 0; idx < dequeued; ++idx) {
      auto* task = tasks[idx];
      task->fn(task);
    }
    executor.completed_tasks.try_enqueue_bulk(tasks.begin(), dequeued);
  }
}

}