#include "cornet/scheduling/executor.h"
#include "cornet/coroutine/atask.h"

namespace cornet {
executor_t::executor_t(int thread_nr, size_t max_task_nr)
: max_task_nr(max_task_nr), pending_tasks(max_task_nr), completed_tasks(max_task_nr) {
  for (int i=0; i < thread_nr; ++i) {
    workers.emplace_back(worker, this);
  }
}

executor_t::~executor_t() {
  terminate();
}

bool executor_t::add(atask_t* t) {
  if (pending_tasks.size_approx() > max_task_nr) return false;
  bool ok = pending_tasks.try_enqueue(t);
  if (ok) ++running_task_nr;
  return ok;
}
size_t executor_t::get_completed(std::array<atask_t*, 32>& tasks) {
  size_t completed = completed_tasks.try_dequeue_bulk(tasks.begin(), 32);
  running_task_nr -= completed;
  return completed;
}
void executor_t::terminate() {
  if (terminated) return;
  terminated = true;
  for (auto& w : workers) {
    if (w.joinable()) {
      w.join();
    }
  }
  atask_t* task;
  while (pending_tasks.try_dequeue(task)) {
    task->fn(task);
    while (!completed_tasks.try_enqueue(task)) {
      std::this_thread::yield();
    }
  }
}

void executor_t::worker(executor_t* p_executor) {
  auto& executor = *p_executor;
  std::array<atask_t*, 8> tasks{};
  while(!executor.terminated) {
    auto dequeued = executor.pending_tasks.wait_dequeue_bulk_timed(tasks.begin(), 8, std::chrono::milliseconds(10));
    if (dequeued == 0) {
      std::this_thread::yield();
      continue;
    }
    for (int idx = 0; idx < dequeued; ++idx) {
      auto* task = tasks[idx];
      task->fn(task);
    }
    while (!executor.completed_tasks.try_enqueue_bulk(tasks.begin(), dequeued)) {
      std::this_thread::yield();
    }
  }
}
bool executor_t::idle() const {
  return running_task_nr == 0;
}

}