#include "cornet/scheduling/executor.h"
#include "cornet/coroutine/atask.h"
#include "cornet/scheduling/context.h"

namespace cornet {
executor_t::executor_t(task_tracker_t& tracker, config_t* config)
: max_task_nr(config ? config->at_path("cornet.context.executor.max_task_nr").value_or(16384) : 16384),
  tracker_(tracker),
  thread_nr_(config ? config->at_path("cornet.context.executor.thread_nr").value_or(1) : 1),
  pending_tasks(max_task_nr),
  completed_tasks(max_task_nr) {
}

void executor_t::ensure_workers() {
  if (workers.empty()) {
    for (int i = 0; i < thread_nr_; ++i) {
      workers.emplace_back(worker, this);
    }
  }
}

executor_t::~executor_t() {
  terminate();
}

bool executor_t::add(std::shared_ptr<atask_t> t) {
  ensure_workers();
  if (pending_tasks.size_approx() > max_task_nr) return false;
  bool ok = pending_tasks.try_enqueue(std::move(t));
  if (ok) tracker_.executor_add();
  return ok;
}
size_t executor_t::get_completed(std::array<std::shared_ptr<atask_t>, 32>& tasks) {
  size_t completed = completed_tasks.try_dequeue_bulk(tasks.begin(), 32);
  tracker_.executor_remove(completed);
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
  std::shared_ptr<atask_t> task;
  while (pending_tasks.try_dequeue(task)) {
    task->fn(task.get());
    while (!completed_tasks.try_enqueue(task)) {
      std::this_thread::yield();
    }
  }
}

void executor_t::worker(executor_t* p_executor) {
  auto& executor = *p_executor;
  std::array<std::shared_ptr<atask_t>, 8> tasks{};
  while (!executor.terminated) {
    auto dequeued = executor.pending_tasks.wait_dequeue_bulk_timed(
        tasks.begin(), 8, std::chrono::milliseconds(10));
    if (dequeued == 0) {
      std::this_thread::yield();
      continue;
    }
    for (size_t idx = 0; idx < dequeued; ++idx) {
      tasks[idx]->fn(tasks[idx].get());
    }
    while (!executor.completed_tasks.try_enqueue_bulk(tasks.begin(), dequeued)) {
      std::this_thread::yield();
    }
    // enqueue-before-wakeup: the owner may be parked in wait_cqes and will not
    // see these completions until its io timeout otherwise
    executor.tracker_.context().wakeup();
  }
}

} // namespace cornet
