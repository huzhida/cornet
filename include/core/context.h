#ifndef CORNET_CONTEXT_H
#define CORNET_CONTEXT_H

#include <queue>
#include "uring.h"
#include "task.h"
#include "uring_task.h"
#include "coro.h"
#include "ringbuffer.h"

namespace cornet {

struct context_t {
  using queue_t = std::queue<std::coroutine_handle<>>;
  struct scheduler_t {
    constexpr static const char* SCHEDULER_TYPE_TIME_SLICE = "Priority";
    constexpr static const char* SCHEDULER_TYPE_FAIR = "Fair";

    virtual ~scheduler_t() = default;
    virtual uint32_t sched(context_t& ctx) = 0;
    static std::unique_ptr<scheduler_t> scheduler(const std::string& scheduler_type = SCHEDULER_TYPE_TIME_SLICE);
    static inline void register_scheduler(const char* name, std::unique_ptr<scheduler_t>(*create)()) {
      registry[name] = create;
    }

   private:
    static std::unordered_map<std::string, std::unique_ptr<scheduler_t>(*)()> registry;
  };

  ~context_t();
  context_t(const context_t&) = delete;
  context_t(context_t&& ctx) = delete;
  context_t& operator=(const context_t&) = delete;
  context_t& operator=(context_t&& ctx)  = delete;
  template<typename C> void spawn(C&& c) {
    sched(std::forward<C>(c));
  }
  template<typename C> void sched(C&& c) {
    using T = std::decay_t<C>;
    if constexpr (std::is_same_v<T, std::coroutine_handle<>>) {
      pending.push(std::forward<C>(c));
    } else if constexpr(std::is_pointer_v<T>) {
      pending.push(c->handle);
    } else {
      pending.push(c.handle);
    }
  }
  void run();
  void run_until(bool (*predicate)());
  void stop();
  uring_t& io_uring();

  static std::mutex contexts_mutex;
  static std::unordered_map<std::thread::id, context_t*> contexts;

  static inline context_t& context() {
    static thread_local context_t ctx;
    return ctx;
  }
  static inline context_t& from_thread(const std::thread& t) {
    std::lock_guard<std::mutex> guard(contexts_mutex);
    auto iter = contexts.at(t.get_id());
    return *iter;
  }

 private:
  context_t();
  uring_t uring;
  queue_t pending;
  std::unique_ptr<scheduler_t> scheduler;
  bool need_stop{false};
};

struct time_slice_scheduler_t : context_t::scheduler_t {
  static inline std::unique_ptr<scheduler_t> create() {
    return std::make_unique<time_slice_scheduler_t>();
  }
  uint32_t sched(context_t& ctx) final;
};

} // cornet

#endif //CORNET_CONTEXT_H
