#ifndef CORNET_TOOLS_H
#define CORNET_TOOLS_H

#include "utask.h"
#include "atask.h"
#include "context.h"

namespace cornet {

struct chain_builder {
  context_t& ctx;
  std::vector<utask_t*> tasks;

  struct chain_awaiter {
    context_t& ctx;
    int pending;
    std::coroutine_handle<> handle;
    std::vector<utask_t*>& tasks;

    static void callback(context_t& ctx, void* data) {
      auto a = (chain_awaiter*)data;
      if(--a->pending == 0) {
        ctx.sched(a->handle);
      }
    }

    explicit chain_awaiter(context_t& ctx, std::vector<utask_t*> & tasks)
    : ctx(ctx), tasks(tasks), pending(tasks.size()) {}

    bool await_ready() const {
      return false;
    }
    void await_suspend(std::coroutine_handle<> h) {
      handle = h;
      for (auto& task : tasks) {
        task->callback = callback;
        task->user_data = this;
      }
      ctx.io_uring().flush();
    }
    auto await_resume() {
      return tasks.back()->value;
    }
  };

  explicit chain_builder(context_t& ctx) : ctx(ctx) {}

  chain_builder& with_link(utask_t& t) {
    t.sqe.with_flags(IOSQE_IO_HARDLINK);
    tasks.emplace_back(&t);
    return *this;
  }

  chain_builder& with_hard_link(utask_t& t) {
    t.sqe.with_flags(IOSQE_IO_HARDLINK);
    tasks.emplace_back(&t);
    return *this;
  }

  chain_awaiter chain(utask_t& t) {
    tasks.emplace_back(&t);
    return chain_awaiter{ctx, tasks};
  }

  template<typename Rep, typename Period>
  chain_awaiter timeout_chain(utask_t& t) {
    struct link_timeout_awaiter : utask_t {
      link_timeout_awaiter(context_t& ctx, std::chrono::duration<Rep, Period> timeout, int flags) : utask_t(ctx) {
        sqe.prep_link_timeout(timeout, flags).with_data(this);
      }
    };
    link_timeout_awaiter timeout_awaiter;
    tasks.emplace_back(&t);
    tasks.emplace_back(&timeout_awaiter);
    return chain_awaiter{ctx, tasks};
  }

};


template<typename V>
struct async_awaiter : public atask_t {
  std::optional<V> value;
  bool completed{false};
  std::function<V()> function_wrapper;
  context_t& ctx;

  explicit async_awaiter(context_t& ctx, std::function<V()>&& func) : ctx(ctx), function_wrapper(std::move(func)) {
    this->fn = [] (atask_t* t) {
      auto* self = (async_awaiter<V>*)(t);
      if constexpr(std::is_void_v<V>) {
        self->completed = true;
        return;
      }
      self->value = self->function_wrapper();
      self->completed = true;
    };
  }

  bool await_ready() {
    return completed;
  }

  void await_suspend(std::coroutine_handle<> h) {
    this->handle = h;
    ctx.sched_async(this);
  }

  V await_resume() {
    if constexpr(std::is_void_v<V>) return;
    return std::move(*value);
  }
};

template<typename Func, typename... Args>
CORNET_NODISCARD auto async(context_t& ctx, Func&& f, Args&&... args) {
  using return_type = std::invoke_result_t<std::decay_t<Func>, std::decay_t<Args>...>;
  auto task = [f=std::forward<Func>(f), ...args=std::forward<Args>(args)] () -> return_type {
    return std::invoke(std::move(f), std::move(args)...);
  };
  return async_awaiter<return_type>{ctx, std::move(task)};
}

template<typename... UTasks>
struct all_awaiter {
  std::tuple<UTasks...> tasks;
  int pending = sizeof...(UTasks);
  std::coroutine_handle<> handle;

  static void callback(context_t& ctx, void* data) {
    auto a = (all_awaiter*)data;
    if(--a->pending == 0) {
      ctx.sched(a->handle);
    }
  }


  template<size_t I>
  void apply_task() {
    auto& task = std::get<I>(tasks);
    task.callback = callback;
    task.user_data = this;
  }

  bool await_ready() const { return false; }
  void await_suspend(std::coroutine_handle<> h) {
    handle = h;
    constexpr size_t N = sizeof...(UTasks);
    [this]<size_t... Is>(std::index_sequence<Is...>) {
      (this->apply_task<Is>(), ...);
    }(std::make_index_sequence<N>{});
  }
  auto await_resume() {
    constexpr size_t N = sizeof...(UTasks);
    return [this]<size_t... Is>(std::index_sequence<Is...>) {
      return std::make_tuple(std::move(std::get<Is>(tasks).value)...);
    }(std::make_index_sequence<N>{});
  }
};

template<typename... UTasks>
CORNET_NODISCARD auto all(UTasks... tasks) {
  return all_awaiter<UTasks...>{std::make_tuple(std::move(tasks)...)};
}

}

#endif //CORNET_TOOLS_H
