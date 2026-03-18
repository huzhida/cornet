#ifndef CORNET_TOOLS_H
#define CORNET_TOOLS_H

#include "utask.h"

namespace cornet {

template<typename... UTasks>
struct chain_awaiter {
  std::tuple<UTasks...> tasks;

  template<size_t I>
  void apply_task(std::coroutine_handle<> h) {
    constexpr size_t N = sizeof...(UTasks);
    auto& task = std::get<I>(tasks);
    if constexpr(I < N-1) {
      task.sqe.with_flags(IOSQE_IO_LINK);
    } else {
      task.handle = h;
    }
    task.sqe.with_data(&task);
  }

  bool await_ready() const { return false; }
  void await_suspend(std::coroutine_handle<> h) {
    constexpr size_t N = sizeof...(UTasks);
    [this, &h]<size_t... Is>(std::index_sequence<Is...>) {
      (this->apply_task<Is>(h), ...);
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
auto chain(UTasks... tasks) {
  return chain_awaiter<UTasks...>{std::make_tuple(std::move(tasks)...)};
}

template<typename... UTasks>
struct all_awaiter {
  std::tuple<UTasks...> tasks;
  int pending = sizeof...(UTasks);
  std::coroutine_handle<> handle;
  action on_complete{this, callback};

  static void callback(context_t& ctx, void* data) {
    auto a = (all_awaiter*)data;
    if(--a->pending == 0) {
      ctx.sched(a->handle);
    }
  }


  template<size_t I>
  void apply_task() {
    auto& task = std::get<I>(tasks);
    task.handle = std::coroutine_handle<>::from_address(&on_complete);
    task.callback = true;
    task.sqe.with_data(&task);
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
auto all(UTasks... tasks) {
  return all_awaiter<UTasks...>{std::make_tuple(std::move(tasks)...)};
}

}

#endif //CORNET_TOOLS_H
