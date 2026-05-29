#include "core/runtime.h"
#include <latch>
#include <barrier>

namespace cornet {

runtime_t::runtime_t(size_t thread_nr)
  : thread_nr_(thread_nr > 0 ? thread_nr : 1),
    contexts_(thread_nr_, nullptr) {}

runtime_t::~runtime_t() {
  if (!stopped_) {
    stop();
  }
  join();
}

void runtime_t::start(std::function<void(context_t&, size_t)> init_fn) {
  std::latch ctx_ready(thread_nr_);
  std::latch init_done(thread_nr_);

  for (size_t i = 0; i < thread_nr_; ++i) {
    workers_.emplace_back([this, i, &ctx_ready, &init_done, init_fn]() {
      auto& ctx = context_t::current();
      ctx.set_keep_alive(true);
      contexts_[i] = &ctx;

      // phase 1: wait until all contexts are registered
      ctx_ready.count_down();
      ctx_ready.wait();

      // phase 2: run user init (now all contexts_[j] are valid)
      if (init_fn) {
        init_fn(ctx, i);
      }
      init_done.count_down();

      ctx.run();
    });
  }

  // block until all threads have initialized
  init_done.wait();
}

void runtime_t::shutdown(std::chrono::nanoseconds timeout) {
  stopped_ = true;
  for (auto* ctx : contexts_) {
    if (ctx) {
      ctx->set_keep_alive(false);
      ctx->shutdown(timeout);
    }
  }
}

void runtime_t::stop() {
  stopped_ = true;
  for (auto* ctx : contexts_) {
    if (ctx) {
      ctx->set_keep_alive(false);
      ctx->stop();
    }
  }
}

void runtime_t::join() {
  for (auto& w : workers_) {
    if (w.joinable()) w.join();
  }
}

context_t* runtime_t::context(size_t index) const {
  if (index >= contexts_.size()) return nullptr;
  return contexts_[index];
}

context_t& runtime_t::next_context() {
  size_t idx = next_index_.fetch_add(1, std::memory_order_relaxed) % thread_nr_;
  return *contexts_[idx];
}

} // namespace cornet
