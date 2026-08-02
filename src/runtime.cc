#include "cornet/scheduling/runtime.h"
#include <latch>

namespace cornet {

runtime_t::runtime_t(config_t* config, size_t thread_nr)
  : config_(config), thread_nr_(thread_nr > 0 ? thread_nr : 1) {
  // Create all contexts upfront so they exist before threads start
  contexts_.reserve(thread_nr_);
  for (size_t i = 0; i < thread_nr_; ++i) {
    contexts_.push_back(std::make_unique<context_t>(config_));
  }
}

runtime_t::~runtime_t() {
  if (!stopped_) {
    stop();
  }
  join();
}

void runtime_t::start(std::function<void(size_t, context_t&)> init_fn) {
  std::latch init_done(thread_nr_);

  for (size_t i = 0; i < thread_nr_; ++i) { 
    workers_.emplace_back([this, i, &init_done, init_fn]() {
      context_t& ctx = *contexts_[i];
      ctx.set_keep_alive(true);

      // run user init before starting the run loop
      if (init_fn) {
        init_fn(i, ctx);
      }
      init_done.count_down();

      ctx.run();
    });
  }

  // block until all threads are ready
  init_done.wait();
}

void runtime_t::shutdown(std::chrono::nanoseconds timeout) {
  stopped_ = true;
  for (auto& ctx : contexts_) {
    if (ctx) ctx->shutdown(timeout);
  }
}

void runtime_t::stop() {
  stopped_ = true;
  for (auto& ctx : contexts_) {
    if (ctx) ctx->stop();
  }
}

void runtime_t::join() {
  for (auto& w : workers_) {
    if (w.joinable()) w.join();
  }
}

} // namespace cornet
