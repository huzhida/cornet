#include "cornet/io_uring/context_cancellation.h"
#include "cornet/scheduling/context.h"
#include "cornet/io_uring/utask.h"
#include "cornet/io_uring/io_slot.h"
#include "cornet/base/expected.h"

#ifndef IORING_ASYNC_CANCEL_ANY
#define IORING_ASYNC_CANCEL_ANY (1U << 2)
#endif

namespace cornet {

void context_cancellation_t::init(context_t& ctx) {
  ctx_ = &ctx;
}

coro_t<expected<int>> context_cancellation_t::cancel_pending_io() {
  // This coroutine is co_awaited; it returns a coroutine_handle to the caller.
  // The actual cancellation logic runs when the coroutine is resumed.
  int canceled_nr = 0;

  // Try CANCEL_ANY first (5.19+)
  if (!ctx_->io_uring().idle()) {
    auto ret = co_await context_t::cancel_awaiter{*ctx_, nullptr, IORING_ASYNC_CANCEL_ANY};
#if !CORNET_LINUX_VERSION_GE_5_19
    if (!ret && ret.error().code == EINVAL) {
      // Kernel doesn't support CANCEL_ANY, fallback to per-slot cancel
      std::vector<uint64_t> active;
      ctx_->io_slots().for_each_active([&](uint64_t sd) { active.push_back(sd); });
      for (auto sd : active) {
        auto r = co_await context_t::cancel_awaiter{*ctx_, reinterpret_cast<void*>(sd), 0};
        if (r && *r > 0) canceled_nr += *r;
      }
      co_return canceled_nr;
    }
#endif
    // CANCEL_ANY supported
    if (!ret) {
      if (ret.error().code == ENOENT) co_return canceled_nr;
      co_return ret;
    }
    if (*ret > 0) canceled_nr += *ret;
  }

  // Continue with CANCEL_ANY
  while (true) {
    auto ret = co_await context_t::cancel_awaiter{*ctx_, nullptr, IORING_ASYNC_CANCEL_ANY};
    if (!ret) {
      if (ret.error().code == ENOENT) co_return canceled_nr;
      co_return ret;
    }
    if (*ret == 0) co_return canceled_nr;
    canceled_nr += *ret;
  }
  co_return canceled_nr;
}

} // namespace cornet
