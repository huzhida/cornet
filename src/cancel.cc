#include "core/cancel.h"
#include "core/context.h"

namespace cornet {

canceler_t::canceler_t() : ctx_(&context_t::current()) {}

void canceler_t::cancel_active_tasks() {
  for (auto* node = active_head_; node; node = node->next) {
    if (node->task && node->task->io_token() != 0) {
      auto* sqe = ctx_->io_uring().get_sqe();
      io_uring_prep_cancel(sqe, reinterpret_cast<void*>(node->task->io_token()), 0);
      io_uring_sqe_set_data(sqe, nullptr);
    }
  }
}

} // namespace cornet
