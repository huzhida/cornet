#include "cornet/coroutine/cancel.h"
#include "cornet/scheduling/context.h"

namespace cornet {

void canceler_t::cancel_subtree() {
  canceler_t* current = this;
  while (current) {
    if (!current->cancelled_) {
      current->cancelled_ = true;
      current->cancel_active_tasks();
    }
    if (current->first_child_) {
      current = current->first_child_;
    } else {
      while (current && current != this) {
        if (current->next_sibling_) {
          current = current->next_sibling_;
          break;
        }
        current = current->parent_;
      }
      if (current == this) break;
    }
  }
}

void canceler_t::cancel_active_tasks() {
  auto* head = active_head_;
  active_head_ = nullptr;

  for (auto* node = head; node; node = node->next) {
#if CORNET_LINUX_VERSION_GE_5_19
    // 5.19+ targets the op by its raw utask_t pointer, which is its user_data
    if (node->task) {
      auto* sqe = ctx_->io_uring().get_sqe();
      io_uring_prep_cancel(sqe, reinterpret_cast<void*>(node->task), 0);
      io_uring_sqe_set_data(sqe, nullptr);
    }
#else
    if (node->task && node->task->io_token() != 0) {
      auto* sqe = ctx_->io_uring().get_sqe();
      io_uring_prep_cancel(sqe, reinterpret_cast<void*>(node->task->io_token()), 0);
      io_uring_sqe_set_data(sqe, nullptr);
    }
#endif
  }
}

} // namespace cornet
