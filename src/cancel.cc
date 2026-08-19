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
  // Detach every node while walking: the op stays inflight and its frame keeps
  // referencing this canceler (tracked_ops_ is untouched; the awaiter decrements
  // it when the CQE finally lands), but the list itself is dismantled now. A
  // late CQE whose awaiter calls unlink_node() then sees a detached node and
  // leaves whatever was linked after the cancel alone — clearing only
  // active_head_ here used to let stale prev/next pointers corrupt exactly that.
  auto* node = active_head_;
  active_head_ = nullptr;

  for (; node; ) {
    auto* next = node->next;
    node->prev = node->next = nullptr;
    if (node->task && node->task->io_token() != 0) {
      auto sqe = ctx_->io_uring().get_sqe();
      if (!sqe) {
        SPDLOG_WARN("cancel sqe unavailable, op left inflight: {}", sqe.error().message());
        node = next;
        continue;
      }
      io_uring_prep_cancel(*sqe, reinterpret_cast<void*>(node->task->io_token()), 0);
      io_uring_sqe_set_data(*sqe, nullptr);
    }
    node = next;
  }
}
} // namespace cornet
