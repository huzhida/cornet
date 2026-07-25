#include "coroutine/cancel.h"
#include "io_uring/cancel_io.h"
#include "scheduling/context.h"

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
  io_.cancel_active_tasks();
}

void canceler_io_t::link_node(cancel_node* node) {
  node->prev = nullptr;
  node->next = active_head_;
  if (active_head_) {
    active_head_->prev = node;
  }
  active_head_ = node;
}

void canceler_io_t::unlink_node(cancel_node* node) {
  if (node->prev) {
    node->prev->next = node->next;
  } else {
    active_head_ = node->next;
  }
  if (node->next) {
    node->next->prev = node->prev;
  }
  node->prev = node->next = nullptr;
}

void canceler_io_t::cancel_active_tasks() {
  auto* head = active_head_;
  active_head_ = nullptr;

  for (auto* node = head; node; node = node->next) {
    if (node->task && node->task->io_token() != 0) {
      auto* sqe = ctx_->io_uring().get_sqe();
      io_uring_prep_cancel(sqe, reinterpret_cast<void*>(node->task->io_token()), 0);
      io_uring_sqe_set_data(sqe, nullptr);
    }
  }
}

} // namespace cornet
