#ifndef CORNET_CANCEL_H
#define CORNET_CANCEL_H

#include <coroutine>
#include "io_uring/utask.h"
#include "base/expected.h"
#include "base/defines.h"
#include "io_uring/cancel_io.h"

namespace cornet {

struct context_t;

/**
 * @brief intrusive list node for tracking active IO operations in a canceler.
 * Embedded in cancellable_awaiter, lifetime matches the co_await duration.
 */
struct cancel_node {
  utask_t* task{nullptr};
  cancel_node* prev{nullptr};
  cancel_node* next{nullptr};
};

// forward declaration
template<typename Awaitable>
struct cancellable_awaiter;

/**
 * @brief canceler. Supports multi-task cancellation and hierarchical propagation.
 * Single-threaded, no atomic operations needed.
 *
 * Features:
 * - O(1) child unlink via doubly-linked sibling list
 * - Multiple concurrent IO operations per canceler via cancel_node list
 * - Iterative cancel propagation (no recursion)
 *
 * Usage:
 *   canceler_t canceler;
 *   ctx.spawn(handle_client(sock, canceler));
 *   canceler.cancel();  // cancel all IO associated with this canceler
 *
 * Hierarchical:
 *   canceler_t parent;
 *   canceler_t child(parent);
 *   parent.cancel();  // propagates to child
 */
struct canceler_t {
  /**
   * @brief construct a root canceler (no parent).
   * The context must be passed explicitly — no global context lookup.
   */
  explicit canceler_t(context_t& ctx)
    : io_() {
    io_.ctx_ = &ctx;
  }

  /**
   * @brief construct a child canceler with a parent canceler.
   * The context must be passed explicitly — no global context lookup.
   */
  explicit canceler_t(context_t& ctx, canceler_t& parent)
    : parent_(&parent), io_() {
    io_.ctx_ = &ctx;  // Each canceler should have its own context, not inherit from parent
    next_sibling_ = parent.first_child_;
    if (next_sibling_) {
      next_sibling_->prev_sibling_ = this;
    }
    parent.first_child_ = this;
  }

  ~canceler_t() {
    if (parent_) {
      if (prev_sibling_) {
        prev_sibling_->next_sibling_ = next_sibling_;
      } else {
        parent_->first_child_ = next_sibling_;
      }
      if (next_sibling_) {
        next_sibling_->prev_sibling_ = prev_sibling_;
      }
    }
  }

  canceler_t(const canceler_t&) = delete;
  canceler_t& operator=(const canceler_t&) = delete;

  /**
   * @brief cancel this canceler and all descendants iteratively.
   */
  void cancel() {
    if (cancelled_) return;
    cancelled_ = true;
    cancel_active_tasks();
    for (auto* child = first_child_; child; child = child->next_sibling_) {
      child->cancel_subtree();
    }
  }

  CORNET_NODISCARD bool is_cancelled() const { return cancelled_; }

  expected<void> reset() {
    if (io_.active_head_) {
      return unexpected(EBUSY);
    }
    cancelled_ = false;
    return {};
  }

  void link_node(cancel_node* node) { io_.link_node(node); }
  void unlink_node(cancel_node* node) { io_.unlink_node(node); }

private:
  void cancel_subtree();
  void cancel_active_tasks();

  bool cancelled_{false};
  canceler_io_t io_;
  canceler_t* parent_{nullptr};
  canceler_t* first_child_{nullptr};
  canceler_t* next_sibling_{nullptr};
  canceler_t* prev_sibling_{nullptr};

  template<typename Awaitable>
  friend struct cancellable_awaiter;
};

/**
 * @brief wraps a utask_t-based awaitable with optional cancellation support.
 * When canceler is nullptr, degrades to a simple passthrough.
 * When canceler is set, automatically registers/unregisters with the canceler.
 */
template<typename Awaitable>
struct cancellable_awaiter {
  Awaitable op_;
  canceler_t* canceler_;
  cancel_node node_;
  bool submitted_{false};

  cancellable_awaiter(Awaitable op, canceler_t* canceler)
    : op_(std::move(op)), canceler_(canceler) {}

  cancellable_awaiter(Awaitable op, canceler_t& canceler)
    : op_(std::move(op)), canceler_(&canceler) {}

  bool await_ready() {
    if (canceler_) [[likely]] {
      if (canceler_->is_cancelled()) return true;
    }
    return op_.await_ready();
  }

  bool await_suspend(std::coroutine_handle<> h) {
    if (canceler_) [[likely]] {
      if (canceler_->is_cancelled()) return false;
      node_.task = &op_;
      canceler_->link_node(&node_);
    }
    op_.await_suspend(h);
    submitted_ = true;
    return true;
  }

  auto await_resume() -> decltype(op_.await_resume()) {
    if (canceler_) [[likely]] {
      if (!submitted_) return unexpected(ECANCELED);
      canceler_->unlink_node(&node_);
    }
    return op_.await_resume();
  }
};

template<typename Awaitable>
cancellable_awaiter<Awaitable> with_cancel(context_t& ctx, Awaitable op, canceler_t& canceler) {
  return {std::move(op), &canceler};
}

} // namespace cornet

#endif //CORNET_CANCEL_H