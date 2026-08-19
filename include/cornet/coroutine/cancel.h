#ifndef CORNET_CANCEL_H
#define CORNET_CANCEL_H

#include <coroutine>

#include "cornet/io_uring/utask.h"
#include "cornet/base/expected.h"
#include "cornet/base/defines.h"

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
  explicit canceler_t(context_t& ctx) : ctx_(&ctx) {}

  /**
   * @brief construct a child canceler with a parent canceler.
   * The context must be passed explicitly — no global context lookup.
   */
  explicit canceler_t(context_t& ctx, canceler_t& parent)
    : ctx_(&ctx), parent_(&parent) {
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
    // List membership alone cannot prove quiescence: cancel_active_tasks()
    // detaches nodes while their ops are still inflight, and those frames keep
    // referencing this canceler until their CQEs land. tracked_ops_ counts them.
    if (tracked_ops_ != 0) {
      return unexpected(EBUSY);
    }
    cancelled_ = false;
    return {};
  }

  /**
   * @brief register an inflight op so cancel() can reach it.
   * Backend-agnostic intrusive list push; the node lives in the awaiter frame.
   */
  void link_node(cancel_node* node) {
    node->prev = nullptr;
    node->next = active_head_;
    if (active_head_) {
      active_head_->prev = node;
    }
    active_head_ = node;
    ++tracked_ops_;
  }

  /**
   * @brief unregister an op that resolved on its own.
   * Idempotent: a node already detached by cancel_active_tasks() (prev/next
   * cleared, not the head) is left untouched, so a late CQE cannot corrupt
   * whatever the list holds now.
   */
  void unlink_node(cancel_node* node) {
    if (!node->prev && !node->next && active_head_ != node) return;
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

private:
  void cancel_subtree();
  /**
   * @brief issue the backend cancel ops for this canceler's own list.
   * The only backend-specific piece of canceler_t; defined in cancel.cc behind
   * the kernel-version switch.
   */
  void cancel_active_tasks();

  /**
   * @brief an op's frame finished with this canceler (CQE landed or submit
   * failed); pairs with the ++tracked_ops_ in link_node.
   */
  void op_resolved() { --tracked_ops_; }

  bool cancelled_{false};
  context_t* ctx_{nullptr};
  cancel_node* active_head_{nullptr};
  // ops whose frames currently reference this canceler; reset() requires 0
  uint32_t tracked_ops_{0};
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
  bool submit_failed_{false};
  bool cancelled_before_submit_{false};
  // set once await_resume() detached this op from the canceler
  bool resolved_{false};

  cancellable_awaiter(Awaitable op, canceler_t* canceler)
    : op_(std::move(op)), canceler_(canceler) {}

  cancellable_awaiter(Awaitable op, canceler_t& canceler)
    : op_(std::move(op)), canceler_(&canceler) {}

  ~cancellable_awaiter() {
    // The coroutine frame is being destroyed while its op is still armed: the
    // node cannot stay on the cancel list (it lives in this frame), or a later
    // cancel() would walk freed memory. Balance the book the same way a normal
    // completion would. The kernel-side completion of the op itself is a
    // separate matter (see the destroy-while-armed contract on utask_t).
    if (canceler_ && submitted_ && !resolved_) {
      canceler_->unlink_node(&node_);
      canceler_->op_resolved();
    }
  }

  cancellable_awaiter(const cancellable_awaiter&) = delete;
  cancellable_awaiter& operator=(const cancellable_awaiter&) = delete;

  bool await_ready() {
    if (canceler_) [[likely]] {
      if (canceler_->is_cancelled()) {
        // Distinguish this from "op completed synchronously in await_ready":
        // both skip await_suspend with submitted_ == false, and only the flag
        // keeps await_resume from misreporting a success as ECANCELED.
        cancelled_before_submit_ = true;
        return true;
      }
    }
    return op_.await_ready();
  }

  bool await_suspend(std::coroutine_handle<> h) {
    if (canceler_) [[likely]] {
      if (canceler_->is_cancelled()) {
        cancelled_before_submit_ = true;
        return false;
      }
      node_.task = &op_;
      canceler_->link_node(&node_);
    }
    if (!op_.await_suspend(h)) {
      // Submission failed, so no CQE is coming. Unlink and resume immediately;
      // staying suspended here would wait on a wakeup that cannot arrive.
      if (canceler_) {
        canceler_->unlink_node(&node_);
        canceler_->op_resolved();
      }
      submit_failed_ = true;
      return false;
    }
    submitted_ = true;
    return true;
  }

  auto await_resume() -> decltype(op_.await_resume()) {
    if (canceler_) [[likely]] {
      // Distinguish "cancelled before submission" from "could not submit": the
      // first is a deliberate cancellation, the second is back pressure, and a
      // caller that retries wants to tell them apart.
      if (cancelled_before_submit_) return unexpected(ECANCELED);
      if (submitted_ && !resolved_) {
        // May already be detached if cancel() ran first; unlink_node is
        // idempotent, but the op is resolved either way. The flag matters when
        // await_resume ran more than once (promise resumption paths) and for
        // the destructor's bookkeeping.
        resolved_ = true;
        canceler_->unlink_node(&node_);
        canceler_->op_resolved();
      }
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