#include "cornet/http/timer_wheel.h"

#include "cornet/concurrency/combinators.h"
#include "cornet/scheduling/context.h"

namespace cornet::http {

timer_wheel_t::timer_wheel_t(context_t& ctx, std::chrono::milliseconds tick)
  : ctx_(ctx), tick_(tick.count() > 0 ? tick : std::chrono::milliseconds(500)) {}

timer_wheel_t::~timer_wheel_t() {
  // Unlink everything so a node outliving the wheel cannot be walked.
  for (auto*& head : slots_) {
    auto* node = head;
    while (node) {
      auto* next = node->next;
      node->prev = node->next = nullptr;
      node->slot = -1;
      node = next;
    }
    head = nullptr;
  }
  armed_ = 0;
}

void timer_wheel_t::link(timer_node_t& node, uint32_t slot) {
  node.slot = int32_t(slot);
  node.prev = nullptr;
  node.next = slots_[slot];
  if (slots_[slot]) slots_[slot]->prev = &node;
  slots_[slot] = &node;
  ++armed_;
}

void timer_wheel_t::unlink(timer_node_t& node) {
  if (!node.armed()) return;
  auto slot = uint32_t(node.slot);
  if (node.prev) {
    node.prev->next = node.next;
  } else {
    slots_[slot] = node.next;
  }
  if (node.next) node.next->prev = node.prev;
  node.prev = node.next = nullptr;
  node.slot = -1;
  --armed_;
}

void timer_wheel_t::arm(timer_node_t& node, std::chrono::milliseconds delay) {
  unlink(node);
  uint64_t ticks = uint64_t((delay + tick_ - std::chrono::milliseconds(1)) / tick_);
  if (ticks == 0) ticks = 1;
  node.rounds = uint32_t(ticks / kSlots);
  auto slot = uint32_t((cursor_ + ticks) % kSlots);
  link(node, slot);
}

void timer_wheel_t::cancel(timer_node_t& node) {
  unlink(node);
}

void timer_wheel_t::advance() {
  cursor_ = (cursor_ + 1) % kSlots;
  ++ticks_;

  auto* node = slots_[cursor_];
  while (node) {
    auto* next = node->next;
    if (node->rounds > 0) {
      // deadline is more than one revolution out; leave it here for the next lap
      --node->rounds;
    } else {
      unlink(*node);
      // The callback may re-arm or destroy the node, so `next` was captured first.
      if (node->on_expire) node->on_expire(node->owner);
    }
    node = next;
  }
}

coro_t<void> timer_wheel_t::run() {
  while (running_ && ctx_.is_running()) {
    // as_system: the tick must not count as user work, or a context with an idle
    // wheel could never reach user_idle() and graceful shutdown would never start
    // draining.
    auto ok = co_await as_system(sleep(ctx_, tick_));
    if (!ok) break;
    if (!running_) break;
    advance();
  }
  co_return;
}

} // namespace cornet::http
