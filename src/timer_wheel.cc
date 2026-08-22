#include "cornet/concurrency/timer_wheel.h"

#include "cornet/concurrency/combinators.h"
#include "cornet/scheduling/context.h"

namespace cornet {

timer_wheel_t::timer_wheel_t(private_tag_t, context_t& ctx, std::chrono::milliseconds tick)
  : ctx_(ctx), tick_(tick.count() > 0 ? tick : kDefaultTick) {}

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
  // An exact wheel multiple lands back on the cursor's slot with the deadline
  // still one revolution out; rounds must count the laps VISITED before this
  // one can fire, so ticks=512 fires after 512 ticks, not 1024.
  node.rounds = uint32_t((ticks - 1) / kSlots);
  auto slot = uint32_t((cursor_ + ticks) % kSlots);
  link(node, slot);
  // Nothing ticks until something is armed, and the runner leaves as soon as
  // nothing is: every arm on an idle wheel starts a new one.
  ensure_runner();
}

void timer_wheel_t::cancel(timer_node_t& node) {
  unlink(node);
}

void timer_wheel_t::ensure_runner() {
  if (runner_live_ || !running_) return;
  runner_live_ = true;
  // shared_from_this rather than a raw this: see run().
  ctx_.spawn(run(shared_from_this()));
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

coro_t<void> timer_wheel_t::run(std::shared_ptr<timer_wheel_t> self) {
  // The reference is held by the frame, not by the caller: the owner may drop its
  // shared_ptr at any suspension point below (a client_t living in a coroutine
  // frame does exactly that when the frame dies), and every member touched after a
  // resume — running_, armed_, the slots — would otherwise be freed memory.
  auto& wheel = *self;
  auto& ctx = wheel.ctx_;
  // An empty wheel has nothing to count down, so the loop ends rather than parking:
  // the frame goes back to the pool, and if this was the last reference the wheel
  // goes with it. The next arm() starts a fresh runner.
  while (wheel.armed_ > 0 && wheel.running_ && ctx.is_running()) {
    // as_system: the tick must not count as user work, or a context with an
    // armed wheel could never reach user_idle() and graceful shutdown would
    // never start draining.
    auto ok = co_await as_system(sleep(ctx, wheel.tick_));
    if (!ok) break;
    if (!wheel.running_) break;
    // May fire callbacks that arm more timers; the loop condition sees those, so
    // no runner is spawned on top of this one.
    wheel.advance();
  }
  wheel.runner_live_ = false;
  co_return;
}

} // namespace cornet
