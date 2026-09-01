#ifndef CORNET_DEADLINE_H
#define CORNET_DEADLINE_H

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <memory>

#include "cornet/base/expected.h"
#include "cornet/coroutine/cancel.h"
#include "cornet/concurrency/timer_wheel.h"
#include "cornet/scheduling/context.h"

namespace cornet {

template<typename Awaitable>
struct deadline_bound_awaiter;   // combinators.h; granted private access below

/**
 * @brief a deadline: one shared absolute end that any number of sequential
 * ops draw against, with a cancellable kill switch. Go's context.WithDeadline.
 *
 * THE MODEL, in one breath: the object is always in one of three eras —
 * (a) ERA: set_budget(B) armed the absolute end E = now+B; every op bound
 *     afterwards shares what's left of it;
 * (b) CAPPED: cap(d) temporarily tightens the position to min(now+d, E) for
 *     the next op — the shared end is what with_deadline restores afterwards;
 * (c) FIRED: the timer rang; the latch and the canceler stay latched, later
 *     ops die instantly, and the ONE way back is reset(). Nothing re-arms a
 *     fired deadline silently, ever.
 *
 * Lifecycle calls (set_budget / cap / reset) go BETWEEN ops, never inside
 * one; with_deadline (combinators.h) is the only in-op visitor allowed, and
 * all it does is restore the shared end (or drop the wheel node when there
 * is none) on the way out.
 *
 * Binding: ops bind canceler(), either through with_deadline (the only
 * recommended form for root scopes) or with plain with_cancel when the op
 * must sit on a CHILD scope (an http server killing reads while sparing
 * its drain window — the tree is canceler_t's business, not this class's).
 *
 * The wheel arrives as a shared_ptr on the long-lived form: this guard
 * co-owns it, so teardown never consults a wheel somebody else freed. The
 * `{ctx, tick, budget}` form fetches the context's coarse wheel itself.
 *
 * Fire terms: O(1) arm/cancel, no allocation, tick-quantized precision.
 * For sub-tick precision on ONE leaf op use the io_uring link_timeout
 * (combinators.h op-level with_timeout); for bounding a whole user coroutine
 * from outside once, coroutine with_timeout remains the generic primitive.
 */
class deadline_t {
 public:
  /**
   * @param ctx the clock source for budget arithmetic (coarse_now_ns)
   * @param wheel shared ownership of the deadline wheel (co-owned here)
   * @param budget first era: B>0 arms the absolute end E=now+B immediately
   * (the whole "connect+handshake+upgrade share 10s" shape in one object);
   * the default opens with no era — pure cap(d)-per-window usage
   * @param timeouts optional counter bumped on every fire
   */
  deadline_t(context_t& ctx, std::shared_ptr<timer_wheel_t> wheel,
             std::chrono::milliseconds budget = std::chrono::milliseconds{0},
             uint64_t* timeouts = nullptr)
    : ctx_(ctx), wheel_(std::move(wheel)), timeouts_(timeouts), canceler_(ctx) {
    node_.owner = this;
    node_.on_expire = [](void* owner) { static_cast<deadline_t*>(owner)->fire(); };
    if (budget.count() > 0) set_budget(budget);
  }

  /**
   * @brief fetches the context's coarse wheel keyed by `tick`.   */
  deadline_t(context_t& ctx, std::chrono::milliseconds tick,
             std::chrono::milliseconds budget = std::chrono::milliseconds{0})
    : deadline_t(ctx, ctx.wheel_for(tick), budget, nullptr) {}

  ~deadline_t() { disarm(); }

  deadline_t(const deadline_t&) = delete;
  deadline_t& operator=(const deadline_t&) = delete;

  /**
   * @brief the canceler this deadline fires on expiry — the one and only kill
   * switch. with_deadline binds it on behalf of ops; owners may also derive
   * CHILD scopes from it (fire cascades down; latching one scope manually,
   * e.g. request_close, leaves the parent and its siblings untouched).
   */
  CORNET_NODISCARD canceler_t& canceler() { return canceler_; }

  /**
   * @brief open a new era: the absolute end becomes E = now+d and every op
   * bound from here on shares it. Replaces any previous era/window (d>0), or
   * ends the current one (d==0: node off the wheel, budget cleared). NEVER
   * touches the fire latch — ending an era is not a fire; reset() is the
   * only way to unlatch.
   */
  void set_budget(std::chrono::milliseconds d) {
    budget_ns_ = d.count() > 0
                     ? ctx_.coarse_now_ns() + uint64_t(d.count()) * 1'000'000ull
                     : 0;
    if (budget_ns_ != 0) wheel_->arm(node_, d);
    else wheel_->cancel(node_);
  }

  /**
   * @brief tighten the next op's window to at most `d` from now — or to what
   * is left of the era, whichever is shorter. with_deadline restores the
   * shared end on the way out, so a cap never leaks into the following op.
   * Fires immediately instead of arming when the era is already spent, so a
   * capped op fails now rather than running one more phase for free.
   */
  void cap(std::chrono::milliseconds d) {
    if (budget_ns_ != 0) {
      auto now = ctx_.coarse_now_ns();
      if (now >= budget_ns_) {
        fire();
        return;
      }
      auto left = std::chrono::milliseconds((budget_ns_ - now) / 1'000'000ull + 1);
      if (left < d) d = left;
    }
    if (d.count() <= 0) d = std::chrono::milliseconds(1);
    wheel_->arm(node_, d);
  }

  /**
   * @brief back from FIRED (or anywhere) to "just constructed": wheel node
   * off, budget cleared, fire latch and canceler unlatched. Goes BETWEEN
   * ops — the quiescence check comes from canceler_t::reset() and fails
   * with EBUSY while any op still holds a reference. CHILD scopes the owner
   * derived from canceler() are NOT unlatched: give them their own reset,
   * or fresh ones.
   */
  CORNET_NODISCARD expected<void> reset() {
    auto ok = canceler_.reset();
    if (!ok) return ok;
    wheel_->cancel(node_);
    budget_ns_ = 0;
    fired_ = false;
    return ok;
  }

  CORNET_NODISCARD bool has_budget() const { return budget_ns_ != 0; }
  CORNET_NODISCARD bool fired() const { return fired_; }

  /**
   * @brief translate an io failure into the deadline's timeout when the
   * deadline caused it. Reading op error first, then this, preserves the real
   * reason for anything the deadline did NOT kill (context shutdown, peer
   * reset).
   */
  CORNET_NODISCARD error_t map(error_t e) const {
    return fired_ ? error_t{ETIMEDOUT, error_domain::System} : e;
  }

 private:
  /**
   * @brief with_deadline's single visit, on the way out of an op: a fire is
   * terminal (the node is already unlinked; the latch stays for map()), an
   * era goes back to its shared end (firing inline when it got spent between
   * ops — the same fairness rule cap() has), and a budgetless window simply
   * comes off the wheel.
   */
  void restore_after_op() {
    if (fired_) return;
    if (budget_ns_ == 0) {
      wheel_->cancel(node_);
      return;
    }
    auto now = ctx_.coarse_now_ns();
    if (now >= budget_ns_) {
      fire();
      return;
    }
    wheel_->arm(node_,
                std::chrono::milliseconds((budget_ns_ - now) / 1'000'000ull + 1));
  }

  void disarm() { wheel_->cancel(node_); }   // teardown only

  void fire() {
    fired_ = true;
    if (timeouts_) ++*timeouts_;
    canceler_.cancel();
  }

  context_t&   ctx_;
  // co-owned: dead only after every user of this wheel is done with it
  std::shared_ptr<timer_wheel_t> wheel_;
  timer_node_t node_{};
  uint64_t*    timeouts_;
  // THE one and only kill switch; owners derive their kill scopes as children
  canceler_t   canceler_;
  uint64_t     budget_ns_{0};
  bool         fired_{false};

  template<typename Awaitable>
  friend struct deadline_bound_awaiter;
};

} // namespace cornet

#endif // CORNET_DEADLINE_H
