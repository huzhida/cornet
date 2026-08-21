#include "cornet/base/metrics.h"
#include "cornet/io_uring/uring.h"
#include "cornet/scheduling/context.h"
#include <liburing.h>

namespace cornet {

namespace {

/**
 * @brief map a configuration flag name to its IORING_SETUP_* bit.
 *
 * Config used to take a raw integer, which meant anyone wanting COOP_TASKRUN
 * had to look up the bit value and hard-code it in TOML. Names are also the
 * only way to keep a config file portable across liburing versions.
 * @return the flag bit, or 0 for an unknown name
 */
uint32_t setup_flag_from_name(std::string_view name) {
  struct entry_t { std::string_view name; uint32_t flag; };
  static constexpr entry_t kFlags[] = {
    {"IOPOLL",         IORING_SETUP_IOPOLL},
    {"SQPOLL",         IORING_SETUP_SQPOLL},
    {"SQ_AFF",         IORING_SETUP_SQ_AFF},
    {"CQSIZE",         IORING_SETUP_CQSIZE},
    {"CLAMP",          IORING_SETUP_CLAMP},
    {"ATTACH_WQ",      IORING_SETUP_ATTACH_WQ},
    {"R_DISABLED",     IORING_SETUP_R_DISABLED},
#ifdef IORING_SETUP_SUBMIT_ALL
    {"SUBMIT_ALL",     IORING_SETUP_SUBMIT_ALL},
#endif
#ifdef IORING_SETUP_COOP_TASKRUN
    {"COOP_TASKRUN",   IORING_SETUP_COOP_TASKRUN},
#endif
#ifdef IORING_SETUP_TASKRUN_FLAG
    {"TASKRUN_FLAG",   IORING_SETUP_TASKRUN_FLAG},
#endif
#ifdef IORING_SETUP_SINGLE_ISSUER
    {"SINGLE_ISSUER",  IORING_SETUP_SINGLE_ISSUER},
#endif
#ifdef IORING_SETUP_DEFER_TASKRUN
    {"DEFER_TASKRUN",  IORING_SETUP_DEFER_TASKRUN},
#endif
  };
  for (const auto& e : kFlags) {
    if (e.name == name) return e.flag;
  }
  SPDLOG_WARN("unknown io_uring setup flag '{}', ignored", name);
  return 0;
}

/**
 * @brief preference-ordered auto flag combinations.
 *
 * Auto-enablement is limited to flags whose kernel-side semantics do not
 * change — SINGLE_ISSUER merely skips SQ locking the framework already
 * guarantees single-threaded. On a kernel that predates it (6.0-), probing
 * falls through to flags=0, which is the pre-optimization behavior.
 *
 * The COOP_TASKRUN / TASKRUN_FLAG / DEFER_TASKRUN family is intentionally
 * absent: io_uring_enter can then return -EEXIST as a *hint* ("task work
 * pending") rather than a strict success/failure, which requires handling
 * the taskwork-pump dance in uring_t::submit / submit_and_wait_cqes.
 * Support can land once a CI bench on a modern kernel verifies the pump
 * logic is correct. Until then the family stays opt-in via TOML flags.
 */
struct auto_flags_t { uint32_t flags; const char* desc; };
constexpr auto_flags_t kAutoFlags[] = {
#ifdef IORING_SETUP_SINGLE_ISSUER
  {IORING_SETUP_SINGLE_ISSUER, "SINGLE_ISSUER"},
#endif
  {0u, "none"},
};

} // namespace

uring_t::uring_t(task_tracker_t& tracker, config_t* config)
  : tracker_(tracker),
    config_(config),
    uring(std::make_unique<io_uring>()) {

  // Default SQ depth: was 128, which forced a mid-loop io_uring_submit when a
  // burst of arm-accept plus N connection recvs landed in the same sched
  // cycle. 1K gives the scheduler room to batch without dropping into the
  // ENOBUFS back-pressure path during ramp-up; CQ is sized independently via
  // corn.uring.cq_size when a workload needs it.
  constexpr uint32_t kDefaultCapacity = 1024;
  uint32_t entries_nr = kDefaultCapacity, cq_entries = 0;
  uint32_t flags = 0;
  bool user_set_flags = false;

  if (config) {
    entries_nr = config->at_path("cornet.context.uring.capacity").value_or(kDefaultCapacity);
    // integer form kept for compatibility; the named array is the documented way
    if (auto raw = config->at_path("cornet.context.uring.flags").value<uint32_t>()) {
      flags = *raw;
      user_set_flags = true;
    }
    if (auto* arr = config->at_path("cornet.context.uring.flags").as_array()) {
      flags = 0;
      user_set_flags = true;
      for (const auto& node : *arr) {
        if (auto name = node.value<std::string_view>()) {
          flags |= setup_flag_from_name(*name);
        }
      }
    }
    cq_entries = config->at_path("cornet.context.uring.cq_size").value_or(0u);
  }

  // A CQ smaller than the peak number of concurrent completions pushes CQEs
  // into the kernel's overflow list, where they cost far more to reap. Sizing
  // it independently of the SQ matters for connection counts well above the
  // SQ depth: each connection holds one recv, but completions arrive in
  // bursts. Kernel requires a power of two and >= SQ entries.
  const uint32_t cq_extra = (cq_entries > 0) ? IORING_SETUP_CQSIZE : 0u;

  if (user_set_flags) {
    // Explicit config wins or fails loudly — silently dropping user-picked
    // flags would let a misconfigured deployment drift unnoticed.
    io_uring_params params{};
    params.flags = flags | cq_extra;
    params.cq_entries = cq_entries;
    if (int ret = io_uring_queue_init_params(entries_nr, uring.get(), &params); ret < 0) {
      SPDLOG_ERROR("failed to init io_uring queue with error: {}", strerror(-ret));
      throw std::runtime_error("failed to init io_uring queue");
    }
    return;
  }

  for (const auto& tier : kAutoFlags) {
    io_uring_params params{};
    params.flags = tier.flags | cq_extra;
    params.cq_entries = cq_entries;
    int ret = io_uring_queue_init_params(entries_nr, uring.get(), &params);
    if (ret == 0) {
      if (tier.flags != 0) {
        SPDLOG_INFO("io_uring: auto-selected setup flags {}", tier.desc);
      }
      return;
    }
    if (ret == -EINVAL || ret == -EOPNOTSUPP) {
      // The kernel rejected a flag it does not know. EINVAL surfaces before
      // the kernel allocates anything, but liburing's userland struct may
      // hold partial state — zero it instead of queue_exit, which expects a
      // successfully-initialized ring.
      *uring = io_uring{};
      continue;
    }
    SPDLOG_ERROR("failed to init io_uring queue with error: {}", strerror(-ret));
    throw std::runtime_error("failed to init io_uring queue");
  }
  // Unreachable: kAutoFlags always has a {0u, ...} sentinel that any
  // io_uring-capable kernel accepts.
  throw std::runtime_error("failed to init io_uring queue with any flag combination");
}

uring_t::~uring_t() {
  if (uring) io_uring_queue_exit(uring.get());
}


expected<io_uring_sqe*> uring_t::get_sqe() {
  CORNET_METRICS_ADD(metrics_->get_sqe_calls);
  auto sqe = io_uring_get_sqe(uring.get());
  if (!sqe) {
    CORNET_METRICS_ADD(metrics_->get_sqe_submit_forced);
    int ret = io_uring_submit(uring.get());
    if (ret > 0) tracker_.io_submit(static_cast<uint32_t>(ret));
    sqe = io_uring_get_sqe(uring.get());
    if (!sqe) {
      // Submitting freed nothing, so the kernel side is saturated. Throwing
      // here would violate the no-exceptions contract and, worse, leave the SQ
      // just as full afterwards — the caller is the only one who can shed load.
      CORNET_METRICS_ADD(metrics_->get_sqe_exhausted);
      SPDLOG_WARN("submission queue exhausted after forced submit");
      return unexpected(ENOBUFS);
    }
  }
  return sqe;
}

expected<void> uring_t::get_sqes(io_uring_sqe** out, size_t n) {
  CORNET_METRICS_ADD_N(metrics_->get_sqe_calls, n);
  // try to acquire all n SQEs without intermediate submit
  for (size_t i = 0; i < n; ++i) {
    out[i] = io_uring_get_sqe(uring.get());
    if (out[i]) continue;
    // Every SQE acquired so far sits in out[0..i) untouched: the ring holds no
    // un-prepped slot from us, so submitting here is safe. Submitting *after*
    // grabbing slots but before prepping them would hand the kernel stale SQE
    // contents (old opcode/fd/buf/user_data) — that is what the retry loop this
    // replaces used to do.
    CORNET_METRICS_ADD(metrics_->get_sqe_submit_forced);
    int ret = io_uring_submit(uring.get());
    if (ret > 0) tracker_.io_submit(static_cast<uint32_t>(ret));
    out[i] = io_uring_get_sqe(uring.get());
    if (!out[i]) {
      // Submitting freed nothing, so the kernel side is saturated. The i slots
      // already grabbed cannot be returned to the ring; prep them as NOPs with
      // null user_data so a later submit() ships something harmless and the CQ
      // side ignores them (null user_data resolves to no utask).
      for (size_t j = 0; j < i; ++j) {
        io_uring_prep_nop(out[j]);
        io_uring_sqe_set_data(out[j], nullptr);
        out[j] = nullptr;
      }
      CORNET_METRICS_ADD(metrics_->get_sqe_exhausted);
      SPDLOG_WARN("submission queue exhausted, could not acquire {} linked sqes", n);
      return unexpected(ENOBUFS);
    }
  }
  return {};
}

int uring_t::submit() {
  CORNET_METRICS_ADD(metrics_->submit_calls);
  CORNET_METRICS_SCOPE_TIMER(metrics_->submit_latency);
  int submit_nr = io_uring_submit(uring.get());
  if (submit_nr < 0) {
    CORNET_METRICS_ADD(metrics_->submit_failures);
    SPDLOG_ERROR("io_uring submit sqe failed with error: {}", strerror(-submit_nr));
    return submit_nr;
  }
  if (submit_nr > 0) tracker_.io_submit(static_cast<uint32_t>(submit_nr));
  CORNET_METRICS_ADD_N(metrics_->submit_sqes, submit_nr);
  return submit_nr;
}

uint32_t uring_t::process_cqes(context_t &ctx, cqe_t cqe) {
  uint32_t count{0}, head;
  io_uring_for_each_cqe(uring.get(), head, cqe) {
    utask_t::process_utask(ctx, cqe);
    ++count;
  }
  io_uring_cq_advance(uring.get(), count);
  tracker_.io_complete(count);
  return count;
}

uint32_t uring_t::wait_cqes(context_t &ctx, uint32_t wait_nr, sigset_t *mask) {
  CORNET_METRICS_ADD(metrics_->wait_calls);
  CORNET_METRICS_SCOPE_TIMER(metrics_->wait_latency);
  cqe_t cqe;
  // liburing flushes pending SQEs as part of the wait; count them for
  // io_inflight_ the same way submit_and_wait_cqes() does, since callers are
  // no longer guaranteed to have emptied the SQ through submit() first.
  const unsigned to_submit = io_uring_sq_ready(uring.get());
  if (io_uring_wait_cqes(uring.get(), &cqe, wait_nr, nullptr, mask) < 0) {
    if (to_submit > 0) tracker_.io_submit(to_submit);
    CORNET_METRICS_ADD(metrics_->wait_timeouts);
    return 0;
  }
  if (to_submit > 0) tracker_.io_submit(to_submit);
  uint32_t n = process_cqes(ctx, cqe);
  CORNET_METRICS_ADD_N(metrics_->wait_cqes_processed, n);
  return n;
}

uint32_t uring_t::peek_cqes(context_t& ctx) {
  CORNET_METRICS_ADD(metrics_->peek_calls);
  cqe_t cqe;
  uint32_t count = 0, head;
  io_uring_for_each_cqe(uring.get(), head, cqe) {
    utask_t::process_utask(ctx, cqe);
    ++count;
  }
  if (count == 0) {
    CORNET_METRICS_ADD(metrics_->peek_empty);
    return 0;
  }
  io_uring_cq_advance(uring.get(), count);
  tracker_.io_complete(count);
  CORNET_METRICS_ADD_N(metrics_->peek_cqes_processed, count);
  return count;
}

} // cornet
