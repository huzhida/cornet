#ifndef CORNET_CLOCK_H
#define CORNET_CLOCK_H

#include <cstdint>
#include <ctime>
#include <chrono>
#include <cstring>

#include "cornet/base/defines.h"

namespace cornet {

/**
 * @brief per-context coarse clock, refreshed once per run-loop iteration.
 *
 * Two consumers want "roughly now" on every single request:
 *   - timers with second-level tolerance (idle / header / body deadlines)
 *   - the HTTP Date header, which only changes once a second
 *
 * Both would otherwise pay a clock_gettime per request. It is a vDSO call, not
 * a syscall, but at millions of requests per second it is pure waste when the
 * answer is allowed to be up to one loop iteration stale. The run loop already
 * knows when it woke up, so it refreshes this once and everyone reads memory.
 *
 * Single-threaded per context: no atomics, no locks.
 */
class clock_cache_t {
 public:
  clock_cache_t() { refresh(); }

  /**
   * @brief re-read the coarse clocks. Called once per run-loop iteration.
   * The date string is only re-rendered when the wall-clock second changes.
   */
  void refresh() {
    timespec mono{};
    clock_gettime(CLOCK_MONOTONIC_COARSE, &mono);
    mono_ns_ = uint64_t(mono.tv_sec) * 1'000'000'000ull + uint64_t(mono.tv_nsec);

    timespec real{};
    clock_gettime(CLOCK_REALTIME_COARSE, &real);
    if (real.tv_sec != real_sec_) {
      real_sec_ = real.tv_sec;
      render_http_date();
    }
  }

  /**
   * @brief coarse monotonic time since an arbitrary epoch.
   * Use for deadlines; never goes backwards across a wall-clock adjustment.
   */
  CORNET_NODISCARD std::chrono::steady_clock::duration now() const {
    return std::chrono::nanoseconds(mono_ns_);
  }

  /**
   * @brief coarse monotonic nanoseconds, for callers that want raw arithmetic.
   */
  CORNET_NODISCARD uint64_t now_ns() const { return mono_ns_; }

  /**
   * @brief coarse wall-clock seconds since the Unix epoch.
   */
  CORNET_NODISCARD std::time_t real_seconds() const { return real_sec_; }

  /**
   * @brief current time as an IMF-fixdate string, e.g.
   * "Sun, 06 Nov 1994 08:49:37 GMT" — exactly what an HTTP Date header needs.
   * Valid until the next refresh(); 29 characters plus a NUL.
   * @return pointer to the cached, NUL-terminated string
   */
  CORNET_NODISCARD const char* http_date() const { return date_; }

  /**
   * @brief length of http_date(), always 29 for a valid date.
   */
  CORNET_NODISCARD uint32_t http_date_len() const { return date_len_; }

 private:
  void render_http_date() {
    static constexpr char kDays[7][4] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
    static constexpr char kMonths[12][4] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
    tm t{};
    gmtime_r(&real_sec_, &t);

    // hand-rolled instead of strftime: strftime consults the locale and costs
    // roughly an order of magnitude more for a fixed, locale-independent format
    char* p = date_;
    std::memcpy(p, kDays[t.tm_wday % 7], 3); p += 3;
    *p++ = ','; *p++ = ' ';
    two_digits(p, t.tm_mday); p += 2;
    *p++ = ' ';
    std::memcpy(p, kMonths[t.tm_mon % 12], 3); p += 3;
    *p++ = ' ';
    int year = t.tm_year + 1900;
    two_digits(p, year / 100); p += 2;
    two_digits(p, year % 100); p += 2;
    *p++ = ' ';
    two_digits(p, t.tm_hour); p += 2;
    *p++ = ':';
    two_digits(p, t.tm_min); p += 2;
    *p++ = ':';
    two_digits(p, t.tm_sec); p += 2;
    std::memcpy(p, " GMT", 4); p += 4;
    *p = '\0';
    date_len_ = uint32_t(p - date_);
  }

  static void two_digits(char* p, int v) {
    p[0] = char('0' + (v / 10) % 10);
    p[1] = char('0' + v % 10);
  }

  uint64_t   mono_ns_{0};
  std::time_t real_sec_{0};
  uint32_t   date_len_{0};
  char       date_[32]{};
};

} // namespace cornet

#endif // CORNET_CLOCK_H
