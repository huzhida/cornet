#ifndef CORNET_HTTP_HEADERS_H
#define CORNET_HTTP_HEADERS_H

#include <cstdint>
#include <string_view>
#include <vector>

#include "cornet/http/buffer.h"
#include "cornet/http/common.h"

namespace cornet::http {

/**
 * @brief one header, stored as offsets rather than pointers.
 *
 * Offsets survive anything the owning buffer does to itself, which is what makes
 * zero-copy parsing safe (see head_buffer_t). `spilled` selects which buffer the
 * value lives in: the header buffer for the normal case, the spill buffer for
 * values the parser had to copy.
 */
struct header_ref {
  uint32_t name_off{0};
  uint32_t value_off{0};
  uint16_t name_len{0};
  uint16_t value_len{0};
  field_t  field{field_t::Other};
  bool     spilled{false};
};

/**
 * @brief the header section of one message.
 *
 * 32 entries live inline, which covers essentially every real request, so the
 * common case never allocates. Beyond that an overflow vector takes over; that
 * costs one allocation on a request that is already unusual.
 *
 * A bitmap of the enumerated fields makes has()/get(field_t) answer without
 * touching the entry array at all for the overwhelmingly common "is this header
 * absent?" question.
 */
class headers_t {
 public:
  static constexpr uint32_t kInline = 32;

  headers_t() = default;

  /**
   * @brief bind the buffers that offsets refer to. Must be called before add().
   */
  void bind(const head_buffer_t* head, const spill_buffer_t* spill) {
    head_ = head;
    spill_ = spill;
  }

  /**
   * @brief drop all entries, keeping the inline storage and the bindings.
   */
  void clear() {
    size_ = 0;
    bitmap_ = 0;
    if (overflow_) overflow_->clear();
  }

  /**
   * @brief append one header.
   * @return TooManyHeaders once the configured limit is hit
   */
  CORNET_NODISCARD expected<void> add(const header_ref& ref, uint32_t max_headers);

  CORNET_NODISCARD uint32_t size() const { return size_; }
  CORNET_NODISCARD bool empty() const { return size_ == 0; }

  /**
   * @brief entry by index, in arrival order.
   */
  CORNET_NODISCARD const header_ref& at(uint32_t i) const {
    return i < kInline ? inline_[i] : (*overflow_)[i - kInline];
  }

  /**
   * @brief whether an enumerated field is present. One bit test.
   */
  CORNET_NODISCARD bool has(field_t f) const {
    return f != field_t::Other && (bitmap_ & (1u << uint32_t(f))) != 0;
  }

  /**
   * @brief value of an enumerated field, or empty if absent.
   * The bitmap short-circuits the scan for absent headers, which is most lookups.
   */
  CORNET_NODISCARD std::string_view get(field_t f) const;

  /**
   * @brief value of an arbitrary header, matched case-insensitively.
   */
  CORNET_NODISCARD std::string_view get(std::string_view name) const;

  /**
   * @brief name of entry i.
   */
  CORNET_NODISCARD std::string_view name_at(uint32_t i) const;

  /**
   * @brief value of entry i.
   */
  CORNET_NODISCARD std::string_view value_at(uint32_t i) const;

  /**
   * @brief whether a comma-separated list header contains a token,
   * e.g. contains_token(field_t::Connection, "close").
   */
  CORNET_NODISCARD bool contains_token(field_t f, std::string_view token) const;

  /**
   * @brief iteration over (name, value) pairs.
   */
  struct entry_t {
    std::string_view name;
    std::string_view value;
    field_t          field;
  };

  class iterator {
   public:
    iterator(const headers_t* h, uint32_t i) : h_(h), i_(i) {}
    entry_t operator*() const { return {h_->name_at(i_), h_->value_at(i_), h_->at(i_).field}; }
    iterator& operator++() { ++i_; return *this; }
    bool operator!=(const iterator& o) const { return i_ != o.i_; }
   private:
    const headers_t* h_;
    uint32_t i_;
  };

  CORNET_NODISCARD iterator begin() const { return {this, 0}; }
  CORNET_NODISCARD iterator end() const { return {this, size_}; }

 private:
  CORNET_NODISCARD std::string_view resolve(uint32_t off, uint32_t len, bool spilled) const;

  header_ref inline_[kInline]{};
  uint32_t   size_{0};
  uint32_t   bitmap_{0};
  // allocated only when a message carries more than kInline headers
  std::vector<header_ref>* overflow_{nullptr};

  const head_buffer_t*  head_{nullptr};
  const spill_buffer_t* spill_{nullptr};

 public:
  ~headers_t() { delete overflow_; }
  headers_t(const headers_t&) = delete;
  headers_t& operator=(const headers_t&) = delete;
};

} // namespace cornet::http

#endif // CORNET_HTTP_HEADERS_H
