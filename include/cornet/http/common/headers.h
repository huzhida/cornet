#ifndef CORNET_HTTP_COMMON_HEADERS_H
#define CORNET_HTTP_COMMON_HEADERS_H

#include <cstdint>
#include <string_view>
#include <vector>

#include "cornet/http/common/buffer.h"
#include "cornet/http/common/protocol.h"

namespace cornet::http {

/**
 * @brief one header, stored as offsets rather than pointers.
 *
 * Offsets survive anything the owning buffer does to itself, which is what makes
 * zero-copy parsing safe (see head_buffer_t). The two `*_spilled` flags select which
 * buffer each half lives in: the header buffer for the normal case, the spill buffer
 * for anything the parser had to copy — a folded value, or a trailer, whose bytes sit
 * in the body region and would be overwritten as the body streams past.
 *
 * The three flags fit in padding the struct already had, so it is still 16 bytes.
 */
struct header_ref {
  uint32_t name_off{0};
  uint32_t value_off{0};
  uint16_t name_len{0};
  uint16_t value_len{0};
  field_t  field{field_t::Other};
  bool     spilled{false};        // value lives in the spill buffer
  bool     name_spilled{false};   // name too
  bool     trailer{false};        // arrived after the body, not with the headers
};

static_assert(sizeof(header_ref) == 16, "header_ref grew; it used to fit in padding");

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
    trailers_ = 0;
    bitmap_ = 0;
    trailer_bitmap_ = 0;
    if (overflow_) overflow_->clear();
  }

  /**
   * @brief append one header or trailer.
   *
   * Headers and trailers share the entry array but are counted and looked up apart,
   * so `max_entries` applies to whichever kind `ref` is.
   * @return TooManyHeaders once the limit for that kind is hit
   */
  CORNET_NODISCARD expected<void> add(const header_ref& ref, uint32_t max_entries);

  /**
   * @brief total entries, headers and trailers together.
   */
  CORNET_NODISCARD uint32_t size() const { return size_; }
  CORNET_NODISCARD bool empty() const { return size_ == 0; }

  /**
   * @brief how many of the entries arrived as trailers.
   */
  CORNET_NODISCARD uint32_t trailer_count() const { return trailers_; }

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
   *
   * Header section only: a trailer never answers a header lookup, so no framing or
   * routing decision can be influenced by something that arrived after the body.
   * The bitmap short-circuits the scan for absent headers, which is most lookups.
   */
  CORNET_NODISCARD std::string_view get(field_t f) const;

  /**
   * @brief value of an arbitrary header, matched case-insensitively.
   * Header section only, like get(field_t).
   */
  CORNET_NODISCARD std::string_view get(std::string_view name) const;

  /**
   * @brief whether this field arrived as a trailer.
   */
  CORNET_NODISCARD bool has_trailer(field_t f) const {
    return f != field_t::Other && (trailer_bitmap_ & (1u << uint32_t(f))) != 0;
  }

  /**
   * @brief value of a trailer field, or empty if it did not arrive as one.
   *
   * Deliberately a separate call rather than a fallback inside get(): code that reads
   * a header should never silently pick up a value the peer appended after the body.
   * Trailers are only complete once the body has been fully read.
   */
  CORNET_NODISCARD std::string_view trailer(field_t f) const;

  /**
   * @brief value of an arbitrary trailer, matched case-insensitively.
   */
  CORNET_NODISCARD std::string_view trailer(std::string_view name) const;

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
  uint32_t   trailers_{0};
  uint32_t   bitmap_{0};
  uint32_t   trailer_bitmap_{0};
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

#endif // CORNET_HTTP_COMMON_HEADERS_H
