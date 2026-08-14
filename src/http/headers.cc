#include "cornet/http/headers.h"

namespace cornet::http {

expected<void> headers_t::add(const header_ref& ref, uint32_t max_headers) {
  if (size_ >= max_headers) {
    return http_unexpected(http_error_t::TooManyHeaders);
  }
  if (size_ < kInline) {
    inline_[size_] = ref;
  } else {
    if (!overflow_) overflow_ = new std::vector<header_ref>();
    overflow_->push_back(ref);
  }
  ++size_;
  if (ref.field != field_t::Other) {
    bitmap_ |= (1u << uint32_t(ref.field));
  }
  return {};
}

std::string_view headers_t::resolve(uint32_t off, uint32_t len, bool spilled) const {
  if (len == 0) return {};
  if (spilled) {
    return spill_ ? spill_->view(off, len) : std::string_view{};
  }
  return head_ ? head_->view(off, len) : std::string_view{};
}

std::string_view headers_t::name_at(uint32_t i) const {
  const auto& h = at(i);
  return resolve(h.name_off, h.name_len, false);
}

std::string_view headers_t::value_at(uint32_t i) const {
  const auto& h = at(i);
  return resolve(h.value_off, h.value_len, h.spilled);
}

std::string_view headers_t::get(field_t f) const {
  // Most lookups are for headers the request did not send; the bitmap answers
  // those without walking the entry array at all.
  if (!has(f)) return {};
  for (uint32_t i = 0; i < size_; ++i) {
    const auto& h = at(i);
    if (h.field == f) return resolve(h.value_off, h.value_len, h.spilled);
  }
  return {};
}

std::string_view headers_t::get(std::string_view name) const {
  // Route through the enum when the name is one we know: an integer compare per
  // entry instead of a string compare per entry.
  if (auto f = field_from_name(name); f != field_t::Other) {
    return get(f);
  }
  for (uint32_t i = 0; i < size_; ++i) {
    const auto& h = at(i);
    if (h.field != field_t::Other) continue;
    if (iequals(resolve(h.name_off, h.name_len, false), name)) {
      return resolve(h.value_off, h.value_len, h.spilled);
    }
  }
  return {};
}

bool headers_t::contains_token(field_t f, std::string_view token) const {
  auto value = get(f);
  if (value.empty()) return false;
  size_t pos = 0;
  while (pos < value.size()) {
    size_t comma = value.find(',', pos);
    auto piece = value.substr(pos, comma == std::string_view::npos ? std::string_view::npos : comma - pos);
    // trim OWS on both ends
    while (!piece.empty() && (piece.front() == ' ' || piece.front() == '\t')) piece.remove_prefix(1);
    while (!piece.empty() && (piece.back() == ' ' || piece.back() == '\t')) piece.remove_suffix(1);
    if (iequals(piece, token)) return true;
    if (comma == std::string_view::npos) break;
    pos = comma + 1;
  }
  return false;
}

} // namespace cornet::http
