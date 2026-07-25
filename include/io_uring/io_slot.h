#ifndef CORNET_IO_SLOT_H
#define CORNET_IO_SLOT_H

#include <cstdint>
#include <vector>
#include "base/defines.h"

namespace cornet {

struct utask_t;

/**
 * @brief slot entry holding a utask pointer and a generation counter.
 * When a slot is freed, generation increments so stale CQEs are detected.
 */
struct io_slot_t {
  utask_t* task{nullptr};
  uint32_t generation{1};
};

/**
 * @brief encodes a slot index and generation into a single 64-bit value
 * suitable for io_uring user_data. Layout: [32-bit index | 32-bit generation]
 */
inline uint64_t encode_slot(uint32_t index, uint32_t generation) {
  return (static_cast<uint64_t>(generation) << 32) | index;
}

/**
 * @brief decode slot index from user_data
 */
inline uint32_t decode_slot_index(uint64_t user_data) {
  return static_cast<uint32_t>(user_data & 0xFFFFFFFF);
}

/**
 * @brief decode generation from user_data
 */
inline uint32_t decode_slot_generation(uint64_t user_data) {
  return static_cast<uint32_t>(user_data >> 32);
}

/**
 * @brief per-context slot table for safe io_uring user_data management.
 * Provides O(1) alloc/free via a free list, and generation-based stale detection.
 * Slots are never moved in memory, so pointers remain stable.
 */
class io_slot_table_t {
 public:
  explicit io_slot_table_t(uint32_t capacity = 4096)
    : slots_(capacity) {
    for (uint32_t i = 0; i < capacity; ++i) {
      free_list_.push_back(i);
    }
  }

  /**
   * @brief allocate a slot for the given task.
   * @param task the utask to associate with this slot
   * @return encoded user_data (slot index + generation)
   */
  uint64_t alloc(utask_t* task) {
    if (free_list_.empty()) {
      uint32_t old_size = slots_.size();
      uint32_t new_size = old_size * 2;
      slots_.resize(new_size);
      for (uint32_t i = old_size; i < new_size; ++i) {
        free_list_.push_back(i);
      }
    }
    uint32_t idx = free_list_.back();
    free_list_.pop_back();
    auto& slot = slots_[idx];
    slot.task = task;
    return encode_slot(idx, slot.generation);
  }

  /**
   * @brief free a slot, incrementing generation to invalidate stale CQEs.
   * @param user_data the encoded user_data returned from alloc()
   */
  void free(uint64_t user_data) {
    uint32_t idx = decode_slot_index(user_data);
    auto& slot = slots_[idx];
    slot.task = nullptr;
    slot.generation++;
    free_list_.push_back(idx);
  }

  /**
   * @brief look up a task by user_data. Returns nullptr if generation mismatches (stale CQE).
   * @param user_data the encoded user_data from the CQE
   * @return valid utask_t pointer, or nullptr if the slot was freed/reused
   */
  utask_t* lookup(uint64_t user_data) const {
    uint32_t idx = decode_slot_index(user_data);
    uint32_t gen = decode_slot_generation(user_data);
    if (idx >= slots_.size()) return nullptr;
    const auto& slot = slots_[idx];
    if (slot.generation != gen) return nullptr;
    return slot.task;
  }

  template<typename F>
  void for_each_active(F&& fn) const {
    for (uint32_t i = 0; i < slots_.size(); ++i) {
      if (slots_[i].task) {
        fn(encode_slot(i, slots_[i].generation));
      }
    }
  }

 private:
  std::vector<io_slot_t> slots_;
  std::vector<uint32_t> free_list_;
};

} // namespace cornet

#endif //CORNET_IO_SLOT_H
