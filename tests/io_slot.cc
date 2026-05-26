#include "core/io_slot.h"
#include "core/utask.h"

#include <gtest/gtest.h>

using namespace cornet;

TEST(io_slot, alloc_and_free) {
  io_slot_table_t table(16);
  utask_t task;
  uint64_t data = table.alloc(&task);
  EXPECT_NE(data, 0u);
  auto* found = table.lookup(data);
  EXPECT_EQ(found, &task);
  table.free(data);
  auto* after_free = table.lookup(data);
  EXPECT_EQ(after_free, nullptr);
}

TEST(io_slot, generation_invalidates_stale) {
  io_slot_table_t table(16);
  utask_t task1, task2;

  uint64_t data1 = table.alloc(&task1);
  table.free(data1);

  uint64_t data2 = table.alloc(&task2);

  auto* stale = table.lookup(data1);
  EXPECT_EQ(stale, nullptr);

  auto* valid = table.lookup(data2);
  EXPECT_EQ(valid, &task2);

  table.free(data2);
}

TEST(io_slot, alloc_all_slots) {
  io_slot_table_t table(4);
  utask_t tasks[4];
  uint64_t datas[4];

  for (int i = 0; i < 4; ++i) {
    datas[i] = table.alloc(&tasks[i]);
    EXPECT_NE(datas[i], 0u);
  }

  for (int i = 0; i < 4; ++i) {
    EXPECT_EQ(table.lookup(datas[i]), &tasks[i]);
  }

  for (int i = 0; i < 4; ++i) {
    table.free(datas[i]);
  }
}

TEST(io_slot, grow_on_exhaust) {
  io_slot_table_t table(2);
  utask_t tasks[4];
  uint64_t datas[4];

  for (int i = 0; i < 4; ++i) {
    datas[i] = table.alloc(&tasks[i]);
  }

  for (int i = 0; i < 4; ++i) {
    EXPECT_EQ(table.lookup(datas[i]), &tasks[i]);
  }

  for (int i = 0; i < 4; ++i) {
    table.free(datas[i]);
  }
}

TEST(io_slot, lookup_invalid_index) {
  io_slot_table_t table(4);
  auto* result = table.lookup(99999);
  EXPECT_EQ(result, nullptr);
}
