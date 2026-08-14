//
// Created by Yi Lu on 9/14/18.
//

#pragma once

#include <glog/logging.h>

namespace aria {

class AdeptRWKey
{
public:
  // local index read bit

  void set_local_index_read_bit()
  {
    clear_local_index_read_bit();
    bitvec |= LOCAL_INDEX_READ_BIT_MASK << LOCAL_INDEX_READ_BIT_OFFSET;
  }

  void clear_local_index_read_bit() { bitvec &= ~(LOCAL_INDEX_READ_BIT_MASK << LOCAL_INDEX_READ_BIT_OFFSET); }

  uint32_t get_local_index_read_bit() const
  {
    return (bitvec >> LOCAL_INDEX_READ_BIT_OFFSET) & LOCAL_INDEX_READ_BIT_MASK;
  }

  // read lock bit

  void set_read_lock_bit()
  {
    clear_read_lock_bit();
    bitvec |= READ_LOCK_BIT_MASK << READ_LOCK_BIT_OFFSET;
  }

  void clear_read_lock_bit() { bitvec &= ~(READ_LOCK_BIT_MASK << READ_LOCK_BIT_OFFSET); }

  uint32_t get_read_lock_bit() const { return (bitvec >> READ_LOCK_BIT_OFFSET) & READ_LOCK_BIT_MASK; }

  // write lock bit

  void set_write_lock_bit()
  {
    clear_write_lock_bit();
    bitvec |= WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET;
  }

  void clear_write_lock_bit() { bitvec &= ~(WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET); }

  uint32_t get_write_lock_bit() const { return (bitvec >> WRITE_LOCK_BIT_OFFSET) & WRITE_LOCK_BIT_MASK; }

  // prepare processed bit

  void set_prepare_processed_bit()
  {
    clear_prepare_processed_bit();
    bitvec |= PREPARE_PROCESSED_BIT_MASK << PREPARE_PROCESSED_BIT_OFFSET;
  }

  void clear_prepare_processed_bit() { bitvec &= ~(PREPARE_PROCESSED_BIT_MASK << PREPARE_PROCESSED_BIT_OFFSET); }

  uint32_t get_prepare_processed_bit() const
  {
    return (bitvec >> PREPARE_PROCESSED_BIT_OFFSET) & PREPARE_PROCESSED_BIT_MASK;
  }

  // execution processed bit

  void set_execution_processed_bit()
  {
    clear_execution_processed_bit();
    bitvec |= EXECUTION_PROCESSED_BIT_MASK << EXECUTION_PROCESSED_BIT_OFFSET;
  }

  void clear_execution_processed_bit() { bitvec &= ~(EXECUTION_PROCESSED_BIT_MASK << EXECUTION_PROCESSED_BIT_OFFSET); }

  uint32_t get_execution_processed_bit() const
  {
    return (bitvec >> EXECUTION_PROCESSED_BIT_OFFSET) & EXECUTION_PROCESSED_BIT_MASK;
  }

  // table id

  void set_table_id(uint32_t table_id)
  {
    DCHECK(table_id < (1 << 5));
    clear_table_id();
    bitvec |= table_id << TABLE_ID_OFFSET;
  }

  void clear_table_id() { bitvec &= ~(TABLE_ID_MASK << TABLE_ID_OFFSET); }

  uint32_t get_table_id() const { return (bitvec >> TABLE_ID_OFFSET) & TABLE_ID_MASK; }
  // partition id

  void set_partition_id(uint32_t partition_id)
  {
    DCHECK(partition_id < (1 << 16));
    clear_partition_id();
    bitvec |= partition_id << PARTITION_ID_OFFSET;
  }

  void clear_partition_id() { bitvec &= ~(PARTITION_ID_MASK << PARTITION_ID_OFFSET); }

  uint32_t get_partition_id() const { return (bitvec >> PARTITION_ID_OFFSET) & PARTITION_ID_MASK; }

  // key
  void set_key(const void *key) { this->key = key; }

  const void *get_key() const { return key; }

  // value
  void set_value(void *value) { this->value = value; }

  void *get_value() const { return value; }

  void clear_blind_bit() { bitvec &= ~(BLIND_BIT_MASK << BLIND_BIT_OFFSET); }

  void set_blind_bit()
  {
    clear_blind_bit();
    bitvec |= BLIND_BIT_MASK << BLIND_BIT_OFFSET;
  }

  uint32_t get_blind_bit() const { return (bitvec >> BLIND_BIT_OFFSET) & BLIND_BIT_MASK; }

  void set_mirror_cache_read_bit()
  {
    bitvec |= MIRROR_CACHE_READ_BIT_MASK << MIRROR_CACHE_READ_BIT_OFFSET;
  }

  void clear_mirror_cache_read_bit()
  {
    bitvec &= ~(MIRROR_CACHE_READ_BIT_MASK << MIRROR_CACHE_READ_BIT_OFFSET);
  }

  uint32_t get_mirror_cache_read_bit() const
  {
    return (bitvec >> MIRROR_CACHE_READ_BIT_OFFSET) & MIRROR_CACHE_READ_BIT_MASK;
  }

  void set_mirror_cache_fill_bit()
  {
    bitvec |= MIRROR_CACHE_FILL_BIT_MASK << MIRROR_CACHE_FILL_BIT_OFFSET;
  }

  void clear_mirror_cache_fill_bit()
  {
    bitvec &= ~(MIRROR_CACHE_FILL_BIT_MASK << MIRROR_CACHE_FILL_BIT_OFFSET);
  }

  uint32_t get_mirror_cache_fill_bit() const
  {
    return (bitvec >> MIRROR_CACHE_FILL_BIT_OFFSET) & MIRROR_CACHE_FILL_BIT_MASK;
  }

  void set_pipelined_read_ready_bit()
  {
    bitvec |= PIPELINED_READ_READY_BIT_MASK << PIPELINED_READ_READY_BIT_OFFSET;
  }

  void clear_pipelined_read_ready_bit()
  {
    bitvec &= ~(PIPELINED_READ_READY_BIT_MASK << PIPELINED_READ_READY_BIT_OFFSET);
  }

  uint32_t get_pipelined_read_ready_bit() const
  {
    return (bitvec >> PIPELINED_READ_READY_BIT_OFFSET) & PIPELINED_READ_READY_BIT_MASK;
  }

  void set_scheduled_lock_bit()
  {
    bitvec |= SCHEDULED_LOCK_BIT_MASK << SCHEDULED_LOCK_BIT_OFFSET;
  }

  void clear_scheduled_lock_bit()
  {
    bitvec &= ~(SCHEDULED_LOCK_BIT_MASK << SCHEDULED_LOCK_BIT_OFFSET);
  }

  uint32_t get_scheduled_lock_bit() const
  {
    return (bitvec >> SCHEDULED_LOCK_BIT_OFFSET) & SCHEDULED_LOCK_BIT_MASK;
  }

  void set_deferred_abort_bit()
  {
    bitvec |= DEFERRED_ABORT_BIT_MASK << DEFERRED_ABORT_BIT_OFFSET;
  }

  void clear_deferred_abort_bit()
  {
    bitvec &= ~(DEFERRED_ABORT_BIT_MASK << DEFERRED_ABORT_BIT_OFFSET);
  }

  uint32_t get_deferred_abort_bit() const
  {
    return (bitvec >> DEFERRED_ABORT_BIT_OFFSET) & DEFERRED_ABORT_BIT_MASK;
  }

  void add_mirror_skip_destination(std::size_t coordinator_id)
  {
    DCHECK(coordinator_id < 64);
    mirror_skip_mask |= uint64_t{1} << coordinator_id;
  }

  void add_mirror_skip_mask(uint64_t destinations) { mirror_skip_mask |= destinations; }

  bool should_skip_mirror_destination(std::size_t coordinator_id) const
  {
    DCHECK(coordinator_id < 64);
    return (mirror_skip_mask & (uint64_t{1} << coordinator_id)) != 0;
  }

  void clear_mirror_cache_decisions()
  {
    clear_mirror_cache_read_bit();
    clear_mirror_cache_fill_bit();
    mirror_skip_mask = 0;
  }

  void set_mirror_cache_entry(const void *entry) { mirror_cache_entry = entry; }

  const void *get_mirror_cache_entry() const { return mirror_cache_entry; }

private:
  uint32_t    bitvec = 0;
  const void *key    = nullptr;
  void       *value  = nullptr;
  uint64_t    mirror_skip_mask = 0;
  const void *mirror_cache_entry = nullptr;

public:
  static constexpr uint32_t TABLE_ID_MASK   = 0x1f;
  static constexpr uint32_t TABLE_ID_OFFSET = 27;

  static constexpr uint32_t PARTITION_ID_MASK   = 0xffff;
  static constexpr uint32_t PARTITION_ID_OFFSET = 11;

  static constexpr uint32_t EXECUTION_PROCESSED_BIT_MASK   = 0x1;
  static constexpr uint32_t EXECUTION_PROCESSED_BIT_OFFSET = 4;

  static constexpr uint32_t PREPARE_PROCESSED_BIT_MASK   = 0x1;
  static constexpr uint32_t PREPARE_PROCESSED_BIT_OFFSET = 3;

  static constexpr uint32_t WRITE_LOCK_BIT_MASK   = 0x1;
  static constexpr uint32_t WRITE_LOCK_BIT_OFFSET = 2;

  static constexpr uint32_t READ_LOCK_BIT_MASK   = 0x1;
  static constexpr uint32_t READ_LOCK_BIT_OFFSET = 1;

  static constexpr uint32_t LOCAL_INDEX_READ_BIT_MASK   = 0x1;
  static constexpr uint32_t LOCAL_INDEX_READ_BIT_OFFSET = 0;

  static constexpr uint32_t BLIND_BIT_MASK   = 0x1;
  static constexpr uint32_t BLIND_BIT_OFFSET = 5;

  static constexpr uint32_t MIRROR_CACHE_READ_BIT_MASK   = 0x1;
  static constexpr uint32_t MIRROR_CACHE_READ_BIT_OFFSET = 6;

  static constexpr uint32_t MIRROR_CACHE_FILL_BIT_MASK   = 0x1;
  static constexpr uint32_t MIRROR_CACHE_FILL_BIT_OFFSET = 7;

  static constexpr uint32_t PIPELINED_READ_READY_BIT_MASK   = 0x1;
  static constexpr uint32_t PIPELINED_READ_READY_BIT_OFFSET = 8;

  static constexpr uint32_t SCHEDULED_LOCK_BIT_MASK   = 0x1;
  static constexpr uint32_t SCHEDULED_LOCK_BIT_OFFSET = 9;

  static constexpr uint32_t DEFERRED_ABORT_BIT_MASK   = 0x1;
  static constexpr uint32_t DEFERRED_ABORT_BIT_OFFSET = 10;
};
}  // namespace aria
