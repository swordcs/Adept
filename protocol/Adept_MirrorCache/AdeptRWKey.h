

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

  // blind write
  void clear_blind_bit() { bitvec &= ~(BLIND_BIT_MASK << BLIND_BIT_OFFSET); }

  void set_blind_bit()
  {
    clear_blind_bit();
    bitvec |= BLIND_BIT_MASK << BLIND_BIT_OFFSET;
  }

  uint32_t get_blind_bit() const { return (bitvec >> BLIND_BIT_OFFSET) & BLIND_BIT_MASK; }

  // cached bit
  void clear_cached_bit() { bitvec &= ~(CACHED_BIT_MASK << CACHED_BIT_OFFSET); }

  void set_cached_bit()
  {
    clear_cached_bit();
    bitvec |= CACHED_BIT_MASK << CACHED_BIT_OFFSET;
  }

  uint32_t get_cached_bit() const { return (bitvec >> CACHED_BIT_OFFSET) & CACHED_BIT_MASK; }

  // dirty bit
  void clear_dirty_bit() { bitvec &= ~(DIRTY_BIT_MASK << DIRTY_BIT_OFFSET); }

  void set_dirty_bit()
  {
    clear_dirty_bit();
    bitvec |= DIRTY_BIT_MASK << DIRTY_BIT_OFFSET;
  }

  uint32_t get_dirty_bit() const { return (bitvec >> DIRTY_BIT_OFFSET) & DIRTY_BIT_MASK; }

  // write in cache bit
  void clear_write_cache_bit() { bitvec &= ~(WRITE_CACHE_BIT_MASK << WRITE_CACHE_BIT_OFFSET); }

  void set_write_cache_bit()
  {
    clear_write_cache_bit();
    bitvec |= WRITE_CACHE_BIT_MASK << WRITE_CACHE_BIT_OFFSET;
  }

  uint32_t get_write_cache_bit() const { return (bitvec >> WRITE_CACHE_BIT_OFFSET) & WRITE_CACHE_BIT_MASK; }

  // local cache read bit
  void clear_cache_read_bit() { bitvec &= ~(CACHE_READ_BIT_MASK << CACHE_READ_BIT_OFFSET); }

  void set_cache_read_bit()
  {
    clear_cache_read_bit();
    bitvec |= CACHE_READ_BIT_MASK << CACHE_READ_BIT_OFFSET;
  }

  uint32_t get_cache_read_bit() const { return (bitvec >> CACHE_READ_BIT_OFFSET) & CACHE_READ_BIT_MASK; }

private:
  /*
   * A bitvec is a 32-bit word.
   *
   * [ table id (5) ] | partition id (16) | unused bit (6) |
   * prepare processed bit (1) | execute processed bit(1) |
   * write lock bit(1) | read lock bit (1) | local index read (1)  ]
   *
   * local index read  is set when the read is from a local read only index.
   * write lock bit is set when a write lock is acquired.
   * read lock bit is set when a read lock is acquired.
   * prepare processed bit is set when process_request has processed this key in
   * prepare phase exucution processed bit is set when process_request has
   * processed this key in execution phase
   */

  uint32_t    bitvec = 0;
  const void *key    = nullptr;
  void       *value  = nullptr;

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

  static constexpr uint32_t CACHED_BIT_MASK   = 0x1;
  static constexpr uint32_t CACHED_BIT_OFFSET = 6;

  static constexpr uint32_t DIRTY_BIT_MASK   = 0x1;
  static constexpr uint32_t DIRTY_BIT_OFFSET = 7;

  static constexpr uint32_t WRITE_CACHE_BIT_MASK   = 0x1;
  static constexpr uint32_t WRITE_CACHE_BIT_OFFSET = 8;

  static constexpr uint32_t CACHE_READ_BIT_MASK   = 0x1;
  static constexpr uint32_t CACHE_READ_BIT_OFFSET = 9;
};
}  // namespace aria