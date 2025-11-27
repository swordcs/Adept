

#pragma once

#include <boost/algorithm/string.hpp>
#include <string>
#include <vector>

#include <glog/logging.h>

namespace aria {

class AdeptHelper
{

public:
  using MetaDataType = std::atomic<uint64_t>;

  static std::vector<std::size_t> string_to_vint(const std::string &str)
  {
    std::vector<std::string> vstr;
    boost::algorithm::split(vstr, str, boost::is_any_of(","));
    std::vector<std::size_t> vint;
    for (auto i = 0u; i < vstr.size(); i++) {
      vint.push_back(std::atoi(vstr[i].c_str()));
    }
    return vint;
  }

  static std::size_t n_lock_manager(
      std::size_t replica_group_id, std::size_t id, const std::vector<std::size_t> &lock_managers)
  {
    CHECK(replica_group_id < lock_managers.size());
    return lock_managers[replica_group_id];
  }

  // assume there are n = 2 lock managers and m = 4 workers
  // the following function maps
  // (2, 2, 4) => 0
  // (3, 2, 4) => 0
  // (4, 2, 4) => 1
  // (5, 2, 4) => 1

  static std::size_t worker_id_to_lock_manager_id(std::size_t id, std::size_t n_lock_manager, std::size_t n_worker)
  {
    if (id < n_lock_manager) {
      return id;
    }
    return (id - n_lock_manager) / (n_worker / n_lock_manager);
  }

  // assume the replication group size is 3 and we have partitions 0..8
  // the 1st coordinator has partition 0, 3, 6.
  // the 2nd coordinator has partition 1, 4, 7.
  // the 3rd coordinator has partition 2, 5, 8.
  // the function first maps all partition id to 0, 1, 2 and then use % hash to
  // assign each partition to a lock manager.

  static std::size_t partition_id_to_lock_manager_id(
      std::size_t partition_id, std::size_t n_lock_manager, std::size_t replica_group_size)
  {
    return partition_id / replica_group_size % n_lock_manager;
  }

  static void read(const std::tuple<MetaDataType *, void *> &row, void *dest, std::size_t size)
  {

    MetaDataType &tid = *std::get<0>(row);
    void         *src = std::get<1>(row);
    std::memcpy(dest, src, size);
  }

  /**
   *
   * The following code is adapted from TwoPLHelper.h
   * For Adept, we can use lower 63 bits for read locks.
   * However, 511 locks are enough and the code above is well tested.
   *
   * [write lock bit (1) |  read lock bit (9) -- 512 - 1 locks | reservation(2) | epoch (32) | position (20)]
   *
   * Max batch size is 2^20 = 1M and max epoch is 2^32 = 4G
   */

  static bool is_read_locked(uint64_t value) { return value & (READ_LOCK_BIT_MASK << READ_LOCK_BIT_OFFSET); }

  static bool is_write_locked(uint64_t value) { return value & (WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET); }

  static bool is_reserve_locked(uint64_t value) { return value & (TUPLE_STATUS_MASK << TUPLE_STATUS_OFFSET); }

  static uint64_t read_lock_num(uint64_t value) { return (value >> READ_LOCK_BIT_OFFSET) & READ_LOCK_BIT_MASK; }

  static uint64_t read_lock_max() { return READ_LOCK_BIT_MASK; }

  static uint64_t read_lock(std::atomic<uint64_t> &a)
  {
    uint64_t old_value, new_value;
    do {
      do {
        old_value = a.load();
      } while (is_write_locked(old_value) || read_lock_num(old_value) == read_lock_max());

      new_value = old_value + (1ull << READ_LOCK_BIT_OFFSET);

    } while (!a.compare_exchange_weak(old_value, new_value));
    return remove_lock_bit(old_value);
  }

  static uint64_t write_lock(std::atomic<uint64_t> &a)
  {
    uint64_t old_value, new_value;

    do {
      do {
        old_value = a.load();
      } while (is_read_locked(old_value) || is_write_locked(old_value));

      new_value = old_value + (1ull << WRITE_LOCK_BIT_OFFSET);

    } while (!a.compare_exchange_weak(old_value, new_value));
    return remove_lock_bit(old_value);
  }

  // fast fail version of write_lock
  static bool try_write_lock(std::atomic<uint64_t> &a)
  {
    uint64_t old_value, new_value;

    do {
      do {
        old_value = a.load();
      } while (is_reserve_locked(old_value));

      if (is_read_locked(old_value) || is_write_locked(old_value)) {
        return false;
      }
      new_value = old_value + (WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);
    } while (!a.compare_exchange_weak(old_value, new_value));
    return true;
  }

  // fast fail version of read_lock
  static bool try_read_lock(std::atomic<uint64_t> &a)
  {
    uint64_t old_value, new_value;

    do {
      do {
        old_value = a.load();
      } while (is_reserve_locked(old_value));

      if (is_write_locked(old_value) || read_lock_num(old_value) == read_lock_max()) {
        return false;
      }
      new_value = old_value + (1ull << READ_LOCK_BIT_OFFSET);
    } while (!a.compare_exchange_weak(old_value, new_value));
    return true;
  }

  // if lock fails, reserve the tuple for further scheduling
  // this should be called in a single thread environment
  static bool try_write_lock_reserve(std::atomic<uint64_t> &a)
  {
    uint64_t old_value, new_value;
    do {
      do {
        old_value = a.load();
      } while (is_reserve_locked(old_value));
      // very likely locked by read lock
      if (is_read_locked(old_value) || is_write_locked(old_value)) {
        new_value = old_value + (1ull << TUPLE_STATUS_OFFSET);
      } else {
        new_value = old_value + (1ull << WRITE_LOCK_BIT_OFFSET);
      }
    } while (!a.compare_exchange_weak(old_value, new_value));
    return !is_read_locked(old_value) && !is_write_locked(old_value);
  }

  // if lock fails, reserve the tuple for further scheduling
  static bool try_read_lock_reserve(std::atomic<uint64_t> &a)
  {
    uint64_t old_value, new_value;
    do {
      do {
        old_value = a.load();
      } while (is_reserve_locked(old_value));
      // very likely locked by write lock
      if (is_write_locked(old_value) || read_lock_num(old_value) == read_lock_max()) {
        new_value = old_value + (1ull << TUPLE_STATUS_OFFSET);
      } else {
        new_value = old_value + (1ull << READ_LOCK_BIT_OFFSET);
      }

    } while (!a.compare_exchange_weak(old_value, new_value));
    return !is_write_locked(old_value) && read_lock_num(old_value) < read_lock_max();
  }

  static bool reserve_lock(std::atomic<uint64_t> &a)
  {
    uint64_t old_value, new_value;
    do {
      do {
        old_value = a.load();
      } while (is_reserve_locked(old_value));

      new_value = old_value + (1ull << (TUPLE_STATUS_OFFSET));

    } while (!a.compare_exchange_weak(old_value, new_value));
    return true;
  }

  // remove reservation bit and return whether the reservation bit was set
  static bool reserve_lock_release(std::atomic<uint64_t> &a)
  {
    uint64_t old_value, new_value;
    do {
      old_value = a.load();
      DCHECK(is_reserve_locked(old_value));

      new_value = old_value - (1ull << (TUPLE_STATUS_OFFSET));

    } while (!a.compare_exchange_weak(old_value, new_value));
    return true;
  }

  static void read_lock_release(std::atomic<uint64_t> &a)
  {
    uint64_t old_value, new_value;
    do {
      old_value = a.load();
      DCHECK(is_read_locked(old_value));
      DCHECK(!is_write_locked(old_value));
      new_value = old_value - (1ull << READ_LOCK_BIT_OFFSET);
    } while (!a.compare_exchange_weak(old_value, new_value));
  }

  static void write_lock_release(std::atomic<uint64_t> &a)
  {
    uint64_t old_value, new_value;
    old_value = a.load();
    DCHECK(!is_read_locked(old_value));
    DCHECK(is_write_locked(old_value));
    new_value = old_value - (1ull << WRITE_LOCK_BIT_OFFSET);
    bool ok   = a.compare_exchange_strong(old_value, new_value);
    DCHECK(ok);
  }

  static void upgrade_read_to_write_lock(std::atomic<uint64_t> &a)
  {
    uint64_t old_value, new_value;
    do {
      old_value = a.load();
      DCHECK(read_lock_num(old_value) == 1);
      DCHECK(!is_write_locked(old_value));
      new_value = old_value - (1ull << READ_LOCK_BIT_OFFSET) + (1ull << WRITE_LOCK_BIT_OFFSET);
    } while (!a.compare_exchange_weak(old_value, new_value));
  }

  static void downgrade_write_to_read_lock(std::atomic<uint64_t> &a)
  {
    uint64_t old_value, new_value;
    do {
      old_value = a.load();
      DCHECK(is_write_locked(old_value));
      new_value = old_value - (1ull << WRITE_LOCK_BIT_OFFSET) + (1ull << READ_LOCK_BIT_OFFSET);
    } while (!a.compare_exchange_weak(old_value, new_value));
  }

  static uint64_t remove_lock_bit(uint64_t value) { return value & ~(LOCK_BIT_MASK << LOCK_BIT_OFFSET); }

  static uint64_t remove_read_lock_bit(uint64_t value) { return value & ~(READ_LOCK_BIT_MASK << READ_LOCK_BIT_OFFSET); }

  static uint64_t remove_write_lock_bit(uint64_t value)
  {
    return value & ~(WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);
  }

  static uint64_t get_tid(uint64_t epoch, uint64_t tid_offset)
  {
    DCHECK(epoch < (1ull << 32));
    DCHECK(tid_offset < (1ull << 20));
    return (epoch << EPOCH_OFFSET) | (tid_offset << POS_OFFSET);
  }

  static uint32_t get_tid_offset(uint64_t value) { return (value >> POS_OFFSET) & POS_MASK; }

  static uint64_t get_tuple_status(uint64_t value) { return (value >> TUPLE_STATUS_OFFSET) & TUPLE_STATUS_MASK; }

  static uint64_t get_waiter(uint64_t value) { return value & TID_MASK; }

  static uint64_t set_waiter(std::atomic<uint64_t> &a, uint64_t waiter)
  {
    uint64_t old_value, new_value;
    do {
      old_value = a.load();
      DCHECK(is_reserve_locked(old_value));
      new_value = (old_value & FULL_MASK << 53) | waiter;
    } while (!a.compare_exchange_weak(old_value, new_value));
    return new_value & TID_MASK;
  }

  static void set_waiter_rw(std::atomic<uint64_t> &a, bool write)
  {
    uint64_t old_value, new_value;
    do {
      old_value = a.load();
      if (write) {
        new_value = old_value | (1ull << WAITER_RW_OFFSET);
      } else {
        new_value = old_value & ~(1ull << WAITER_RW_OFFSET);
      }
    } while (!a.compare_exchange_weak(old_value, new_value));
  }

  static bool get_waiter_rw(uint64_t value) { return (value >> WAITER_RW_OFFSET) & WAITER_RW_MASK; }

public:
  static constexpr int      LOCK_BIT_OFFSET = 54;
  static constexpr uint64_t LOCK_BIT_MASK   = 0x3ffull;

  static constexpr int      READ_LOCK_BIT_OFFSET = 54;
  static constexpr uint64_t READ_LOCK_BIT_MASK   = 0x1ffull;

  static constexpr int      WRITE_LOCK_BIT_OFFSET = 63;
  static constexpr uint64_t WRITE_LOCK_BIT_MASK   = 0x1ull;

  static constexpr int      TUPLE_STATUS_OFFSET = 53;
  static constexpr uint64_t TUPLE_STATUS_MASK   = 0x1ull;

  static constexpr int      WAITER_RW_OFFSET = 52;
  static constexpr uint64_t WAITER_RW_MASK   = 0x1ull;

  static constexpr int      EPOCH_OFFSET = 20;
  static constexpr uint64_t EPOCH_MASK   = 0xffffffffull;

  static constexpr int      POS_OFFSET = 0;
  static constexpr uint64_t POS_MASK   = 0xfffffull;

  static constexpr uint64_t FULL_MASK = 0xffffffffffffffffull;

  static constexpr uint64_t TID_MASK = EPOCH_MASK << EPOCH_OFFSET | POS_MASK << POS_OFFSET;
};
}  // namespace aria