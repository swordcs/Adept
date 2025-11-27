

#pragma once

#include "common/Operation.h"
#include "core/Coroutine.h"
#include "core/Defs.h"
#include "protocol/Adept/AdeptHelper.h"
#include "protocol/Adept/AdeptPartitioner.h"
#include "protocol/Adept/AdeptRWKey.h"
#include <chrono>
#include <glog/logging.h>
#include <thread>
#include <mutex>
#include <array>

namespace aria {
class AdeptTransaction
{

public:
  using MetaDataType = std::atomic<uint64_t>;

  AdeptTransaction(std::size_t coordinator_id, std::size_t partition_id, Partitioner &partitioner)
      : coordinator_id(coordinator_id),
        partition_id(partition_id),
        startTime(std::chrono::steady_clock::now()),
        partitioner(partitioner)
  {
    reset();
  }

  virtual ~AdeptTransaction() = default;

  void reset()
  {
    local_read.store(0);
    saved_local_read = 0;
    remote_read.store(0);
    saved_remote_read       = 0;
    abort_no_retry          = false;
    distributed_transaction = false;
    execution_phase         = false;
    network_size.store(0);
    active_coordinators.clear();
    operation.clear();
    readSet.clear();
    writeSet.clear();

    // Initialize fixed-size read bitmap with 1s
    for (auto &b : read_bitmap) {
      b.store(1, std::memory_order_relaxed);
    }
    // Initialize fixed-size blocked bitmap with false
    for (auto &b : blocked_bitmap) {
      b = false;
    }
  }

  virtual TransactionResult execute(std::size_t worker_id) = 0;

  virtual Task<TransactionResult> execute_coro(std::size_t worker_id) = 0;

  virtual void reset_query() = 0;

  template <class KeyType, class ValueType>
  void search_local_index(std::size_t table_id, std::size_t partition_id, const KeyType &key, ValueType &value)
  {

    if (execution_phase) {
      return;
    }

    AdeptRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_local_index_read_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_read(std::size_t table_id, std::size_t partition_id, const KeyType &key, ValueType &value)
  {

    if (execution_phase) {
      return;
    }

    AdeptRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_read_lock_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_update(
      std::size_t table_id, std::size_t partition_id, const KeyType &key, ValueType &value, bool blind = false)
  {
    if (execution_phase) {
      return;
    }

    AdeptRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);
    if (blind) {
      readKey.set_blind_bit();
    }
    readKey.set_write_lock_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void update(
      std::size_t table_id, std::size_t partition_id, const KeyType &key, const ValueType &value, bool blind = false)
  {

    if (execution_phase) {
      return;
    }

    AdeptRWKey writeKey;

    writeKey.set_table_id(table_id);
    writeKey.set_partition_id(partition_id);

    writeKey.set_key(&key);
    // the object pointed by value will not be updated
    writeKey.set_value(const_cast<ValueType *>(&value));
    if (blind) {
      writeKey.set_blind_bit();
    }
    writeKey.set_write_lock_bit();

    add_to_write_set(writeKey);
  }

  std::size_t add_to_read_set(const AdeptRWKey &key)
  {
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const AdeptRWKey &key)
  {
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

  void set_id(uint32_t epoch, std::size_t tid_offset)
  {
    this->epoch      = epoch;
    this->tid_offset = tid_offset;
    this->id         = AdeptHelper::get_tid(epoch, tid_offset);
  }

  void setup_process_requests_in_prepare_phase()
  {
    // process the reads in read-only index
    // for general reads, increment the local_read and remote_read counter.
    // the function may be called multiple times, the keys are processed in
    // reverse order.
    process_requests = [this](std::size_t worker_id) {
      // cannot use unsigned type in reverse iteration
      for (int i = int(readSet.size()) - 1; i >= 0; i--) {
        // early return
        if (readSet[i].get_prepare_processed_bit()) {
          break;
        }

        if (readSet[i].get_local_index_read_bit()) {
          // this is a local index read
          auto &readKey = readSet[i];
          local_index_read_handler(
              readKey.get_table_id(), readKey.get_partition_id(), readKey.get_key(), readKey.get_value());
        } else {

          if (partitioner.has_master_partition(readSet[i].get_partition_id())) {
            local_read.fetch_add(1);
          } else {
            remote_read.fetch_add(1);
          }
        }

        readSet[i].set_prepare_processed_bit();
      }
      return false;
    };
  }

  void setup_process_requests_in_execution_phase(
      std::size_t n_lock_manager, std::size_t n_worker, std::size_t replica_group_size)
  {
    process_requests_coro = [this, n_lock_manager, n_worker, replica_group_size](std::size_t worker_id) -> Task<bool> {
      auto lock_manager_id = AdeptHelper::worker_id_to_lock_manager_id(worker_id, n_lock_manager, n_worker);

      for (int i = int(readSet.size()) - 1; i >= 0; i--) {
        if (readSet[i].get_local_index_read_bit()) {
          continue;
        }

        if (AdeptHelper::partition_id_to_lock_manager_id(
                readSet[i].get_partition_id(), n_lock_manager, replica_group_size) != lock_manager_id) {
          continue;
        }

        if (readSet[i].get_execution_processed_bit()) {
          break;
        }

        auto &readKey = readSet[i];
        read_handler(worker_id,
            readKey.get_table_id(),
            readKey.get_partition_id(),
            id,
            i,
            readKey.get_key(),
            readKey.get_value(),
            readKey.get_cache_read_bit());

        readSet[i].set_execution_processed_bit();
      }

      message_flusher(worker_id);

      if (active_coordinators[coordinator_id]) {
        bool ready = false;
        do {
          ready = local_read.load() <= 0 && remote_read.load() <= 0;
          if (!ready) {
            // process remote reads for other workers
            remote_request_handler(worker_id);
            co_await std::suspend_always{};
          }
        } while (!ready);

        co_return false;
      } else {
        co_return true;
      }
    };
  }

  void save_read_count()
  {
    saved_local_read  = local_read.load();
    saved_remote_read = remote_read.load();
  }

  void load_read_count()
  {
    local_read.store(saved_local_read);
    remote_read.store(saved_remote_read);
  }

  void clear_execution_bit()
  {
    for (auto i = 0u; i < readSet.size(); i++) {

      if (readSet[i].get_local_index_read_bit()) {
        continue;
      }

      readSet[i].clear_execution_processed_bit();
    }
  }

public:
  uint32_t    epoch;
  uint64_t    id;  // [...(10 bit), ...(2 bit), epoch(32 bit), tid_offset(20 bit)]
  std::size_t coordinator_id, partition_id, tid_offset;

  std::atomic<int32_t> blocked_counter{-1};

  std::chrono::steady_clock::time_point startTime;
  std::atomic<int32_t>                  network_size;
  std::atomic<int32_t>                  local_read, remote_read;
  int32_t                               saved_local_read, saved_remote_read;

  bool abort_no_retry;
  bool distributed_transaction;
  bool execution_phase;

  bool blocked = false;

  std::function<bool(std::size_t)> process_requests;

  // table id, partition id, key, value
  std::function<void(std::size_t, std::size_t, const void *, void *)> local_index_read_handler;

  // table id, partition id, id, key_offset, key, value
  std::function<void(std::size_t, std::size_t, std::size_t, std::size_t, uint32_t, const void *, void *, bool)>
      read_handler;

  // processed a request?
  std::function<std::size_t(std::size_t)> remote_request_handler;

  std::function<void(std::size_t)> message_flusher;

  Partitioner            &partitioner;
  std::vector<bool>       active_coordinators;
  Operation               operation;  // never used
  std::vector<AdeptRWKey> readSet, writeSet;

  std::array<std::atomic<int32_t>, 10> read_bitmap;

  std::array<bool, 10> blocked_bitmap;

  std::function<Task<bool>(std::size_t)> process_requests_coro;
};
}  // namespace aria