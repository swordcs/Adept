//
// Created by Yi Lu on 2019-09-05.
//

#pragma once

#include "common/Operation.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/Caracal/CaracalHelper.h"
#include "protocol/Caracal/CaracalRWKey.h"
#include <atomic>
#include <chrono>
#include <glog/logging.h>
#include <thread>

namespace aria {

class CaracalTransaction
{

public:
  using MetaDataType = std::atomic<uint64_t>;

  CaracalTransaction(std::size_t coordinator_id, std::size_t partition_id, Partitioner &partitioner)
      : coordinator_id(coordinator_id),
        partition_id(partition_id),
        startTime(std::chrono::steady_clock::now()),
        partitioner(partitioner)
  {
    reset();
  }

  virtual ~CaracalTransaction() = default;

  void reset()
  {
    local_read.store(0);
    saved_local_read = 0;
    remote_read.store(0);
    saved_remote_read = 0;

    abort_read_not_ready = false;
    abort_no_retry       = false;

    distributed_transaction = false;
    execution_phase         = false;
    pendingResponses        = 0;
    pendingPieces.store(0);
    network_size           = 0;
    execution_write_cursor = 0;
    blocked_on             = nullptr;

    operation.clear();
    readSet.clear();
    writeSet.clear();
  }

  virtual TransactionResult execute(std::size_t worker_id) = 0;

  virtual void reset_query() = 0;

  template <class KeyType, class ValueType>
  void search_local_index(std::size_t table_id, std::size_t partition_id, const KeyType &key, ValueType &value)
  {
    if (execution_phase) {
      return;
    }

    CaracalRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_local_index_read_bit();
    readKey.set_read_request_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_read(std::size_t table_id, std::size_t partition_id, const KeyType &key, ValueType &value)
  {
    if (execution_phase) {
      return;
    }
    CaracalRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_read_request_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_update(
      std::size_t table_id, std::size_t partition_id, const KeyType &key, ValueType &value, bool blind = false)
  {
    if (execution_phase) {
      return;
    }

    CaracalRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_read_request_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void update(
      std::size_t table_id, std::size_t partition_id, const KeyType &key, const ValueType &value, bool blind = false)
  {
    if (execution_phase) {
      CHECK(execution_write_cursor < writeSet.size());
      auto &writeKey = writeSet[execution_write_cursor++];
      DCHECK(writeKey.get_table_id() == table_id);
      DCHECK(writeKey.get_partition_id() == partition_id);
      writeKey.set_key(&key);
      writeKey.set_value(const_cast<ValueType *>(&value));
      return;
    }

    CaracalRWKey writeKey;

    writeKey.set_table_id(table_id);
    writeKey.set_partition_id(partition_id);

    writeKey.set_key(&key);
    // the object pointed by value will not be updated
    writeKey.set_value(const_cast<ValueType *>(&value));

    add_to_write_set(writeKey);
  }

  std::size_t add_to_read_set(const CaracalRWKey &key)
  {
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const CaracalRWKey &key)
  {
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

  void set_id(uint32_t epoch, std::size_t tid_offset)
  {
    this->epoch      = epoch;
    this->tid_offset = tid_offset;
    this->id         = CaracalHelper::get_tid(epoch, tid_offset);
  }

  void begin_execution_attempt()
  {
    execution_write_cursor = 0;
    abort_read_not_ready    = false;
  }

  bool execution_writes_complete() const { return execution_write_cursor == writeSet.size(); }

  bool waiting_for_remote_reads() const { return pendingResponses > 0; }

  void suspend_on(std::atomic<uint64_t> *placeholder) { blocked_on = placeholder; }

  bool locally_blocked() const { return blocked_on != nullptr && !CaracalHelper::is_placeholder_ready(*blocked_on); }

  void clear_local_blocker(std::atomic<uint64_t> *placeholder = nullptr)
  {
    if (placeholder == nullptr || blocked_on == placeholder)
      blocked_on = nullptr;
  }

  void reset_for_epoch()
  {
    abort_read_not_ready    = false;
    distributed_transaction = false;
    pendingResponses        = 0;
    pendingPieces.store(0);
    network_size           = 0;
    execution_write_cursor = 0;
    blocked_on             = nullptr;
    startTime              = std::chrono::steady_clock::now();
    load_read_count();

    for (auto &readKey : readSet) {
      readKey.clear_cached_row();
      if (!readKey.get_local_index_read_bit())
        readKey.set_read_request_bit();
      readKey.clear_execution_processed_bit();
    }
    for (auto &writeKey : writeSet)
      writeKey.clear_cached_row();
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
  void setup_process_requests_in_execution_phase()
  {
    process_requests = [this](std::size_t worker_id) {
      // A previous execution attempt already issued this transaction's
      // remote reads. Yield to the batch driver instead of sending duplicates
      // or occupying the worker while the responses are in flight.
      if (waiting_for_remote_reads())
        return true;

      // cannot use unsigned type in reverse iteration
      for (int i = int(readSet.size()) - 1; i >= 0; i--) {
        if (!readSet[i].get_read_request_bit()) {
          continue;
        }
        CaracalRWKey &readKey = readSet[i];
        read_handler(readKey, id, i);
      }
      if (waiting_for_remote_reads()) {
        message_flusher();
        return true;
      }
      return abort_read_not_ready;
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

  bool is_read_only() { return writeSet.size() == 0; }

public:
  std::size_t                           coordinator_id, partition_id, id, tid_offset;
  uint32_t                              epoch;
  std::chrono::steady_clock::time_point startTime;
  std::size_t                           pendingResponses;
  std::atomic<uint32_t>                 pendingPieces;
  std::size_t                           network_size;
  std::size_t                           execution_write_cursor;
  std::atomic<uint64_t>                *blocked_on;
  std::atomic<int32_t>                  local_read, remote_read;
  int32_t                               saved_local_read, saved_remote_read;

  bool abort_read_not_ready, abort_no_retry;
  bool distributed_transaction;
  bool execution_phase;

  std::function<bool(std::size_t)> process_requests;

  // table id, partition id, key, value
  std::function<void(std::size_t, std::size_t, const void *, void *)> local_index_read_handler;

  // read_key, id, key_offset
  std::function<void(CaracalRWKey &, std::size_t, std::size_t)> read_handler;

  std::function<void()> message_flusher;

  Partitioner              &partitioner;
  Operation                 operation;  // never used
  std::vector<CaracalRWKey> readSet, writeSet;
};  // namespace aria
}  // namespace aria
