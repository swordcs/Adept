//
// Created by Yi Lu on 9/14/18.
//

#pragma once

#include "common/Operation.h"
#include "core/Coroutine.h"
#include "core/Defs.h"
#include "protocol/AsyncCalvin/AsyncCalvinHelper.h"
#include "protocol/AsyncCalvin/AsyncCalvinPartitioner.h"
#include "protocol/AsyncCalvin/AsyncCalvinRWKey.h"
#include <chrono>
#include <deque>
#include <glog/logging.h>
#include <mutex>
#include <thread>

namespace aria {
class AsyncCalvinTransaction;

// READ_REQUEST messages can be delivered by different worker threads, while
// the suspended coroutine must be resumed by its owning executor. This queue
// is therefore multi-producer and single-consumer.
class AsyncCalvinRemoteWaitQueue
{
public:
  void push(AsyncCalvinTransaction *transaction)
  {
    std::lock_guard<std::mutex> guard(mutex);
    queue.push_back(transaction);
  }

  bool try_pop(AsyncCalvinTransaction *&transaction)
  {
    std::lock_guard<std::mutex> guard(mutex);
    if (queue.empty()) {
      return false;
    }
    transaction = queue.front();
    queue.pop_front();
    return true;
  }

  bool empty() const
  {
    std::lock_guard<std::mutex> guard(mutex);
    return queue.empty();
  }

private:
  mutable std::mutex                   mutex;
  std::deque<AsyncCalvinTransaction *> queue;
};

class AsyncCalvinTransaction
{

public:
  using MetaDataType = std::atomic<uint64_t>;

  AsyncCalvinTransaction(std::size_t coordinator_id, std::size_t partition_id, Partitioner &partitioner)
      : coordinator_id(coordinator_id),
        partition_id(partition_id),
        startTime(std::chrono::steady_clock::now()),
        partitioner(partitioner)
  {
    reset();
  }

  virtual ~AsyncCalvinTransaction() = default;

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
    lock_counter.store(0);
    blocked_counter.store(0);
    ready_for_execution.store(false);
    execution_enqueued.store(false);
    {
      std::lock_guard<std::mutex> guard(remote_wait_mutex);
      remote_wait_armed.store(false);
      remote_resume_enqueued.store(false);
      remote_resume_queue.store(nullptr);
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

    AsyncCalvinRWKey readKey;

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

    AsyncCalvinRWKey readKey;

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

    AsyncCalvinRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);
    // Async Calvin remains a single-version FIFO locking baseline. Blind
    // writes therefore take the same exclusive lock as read-modify-writes.
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

    AsyncCalvinRWKey writeKey;

    writeKey.set_table_id(table_id);
    writeKey.set_partition_id(partition_id);

    writeKey.set_key(&key);
    // the object pointed by value will not be updated
    writeKey.set_value(const_cast<ValueType *>(&value));
    writeKey.set_write_lock_bit();

    add_to_write_set(writeKey);
  }

  std::size_t add_to_read_set(const AsyncCalvinRWKey &key)
  {
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const AsyncCalvinRWKey &key)
  {
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

  void set_id(uint32_t epoch, std::size_t tid_offset)
  {
    this->epoch      = epoch;
    this->tid_offset = tid_offset;
    this->id         = AsyncCalvinHelper::get_tid(epoch, tid_offset);
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

  void setup_process_requests_in_execution_phase(std::size_t, std::size_t, std::size_t)
  {
    process_requests_coro = [this](std::size_t worker_id) -> Task<bool> {
      for (int i = int(readSet.size()) - 1; i >= 0; i--) {
        if (readSet[i].get_local_index_read_bit()) {
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
            readKey.get_value());

        readSet[i].set_execution_processed_bit();
      }

      message_flusher(worker_id);

      if (active_coordinators[coordinator_id]) {
        // Deneva returns WAIT here and resumes the transaction only when the
        // final remote-read message arrives. The executor owns the suspended
        // task; this coroutine therefore does not poll the network queue.
        while (local_read.load(std::memory_order_acquire) > 0 || remote_read.load(std::memory_order_acquire) > 0) {
          co_await std::suspend_always{};
        }

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

  void arm_remote_read_wait(AsyncCalvinRemoteWaitQueue *queue)
  {
    AsyncCalvinRemoteWaitQueue *ready_queue = nullptr;
    {
      std::lock_guard<std::mutex> guard(remote_wait_mutex);
      remote_resume_queue.store(queue, std::memory_order_release);
      remote_wait_armed.store(true, std::memory_order_release);
      ready_queue = mark_remote_resume_if_ready();
    }
    if (ready_queue != nullptr) {
      ready_queue->push(this);
    }
  }

  void disarm_remote_read_wait()
  {
    std::lock_guard<std::mutex> guard(remote_wait_mutex);
    remote_wait_armed.store(false, std::memory_order_release);
    remote_resume_enqueued.store(false, std::memory_order_release);
  }

  void complete_remote_read()
  {
    auto previous = remote_read.fetch_sub(1, std::memory_order_acq_rel);
    CHECK(previous > 0);
    if (previous == 1) {
      AsyncCalvinRemoteWaitQueue *ready_queue = nullptr;
      {
        std::lock_guard<std::mutex> guard(remote_wait_mutex);
        ready_queue = mark_remote_resume_if_ready();
      }
      if (ready_queue != nullptr) {
        ready_queue->push(this);
      }
    }
  }

private:
  AsyncCalvinRemoteWaitQueue *mark_remote_resume_if_ready()
  {
    if (remote_read.load(std::memory_order_acquire) > 0 || !remote_wait_armed.load(std::memory_order_acquire)) {
      return nullptr;
    }

    auto *queue = remote_resume_queue.load(std::memory_order_acquire);
    if (queue == nullptr || remote_resume_enqueued.load(std::memory_order_acquire)) {
      return nullptr;
    }
    remote_resume_enqueued.store(true, std::memory_order_release);
    return queue;
  }

public:
  uint32_t    epoch;
  uint64_t    id;  // [...(10 bit), ...(2 bit), epoch(32 bit), tid_offset(20 bit)]
  std::size_t coordinator_id, partition_id, tid_offset;

  std::atomic<int32_t> lock_counter{0};
  std::atomic<int32_t> blocked_counter{0};

  std::atomic<bool>                         ready_for_execution{false};
  std::atomic<bool>                         execution_enqueued{false};
  std::atomic<bool>                         remote_wait_armed{false};
  std::atomic<bool>                         remote_resume_enqueued{false};
  std::atomic<AsyncCalvinRemoteWaitQueue *> remote_resume_queue{nullptr};
  std::mutex                                remote_wait_mutex;

  std::chrono::steady_clock::time_point startTime;
  std::atomic<int32_t>                  network_size;
  std::atomic<int32_t>                  local_read, remote_read;
  int32_t                               saved_local_read, saved_remote_read;

  bool abort_no_retry;
  bool distributed_transaction;
  bool execution_phase;

  std::function<bool(std::size_t)> process_requests;

  // table id, partition id, key, value
  std::function<void(std::size_t, std::size_t, const void *, void *)> local_index_read_handler;

  // table id, partition id, id, key_offset, key, value
  std::function<void(std::size_t, std::size_t, std::size_t, std::size_t, uint32_t, const void *, void *)> read_handler;

  std::function<void(std::size_t)> message_flusher;

  Partitioner           &partitioner;
  std::vector<bool>      active_coordinators;
  Operation              operation;  // never used
  std::vector<AsyncCalvinRWKey> readSet, writeSet;

  std::function<Task<bool>(std::size_t)> process_requests_coro;
};
}  // namespace aria
