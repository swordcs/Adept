//
// Created by Yi Lu on 9/14/18.
//

#pragma once

#include "core/Table.h"
#include "protocol/Adept/AdeptHelper.h"
#include "protocol/Adept/AdeptMessage.h"
#include "protocol/Adept/AdeptPartitioner.h"
#include "protocol/Adept/AdeptTransaction.h"

#include <vector>

namespace aria {

template <class Database>
class Adept
{
public:
  using DatabaseType    = Database;
  using MetaDataType    = std::atomic<uint64_t>;
  using ContextType     = typename DatabaseType::ContextType;
  using MessageType     = AdeptMessage;
  using TransactionType = AdeptTransaction;

  using MessageFactoryType = AdeptMessageFactory;
  using MessageHandlerType = AdeptMessageHandler;

  using BlockedTxnEntryType  = AdeptBlockedTxnEntry;
  using BlockedTxnsQueueType = std::deque<BlockedTxnEntryType>;
  using BlockedTxnsType      = HashMap<1000, uint64_t, BlockedTxnsQueueType>;

  Adept(DatabaseType &db, AdeptPartitioner &partitioner, BlockedTxnsType *&blocked_txns,
      std::atomic<uint64_t> &global_blocked_counter)
      : db(db),
        partitioner(partitioner),
        blocked_txns(blocked_txns),
        global_blocked_counter(global_blocked_counter)
  {}

  void abort(TransactionType &txn)
  {
    abort_local_write_versions(txn);
    finish_mirror_cache_versions(txn, false);
    release_read_locks(txn);
    release_write_locks(txn);
    txn.abort_registration_complete.store(true, std::memory_order_release);
    notify_abort_dependents_if_ready(txn);
  }

  bool commit(TransactionType &txn)
  {

    finish_local_write_versions(txn);
    finish_mirror_cache_versions(txn, true);
    notify_mirror_dependents(txn);

    release_read_locks(txn);
    release_write_locks(txn);

    return true;
  }

  void finish_mirror_cache_versions(TransactionType &txn, bool commit)
  {
    for (auto &read_key : txn.readSet) {
      if (!read_key.get_mirror_cache_fill_bit()) {
        continue;
      }

      auto *table = db.find_table(read_key.get_table_id(), read_key.get_partition_id());
      if (commit) {
        const void *value = find_write_value(txn, read_key, *table);
        if (value == nullptr) {
          CHECK(!read_key.get_blind_bit());
          value = read_key.get_value();
        }
        table->update_version_last(read_key.get_key(), value, txn.id);
        release_write_version(read_key, txn.id);
      } else if (abort_version(txn, read_key, *table)) {
        release_write_version(read_key, txn.id);
      }
    }
  }

  void finish_local_write_versions(TransactionType &txn)
  {
    for (auto &read_key : txn.readSet) {
      if (!read_key.get_scheduled_lock_bit() || !read_key.get_write_lock_bit() ||
          !partitioner.has_master_partition(read_key.get_partition_id())) {
        continue;
      }

      auto       *table = db.find_table(read_key.get_table_id(), read_key.get_partition_id());
      const void *value = find_write_value(txn, read_key, *table);
      if (value == nullptr) {
        CHECK(!read_key.get_blind_bit());
        std::vector<char> previous_value(table->value_size());
        AdeptHelper::read(table->search_prev(read_key.get_key(), txn.id), previous_value.data(), table->value_size());
        table->update_version_last(read_key.get_key(), previous_value.data(), txn.id);
      } else {
        table->update_version_last(read_key.get_key(), value, txn.id);
      }
    }
  }

  void abort_local_write_versions(TransactionType &txn)
  {
    for (auto &read_key : txn.readSet) {
      if (!read_key.get_scheduled_lock_bit() || !read_key.get_write_lock_bit() ||
          !partitioner.has_master_partition(read_key.get_partition_id())) {
        continue;
      }

      auto *table = db.find_table(read_key.get_table_id(), read_key.get_partition_id());
      abort_version(txn, read_key, *table);
    }
  }

  bool abort_version(TransactionType &txn, AdeptRWKey &write_key, ITable &table)
  {
    auto &predecessor = table.search_metadata_prev(write_key.get_key(), txn.id);
    if (AdeptHelper::try_read_lock_reserve(predecessor)) {
      propagate_aborted_version(txn, write_key, table);
      AdeptHelper::read_lock_release(predecessor);
      return true;
    }
    enqueue_abort_propagation(txn, write_key, predecessor);
    return false;
  }

  void propagate_aborted_version(TransactionType &txn, AdeptRWKey &write_key, ITable &table)
  {
    std::vector<char> previous_value(table.value_size());
    AdeptHelper::read(table.search_prev(write_key.get_key(), txn.id), previous_value.data(), table.value_size());
    table.update_version_last(write_key.get_key(), previous_value.data(), txn.id);
  }

  void enqueue_abort_propagation(TransactionType &txn, AdeptRWKey &write_key, MetaDataType &predecessor)
  {
    CHECK(AdeptHelper::is_reserve_locked(predecessor.load()));
    uint64_t waiter = AdeptHelper::get_waiter(predecessor);
    if (waiter == 0) {
      waiter = global_blocked_counter.fetch_add(1);
      CHECK(waiter > 0 && waiter <= AdeptHelper::TID_MASK);
      AdeptHelper::set_waiter(predecessor, waiter);
    } else {
      CHECK(blocked_txns->contains(waiter));
    }
    (*blocked_txns)[waiter].push_back(
        BlockedTxnEntryType{AdeptBlockedWaitType::ABORT_PROPAGATION, &txn, &write_key});
    write_key.set_deferred_abort_bit();
    txn.deferred_abort_counter.fetch_add(1, std::memory_order_release);
    AdeptHelper::reserve_lock_release(predecessor);
  }

  void notify_mirror_dependents(TransactionType &txn)
  {
    for (auto *dependent : txn.mirror_dependents) {
      notify_granted(dependent);
    }
  }

  void notify_abort_dependents_if_ready(TransactionType &txn)
  {
    if (txn.abort_registration_complete.load(std::memory_order_acquire) &&
        txn.deferred_abort_counter.load(std::memory_order_acquire) == 0 &&
        !txn.abort_dependents_notified.exchange(true, std::memory_order_acq_rel)) {
      notify_mirror_dependents(txn);
    }
  }

  void notify_granted(TransactionType *txn)
  {
    auto previous = txn->blocked_counter.fetch_sub(1, std::memory_order_acq_rel);
    CHECK(previous > 0);
    if (previous == 1 && txn->ready_for_execution.load(std::memory_order_acquire) &&
        !txn->execution_enqueued.exchange(true)) {
      transaction_queue->push(txn);
    }
  }

  void wakeup_transfer_lock(MetaDataType &tid, bool from_write = false)
  {
    uint64_t waiter = AdeptHelper::get_waiter(tid);
    CHECK(waiter > 0);
    CHECK(blocked_txns->contains(waiter));
    auto &queue = (*blocked_txns)[waiter];

    CHECK(!queue.empty());
    bool first                 = true;
    bool granted_normal_reader = false;
    while (!queue.empty()) {
      auto entry = queue.front();
      if (entry.wait_type == AdeptBlockedWaitType::WRITE && granted_normal_reader) {
        break;
      }
      queue.pop_front();
      if (queue.empty()) {
        AdeptHelper::set_waiter(tid, 0);
      }

      if (entry.wait_type == AdeptBlockedWaitType::WRITE) {
        CHECK(queue.empty());
        if (first) {
          from_write ? AdeptHelper::write_lock_release(tid) : AdeptHelper::read_lock_release(tid);
        }
        notify_granted(entry.transaction);
        return;
      }

      if (first) {
        if (from_write) {
          AdeptHelper::downgrade_write_to_read_lock(tid);
        }
      } else {
        AdeptHelper::read_lock(tid);
      }

      if (entry.wait_type == AdeptBlockedWaitType::ABORT_PROPAGATION) {
        complete_abort_propagation(*entry.transaction, *entry.write_key);
        AdeptHelper::read_lock_release(tid);
      } else {
        granted_normal_reader = true;
        notify_granted(entry.transaction);
      }
      first = false;
    }
  }

  void complete_abort_propagation(TransactionType &txn, AdeptRWKey &write_key)
  {
    auto *table = db.find_table(write_key.get_table_id(), write_key.get_partition_id());
    propagate_aborted_version(txn, write_key, *table);
    release_write_version(write_key, txn.id);
    auto previous = txn.deferred_abort_counter.fetch_sub(1, std::memory_order_acq_rel);
    CHECK(previous > 0);
    if (previous == 1) {
      notify_abort_dependents_if_ready(txn);
    }
  }

  void release_write_version(AdeptRWKey &write_key, uint64_t transaction_id)
  {
    auto *table = db.find_table(write_key.get_table_id(), write_key.get_partition_id());
    auto &tid   = table->search_metadata(write_key.get_key(), transaction_id);
    AdeptHelper::reserve_lock(tid);
    if (AdeptHelper::get_waiter(tid) > 0) {
      wakeup_transfer_lock(tid, true);
    } else {
      AdeptHelper::write_lock_release(tid);
    }
    AdeptHelper::reserve_lock_release(tid);
  }

  void release_read_locks(TransactionType &txn)
  {
    // release read locks
    auto &readSet = txn.readSet;

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readKey     = readSet[i];
      auto  tableId     = readKey.get_table_id();
      auto  partitionId = readKey.get_partition_id();
      auto  table       = db.find_table(tableId, partitionId);

      if (!partitioner.has_master_partition(partitionId)) {
        continue;
      }

      if (!readKey.get_scheduled_lock_bit() || !readKey.get_read_lock_bit()) {
        continue;
      }

      auto                   key   = readKey.get_key();
      std::atomic<uint64_t> &tid   = table->search_metadata_version_last(key, txn.id);

      AdeptHelper::reserve_lock(tid);
      // I am the last reader
      if (AdeptHelper::get_waiter(tid) > 0 && AdeptHelper::read_lock_num(tid) == 1) {
        wakeup_transfer_lock(tid, false);
        AdeptHelper::reserve_lock_release(tid);
        continue;
      }
      AdeptHelper::read_lock_release(tid);
      AdeptHelper::reserve_lock_release(tid);
    }
  }

  void release_write_locks(TransactionType &txn)
  {

    auto &readSet = txn.readSet;

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &writeKey    = readSet[i];
      auto  partitionId = writeKey.get_partition_id();

      if (!partitioner.has_master_partition(partitionId) || !writeKey.get_scheduled_lock_bit() ||
          !writeKey.get_write_lock_bit() || writeKey.get_deferred_abort_bit()) {
        continue;
      }

      release_write_version(writeKey, txn.id);
    }
  }

  void set_executor_txn_queue(LockfreeQueue<TransactionType *> *transaction_queue)
  {
    this->transaction_queue = transaction_queue;
  }

private:
  const void *find_write_value(TransactionType &txn, const AdeptRWKey &read_key, ITable &table) const
  {
    for (auto it = txn.writeSet.rbegin(); it != txn.writeSet.rend(); ++it) {
      if (it->get_table_id() == read_key.get_table_id() &&
          it->get_partition_id() == read_key.get_partition_id() &&
          table.key_equal(it->get_key(), read_key.get_key())) {
        return it->get_value();
      }
    }
    return nullptr;
  }

  DatabaseType                     &db;
  AdeptPartitioner                  &partitioner;
  BlockedTxnsType                 *&blocked_txns;
  std::atomic<uint64_t>            &global_blocked_counter;
  LockfreeQueue<TransactionType *> *transaction_queue;
};
}  // namespace aria
