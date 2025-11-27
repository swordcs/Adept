

#pragma once

#include "core/Table.h"
#include "protocol/Adept/AdeptHelper.h"
#include "protocol/Adept/AdeptMessage.h"
#include "protocol/Adept/AdeptPartitioner.h"
#include "protocol/Adept/AdeptTransaction.h"

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

  using BlockedTxnEntryType  = std::tuple<bool, TransactionType *>;
  using BlockedTxnsQueueType = std::deque<BlockedTxnEntryType>;
  using BlockedTxnsType      = HashMap<1000, uint64_t, BlockedTxnsQueueType>;

  Adept(DatabaseType &db, AdeptPartitioner &partitioner, BlockedTxnsType *&blocked_txns)
      : db(db), partitioner(partitioner), blocked_txns(blocked_txns)
  {}

  void abort(
      TransactionType &txn, std::size_t lock_manager_id, std::size_t n_lock_manager, std::size_t replica_group_size)
  {
    // release read locks
    release_read_locks(txn, lock_manager_id, n_lock_manager, replica_group_size);
  }

  bool commit(
      TransactionType &txn, std::size_t lock_manager_id, std::size_t n_lock_manager, std::size_t replica_group_size)
  {

    // write to db
    write(txn, lock_manager_id, n_lock_manager, replica_group_size);

    // release read/write locks
    release_read_locks(txn, lock_manager_id, n_lock_manager, replica_group_size);
    release_write_locks(txn, lock_manager_id, n_lock_manager, replica_group_size);

    return true;
  }

  void wakeup_transfer_lock(aria::AdeptRWKey &readKey, MetaDataType &tid, bool from_write = false)
  {
    uint64_t waiter = AdeptHelper::get_waiter(tid);
    DCHECK(waiter > 0);
    DCHECK(blocked_txns->contains(waiter));
    auto &queue = (*blocked_txns)[waiter];

    // no need read lock, since write ration is high
    DCHECK(!queue.empty());
    BlockedTxnEntryType &front_txn = queue.front();
    queue.pop_front();

    if (queue.empty()) {
      AdeptHelper::set_waiter(tid, 0);
    }

    bool             is_write_wait = std::get<0>(front_txn);
    TransactionType *txn_ptr       = std::get<1>(front_txn);

    while (txn_ptr->blocked_counter.load() < 0)
      continue;  // wait till all lock res detemined

    DCHECK(txn_ptr->blocked_counter.load() > 0);

    if (from_write && !is_write_wait) {
      AdeptHelper::downgrade_write_to_read_lock(tid);
    } else if (is_write_wait) {
      DCHECK(queue.empty());
      from_write ? AdeptHelper::write_lock_release(tid) : AdeptHelper::read_lock_release(tid);
    }

    int32_t current_blocked = txn_ptr->blocked_counter.fetch_sub(1);

    if (current_blocked == 1) {
      transaction_queue->push(txn_ptr);
    }

    while (!is_write_wait && !queue.empty()) {

      BlockedTxnEntryType next_txn = queue.front();

      if (!(is_write_wait = std::get<0>(next_txn))) {
        AdeptHelper::read_lock(tid);
        queue.pop_front();

        if (queue.empty()) {
          AdeptHelper::set_waiter(tid, 0);
        }

        while (std::get<1>(next_txn)->blocked_counter.load() < 0)
          continue;  // wait till all lock res detemined

        if (std::get<1>(next_txn)->blocked_counter.fetch_sub(1) == 1) {
          transaction_queue->push(std::get<1>(next_txn));
        }
      }
    };
  }

  void write(
      TransactionType &txn, std::size_t lock_manager_id, std::size_t n_lock_manager, std::size_t replica_group_size)
  {

    auto &writeSet = txn.writeSet;
    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey    = writeSet[i];
      auto  tableId     = writeKey.get_table_id();
      auto  partitionId = writeKey.get_partition_id();
      auto  table       = db.find_table(tableId, partitionId);

      if (!partitioner.has_master_partition(partitionId)) {
        continue;
      }

      if (AdeptHelper::partition_id_to_lock_manager_id(
              writeKey.get_partition_id(), n_lock_manager, replica_group_size) != lock_manager_id) {
        continue;
      }

      auto key   = writeKey.get_key();
      auto value = writeKey.get_value();
      table->update_version_last(key, value, txn.id);
    }
  }

  void release_read_locks(
      TransactionType &txn, std::size_t lock_manager_id, std::size_t n_lock_manager, std::size_t replica_group_size)
  {
    // release read locks
    auto &readSet = txn.readSet;

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readKey     = readSet[i];
      auto  tableId     = readKey.get_table_id();
      auto  partitionId = readKey.get_partition_id();
      auto  table       = db.find_table(tableId, partitionId);

      bool cached = readKey.get_write_cache_bit();

      if (!cached) {
        if (!partitioner.has_master_partition(partitionId)) {
          continue;
        }

        // write lock in cache also processed here
        if (!readKey.get_read_lock_bit()) {
          continue;
        }

        if (AdeptHelper::partition_id_to_lock_manager_id(
                readKey.get_partition_id(), n_lock_manager, replica_group_size) != lock_manager_id) {
          continue;
        }
      }

      auto                   key   = readKey.get_key();
      auto                   value = readKey.get_value();
      std::atomic<uint64_t> &tid   = table->search_metadata_version_last(key, txn.id);

      // write to cache locally
      if (readKey.get_write_cache_bit()) {
        CHECK(AdeptHelper::is_write_locked(tid.load()));
        table->update_version_last(key, value, txn.id);
        AdeptHelper::write_lock_release(tid);
        continue;
      }

      AdeptHelper::reserve_lock(tid);
      DCHECK(AdeptHelper::is_read_locked(tid.load()));
      // I am the last reader
      if (AdeptHelper::get_waiter(tid) > 0 && AdeptHelper::read_lock_num(tid) == 1) {
        wakeup_transfer_lock(readKey, tid, false);
        AdeptHelper::reserve_lock_release(tid);
        continue;
      }
      AdeptHelper::read_lock_release(tid);
      AdeptHelper::reserve_lock_release(tid);
    }
  }

  void release_write_locks(
      TransactionType &txn, std::size_t lock_manager_id, std::size_t n_lock_manager, std::size_t replica_group_size)
  {

    // release write lock
    auto &writeSet = txn.writeSet;

    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey    = writeSet[i];
      auto  tableId     = writeKey.get_table_id();
      auto  partitionId = writeKey.get_partition_id();
      auto  table       = db.find_table(tableId, partitionId);

      if (!partitioner.has_master_partition(partitionId)) {
        continue;
      }

      if (AdeptHelper::partition_id_to_lock_manager_id(
              writeKey.get_partition_id(), n_lock_manager, replica_group_size) != lock_manager_id) {
        continue;
      }

      DCHECK(writeKey.get_write_lock_bit());

      auto                   key   = writeKey.get_key();
      auto                   value = writeKey.get_value();
      std::atomic<uint64_t> &tid   = table->search_metadata_version_last(key, txn.id);

      AdeptHelper::reserve_lock(tid);
      if (AdeptHelper::get_waiter(tid) > 0) {
        wakeup_transfer_lock(writeKey, tid, true);
        AdeptHelper::reserve_lock_release(tid);
        continue;
      }
      AdeptHelper::write_lock_release(tid);
      AdeptHelper::reserve_lock_release(tid);
    }
  }

  void set_executor_txn_queue(LockfreeQueue<TransactionType *> *transaction_queue)
  {
    this->transaction_queue = transaction_queue;
  }

private:
  DatabaseType                     &db;
  AdeptPartitioner                 &partitioner;
  BlockedTxnsType                 *&blocked_txns;
  LockfreeQueue<TransactionType *> *transaction_queue;
};
}  // namespace aria