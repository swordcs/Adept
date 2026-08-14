//
// Created by Yi Lu on 9/14/18.
//

#pragma once

#include "core/Table.h"
#include "protocol/AsyncCalvin/AsyncCalvinHelper.h"
#include "protocol/AsyncCalvin/AsyncCalvinMessage.h"
#include "protocol/AsyncCalvin/AsyncCalvinPartitioner.h"
#include "protocol/AsyncCalvin/AsyncCalvinTransaction.h"
#include <unordered_map>

namespace aria {

template <class Database>
class AsyncCalvin
{
public:
  using DatabaseType    = Database;
  using MetaDataType    = std::atomic<uint64_t>;
  using ContextType     = typename DatabaseType::ContextType;
  using MessageType     = AsyncCalvinMessage;
  using TransactionType = AsyncCalvinTransaction;

  using MessageFactoryType = AsyncCalvinMessageFactory;
  using MessageHandlerType = AsyncCalvinMessageHandler;

  using BlockedTxnEntryType  = std::tuple<bool, TransactionType *>;
  using BlockedTxnsQueueType = std::deque<BlockedTxnEntryType>;
  using BlockedTxnsType = HashMap<1000, uint64_t, std::shared_ptr<BlockedTxnsQueueType>>;

  AsyncCalvin(DatabaseType &db, AsyncCalvinPartitioner &partitioner, BlockedTxnsType *&blocked_txns)
      : db(db), partitioner(partitioner), blocked_txns(blocked_txns)
  {}

  void abort(
      TransactionType &txn, std::size_t lock_manager_id, std::size_t n_lock_manager, std::size_t replica_group_size)
  {
    release_locks(txn);
  }

  bool commit(
      TransactionType &txn, std::size_t lock_manager_id, std::size_t n_lock_manager, std::size_t replica_group_size)
  {

    // write to db
    write(txn, lock_manager_id, n_lock_manager, replica_group_size);

    release_locks(txn);

    return true;
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

      auto key   = writeKey.get_key();
      auto value = writeKey.get_value();
      table->update(key, value);
    }
  }

  void notify_granted(TransactionType *txn)
  {
    auto previous = txn->blocked_counter.fetch_sub(1);
    CHECK(previous > 0);
    if (previous == 1 && txn->ready_for_execution.load(std::memory_order_acquire) &&
        !txn->execution_enqueued.exchange(true)) {
      transaction_queue->push(txn);
    }
  }

  void transfer_or_unlock(AsyncCalvinRWKey &rwKey, MetaDataType &tid)
  {
    const bool from_write = rwKey.get_write_lock_bit();
    AsyncCalvinHelper::reserve_lock(tid);

    auto waiter = AsyncCalvinHelper::get_waiter(tid.load());
    if (waiter == 0) {
      if (from_write)
        AsyncCalvinHelper::write_lock_release(tid);
      else
        AsyncCalvinHelper::read_lock_release(tid);
      AsyncCalvinHelper::reserve_lock_release(tid);
      return;
    }

    // A writer cannot be granted until the final current reader leaves.
    if (!from_write && AsyncCalvinHelper::read_lock_num(tid.load()) > 1) {
      AsyncCalvinHelper::read_lock_release(tid);
      AsyncCalvinHelper::reserve_lock_release(tid);
      return;
    }

    CHECK(blocked_txns->contains(waiter));
    auto queue_ptr = (*blocked_txns)[waiter];
    CHECK(queue_ptr && !queue_ptr->empty());
    auto &queue = *queue_ptr;
    std::vector<TransactionType *> granted;

    if (std::get<0>(queue.front())) {
      granted.push_back(std::get<1>(queue.front()));
      queue.pop_front();
      if (!from_write) {
        AsyncCalvinHelper::upgrade_read_to_write_lock(tid);
      }
    } else {
      while (!queue.empty() && !std::get<0>(queue.front()) &&
             granted.size() < AsyncCalvinHelper::read_lock_max()) {
        granted.push_back(std::get<1>(queue.front()));
        queue.pop_front();
      }
      if (from_write)
        AsyncCalvinHelper::transfer_write_to_read_locks(tid, granted.size());
      else
        AsyncCalvinHelper::transfer_last_read_to_read_locks(tid, granted.size());
    }

    if (queue.empty()) {
      AsyncCalvinHelper::set_waiter(tid, 0);
    }
    AsyncCalvinHelper::reserve_lock_release(tid);

    for (auto *txn : granted) {
      notify_granted(txn);
    }
  }

  void release_locks(TransactionType &txn)
  {
    auto &readSet = txn.readSet;
    std::vector<std::size_t> lock_keys;
    std::unordered_map<MetaDataType *, std::size_t> lock_positions;

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readKey     = readSet[i];
      auto  tableId     = readKey.get_table_id();
      auto  partitionId = readKey.get_partition_id();
      auto  table       = db.find_table(tableId, partitionId);

      if (!partitioner.has_master_partition(partitionId) || readKey.get_local_index_read_bit()) {
        continue;
      }
      auto &tid = table->search_metadata(readKey.get_key());
      auto [it, inserted] = lock_positions.emplace(&tid, lock_keys.size());
      if (inserted) {
        lock_keys.push_back(i);
      } else if (readKey.get_write_lock_bit()) {
        lock_keys[it->second] = i;
      }
    }

    for (auto i : lock_keys) {
      auto &readKey = readSet[i];
      auto *table = db.find_table(readKey.get_table_id(), readKey.get_partition_id());
      auto &tid = table->search_metadata(readKey.get_key());
      transfer_or_unlock(readKey, tid);
    }
  }

  void set_executor_txn_queue(LockfreeQueue<TransactionType *> *transaction_queue)
  {
    this->transaction_queue = transaction_queue;
  }

private:
  DatabaseType                     &db;
  AsyncCalvinPartitioner                  &partitioner;
  BlockedTxnsType                 *&blocked_txns;
  LockfreeQueue<TransactionType *> *transaction_queue;
};
}  // namespace aria
