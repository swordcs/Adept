//
// Created by Yi Lu on 9/13/18.
//

#pragma once

#define GLOG_USE_GLOG_EXPORT
#include "common/Percentile.h"
#include "core/Delay.h"
#include "core/Worker.h"
#include "core/Coroutine.h"
#include "glog/logging.h"

#include "protocol/AsyncCalvin/AsyncCalvin.h"
#include "protocol/AsyncCalvin/AsyncCalvinHelper.h"
#include "protocol/AsyncCalvin/AsyncCalvinMessage.h"
#include "protocol/AsyncCalvin/AsyncCalvinPartitioner.h"

#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <thread>
#include <queue>

namespace aria {

template <class Workload>
class AsyncCalvinExecutor : public Worker
{
public:
  using WorkloadType       = Workload;
  using DatabaseType       = typename WorkloadType::DatabaseType;
  using StorageType        = typename WorkloadType::StorageType;
  using TransactionType    = AsyncCalvinTransaction;
  using ContextType        = typename DatabaseType::ContextType;
  using RandomType         = typename DatabaseType::RandomType;
  using ProtocolType       = AsyncCalvin<DatabaseType>;
  using MessageType        = AsyncCalvinMessage;
  using MessageFactoryType = AsyncCalvinMessageFactory;
  using MessageHandlerType = AsyncCalvinMessageHandler;
  using MetaDataType       = std::atomic<uint64_t>;
  // for blocker handler, atomic for rwlock and blocked counter in this queue
  using BlockedTxnEntryType  = std::tuple<bool, TransactionType *>;
  using BlockedTxnsQueueType = std::deque<BlockedTxnEntryType>;
  using BlockedTxnsType = HashMap<1000, uint64_t, std::shared_ptr<BlockedTxnsQueueType>>;

  AsyncCalvinExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db, const ContextType &context,
      std::vector<std::unique_ptr<TransactionType>> *&transactions_ptr, std::vector<StorageType> &storages,
      std::atomic<uint32_t> &lock_manager_status, std::atomic<uint32_t> &worker_status,
      std::atomic<uint32_t> &n_complete_workers, std::atomic<uint32_t> &n_started_workers,
      BlockedTxnsType *&blocked_txns, std::atomic<uint64_t> &globalBlockedCounter)
      : Worker(coordinator_id, id),
        db(db),
        context(context),
        transactions_ptr(transactions_ptr),
        storages(storages),
        lock_manager_status(lock_manager_status),
        worker_status(worker_status),
        n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        blocked_txns(blocked_txns),
        partitioner(coordinator_id, context.coordinator_num, AsyncCalvinHelper::string_to_vint(context.replica_group)),
        workload(coordinator_id, db, random, partitioner),
        n_lock_manager(AsyncCalvinHelper::n_lock_manager(
            partitioner.replica_group_id, id, AsyncCalvinHelper::string_to_vint(context.lock_manager))),
        n_workers(context.worker_num - n_lock_manager),
        lock_manager_id(AsyncCalvinHelper::worker_id_to_lock_manager_id(id, n_lock_manager, n_workers)),
        init_transaction(false),
        random(id),  // make sure each worker has a different seed.
        protocol(db, partitioner, blocked_txns),
        delay(std::make_unique<SameDelay>(coordinator_id, context.coordinator_num, context.delay_time)),
        globalBlockedCounter(globalBlockedCounter)
  {

    for (auto i = 0u; i < context.coordinator_num; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }
    messageHandlers = MessageHandlerType::get_message_handlers();
    protocol.set_executor_txn_queue(&wakeup_txn_queue);

    CHECK(n_workers > 0 && n_workers % n_lock_manager == 0);
  }

  ~AsyncCalvinExecutor() = default;

  void start() override
  {
    LOG(INFO) << "AsyncCalvinExecutor " << id << " started. ";

    for (;;) {

      ExecutorStatus status;
      do {
        status = static_cast<ExecutorStatus>(worker_status.load());

        if (status == ExecutorStatus::EXIT) {
          LOG(INFO) << "AsyncCalvinExecutor " << id << " exits. ";
          return;
        }
      } while (status != ExecutorStatus::Analysis);

      n_started_workers.fetch_add(1);
      analyze_transactions();
      n_complete_workers.fetch_add(1);

      // wait to Execute

      while (static_cast<ExecutorStatus>(worker_status.load()) == ExecutorStatus::Analysis) {
        std::this_thread::yield();
      }

      n_started_workers.fetch_add(1);
      // work as lock manager
      if (id < n_lock_manager) {
        // schedule transactions
        schedule_transactions();
      } else {
        // work as executor
        run_transactions();
      }

      n_complete_workers.fetch_add(1);

      // wait to Analysis

      while (static_cast<ExecutorStatus>(worker_status.load()) == ExecutorStatus::Execute) {
        process_request();
      }
    }
  }

  void onExit() override
  {
    LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50) << " us (50%) " << percentile.nth(75)
              << " us (75%) " << percentile.nth(95) << " us (95%) " << percentile.nth(99)
              << " us (99%); FIFO wait queues allocated: " << globalBlockedCounter.load() - 1 << ".";
    percentile.box_print();
  }

  void push_message(Message *message) override { in_queue.push(message); }

  Message *pop_message() override
  {
    if (out_queue.empty())
      return nullptr;

    Message *message = out_queue.front();

    if (delay->delay_enabled()) {
      auto now = std::chrono::steady_clock::now();
      if (std::chrono::duration_cast<std::chrono::microseconds>(now - message->time).count() < delay->message_delay()) {
        return nullptr;
      }
    }

    bool ok = out_queue.pop();
    CHECK(ok);

    return message;
  }

  void flush_messages()
  {

    for (auto i = 0u; i < messages.size(); i++) {
      if (i == coordinator_id)
        continue;

      if (messages[i]->get_message_count() == 0)
        continue;

      auto message = messages[i].release();

      out_queue.push(message);
      messages[i] = std::make_unique<Message>();
      init_message(messages[i].get(), i);
    }
  }

  void init_message(Message *message, std::size_t dest_node_id)
  {
    message->set_source_node_id(coordinator_id);
    message->set_dest_node_id(dest_node_id);
    message->set_worker_id(id);
  }

  void analyze_transactions()
  {
    auto phase_start = Clock::now();
    for (auto i = id; i < (*transactions_ptr).size(); i += context.worker_num) {
      (*transactions_ptr)[i]->startTime = std::chrono::steady_clock::now();
      // prepare transaction (analyze read/write set, lock read/write set)
      prepare_transaction(*(*transactions_ptr)[i]);
    }
    add_schedule_time(phase_start);
  }

  void prepare_transaction(TransactionType &txn)
  {

    setup_prepare_handlers(txn);
    // run execute to prepare read/write set
    auto result = txn.execute(id);
    if (result == TransactionResult::ABORT_NORETRY)
      txn.abort_no_retry = true;

    if (context.same_batch)
      txn.save_read_count();

    analyze_active_coordinator(txn);

    // setup handlers for execution
    setup_execute_handlers(txn);
    txn.execution_phase = true;
  }

  void analyze_active_coordinator(TransactionType &transaction)
  {

    // assuming no blind write
    auto &readSet             = transaction.readSet;
    auto &active_coordinators = transaction.active_coordinators;
    active_coordinators       = std::vector<bool>(partitioner.total_coordinators(), false);
    std::unordered_set<MetaDataType *> local_locks;

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readkey = readSet[i];
      if (readkey.get_local_index_read_bit())
        continue;
      auto partitionID = readkey.get_partition_id();

      if (partitioner.has_master_partition(partitionID)) {
        auto *table = db.find_table(readkey.get_table_id(), partitionID);
        local_locks.insert(&table->search_metadata(readkey.get_key()));
      }

      if (readkey.get_write_lock_bit()) {
        active_coordinators[partitioner.master_coordinator(partitionID)] = true;
      }
    }
    transaction.lock_counter.store(local_locks.size());
  }

  void schedule_transactions()
  {
    auto phase_start = Clock::now();

    // Scan the complete deterministic batch without waiting on a conflicting
    // tuple. Failed requests join a per-record FIFO queue and are restarted by
    // lock transfer on commit.
    std::size_t request_id = 0;

    for (auto i = 0u; i < (*transactions_ptr).size(); i++) {
      // do not grant locks to abort no retry transaction
      if (!(*transactions_ptr)[i]->abort_no_retry) {
        bool  grant_lock = false;
        auto &readSet    = (*transactions_ptr)[i]->readSet;

        auto &lock_counter        = (*transactions_ptr)[i]->lock_counter;
        auto &blocked_counter     = (*transactions_ptr)[i]->blocked_counter;
        auto &ready_for_execution = (*transactions_ptr)[i]->ready_for_execution;
        auto &execution_enqueued  = (*transactions_ptr)[i]->execution_enqueued;

        std::vector<std::size_t> lock_keys;
        std::unordered_map<MetaDataType *, std::size_t> lock_positions;
        for (auto k = 0u; k < readSet.size(); k++) {
          auto &readKey = readSet[k];
          auto partitionId = readKey.get_partition_id();
          if (readKey.get_local_index_read_bit() || !partitioner.has_master_partition(partitionId) ||
              AsyncCalvinHelper::partition_id_to_lock_manager_id(
                  partitionId, n_lock_manager, partitioner.replica_group_size) != lock_manager_id) {
            continue;
          }
          auto *table = db.find_table(readKey.get_table_id(), partitionId);
          auto *metadata = &table->search_metadata(readKey.get_key());
          auto [it, inserted] = lock_positions.emplace(metadata, lock_keys.size());
          if (inserted) {
            lock_keys.push_back(k);
          } else if (readKey.get_write_lock_bit()) {
            // Collapse duplicate accesses and retain the strongest lock. This
            // prevents a transaction from queueing behind its own lock.
            lock_keys[it->second] = k;
          }
        }

        int32_t processed_locks = lock_keys.size();

        std::vector<std::size_t> failed_locks;
        for (auto k : lock_keys) {
          // whether this lock is successfully acquired
          bool lock_succ = false;

          if (!do_lock_tuple(readSet[k], lock_succ, false))
            continue;

          grant_lock = true;
          if (!lock_succ) {
            failed_locks.push_back(k);
          }
        }

        if (!failed_locks.empty()) {
          bool lock_succ;
          for (auto k : failed_locks) {
            // reserve the tuple when lock fails
            do_lock_tuple(readSet[k], lock_succ, true);
            if (!lock_succ) {
              register_blocked_tuple(readSet[k], (*transactions_ptr)[i].get());
            }
          }
        }

        auto locks_left = lock_counter.fetch_add(-processed_locks) - processed_locks;

        if (locks_left <= 0) {
          ready_for_execution.store(true, std::memory_order_release);
          if (grant_lock && blocked_counter.load() <= 0 && !execution_enqueued.exchange(true)) {
            auto worker = get_available_worker(request_id++);
            all_executors[worker]->transaction_queue.push((*transactions_ptr)[i].get());
          }
        }

      } else {
        // Logical completion is recorded once by AsyncCalvinManager after
        // every coordinator has finished the batch.
      }
    }
    set_lock_manager_bit(id);
    add_schedule_time(phase_start);
  }

  void run_transactions()
  {
    while (!get_lock_manager_bit(lock_manager_id) || !all_transactions_done()) {
      // Like Deneva's worker loop, incoming messages drive transaction
      // continuation. Suspended tasks are never scanned or resumed by polling.
      process_request();

      TransactionType *transaction = nullptr;
      while (remote_resume_txn_queue.try_pop(transaction)) {
        resume_remote_waiter(transaction);
      }

      // Admit one transaction per pass so message continuations remain
      // interleaved with new work, as in Deneva's work-item loop.
      if (!transaction_queue.empty() || !wakeup_txn_queue.empty()) {
        auto            &txn_queue   = transaction_queue.empty() ? wakeup_txn_queue : transaction_queue;
        TransactionType *transaction = txn_queue.front();
        bool             ok          = txn_queue.pop();
        DCHECK(ok);
        auto execute_start = Clock::now();
        auto task          = transaction->execute_coro(id);
        add_execute_time(execute_start);
        if (!task.done()) {
          auto pending = pending_transactions.emplace(
              pending_transactions.end(), std::move(task), transaction, Clock::now());
          auto inserted = pending_transaction_index.emplace(transaction, pending).second;
          CHECK(inserted) << "transaction already has a suspended task";
          transaction->arm_remote_read_wait(&remote_resume_txn_queue);
        } else {
          commit_or_abort(transaction, task.get_value());
        }
      }
    }
  }

  bool all_transactions_done()
  {
    return transaction_queue.empty() && wakeup_txn_queue.empty() && remote_resume_txn_queue.empty() &&
           pending_transactions.empty() && pending_transaction_index.empty();
  }

  void register_blocked_tuple(aria::AsyncCalvinRWKey &rwKey, TransactionType *txn_ptr)
  {
    auto          tableId     = rwKey.get_table_id();
    auto          partitionId = rwKey.get_partition_id();
    auto          table       = db.find_table(tableId, partitionId);
    auto          key         = rwKey.get_key();
    MetaDataType &tid         = table->search_metadata(key);

    DCHECK(AsyncCalvinHelper::is_reserve_locked(tid.load()));
    DCHECK(AsyncCalvinHelper::is_read_locked(tid.load()) || AsyncCalvinHelper::is_write_locked(tid.load()));

    uint64_t waiter = AsyncCalvinHelper::get_waiter(tid);
    if (waiter != 0) {
      DCHECK(blocked_txns->contains(waiter));
    } else {
      waiter = globalBlockedCounter.fetch_add(1);
      AsyncCalvinHelper::set_waiter(tid, waiter);
    }
    auto &queue_ptr = (*blocked_txns)[waiter];
    if (!queue_ptr) {
      queue_ptr = std::make_shared<BlockedTxnsQueueType>();
    }
    // Publish the dependency before releasing the tuple reservation; an owner
    // may commit and transfer the lock immediately afterwards.
    txn_ptr->blocked_counter.fetch_add(1);
    queue_ptr->push_back(std::make_tuple(rwKey.get_write_lock_bit(), txn_ptr));

    AsyncCalvinHelper::reserve_lock_release(tid);
  }

  // returns whether this tuple should be handled by this lock manager
  bool do_lock_tuple(aria::AsyncCalvinRWKey &readKey, bool &lock_succ, bool reserve)
  {
    auto tableId     = readKey.get_table_id();
    auto partitionId = readKey.get_partition_id();

    // normal lock
    if (!reserve && !partitioner.has_master_partition(partitionId)) {
      return false;
    };

    auto table = db.find_table(tableId, partitionId);
    auto key   = readKey.get_key();

    if (!reserve) {  // normal lock
      if (readKey.get_local_index_read_bit()) {
        return false;
      };

      if (AsyncCalvinHelper::partition_id_to_lock_manager_id(
              readKey.get_partition_id(), n_lock_manager, partitioner.replica_group_size) != lock_manager_id) {
        return false;
      }
    }

    std::atomic<uint64_t> &tid = table->search_metadata(key);

    if (readKey.get_write_lock_bit())
      lock_succ = reserve ? AsyncCalvinHelper::try_write_lock_reserve(tid) : AsyncCalvinHelper::try_write_lock(tid);
    else if (readKey.get_read_lock_bit())
      lock_succ = reserve ? AsyncCalvinHelper::try_read_lock_reserve(tid) : AsyncCalvinHelper::try_read_lock(tid);
    else
      CHECK(false);

    return true;
  }

  void resume_remote_waiter(TransactionType *transaction)
  {
    auto indexed = pending_transaction_index.find(transaction);
    CHECK(indexed != pending_transaction_index.end()) << "message resumed a transaction without a suspended task";
    auto it = indexed->second;

    transaction->disarm_remote_read_wait();
    add_network_wait_time(std::get<2>(*it));

    auto execute_start = Clock::now();
    std::get<0>(*it).resume();
    add_execute_time(execute_start);
    if (std::get<0>(*it).done()) {
      commit_or_abort(transaction, std::get<0>(*it).get_value());
      pending_transaction_index.erase(indexed);
      pending_transactions.erase(it);
    } else {
      std::get<2>(*it) = Clock::now();
      transaction->arm_remote_read_wait(&remote_resume_txn_queue);
    }
  }

  // new transaction
  void commit_or_abort(TransactionType *transaction, TransactionResult result)
  {
    n_network_size.fetch_add(transaction->network_size.load());
    if (result == TransactionResult::READY_TO_COMMIT) {
      auto execute_start = Clock::now();
      protocol.commit(*transaction, lock_manager_id, n_lock_manager, partitioner.replica_group_size);
      add_execute_time(execute_start);
      auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - transaction->startTime)
                         .count();
      percentile.add(latency);

    } else if (result == TransactionResult::ABORT) {
      auto execute_start = Clock::now();
      protocol.abort(*transaction, lock_manager_id, n_lock_manager, partitioner.replica_group_size);
      add_execute_time(execute_start);
    } else {
      CHECK(false) << "abort no retry transaction should not be scheduled.";
    }
  }

  void setup_execute_handlers(TransactionType &txn)
  {
    txn.read_handler = [this, &txn](std::size_t worker_id,
                           std::size_t          table_id,
                           std::size_t          partition_id,
                           uint64_t             id,
                           uint32_t             key_offset,
                           const void          *key,
                           void                *value) {
      auto *worker = this->all_executors[worker_id];
      if (worker->partitioner.has_master_partition(partition_id)) {
        ITable *table = worker->db.find_table(table_id, partition_id);
        AsyncCalvinHelper::read(table->search(key), value, table->value_size());

        auto &active_coordinators = txn.active_coordinators;
        for (auto i = 0u; i < active_coordinators.size(); i++) {
          if (i == worker->coordinator_id || !active_coordinators[i])
            continue;
          auto sz = MessageFactoryType::new_read_message(*worker->messages[i], *table, id, key_offset, value);
          txn.network_size.fetch_add(sz);
          txn.distributed_transaction = true;
        }

        txn.local_read.fetch_add(-1);
      }
    };

    txn.setup_process_requests_in_execution_phase(n_lock_manager, n_workers, partitioner.replica_group_size);
    txn.message_flusher = [this](std::size_t worker_id) {
      auto *worker = this->all_executors[worker_id];
      worker->flush_messages();
    };
  }

  void setup_prepare_handlers(TransactionType &txn)
  {
    txn.local_index_read_handler = [this](
                                       std::size_t table_id, std::size_t partition_id, const void *key, void *value) {
      ITable *table = this->db.find_table(table_id, partition_id);
      AsyncCalvinHelper::read(table->search(key), value, table->value_size());
    };
    txn.setup_process_requests_in_prepare_phase();
  }

  void set_all_executors(const std::vector<AsyncCalvinExecutor *> &executors) { all_executors = executors; }

  std::size_t get_available_worker(std::size_t request_id)
  {
    // assume there are n lock managers and m workers
    // 0, 1, .. n-1 are lock managers
    // n, n + 1, .., n + m -1 are workers

    // the first lock managers assign transactions to n, .. , n + m/n - 1

    auto start_worker_id = n_lock_manager + n_workers / n_lock_manager * id;
    auto len             = n_workers / n_lock_manager;
    return request_id % len + start_worker_id;
  }

  void set_lock_manager_bit(int id)
  {
    uint32_t old_value, new_value;
    do {
      old_value = lock_manager_status.load();
      DCHECK(((old_value >> id) & 1) == 0);
      new_value = old_value | (1 << id);
    } while (!lock_manager_status.compare_exchange_weak(old_value, new_value));
  }

  bool get_lock_manager_bit(int id) { return (lock_manager_status.load() >> id) & 1; }

  std::size_t process_request()
  {
    std::size_t size = 0;

    while (static_cast<ExecutorStatus>(worker_status.load()) != ExecutorStatus::EXIT && !in_queue.empty()) {
      std::unique_ptr<Message> message(in_queue.front());
      bool                     ok = in_queue.pop();
      CHECK(ok);

      for (auto it = message->begin(); it != message->end(); it++) {

        MessagePiece messagePiece = *it;
        auto         type         = messagePiece.get_message_type();
        DCHECK(type < messageHandlers.size());
        ITable *table = db.find_table(messagePiece.get_table_id(), messagePiece.get_partition_id());
        messageHandlers[type](messagePiece, *messages[message->get_source_node_id()], *table, (*transactions_ptr));
      }

      size += message->get_message_count();
      flush_messages();
    }
    return size;
  }

private:
  DatabaseType                                   &db;
  const ContextType                              &context;
  std::vector<std::unique_ptr<TransactionType>> *&transactions_ptr;
  std::vector<StorageType>                       &storages;
  std::atomic<uint32_t>                          &lock_manager_status, &worker_status;
  std::atomic<uint32_t>                          &n_complete_workers, &n_started_workers;
  AsyncCalvinPartitioner                                 partitioner;
  WorkloadType                                    workload;
  std::size_t                                     n_lock_manager, n_workers;
  std::size_t                                     lock_manager_id;
  bool                                            init_transaction;
  RandomType                                      random;
  ProtocolType                                           protocol;
  std::unique_ptr<Delay>                                 delay;
  Percentile<int64_t>                                    percentile;
  std::vector<std::unique_ptr<Message>>                  messages;
  std::vector<std::function<void(MessagePiece, Message &, ITable &, std::vector<std::unique_ptr<TransactionType>> &)>>
                                     messageHandlers;
  LockfreeQueue<Message *>           in_queue, out_queue;
  LockfreeQueue<TransactionType *>   transaction_queue;
  LockfreeQueue<TransactionType *>   wakeup_txn_queue;
  AsyncCalvinRemoteWaitQueue         remote_resume_txn_queue;
  std::vector<AsyncCalvinExecutor *> all_executors;

  // tid -> blocked transaction queue
  BlockedTxnsType      *&blocked_txns;
  std::atomic<uint64_t> &globalBlockedCounter;

  using PendingTransaction = std::tuple<Task<TransactionResult>, TransactionType *, Clock::time_point>;
  using PendingIterator    = typename std::list<PendingTransaction>::iterator;

  std::list<PendingTransaction>                          pending_transactions;
  std::unordered_map<TransactionType *, PendingIterator> pending_transaction_index;
};
}  // namespace aria
