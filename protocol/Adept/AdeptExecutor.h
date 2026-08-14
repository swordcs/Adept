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

#include "protocol/Adept/Adept.h"
#include "protocol/Adept/AdeptHelper.h"
#include "protocol/Adept/AdeptMessage.h"
#include "protocol/Adept/AdeptMirrorCache.h"
#include "protocol/Adept/AdeptPartitioner.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <queue>
#include <thread>
#include <unordered_map>

namespace aria {

template <class Workload>
class AdeptExecutor : public Worker
{
public:
  using WorkloadType       = Workload;
  using DatabaseType       = typename WorkloadType::DatabaseType;
  using StorageType        = typename WorkloadType::StorageType;
  using TransactionType    = AdeptTransaction;
  using ContextType        = typename DatabaseType::ContextType;
  using RandomType         = typename DatabaseType::RandomType;
  using ProtocolType       = Adept<DatabaseType>;
  using MessageType        = AdeptMessage;
  using MessageFactoryType = AdeptMessageFactory;
  using MessageHandlerType = AdeptMessageHandler;
  using MetaDataType       = std::atomic<uint64_t>;
  // for blocker handler, atomic for rwlock and blocked counter in this queue
  using BlockedTxnEntryType  = AdeptBlockedTxnEntry;
  using BlockedTxnsQueueType = std::deque<BlockedTxnEntryType>;
  using BlockedTxnsType      = HashMap<1000, uint64_t, BlockedTxnsQueueType>;

  AdeptExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db, const ContextType &context,
      std::vector<std::unique_ptr<TransactionType>> *&transactions_ptr, std::vector<StorageType> &storages,
      std::atomic<uint32_t> &lock_manager_status, std::atomic<uint32_t> &worker_status,
      std::atomic<uint32_t> &n_complete_workers, std::atomic<uint32_t> &n_started_workers,
      BlockedTxnsType *&blocked_txns, std::atomic<uint64_t> &globalBlockedCounter, AdeptMirrorCache &mirror_cache)
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
        partitioner(coordinator_id, context.coordinator_num, AdeptHelper::string_to_vint(context.replica_group)),
        workload(coordinator_id, db, random, partitioner),
        n_lock_manager(AdeptHelper::n_lock_manager(
            partitioner.replica_group_id, AdeptHelper::string_to_vint(context.lock_manager))),
        n_workers(context.worker_num >= n_lock_manager ? context.worker_num - n_lock_manager : 0),
        lock_manager_id(AdeptHelper::worker_id_to_lock_manager_id(id, n_lock_manager, n_workers)),
        init_transaction(false),
        random(id),  // make sure each worker has a different seed.
        protocol(db, partitioner, blocked_txns, globalBlockedCounter),
        delay(std::make_unique<SameDelay>(coordinator_id, context.coordinator_num, context.delay_time)),
        blocked_optimize(context.blocked_optimize),
        globalBlockedCounter(globalBlockedCounter),
        mirror_cache(mirror_cache)
  {

    for (auto i = 0u; i < context.coordinator_num; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }
    messageHandlers = MessageHandlerType::get_message_handlers();
    protocol.set_executor_txn_queue(&wakeup_txn_queue);

    CHECK(n_workers >= n_lock_manager && n_workers % n_lock_manager == 0);
  }

  ~AdeptExecutor() = default;

  void start() override
  {
    LOG(INFO) << "AdeptExecutor " << id << " started. ";

    for (;;) {

      ExecutorStatus status;
      do {
        status = static_cast<ExecutorStatus>(worker_status.load());

        if (status == ExecutorStatus::EXIT) {
          LOG(INFO) << "AdeptExecutor " << id << " exits. ";
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
              << " us (99%); current global blocked counter: " << globalBlockedCounter.load()
              << "; pipelined transactions: " << pipelined_transactions
              << "; pipelined reads: " << pipelined_reads
              << "; dependency-overlapped transactions: " << pipelined_overlap_transactions
              << "; dependency-overlapped reads: " << pipelined_overlap_reads << ".";
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
    mirror_cache.annotate_transaction(txn, db);

    // setup handlers for execution
    setup_execute_handlers(txn);
    txn.execution_phase = true;
  }

  void analyze_active_coordinator(TransactionType &transaction)
  {

    auto &readSet             = transaction.readSet;
    auto &active_coordinators = transaction.active_coordinators;
    active_coordinators       = std::vector<bool>(partitioner.total_coordinators(), false);
    std::unordered_map<MetaDataType *, std::size_t> local_locks;

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &current = readSet[i];
      if (current.get_local_index_read_bit()) {
        continue;
      }
      auto *table = db.find_table(current.get_table_id(), current.get_partition_id());
      for (auto j = 0u; j < i; j++) {
        auto &previous = readSet[j];
        if (previous.get_local_index_read_bit() || previous.get_table_id() != current.get_table_id() ||
            previous.get_partition_id() != current.get_partition_id() ||
            !table->key_equal(previous.get_key(), current.get_key())) {
          continue;
        }
        if (current.get_write_lock_bit() &&
            (previous.get_read_lock_bit() ||
                (previous.get_write_lock_bit() && !previous.get_blind_bit()))) {
          current.clear_blind_bit();
        }
        if (previous.get_write_lock_bit() &&
            (current.get_read_lock_bit() || (current.get_write_lock_bit() && !current.get_blind_bit()))) {
          previous.clear_blind_bit();
        }
      }
    }

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readkey = readSet[i];
      readkey.clear_scheduled_lock_bit();
      if (readkey.get_local_index_read_bit())
        continue;
      auto partitionID = readkey.get_partition_id();

      if (partitioner.has_master_partition(partitionID)) {
        auto *table    = db.find_table(readkey.get_table_id(), partitionID);
        auto *metadata = &table->search_metadata_last(readkey.get_key());
        auto [it, inserted] = local_locks.emplace(metadata, i);
        if (inserted) {
          readkey.set_scheduled_lock_bit();
        } else {
          auto &selected = readSet[it->second];
          if (readkey.get_write_lock_bit() && !selected.get_write_lock_bit()) {
            const bool needs_predecessor = selected.get_read_lock_bit() || !readkey.get_blind_bit();
            selected.clear_scheduled_lock_bit();
            readkey.set_scheduled_lock_bit();
            if (needs_predecessor) {
              readkey.clear_blind_bit();
            }
            it->second = i;
          } else if (selected.get_write_lock_bit() &&
                     (readkey.get_read_lock_bit() ||
                         (readkey.get_write_lock_bit() && !readkey.get_blind_bit()))) {
            selected.clear_blind_bit();
          }
        }
      }

      if (readkey.get_write_lock_bit()) {
        active_coordinators[partitioner.master_coordinator(partitionID)] = true;
      }
    }
    if (std::none_of(active_coordinators.begin(), active_coordinators.end(), [](bool active) { return active; })) {
      active_coordinators[partitioner.master_coordinator(transaction.partition_id)] = true;
    }
    CHECK(local_locks.size() <= static_cast<std::size_t>(std::numeric_limits<int32_t>::max()));
    transaction.lock_counter.store(static_cast<int32_t>(local_locks.size()));
  }

  void schedule_transactions()
  {
    auto phase_start = Clock::now();

    // grant locks, once all locks are acquired, assign the transaction to
    // a worker thread in a round-robin manner.
    std::size_t request_id = 0;

    for (auto i = 0u; i < (*transactions_ptr).size(); i++) {
      // do not grant locks to abort no retry transaction
      if (!(*transactions_ptr)[i]->abort_no_retry) {
        bool  grant_lock   = false;
        bool  spin         = !blocked_optimize;
        auto &readSet      = (*transactions_ptr)[i]->readSet;

        auto &lock_counter        = (*transactions_ptr)[i]->lock_counter;
        auto &blocked_counter     = (*transactions_ptr)[i]->blocked_counter;
        auto &scheduling_claimed  = (*transactions_ptr)[i]->scheduling_claimed;
        auto &ready_for_execution = (*transactions_ptr)[i]->ready_for_execution;
        auto &execution_enqueued  = (*transactions_ptr)[i]->execution_enqueued;
        auto &active_coordinators = (*transactions_ptr)[i]->active_coordinators;

        int32_t processed_locks = 0;

        std::vector<std::size_t> failed_locks;
        for (auto k = 0u; k < readSet.size(); k++) {
          // whether this lock is successfully acquired
          bool lock_succ = false;

          if (!readSet[k].get_scheduled_lock_bit() ||
              !do_lock_tuple(readSet[k], (*transactions_ptr)[i].get(), lock_succ, spin, false))
            continue;

          grant_lock = true;
          processed_locks++;
          if (!lock_succ) {
            failed_locks.push_back(k);
          } else if (context.adept_pipelined_shipping && should_pipeline_read(*(*transactions_ptr)[i], readSet[k])) {
            readSet[k].set_pipelined_read_ready_bit();
          }
        }

        if (!failed_locks.empty()) {
          bool lock_succ;
          for (auto k : failed_locks) {
            // reserve the tuple when lock fails
            do_lock_tuple(readSet[k], (*transactions_ptr)[i].get(), lock_succ, false, true);
            if (!lock_succ) {
              register_blocked_tuple(readSet[k], (*transactions_ptr)[i].get());
            } else if (context.adept_pipelined_shipping && should_pipeline_read(*(*transactions_ptr)[i], readSet[k])) {
              readSet[k].set_pipelined_read_ready_bit();
            }
          }
        }

        auto previous_locks = lock_counter.fetch_sub(processed_locks, std::memory_order_acq_rel);
        CHECK(previous_locks >= processed_locks);
        auto locks_left = previous_locks - processed_locks;

        if (locks_left == 0 &&
            (processed_locks > 0 || active_coordinators[coordinator_id]) &&
            !scheduling_claimed.exchange(true, std::memory_order_acq_rel)) {
          const bool pipeline_reads = context.adept_pipelined_shipping &&
                                      blocked_counter.load(std::memory_order_acquire) > 0 &&
                                      has_pipelined_reads(*(*transactions_ptr)[i]);
          if (pipeline_reads) {
            // The synthetic dependency prevents normal execution from racing
            // with the worker that ships the already-acquired tuple values.
            blocked_counter.fetch_add(1, std::memory_order_acq_rel);
            auto worker = get_available_worker(request_id++);
            all_executors[worker]->pipelined_read_txn_queue.push((*transactions_ptr)[i].get());
          }

          ready_for_execution.store(true, std::memory_order_release);
          const bool executable_here = grant_lock || (processed_locks == 0 && active_coordinators[coordinator_id]);
          if (executable_here && blocked_counter.load(std::memory_order_acquire) == 0 &&
              !execution_enqueued.exchange(true)) {
            auto worker = get_available_worker(request_id++);
            all_executors[worker]->transaction_queue.push((*transactions_ptr)[i].get());
          }
        }

      } else {
        // Logical completion is recorded once by AdeptManager after every
        // coordinator has finished the batch.
      }
    }
    set_lock_manager_bit(id);
    add_schedule_time(phase_start);
  }

  void run_transactions()
  {
    while (!get_lock_manager_bit(lock_manager_id) || !all_transactions_done()) {
      process_request();

      if (!pipelined_read_txn_queue.empty()) {
        send_pipelined_reads();
      }

      TransactionType *transaction = nullptr;
      while (remote_resume_txn_queue.try_pop(transaction)) {
        resume_remote_waiter(transaction);
      }

      // Interleave admission with message-driven continuations instead of
      // draining the whole ready queue before processing replies.
      if (!transaction_queue.empty() || !wakeup_txn_queue.empty()) {
        // Dependency continuations are on the critical path for later tuple
        // versions. Run them before fresh independent admissions.
        auto            &txn_queue   = wakeup_txn_queue.empty() ? transaction_queue : wakeup_txn_queue;
        TransactionType *transaction = txn_queue.front();
        bool             ok          = txn_queue.pop();
        DCHECK(ok);
        auto execute_start = Clock::now();
        auto task = transaction->execute_coro(id);
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
    return transaction_queue.empty() && wakeup_txn_queue.empty() && pipelined_read_txn_queue.empty() &&
           remote_resume_txn_queue.empty() && pending_transactions.empty() && pending_transaction_index.empty();
  }

  bool should_pipeline_read(const TransactionType &transaction, const AdeptRWKey &read_key) const
  {
    if (read_key.get_blind_bit()) {
      return false;
    }
    for (std::size_t destination = 0; destination < transaction.active_coordinators.size(); destination++) {
      if (destination != coordinator_id && transaction.active_coordinators[destination] &&
          !read_key.should_skip_mirror_destination(destination)) {
        return true;
      }
    }
    return false;
  }

  bool has_pipelined_reads(const TransactionType &transaction) const
  {
    return std::any_of(transaction.readSet.begin(), transaction.readSet.end(),
        [](const auto &read_key) { return read_key.get_pipelined_read_ready_bit(); });
  }

  void send_pipelined_reads()
  {
    do {
      auto *transaction = pipelined_read_txn_queue.front();
      bool  ok          = pipelined_read_txn_queue.pop();
      DCHECK(ok);

      std::size_t shipped = 0;
      for (std::size_t key_offset = 0; key_offset < transaction->readSet.size(); key_offset++) {
        auto &read_key = transaction->readSet[key_offset];
        if (!read_key.get_pipelined_read_ready_bit() || read_key.get_execution_processed_bit()) {
          continue;
        }

        transaction->read_handler(id,
            read_key.get_table_id(),
            read_key.get_partition_id(),
            transaction->id,
            key_offset,
            read_key.get_key(),
            read_key.get_value());
        read_key.set_execution_processed_bit();
        shipped++;
      }

      CHECK(shipped > 0) << "pipelined read task contains no acquired remote-bound reads";
      pipelined_transactions++;
      pipelined_reads += shipped;

      // A shipping task is a latency boundary. Flushing it independently keeps
      // later ready tasks from delaying this transaction's tuple values and
      // lets a fully cached transaction eliminate its message altogether.
      flush_messages();

      // One counter slot is the synthetic shipping dependency itself. A value
      // above one at flush time proves that the data left while a real tuple or
      // cache dependency was still unresolved.
      if (transaction->blocked_counter.load(std::memory_order_acquire) > 1) {
        pipelined_overlap_transactions++;
        pipelined_overlap_reads += shipped;
      }
      protocol.notify_granted(transaction);
    } while (!pipelined_read_txn_queue.empty());
  }

  bool register_blocked_tuple(aria::AdeptRWKey &rwKey, TransactionType *txn_ptr)
  {
    auto          tableId     = rwKey.get_table_id();
    auto          partitionId = rwKey.get_partition_id();
    auto          table       = db.find_table(tableId, partitionId);
    auto          key         = rwKey.get_key();
    MetaDataType &tid         = table->search_metadata_last(key);

    CHECK(AdeptHelper::is_reserve_locked(tid.load()));
    CHECK(AdeptHelper::is_read_locked(tid.load()) || AdeptHelper::is_write_locked(tid.load()));

    CHECK(!rwKey.get_blind_bit());

    uint64_t waiter = AdeptHelper::get_waiter(tid);
    // write lock always insert a placeholder
    if (waiter != 0) {
      CHECK(blocked_txns->contains(waiter));
    } else {
      waiter = globalBlockedCounter.fetch_add(1);
      CHECK(waiter > 0 && waiter <= AdeptHelper::TID_MASK);
      AdeptHelper::set_waiter(tid, waiter);
    }
    auto &queue = (*blocked_txns)[waiter];

    if (rwKey.get_write_lock_bit()) {
      txn_ptr->blocked_counter.fetch_add(1, std::memory_order_release);
      queue.push_back(BlockedTxnEntryType{AdeptBlockedWaitType::WRITE, txn_ptr, nullptr});
      MetaDataType &ph_tid = table->insert_pure_holder(key, txn_ptr->id);
      AdeptHelper::write_lock(ph_tid);
    } else {
      txn_ptr->blocked_counter.fetch_add(1, std::memory_order_release);
      queue.push_back(BlockedTxnEntryType{AdeptBlockedWaitType::READ, txn_ptr, nullptr});
    }

    // set reservation bits
    AdeptHelper::reserve_lock_release(tid);
    return false;
  }
  bool do_lock_tuple(
      aria::AdeptRWKey &readKey, TransactionType *txn_ptr, bool &lock_succ, bool spin = true, bool reserve = false)
  {
    auto tableId     = readKey.get_table_id();
    auto partitionId = readKey.get_partition_id();

    if (!reserve && !partitioner.has_master_partition(partitionId)) {
      return false;
    };

    auto table = db.find_table(tableId, partitionId);
    auto key   = readKey.get_key();

    if (!reserve) {
      if (readKey.get_local_index_read_bit() || !readKey.get_scheduled_lock_bit()) {
        return false;
      };

      if (AdeptHelper::partition_id_to_lock_manager_id(
              readKey.get_partition_id(), n_lock_manager, partitioner.replica_group_size) != lock_manager_id) {
        return false;
      }
    }

    if (!reserve && readKey.get_write_lock_bit() && readKey.get_blind_bit()) {
      MetaDataType &placeholder = table->insert_pure_holder(key, txn_ptr->id);
      AdeptHelper::write_lock(placeholder);
      lock_succ = true;
      return true;
    }

    std::atomic<uint64_t> &tid = table->search_metadata_last(key);

    if (readKey.get_write_lock_bit()) {
      if (spin) {
        AdeptHelper::write_lock(tid);
        lock_succ = true;
      } else {
        lock_succ = reserve ? AdeptHelper::try_write_lock_reserve(tid) : AdeptHelper::try_write_lock(tid);
      }
    } else if (readKey.get_read_lock_bit()) {
      if (spin) {
        AdeptHelper::read_lock(tid);
        lock_succ = true;
      } else {
        lock_succ = reserve ? AdeptHelper::try_read_lock_reserve(tid) : AdeptHelper::try_read_lock(tid);
      }
    } else {
      CHECK(false);
    }

    if (lock_succ && readKey.get_write_lock_bit()) {
      MetaDataType &placeholder = table->insert_pure_holder(key, txn_ptr->id);
      AdeptHelper::write_lock(placeholder);
      AdeptHelper::write_lock_release(tid);
    }

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
      protocol.commit(*transaction);
      add_execute_time(execute_start);
      auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - transaction->startTime)
                         .count();
      percentile.add(latency);

    } else if (result == TransactionResult::ABORT) {
      auto execute_start = Clock::now();
      protocol.abort(*transaction);
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
        AdeptHelper::read(table->search_prev(key, id), value, table->value_size());

        auto &active_coordinators = txn.active_coordinators;
        auto &read_key = txn.readSet[key_offset];
        for (auto i = 0u; i < active_coordinators.size(); i++) {
          if (i == worker->coordinator_id || !active_coordinators[i] ||
              read_key.should_skip_mirror_destination(i))
            continue;
          auto sz = MessageFactoryType::new_read_value_message(*worker->messages[i], *table, id, key_offset, value);
          txn.network_size.fetch_add(sz);
          txn.distributed_transaction = true;
        }

        auto previous = txn.local_read.fetch_sub(1, std::memory_order_acq_rel);
        CHECK(previous > 0);
      } else if (txn.readSet[key_offset].get_mirror_cache_read_bit()) {
        ITable *table = worker->db.find_table(table_id, partition_id);
        AdeptHelper::read(table->search_prev(key, id), value, table->value_size());
        txn.complete_remote_read();
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
      AdeptHelper::read(table->search(key), value, table->value_size());
    };
    txn.setup_process_requests_in_prepare_phase();
  }

  void set_all_executors(const std::vector<AdeptExecutor *> &executors) { all_executors = executors; }

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
      new_value = old_value | (uint32_t{1} << id);
    } while (!lock_manager_status.compare_exchange_weak(old_value, new_value));
  }

  bool get_lock_manager_bit(int id) { return (lock_manager_status.load() >> id) & uint32_t{1}; }

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
  AdeptPartitioner                                 partitioner;
  WorkloadType                                    workload;
  std::size_t                                     n_lock_manager, n_workers;
  std::size_t                                     lock_manager_id;
  bool                                            init_transaction;
  RandomType                                      random;
  ProtocolType                                    protocol;
  std::unique_ptr<Delay>                          delay;
  Percentile<int64_t>                             percentile;
  std::vector<std::unique_ptr<Message>>           messages;
  std::vector<std::function<void(MessagePiece, Message &, ITable &, std::vector<std::unique_ptr<TransactionType>> &)>>
                                   messageHandlers;
  LockfreeQueue<Message *>         in_queue, out_queue;
  LockfreeQueue<TransactionType *> transaction_queue;
  LockfreeQueue<TransactionType *> wakeup_txn_queue;
  LockfreeQueue<TransactionType *> pipelined_read_txn_queue;
  AdeptRemoteWaitQueue              remote_resume_txn_queue;
  std::vector<AdeptExecutor *>      all_executors;

  // tid -> blocked transaction queue
  BlockedTxnsType      *&blocked_txns;
  bool                   blocked_optimize;
  std::atomic<uint64_t> &globalBlockedCounter;
  AdeptMirrorCache       &mirror_cache;
  uint64_t               pipelined_transactions = 0;
  uint64_t               pipelined_reads        = 0;
  uint64_t               pipelined_overlap_transactions = 0;
  uint64_t               pipelined_overlap_reads        = 0;

  using PendingTransaction = std::tuple<Task<TransactionResult>, TransactionType *, Clock::time_point>;
  using PendingIterator    = typename std::list<PendingTransaction>::iterator;

  std::list<PendingTransaction>                          pending_transactions;
  std::unordered_map<TransactionType *, PendingIterator> pending_transaction_index;
};
}  // namespace aria
