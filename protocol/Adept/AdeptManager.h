//
// Created by Yi Lu on 9/13/18.
//

#pragma once

#include "core/Manager.h"
#include "protocol/Adept/Adept.h"
#include "protocol/Adept/AdeptExecutor.h"
#include "protocol/Adept/AdeptHelper.h"
#include "protocol/Adept/AdeptMirrorCache.h"
#include "protocol/Adept/AdeptPartitioner.h"
#include "protocol/Adept/AdeptTransaction.h"
#include "protocol/Adept/AdeptTxnGenerator.h"

#include <thread>
#include <vector>

namespace aria {

template <class Workload>
class AdeptManager : public aria::Manager
{
public:
  using base_type = aria::Manager;

  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType  = typename WorkloadType::StorageType;

  using TransactionType = AdeptTransaction;
  static_assert(
      std::is_same<typename WorkloadType::TransactionType, TransactionType>::value, "Transaction types do not match.");
  using ContextType = typename DatabaseType::ContextType;
  using RandomType  = typename DatabaseType::RandomType;
  using TransactionBatch = typename AdeptTxnGenerator<WorkloadType>::TransactionBatch;

  using BlockedTxnEntryType  = AdeptBlockedTxnEntry;
  using BlockedTxnsQueueType = std::deque<BlockedTxnEntryType>;
  using BlockedTxnsType      = HashMap<1000, uint64_t, BlockedTxnsQueueType>;

  AdeptManager(std::size_t coordinator_id, std::size_t id, DatabaseType &db, const ContextType &context,
      std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag),
        db(db),
        partitioner(coordinator_id, context.coordinator_num, AdeptHelper::string_to_vint(context.replica_group)),
        mirror_cache(context.mirror_cache_size, context.mirror_cache_warmup_batches, context.coordinator_num),
        transactions_ptr(nullptr),
        epoch(1)
  {
    CHECK(context.mvcc);
    CHECK(context.batch_size > 0 && context.batch_size < (uint64_t{1} << 20));
    storages.resize(context.batch_size);
    blocked_txns = new BlockedTxnsType();
    benchmark_ready.store(!mirror_cache.enabled());
  }

  ~AdeptManager() override { delete blocked_txns; }

  bool measurement_ready() const override { return benchmark_ready.load(std::memory_order_acquire); }

  void coordinator_start() override
  {
    while (!stopFlag.load()) {

      // the coordinator on each machine generates
      // a batch of transactions using the same random seed.

      // LOG(INFO) << "Seed: " << random.get_seed();

      auto batch = txn_generator->get_batch(true);
      if (!batch || stopFlag.load()) {
        break;
      }

      // pass the ownership of the batch to all executors
      transactions_ptr = batch.get();
      std::atomic_thread_fence(std::memory_order_release);

      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::Analysis);
      // Allow each worker to analyse the read/write set
      // each worker analyse i, i + n, i + 2n transaction
      wait_all_workers_start();
      wait_all_workers_finish();

      mirror_cache.analyze_and_plan(*transactions_ptr, db, partitioner, coordinator_id);

      // wait for all machines until they finish the analysis phase.
      wait4_ack();

      // Allow each worker to run transactions
      // DB is partitioned by the number of lock managers.
      // The first k workers act as lock managers to grant locks to other
      // workers The remaining workers run transactions upon assignment via the
      // queue.
      n_started_workers.store(0);
      n_completed_workers.store(0);
      clear_lock_manager_status();
      signal_worker(ExecutorStatus::Execute);
      wait_all_workers_start();
      wait_all_workers_finish();
      // wait for all machines until they finish the execution phase.
      wait4_ack();

      if (mirror_cache.ready_for_measurement()) {
        benchmark_ready.store(true, std::memory_order_release);
      }

      record_completed_batch();

      garbage_collect();
    }

    mirror_cache.log_summary(coordinator_id);
    signal_worker(ExecutorStatus::EXIT);
  }

  void non_coordinator_start() override
  {
    std::unique_ptr<TransactionBatch> batch;

    for (;;) {
      // LOG(INFO) << "Seed: " << random.get_seed();
      ExecutorStatus status = wait4_signal();
      if (status == ExecutorStatus::EXIT) {
        garbage_collect();
        mirror_cache.log_summary(coordinator_id);
        set_worker_status(ExecutorStatus::EXIT);
        break;
      }
      garbage_collect();

      DCHECK(status == ExecutorStatus::Analysis);
      // the coordinator on each machine generates
      // a batch of transactions using the same random seed.
      // Allow each worker to analyse the read/write set
      // each worker analyse i, i + n, i + 2n transaction

      batch = txn_generator->get_batch(true);
      // The coordinator owns cluster shutdown. Dropping an announced batch on
      // a non-coordinator leaves the coordinator waiting forever for its ACK.
      CHECK(batch) << "transaction generator stopped after an Analysis signal";

      // pass the ownership of the batch to all executors
      transactions_ptr = batch.get();
      std::atomic_thread_fence(std::memory_order_release);

      n_started_workers.store(0);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::Analysis);
      wait_all_workers_start();
      wait_all_workers_finish();

      mirror_cache.analyze_and_plan(*transactions_ptr, db, partitioner, coordinator_id);

      send_ack();

      status = wait4_signal();
      DCHECK(status == ExecutorStatus::Execute);
      // Allow each worker to run transactions
      // DB is partitioned by the number of lock managers.
      // The first k workers act as lock managers to grant locks to other
      // workers The remaining workers run transactions upon assignment via the
      // queue.
      n_started_workers.store(0);
      n_completed_workers.store(0);
      clear_lock_manager_status();
      set_worker_status(ExecutorStatus::Execute);
      wait_all_workers_start();
      wait_all_workers_finish();
      send_ack();

      if (mirror_cache.ready_for_measurement()) {
        benchmark_ready.store(true, std::memory_order_release);
      }

    }
  }

  void add_worker(const std::shared_ptr<AdeptExecutor<WorkloadType>> &w) { workers.push_back(w); }

  void clear_lock_manager_status() { lock_manager_status.store(0); }

  void set_txn_generators(std::vector<std::shared_ptr<TxnGenerator>> &txn_generators)
  {
    CHECK(txn_generators.size() == 1);
    this->txn_generator = static_cast<AdeptTxnGenerator<WorkloadType> *>(txn_generators[0].get());
  }

  void garbage_collect()
  {
    mirror_cache.garbage_collect(db, coordinator_id);

    if (transactions_ptr != nullptr) {
      for (const auto &transaction : *transactions_ptr) {
        CHECK(transaction->blocked_counter.load(std::memory_order_acquire) == 0);
        CHECK(transaction->deferred_abort_counter.load(std::memory_order_acquire) == 0);
        for (const auto &read_key : transaction->readSet) {
          if (!read_key.get_scheduled_lock_bit() || !read_key.get_write_lock_bit() ||
              !partitioner.has_master_partition(read_key.get_partition_id())) {
            continue;
          }
          auto *table = db.find_table(read_key.get_table_id(), read_key.get_partition_id());
          table->garbage_collect(read_key.get_key());
        }
      }
    }

    if (!blocked_txns)
      return;

    delete blocked_txns;
    blocked_txns = new BlockedTxnsType();
  }

  void record_completed_batch()
  {
    uint64_t committed = 0;
    uint64_t rejected  = 0;
    for (const auto &transaction : *transactions_ptr) {
      if (transaction->abort_no_retry)
        rejected++;
      else
        committed++;
    }
    // Every coordinator executes the same deterministic batch. Count it only
    // after the cluster-wide execution barrier, not at scheduler submission.
    n_commit.fetch_add(committed);
    n_abort_no_retry.fetch_add(rejected);
  }

public:
  RandomType                                               random;
  DatabaseType                                            &db;
  AdeptPartitioner                                          partitioner;
  AdeptMirrorCache                                          mirror_cache;
  std::atomic<uint32_t>                                    lock_manager_status;
  std::vector<std::shared_ptr<AdeptExecutor<WorkloadType>>> workers;
  std::vector<StorageType>                                 storages;
  std::vector<std::unique_ptr<TransactionType>>           *transactions_ptr;
  std::atomic<uint64_t>                                    globalBlockedCounter{1};

  AdeptTxnGenerator<WorkloadType> *txn_generator;
  std::atomic<uint32_t>           epoch;
  BlockedTxnsType                *blocked_txns;
  std::atomic<bool>               benchmark_ready{false};
};
}  // namespace aria
