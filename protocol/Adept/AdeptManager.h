

#pragma once

#include "core/Manager.h"
#include "protocol/Adept/Adept.h"
#include "protocol/Adept/AdeptExecutor.h"
#include "protocol/Adept/AdeptHelper.h"
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

  using BlockedTxnEntryType  = std::tuple<bool, TransactionType *>;
  using BlockedTxnsQueueType = std::deque<BlockedTxnEntryType>;
  using BlockedTxnsType      = HashMap<1000, uint64_t, BlockedTxnsQueueType>;

  AdeptManager(std::size_t coordinator_id, std::size_t id, DatabaseType &db, const ContextType &context,
      std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag),
        db(db),
        epoch(0),
        partitioner(coordinator_id, context.coordinator_num, AdeptHelper::string_to_vint(context.replica_group))
  {
    storages.resize(context.batch_size);
    blocked_txns = new BlockedTxnsType();
  }

  void coordinator_start() override
  {

    std::size_t n_workers      = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

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

      garbage_collect();
    }

    signal_worker(ExecutorStatus::EXIT);
  }

  void non_coordinator_start() override
  {

    std::size_t n_workers      = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    for (;;) {
      // LOG(INFO) << "Seed: " << random.get_seed();
      ExecutorStatus status = wait4_signal();
      if (status == ExecutorStatus::EXIT) {
        set_worker_status(ExecutorStatus::EXIT);
        break;
      }
      garbage_collect();

      DCHECK(status == ExecutorStatus::Analysis);
      // the coordinator on each machine generates
      // a batch of transactions using the same random seed.
      // Allow each worker to analyse the read/write set
      // each worker analyse i, i + n, i + 2n transaction

      auto batch = txn_generator->get_batch(true);
      if (!batch || stopFlag.load()) {
        break;
      }

      // pass the ownership of the batch to all executors
      transactions_ptr = batch.get();
      std::atomic_thread_fence(std::memory_order_release);

      n_started_workers.store(0);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::Analysis);
      wait_all_workers_start();
      wait_all_workers_finish();

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
    }
  }

  void add_worker(const std::shared_ptr<AdeptExecutor<WorkloadType>> &w) { workers.push_back(w); }

  void clear_lock_manager_status() { lock_manager_status.store(0); }

  void set_txn_generators(std::vector<std::shared_ptr<TxnGenerator>> &txn_generators)
  {
    // Adept only uses one txn generator
    this->txn_generator = static_cast<AdeptTxnGenerator<WorkloadType> *>(txn_generators[0].get());
  }

  void garbage_collect()
  {
    if (!blocked_txns)
      return;

    delete blocked_txns;
    blocked_txns = new BlockedTxnsType();
  }

public:
  RandomType                                                random;
  DatabaseType                                             &db;
  AdeptPartitioner                                          partitioner;
  std::atomic<uint32_t>                                     lock_manager_status;
  std::vector<std::shared_ptr<AdeptExecutor<WorkloadType>>> workers;
  std::vector<StorageType>                                  storages;
  std::vector<std::unique_ptr<TransactionType>>            *transactions_ptr;
  std::atomic<uint64_t>                                     globalBlockedCounter{1};

  AdeptTxnGenerator<WorkloadType> *txn_generator;
  std::atomic<uint32_t>            epoch;  // add epoch to AdeptManager
  BlockedTxnsType                 *blocked_txns;
};
}  // namespace aria