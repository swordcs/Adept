//
// Created by Yi Lu on 1/14/20.
//

#pragma once

#include "core/Manager.h"
#include "protocol/Pwv/PwvExecutor.h"
#include "protocol/Pwv/PwvHelper.h"
#include "protocol/Pwv/PwvTransaction.h"

#include <algorithm>
#include <cstdint>
#include <thread>
#include <unordered_map>
#include <vector>

namespace aria {
template <class Database>
class PwvManager : public aria::Manager
{
public:
  using base_type       = aria::Manager;
  using DatabaseType    = Database;
  using WorkloadType    = PwvWorkload<Database>;
  using StorageType     = typename DatabaseType::StorageType;
  using TransactionType = PwvTransaction;
  using ContextType     = typename DatabaseType::ContextType;
  using RandomType      = typename DatabaseType::RandomType;

  PwvManager(std::size_t coordinator_id, std::size_t id, DatabaseType &db, const ContextType &context,
      std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag), db(db), epoch(0)
  {

    storages.resize(context.batch_size);
    transactions.resize(context.batch_size);
  }

  void coordinator_start() override
  {
    while (!stopFlag.load()) {
      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::Pwv_Analysis);
      wait_all_workers_start();
      wait_all_workers_finish();
      auto max_dependency_level = build_dependency_levels();
      wait4_ack();

      // Transactions at one level have no conflicts with one another. A
      // successor level is released only after every node has completed both
      // stages of the predecessor level, so it cannot execute or ship a value
      // produced from an uncommitted predecessor.
      for (std::size_t level = 0; level <= max_dependency_level; level++) {
        epoch.store(static_cast<uint32_t>(level));
        run_coordinator_stage(ExecutorStatus::Pwv_Execute);
        run_coordinator_stage(ExecutorStatus::Pwv_Execute_Stage2);
      }

      auto aborted = static_cast<std::size_t>(std::count_if(
          transactions.begin(), transactions.end(), [](const auto &transaction) { return transaction->is_aborted(); }));
      n_commit.fetch_add(transactions.size() - aborted);
      n_abort_no_retry.fetch_add(aborted);
    }

    signal_worker(ExecutorStatus::EXIT);
  }

  void non_coordinator_start() override
  {
    std::size_t execution_level      = 0;
    std::size_t max_dependency_level = 0;

    for (;;) {
      auto status = wait4_signal();
      if (status == ExecutorStatus::EXIT) {
        set_worker_status(ExecutorStatus::EXIT);
        break;
      }

      if (status == ExecutorStatus::Pwv_Analysis) {
        execution_level = 0;
      } else {
        CHECK(status == ExecutorStatus::Pwv_Execute || status == ExecutorStatus::Pwv_Execute_Stage2);
        CHECK(execution_level <= max_dependency_level);
        epoch.store(static_cast<uint32_t>(execution_level));
      }

      n_started_workers.store(0);
      n_completed_workers.store(0);
      set_worker_status(status);
      wait_all_workers_start();
      wait_all_workers_finish();

      if (status == ExecutorStatus::Pwv_Analysis) {
        max_dependency_level = build_dependency_levels();
      } else if (status == ExecutorStatus::Pwv_Execute_Stage2) {
        execution_level++;
      }
      send_ack();
    }
  }

private:
  struct ResourceKey
  {
    const ITable               *table;
    const ITable::MetaDataType *row;

    bool operator==(const ResourceKey &other) const { return table == other.table && row == other.row; }
  };

  struct ResourceKeyHash
  {
    std::size_t operator()(const ResourceKey &resource) const
    {
      auto seed = std::hash<const ITable *>{}(resource.table);
      seed ^= std::hash<const ITable::MetaDataType *>{}(resource.row) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
      return seed;
    }
  };

  struct ConflictFrontier
  {
    bool        has_writer       = false;
    std::size_t writer_level     = 0;
    bool        has_readers      = false;
    std::size_t max_reader_level = 0;
  };

  ResourceKey resource_key(const PwvRWKey &rwkey)
  {
    auto *table = db.find_table(rwkey.get_table_id(), rwkey.get_partition_id());
    CHECK(table != nullptr);
    CHECK(rwkey.get_key() != nullptr);
    auto &row = table->search_metadata(rwkey.get_key());
    return ResourceKey{table, &row};
  }

  std::size_t build_dependency_levels()
  {
    std::unordered_map<ResourceKey, ConflictFrontier, ResourceKeyHash> frontiers;
    std::size_t                                                        max_dependency_level = 0;

    for (auto &transaction : transactions) {
      CHECK(transaction != nullptr);
      transaction->dependency_level = 0;
      if (transaction->is_aborted())
        continue;

      std::unordered_map<ResourceKey, bool, ResourceKeyHash> accesses;

      for (const auto &piece : transaction->pieces) {
        for (const auto &read_key : piece->readSet)
          accesses.emplace(resource_key(read_key), false);
        for (const auto &write_key : piece->writeSet)
          accesses[resource_key(write_key)] = true;
      }

      std::size_t level = 0;
      for (const auto &access : accesses) {
        auto frontier = frontiers.find(access.first);
        if (frontier == frontiers.end())
          continue;

        if (frontier->second.has_writer)
          level = std::max(level, frontier->second.writer_level + 1);
        if (access.second && frontier->second.has_readers)
          level = std::max(level, frontier->second.max_reader_level + 1);
      }

      transaction->dependency_level = level;
      max_dependency_level          = std::max(max_dependency_level, level);

      for (const auto &access : accesses) {
        auto &frontier = frontiers[access.first];
        if (access.second) {
          frontier.has_writer       = true;
          frontier.writer_level     = level;
          frontier.has_readers      = false;
          frontier.max_reader_level = 0;
        } else {
          frontier.has_readers      = true;
          frontier.max_reader_level = std::max(frontier.max_reader_level, level);
        }
      }
    }

    return max_dependency_level;
  }

  void run_coordinator_stage(ExecutorStatus status)
  {
    n_started_workers.store(0);
    n_completed_workers.store(0);
    signal_worker(status);
    wait_all_workers_start();
    wait_all_workers_finish();
    wait4_ack();
  }

public:
  RandomType                                    random;
  DatabaseType                                 &db;
  std::atomic<uint32_t>                         epoch;
  std::vector<StorageType>                      storages;
  std::vector<std::unique_ptr<TransactionType>> transactions;
};
}  // namespace aria
