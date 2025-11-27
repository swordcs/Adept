/*
 * @Description: Transaction generator factory for creating protocol-specific generators
 * @Date: 2025-09-16
 */

#pragma once

#include "benchmark/tpcc/Workload.h"
#include "benchmark/ycsb/Workload.h"
#include "core/Context.h"
#include "core/TxnGenerator.h"
#include <memory>
#include <unordered_set>
#include <vector>

#include "protocol/Queue/QueueTxnGenerator.h"
#include "protocol/Calvin/CalvinTxnGenerator.h"

namespace aria {

template <class Context>
class InferWorkloadType
{};

template <>
class InferWorkloadType<aria::tpcc::Context>
{
public:
  template <class Transaction>
  using WorkloadType = aria::tpcc::Workload<Transaction>;
};

template <>
class InferWorkloadType<aria::ycsb::Context>
{
public:
  template <class Transaction>
  using WorkloadType = aria::ycsb::Workload<Transaction>;
};

class TxnGeneratorFactory
{
public:
  template <class Database, class Context>
  static std::vector<std::shared_ptr<TxnGenerator>> create_generators(
      std::size_t coordinator_id, Database &db, const Context &context, Manager *manager, std::atomic<bool> &stop_flag)
  {
    std::unordered_set<std::string> protocols = {"Calvin", "Adept", "Queue"};
    CHECK(protocols.count(context.protocol) == 1);

    std::vector<std::shared_ptr<TxnGenerator>> generators;

    if (context.protocol == "Calvin") {
      using TransactionType = aria::CalvinTransaction;
      using WorkloadType    = typename InferWorkloadType<Context>::template WorkloadType<TransactionType>;

      auto calvin_manager = static_cast<CalvinManager<WorkloadType> *>(manager);
      for (auto i = 0u; i < context.txn_generator_num; i++) {
        auto generator = std::make_shared<CalvinTxnGenerator<WorkloadType>>(
            coordinator_id, i, db, context, calvin_manager->storages, stop_flag);
        generators.push_back(generator);
      }
    } else if (context.protocol == "Adept") {
      using TransactionType = aria::AdeptTransaction;
      using WorkloadType    = typename InferWorkloadType<Context>::template WorkloadType<TransactionType>;

      auto Adept_manager = static_cast<AdeptManager<WorkloadType> *>(manager);
      for (auto i = 0u; i < context.txn_generator_num; i++) {
        auto generator = std::make_shared<AdeptTxnGenerator<WorkloadType>>(
            coordinator_id, i, db, context, Adept_manager->storages, Adept_manager->epoch, stop_flag);
        generators.push_back(generator);
      }
    } else if (context.protocol == "Queue") {
      using TransactionType = aria::QueueTransaction;
      using WorkloadType    = typename InferWorkloadType<Context>::template WorkloadType<TransactionType>;

      auto queue_manager = static_cast<QueueManager<WorkloadType> *>(manager);
      for (auto i = 0u; i < context.txn_generator_num; i++) {
        auto generator = std::make_shared<QueueTxnGenerator<WorkloadType>>(
            coordinator_id, i, db, context, queue_manager->storages, stop_flag);
        generators.push_back(generator);
      }
    } else {
      CHECK(false) << "protocol: " << context.protocol << " is not supported.";
    }

    manager->set_txn_generators(generators);

    return generators;
  }
};

}  // namespace aria