/*
 * @Description: Calvin protocol transaction generator
 * @Date: 2025-09-16
 */
#pragma once

#include "protocol/Calvin/Calvin.h"
#include "protocol/Calvin/CalvinTransaction.h"
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace aria {

template <class Workload>
class CalvinTxnGenerator : public TxnGenerator
{
public:
  using WorkloadType     = Workload;
  using DatabaseType     = typename WorkloadType::DatabaseType;
  using StorageType      = typename WorkloadType::StorageType;
  using TransactionType  = CalvinTransaction;
  using ContextType      = typename DatabaseType::ContextType;
  using RandomType       = typename DatabaseType::RandomType;
  using TransactionBatch = std::vector<std::unique_ptr<TransactionType>>;

  CalvinTxnGenerator(std::size_t coordinator_id, std::size_t id, DatabaseType &db, const ContextType &context,
      std::vector<StorageType> &storages, std::atomic<bool> &stop_flag)
      : coordinator_id(coordinator_id),
        id(id),
        db(db),
        context(context),
        storages(storages),
        partitioner(coordinator_id, context.coordinator_num, CalvinHelper::string_to_vint(context.replica_group)),
        workload(coordinator_id, db, random, partitioner),
        random(id),
        stop_flag(stop_flag)
  {}

  ~CalvinTxnGenerator() = default;

  void start() override
  {
    for (;;) {
      if (stop_flag.load()) {
        break;
      }

      auto new_batch = generate_transaction_batch();

      std::unique_lock<std::mutex> lock(batch_mutex);

      do {
        auto wait_result = condition.wait_for(
            lock, std::chrono::microseconds(500), [this] { return stop_flag.load() || !batch_ready; });

        if (stop_flag.load()) {
          return;
        }

        if (!batch_ready) {
          break;
        }
        // timeout
      } while (true);

      current_batch = std::move(new_batch);
      batch_ready   = true;
      condition.notify_all();
    }
  }

  std::unique_ptr<TransactionBatch> get_batch(bool wait = true)
  {
    // fast fail style
    std::unique_lock<std::mutex> lock(batch_mutex);

    if (!wait) {
      return batch_ready ? std::move(current_batch) : nullptr;
    }

    do {
      auto wait_result =
          condition.wait_for(lock, std::chrono::microseconds(500), [this] { return stop_flag.load() || batch_ready; });

      if (stop_flag.load()) {
        return nullptr;
      }

      if (batch_ready) {
        break;
      }
      // timeout
    } while (true);

    auto batch  = std::move(current_batch);
    batch_ready = false;
    condition.notify_all();
    return batch;
  }

private:
  std::unique_ptr<TransactionBatch> generate_transaction_batch()
  {
    auto batch = std::make_unique<TransactionBatch>(context.batch_size);

    for (auto i = 0u; i < context.batch_size; i++) {
      auto partition_id = random.uniform_dist(0, context.partition_num - 1);
      (*batch)[i]       = workload.next_transaction(context, partition_id, storages[i]);
      (*batch)[i]->set_id(i);
    }

    return batch;
  }

private:
  std::size_t               coordinator_id;
  std::size_t               id;
  DatabaseType             &db;
  const ContextType        &context;
  std::vector<StorageType> &storages;
  CalvinPartitioner         partitioner;
  WorkloadType              workload;
  RandomType                random;
  std::atomic<bool>        &stop_flag;

  std::mutex              batch_mutex;
  std::condition_variable condition;

  bool                              batch_ready = false;
  std::unique_ptr<TransactionBatch> current_batch;
};

}  // namespace aria