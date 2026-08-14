//
// Created by Yi Lu on 1/14/20.
//

#pragma once

#define GLOG_USE_GLOG_EXPORT
#include "common/Percentile.h"
#include "core/Delay.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/Pwv/PwvHelper.h"
#include "protocol/Pwv/PwvMessage.h"
#include "protocol/Pwv/PwvTransaction.h"
#include "protocol/Pwv/PwvWorkload.h"

#include <chrono>
#include <thread>

namespace aria {

template <class Database>
class PwvExecutor : public Worker
{
public:
  using DatabaseType    = Database;
  using WorkloadType    = PwvWorkload<Database>;
  using StorageType     = typename DatabaseType::StorageType;
  using TransactionType = PwvTransaction;
  using ContextType     = typename DatabaseType::ContextType;
  using RandomType      = typename DatabaseType::RandomType;

  PwvExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db, const ContextType &context,
      std::vector<std::unique_ptr<TransactionType>> &transactions, std::vector<StorageType> &storages,
      std::atomic<uint32_t> &epoch, std::atomic<uint32_t> &worker_status, std::atomic<uint32_t> &n_complete_workers,
      std::atomic<uint32_t> &n_started_workers)
      : Worker(coordinator_id, id),
        db(db),
        context(context),
        transactions(transactions),
        storages(storages),
        execution_level(epoch),
        worker_status(worker_status),
        n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        workload(db, random),
        random(id),
        delay(std::make_unique<SameDelay>(coordinator_id, context.coordinator_num, context.delay_time))
  {}

  ~PwvExecutor() = default;

  void start() override
  {
    LOG(INFO) << "PwvExecutor " << id << " started.";

    auto previous_status = ExecutorStatus::STOP;
    for (;;) {
      ExecutorStatus status = ExecutorStatus::STOP;
      do {
        status = static_cast<ExecutorStatus>(worker_status.load());
        if (status == ExecutorStatus::EXIT) {
          LOG(INFO) << "PwvExecutor " << id << " exits.";
          return;
        }
        if (process_request() == 0)
          std::this_thread::yield();
      } while (status == previous_status || status == ExecutorStatus::STOP);

      previous_status = status;
      n_started_workers.fetch_add(1);
      if (status == ExecutorStatus::Pwv_Analysis) {
        generate_transactions();
      } else if (status == ExecutorStatus::Pwv_Execute) {
        run_stage(0);
      } else {
        CHECK(status == ExecutorStatus::Pwv_Execute_Stage2);
        run_stage(1);
      }
      n_complete_workers.fetch_add(1);
    }
  }

  void onExit() override
  {
    LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50) << " us (50%) " << percentile.nth(75)
              << " us (75%) " << percentile.nth(95) << " us (95%) " << percentile.nth(99) << " us (99%).";
  }

  void push_message(Message *message) override { in_queue.push(message); }

  Message *pop_message() override
  {
    if (out_queue.empty())
      return nullptr;
    Message *message = out_queue.front();
    if (delay->delay_enabled()) {
      auto now = std::chrono::steady_clock::now();
      if (std::chrono::duration_cast<std::chrono::microseconds>(now - message->time).count() < delay->message_delay())
        return nullptr;
    }
    bool ok = out_queue.pop();
    CHECK(ok);
    return message;
  }

  void generate_transactions()
  {
    for (auto i = id; i < transactions.size(); i += context.worker_num) {
      auto partition_id = random.uniform_dist(0, context.partition_num - 1);
      transactions[i]   = workload.next_transaction(context, partition_id, storages[i]);
      transactions[i]->build_pieces();
      transactions[i]->startTime = std::chrono::steady_clock::now();
      transactions[i]->pending_results[0].store(0);
      transactions[i]->pending_results[1].store(0);

      if (transactions[i]->is_aborted())
        continue;

      auto home = transactions[i]->partition_id % context.coordinator_num;
      if (home != coordinator_id)
        continue;
      for (auto piece_id = 0u; piece_id < transactions[i]->pieces.size(); piece_id++) {
        auto &piece = *transactions[i]->pieces[piece_id];
        if (piece.piece_partition_id() % context.coordinator_num == home)
          continue;
        auto stage = transactions[i]->piece_stage(piece_id);
        CHECK(stage < 2);
        transactions[i]->pending_results[stage].fetch_add(piece.readSet.size());
      }
    }
  }

  void send_piece_results(std::size_t txn_id, std::size_t piece_id, std::size_t stage, PwvStatement &piece)
  {
    auto home = transactions[txn_id]->partition_id % context.coordinator_num;
    if (home == coordinator_id)
      return;

    for (auto read_offset = 0u; read_offset < piece.readSet.size(); read_offset++) {
      auto &read_key = piece.readSet[read_offset];
      auto *table    = db.find_table(read_key.get_table_id(), read_key.get_partition_id());
      auto  message  = std::make_unique<Message>();
      message->set_source_node_id(coordinator_id);
      message->set_dest_node_id(home);
      message->set_worker_id(txn_id % context.worker_num);
      auto bytes = PwvMessageFactory::new_piece_result(*message,
          *table,
          static_cast<uint32_t>(txn_id),
          static_cast<uint32_t>(piece_id),
          static_cast<uint32_t>(read_offset),
          static_cast<uint32_t>(stage),
          read_key.get_value());
      n_network_size.fetch_add(bytes);
      out_queue.push(message.release());
    }
  }

  void run_stage(std::size_t stage)
  {
    auto level = execution_level.load();
    for (auto txn_id = 0u; txn_id < transactions.size(); txn_id++) {
      auto &txn = *transactions[txn_id];
      if (txn.dependency_level != level)
        continue;
      if (txn.is_aborted())
        continue;
      for (auto piece_id = 0u; piece_id < txn.pieces.size(); piece_id++) {
        if (txn.piece_stage(piece_id) != stage)
          continue;
        auto &piece        = *txn.pieces[piece_id];
        auto  partition_id = piece.piece_partition_id();
        if (partition_id % context.coordinator_num != coordinator_id)
          continue;
        // A partition has one deterministic execution worker. Scanning txn_id
        // in order preserves PWV's piece order without tuple locks.
        auto local_worker = (partition_id / context.coordinator_num) % context.worker_num;
        if (local_worker != id)
          continue;

        piece.execute();
        send_piece_results(txn_id, piece_id, stage, piece);
      }
    }

    // The transaction's home worker is its rendezvous point. It does not let
    // the distributed stage finish until every remote piece result is visible.
    for (auto txn_id = id; txn_id < transactions.size(); txn_id += context.worker_num) {
      auto &txn = *transactions[txn_id];
      if (txn.dependency_level != level)
        continue;
      if (txn.is_aborted())
        continue;
      if (txn.partition_id % context.coordinator_num != coordinator_id)
        continue;
      while (txn.pending_results[stage].load() > 0) {
        if (process_request() == 0)
          std::this_thread::yield();
      }

      if (stage == 1) {
        auto latency =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - txn.startTime)
                .count();
        percentile.add(latency);
      }
    }
  }

  std::size_t process_request()
  {
    std::size_t size = 0;
    while (!in_queue.empty()) {
      std::unique_ptr<Message> message(in_queue.front());
      bool                     ok = in_queue.pop();
      CHECK(ok);
      for (auto it = message->begin(); it != message->end(); it++) {
        MessagePiece piece = *it;
        CHECK(piece.get_message_type() == static_cast<uint32_t>(PwvMessage::PIECE_RESULT));
        auto *table = db.find_table(piece.get_table_id(), piece.get_partition_id());
        PwvMessageHandler::piece_result(piece, *table, transactions);
        n_network_size.fetch_add(piece.get_message_length());
        size++;
      }
    }
    return size;
  }

private:
  DatabaseType                                  &db;
  const ContextType                             &context;
  std::vector<std::unique_ptr<TransactionType>> &transactions;
  std::vector<StorageType>                      &storages;
  std::atomic<uint32_t>                         &execution_level, &worker_status;
  std::atomic<uint32_t>                         &n_complete_workers, &n_started_workers;
  WorkloadType                                   workload;
  RandomType                                     random;
  std::unique_ptr<Delay>                         delay;
  Percentile<int64_t>                            percentile;
  LockfreeQueue<Message *>                       in_queue, out_queue;
};

}  // namespace aria
