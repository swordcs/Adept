

#pragma once

#define GLOG_USE_GLOG_EXPORT
#include "common/Percentile.h"
#include "core/Delay.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/Queue/Queue.h"
#include "protocol/Queue/QueueHelper.h"
#include "protocol/Queue/QueueMessage.h"
#include "protocol/Queue/QueuePartitioner.h"

#include <chrono>
#include <thread>

namespace aria {

template <class Workload>
class QueueExecutor : public Worker
{
public:
  using WorkloadType       = Workload;
  using DatabaseType       = typename WorkloadType::DatabaseType;
  using StorageType        = typename WorkloadType::StorageType;
  using TransactionType    = QueueTransaction;
  using ContextType        = typename DatabaseType::ContextType;
  using RandomType         = typename DatabaseType::RandomType;
  using ProtocolType       = Queue<DatabaseType>;
  using MessageType        = QueueMessage;
  using MessageFactoryType = QueueMessageFactory;
  using MessageHandlerType = QueueMessageHandler;

  QueueExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db, const ContextType &context,
      std::vector<std::unique_ptr<TransactionType>> *&transactions_ptr, std::vector<StorageType> &storages,
      std::atomic<uint32_t> &lock_manager_status, std::atomic<uint32_t> &worker_status,
      std::atomic<uint32_t> &n_complete_workers, std::atomic<uint32_t> &n_started_workers)
      : Worker(coordinator_id, id),
        db(db),
        context(context),
        transactions_ptr(transactions_ptr),
        storages(storages),
        lock_manager_status(lock_manager_status),
        worker_status(worker_status),
        n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        partitioner(coordinator_id, context.coordinator_num, QueueHelper::string_to_vint(context.replica_group)),
        workload(coordinator_id, db, random, partitioner),
        n_workers(context.worker_num - n_lock_manager),
        init_transaction(false),
        random(id),  // make sure each worker has a different seed.
        protocol(db, partitioner),
        delay(std::make_unique<SameDelay>(coordinator_id, context.coordinator_num, context.delay_time))
  {

    for (auto i = 0u; i < context.coordinator_num; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }

    messageHandlers = MessageHandlerType::get_message_handlers();
    CHECK(n_workers > 0);
  }

  ~QueueExecutor() = default;

  void start() override
  {
    LOG(INFO) << "QueueExecutor " << id << " started. ";

    for (;;) {

      ExecutorStatus status;
      do {
        status = static_cast<ExecutorStatus>(worker_status.load());

        if (status == ExecutorStatus::EXIT) {
          LOG(INFO) << "QueueExecutor " << id << " exits. ";
          return;
        }
      } while (status != ExecutorStatus::Analysis);

      n_started_workers.fetch_add(1);
      std::size_t commit = analyze_transactions();
      n_commit.fetch_add(commit);
      n_complete_workers.fetch_add(1);

      // wait to Execute
      while (static_cast<ExecutorStatus>(worker_status.load()) == ExecutorStatus::Analysis) {
        std::this_thread::yield();
      }

      n_started_workers.fetch_add(1);
      // work as executor
      run_transactions();
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
              << " us (75%) " << percentile.nth(95) << " us (95%) " << percentile.nth(99) << " us (99%).";
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

  std::size_t analyze_transactions()
  {
    std::size_t commit = 0;
    for (auto i = id; i < (*transactions_ptr).size(); i += context.worker_num) {
      (*transactions_ptr)[i]->startTime = std::chrono::steady_clock::now();
      // prepare transaction (analyze read/write set, lock read/write set)
      prepare_transaction(*(*transactions_ptr)[i]);
      (*transactions_ptr)[i]->build_pieces(context.worker_num);
      if (!(*transactions_ptr)[i]->abort_no_retry) {
        commit++;
      }
    }
    return commit;
  }

  void prepare_transaction(TransactionType &txn)
  {

    setup_prepare_handlers(txn);
    // run execute to prepare read/write set
    auto result = txn.execute(id);
    if (result == TransactionResult::ABORT_NORETRY)
      txn.abort_no_retry = true;

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

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readkey = readSet[i];
      if (readkey.get_local_index_read_bit())
        continue;
      auto partitionID = readkey.get_partition_id();
      if (readkey.get_write_lock_bit())
        active_coordinators[partitioner.master_coordinator(partitionID)] = true;
    }
  }

  void run_transactions()
  {

    for (auto i = 0u; i < (*transactions_ptr).size(); i++) {
      auto transaction = (*transactions_ptr)[i].get();
      auto result      = transaction->execute(id);
      n_network_size.fetch_add(transaction->network_size.load());

      if (transaction->id % context.worker_num != id) {
        continue;
      }

      if (transaction->abort_no_retry) {
        n_abort_no_retry.fetch_add(1);
      } else if (result == TransactionResult::READY_TO_COMMIT) {
        protocol.commit(*transaction);
        auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - transaction->startTime)
                           .count();
        percentile.add(latency);
      } else if (result == TransactionResult::ABORT) {
        protocol.abort(*transaction);
      } else {
        CHECK(false) << "abort no retry transaction should not be scheduled.";
      }
    }
  }

  void setup_execute_handlers(TransactionType &txn)
  {
    txn.read_handler = [this, &txn](std::size_t worker_id,
                           std::size_t          table_id,
                           std::size_t          partition_id,
                           std::size_t          id,
                           uint32_t             key_offset,
                           const void          *key,
                           void                *value) {
      auto *worker = this->all_executors[worker_id];
      if (worker->partitioner.has_master_partition(partition_id)) {
        ITable *table = worker->db.find_table(table_id, partition_id);
        QueueHelper::read(table->search(key), value, table->value_size());

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
    txn.remote_request_handler = [this](std::size_t worker_id) {
      auto *worker = this->all_executors[worker_id];
      return worker->process_request();
    };
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
      QueueHelper::read(table->search(key), value, table->value_size());
    };
    txn.setup_process_requests_in_prepare_phase();
  }

  void set_all_executors(const std::vector<QueueExecutor *> &executors) { all_executors = executors; }

  std::size_t process_request()
  {

    std::size_t size = 0;

    while (!in_queue.empty()) {
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
  QueuePartitioner                                partitioner;
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
  std::vector<QueueExecutor *>     all_executors;
};
}  // namespace aria
