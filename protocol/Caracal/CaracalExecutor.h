//
// Created by Yi Lu on 2019-09-05.
//

#pragma once

#define GLOG_USE_GLOG_EXPORT
#include "common/Percentile.h"
#include "core/Delay.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/Caracal/Caracal.h"
#include "protocol/Caracal/CaracalContention.h"
#include "protocol/Caracal/CaracalHelper.h"
#include "protocol/Caracal/CaracalMessage.h"
#include "protocol/Caracal/CaracalPartitioner.h"
#include "protocol/Caracal/CaracalPiece.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <deque>
#include <string>
#include <thread>
#include <vector>

namespace aria {

template <class Workload>
class CaracalExecutor : public Worker
{
public:
  using WorkloadType    = Workload;
  using DatabaseType    = typename WorkloadType::DatabaseType;
  using StorageType     = typename WorkloadType::StorageType;
  using TransactionType = CaracalTransaction;
  using ContextType     = typename DatabaseType::ContextType;
  using RandomType      = typename DatabaseType::RandomType;
  using ProtocolType    = Caracal<DatabaseType>;

  using MessageType        = CaracalMessage;
  using MessageFactoryType = CaracalMessageFactory;
  using MessageHandlerType = CaracalMessageHandler;

  CaracalExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db, const ContextType &context,
      std::vector<std::unique_ptr<TransactionType>> &transactions, std::vector<StorageType> &storages,
      std::atomic<uint32_t> &epoch, std::atomic<uint32_t> &worker_status, std::atomic<uint32_t> &n_complete_workers,
      std::atomic<uint32_t> &n_started_workers, CaracalContention &contention, CaracalPieceScheduler &piece_scheduler)
      : Worker(coordinator_id, id),
        db(db),
        context(context),
        transactions(transactions),
        storages(storages),
        epoch(epoch),
        worker_status(worker_status),
        n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        contention(contention),
        piece_scheduler(piece_scheduler),
        partitioner(coordinator_id, context.coordinator_num),
        workload(coordinator_id, db, random, partitioner),
        init_transaction(false),
        random(id),  // make sure each worker has a different seed.
        sleep_random(reinterpret_cast<uint64_t>(this)),
        protocol(db, partitioner),
        delay(std::make_unique<SameDelay>(coordinator_id, context.coordinator_num, context.delay_time))
  {

    for (auto i = 0u; i < context.coordinator_num; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }
    messageHandlers = MessageHandlerType::get_message_handlers();
  }

  ~CaracalExecutor() = default;

  void start() override
  {

    LOG(INFO) << "CaracalExecutor " << id << " started. ";

    for (;;) {

      ExecutorStatus status;
      do {
        status = static_cast<ExecutorStatus>(worker_status.load());

        if (status == ExecutorStatus::EXIT) {
          LOG(INFO) << "CaracalExecutor " << id << " exits. ";
          return;
        }
      } while (status != ExecutorStatus::Caracal_Analysis);

      n_started_workers.fetch_add(1);
      generate_transactions();
      n_complete_workers.fetch_add(1);
      // wait to Execute
      while (static_cast<ExecutorStatus>(worker_status.load()) == ExecutorStatus::Caracal_Analysis) {
        std::this_thread::yield();
      }

      n_started_workers.fetch_add(1);
      insert_write_sets();
      n_complete_workers.fetch_add(1);
      // wait to insert
      while (static_cast<ExecutorStatus>(worker_status.load()) == ExecutorStatus::Caracal_Insert) {
        process_request();
      }

      n_started_workers.fetch_add(1);
      run_transactions();
      n_complete_workers.fetch_add(1);
      // wait to execute
      while (static_cast<ExecutorStatus>(worker_status.load()) == ExecutorStatus::Caracal_Execute) {
        process_request();
        process_one_piece();
      }

      n_started_workers.fetch_add(1);
      garbage_collect();
      n_complete_workers.fetch_add(1);
      // wait to gc
      while (static_cast<ExecutorStatus>(worker_status.load()) == ExecutorStatus::Caracal_GC) {
        process_request();
      }
    }
  }

  void onExit() override
  {
    LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50) << " us (50%) " << percentile.nth(75)
              << " us (75%) " << percentile.nth(95) << " us (95%) " << percentile.nth(99) << " us (99%).";
    LOG(INFO) << "Worker " << id << " Caracal writes: " << local_pieces_executed << " local pieces, "
              << remote_pieces_executed << " remote pieces, " << inline_writes << " inline writes.";
    LOG(INFO) << "Worker " << id << " Caracal remote-read suspensions: " << remote_read_suspensions << ".";
    LOG(INFO) << "Worker " << id << " Caracal deferred remote reads: " << deferred_read_requests << ".";
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

  void generate_transactions()
  {
    uint32_t cur_epoch = epoch.load();
    for (auto i = id; i < transactions.size(); i += context.worker_num) {
      if (!context.same_batch || !init_transaction) {
        // generate transaction
        auto partition_id = random.uniform_dist(0, context.partition_num - 1);
        transactions[i]   = workload.next_transaction(context, partition_id, storages[i]);
        prepare_transaction(*transactions[i]);
      } else {
        transactions[i]->reset_for_epoch();
      }
      transactions[i]->set_id(epoch, i);
    }
    init_transaction = true;
  }

  void prepare_transaction(TransactionType &txn)
  {

    setup_prepare_handlers(txn);
    // run execute to prepare read/write set
    auto result = txn.execute(id);
    if (result == TransactionResult::ABORT_NORETRY) {
      txn.abort_no_retry = true;
      n_abort_no_retry.fetch_add(1);
    }

    if (context.same_batch)
      txn.save_read_count();

    // setup handlers for execution
    setup_execute_handlers(txn);
    txn.execution_phase = true;
  }

  void setup_prepare_handlers(TransactionType &txn)
  {
    txn.local_index_read_handler = [this](
                                       std::size_t table_id, std::size_t partition_id, const void *key, void *value) {
      ITable *table = this->db.find_table(table_id, partition_id);
      CaracalHelper::read(table->search(key), value, table->value_size());
    };
    txn.setup_process_requests_in_prepare_phase();
  }

  void setup_execute_handlers(TransactionType &txn)
  {
    txn.read_handler = [this, &txn](CaracalRWKey &readKey, std::size_t tid, uint32_t key_offset) {
      auto        table_id         = readKey.get_table_id();
      auto        partition_id     = readKey.get_partition_id();
      const void *key              = readKey.get_key();
      void       *value            = readKey.get_value();
      bool        local_index_read = readKey.get_local_index_read_bit();
      bool        local_read       = false;

      if (this->partitioner.has_master_partition(partition_id))
        local_read = true;

      ITable *table = db.find_table(table_id, partition_id);
      if (local_read || local_index_read) {
        std::tuple<std::atomic<uint64_t> *, void *> row;
        if (readKey.get_tid() != nullptr && readKey.get_cached_value() != nullptr) {
          row = std::make_tuple(readKey.get_tid(), readKey.get_cached_value());
        } else {
          row = table->search_prev(key, tid);
          readKey.set_tid(std::get<0>(row));
          readKey.set_cached_value(std::get<1>(row));
        }
        std::atomic<uint64_t> &placeholder = *std::get<0>(row);

        if (context.bohm_single_spin) {
          while (!CaracalHelper::is_placeholder_ready(placeholder)) {
            process_request();
            process_one_piece(tid);
            std::this_thread::yield();
          }
          CaracalHelper::read(row, value, table->value_size());
          readKey.clear_read_request_bit();
          txn.clear_local_blocker(&placeholder);
        } else {
          bool success = CaracalHelper::is_placeholder_ready(placeholder);
          if (success) {
            CaracalHelper::read(row, value, table->value_size());
            readKey.clear_read_request_bit();
            txn.clear_local_blocker(&placeholder);
          } else {
            txn.abort_read_not_ready = true;
            txn.suspend_on(&placeholder);
          }
        }
      } else {
        auto coordinatorID = this->partitioner.master_coordinator(partition_id);
        txn.network_size +=
            MessageFactoryType::new_read_message(*(this->messages[coordinatorID]), *table, tid, key_offset, key);
        txn.distributed_transaction = true;
        txn.pendingResponses++;
      }
    };

    txn.setup_process_requests_in_execution_phase();
    txn.message_flusher = [this]() { this->flush_messages(); };
  }

  void insert_write_sets()
  {
    // A row is a shared initialization task. One worker claims the task and
    // appends all of its versions in serial order, eliminating per-version
    // lock bouncing on hot rows while retaining parallelism across rows.
    for (;;) {
      auto *group = contention.claim_initialization_group();
      if (group == nullptr)
        break;
      if (group->versions.empty())
        continue;

      auto partition_id = group->versions.front().table->partitionID();
      if (!partitioner.has_master_partition(partition_id))
        continue;

      for (auto &version : group->versions) {
        version.table->insert(version.key, version.value, version.transaction_id);
        auto &placeholder = version.table->search_metadata(version.key, version.transaction_id);
        version.write_key->set_tid(&placeholder);
      }
    }
  }

  void run_transactions()
  {
    struct TransactionState
    {
      TransactionType *transaction   = nullptr;
      bool             body_complete = false;
      bool             complete      = false;
    };
    std::vector<TransactionState> states;

    auto add_transaction = [&states](TransactionType *transaction) {
      if (!transaction->abort_no_retry)
        states.push_back({transaction, false, false});
    };
    if (context.bohm_local) {
      for (auto i = id; i < transactions.size(); i += context.worker_num) {
        if (!partitioner.has_master_partition(transactions[i]->partition_id))
          continue;
        add_transaction(transactions[i].get());
      }
    } else {
      for (auto i = id + coordinator_id * context.worker_num; i < transactions.size();
           i += context.worker_num * context.coordinator_num) {
        add_transaction(transactions[i].get());
      }
    }

    std::size_t remaining = states.size();
    std::size_t cursor    = 0;
    std::size_t completed_transactions = 0;
    while (remaining > 0) {
      bool progress = process_request() > 0;
      progress      = process_one_piece() || progress;

      auto &state = states[cursor];
      cursor      = (cursor + 1) % states.size();
      if (state.complete)
        continue;

      auto &transaction = *state.transaction;
      if (!state.body_complete) {
        if (transaction.waiting_for_remote_reads()) {
          if (!progress)
            std::this_thread::yield();
          continue;
        }
        if (transaction.locally_blocked()) {
          if (!progress)
            std::this_thread::yield();
          continue;
        }
        transaction.clear_local_blocker();
        transaction.begin_execution_attempt();
        auto result = transaction.execute(id);
        progress    = true;
        if (result == TransactionResult::READY_TO_COMMIT) {
          CHECK(transaction.execution_writes_complete());
          dispatch_writes(transaction);
          flush_messages();
          state.body_complete = true;
        } else if (result == TransactionResult::ABORT) {
          if (transaction.waiting_for_remote_reads()) {
            remote_read_suspensions++;
          } else {
            protocol.abort(transaction, messages);
          }
        } else {
          CHECK(false) << "abort no retry transactions should not be scheduled.";
        }
      }

      if (state.body_complete && transaction.pendingResponses == 0 && transaction.pendingPieces.load() == 0) {
        state.complete = true;
        remaining--;
        completed_transactions++;
        n_network_size.fetch_add(transaction.network_size);
        // Count the completed logical transaction exactly once. Piece tasks,
        // read messages, retries, and write acknowledgements are not commits.
        n_commit.fetch_add(1);
        auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - transaction.startTime)
                           .count();
        percentile.add(latency);
      }

      if (!progress)
        std::this_thread::yield();
    }
    CHECK(completed_transactions == states.size());
    flush_messages();
  }

  void garbage_collect()
  {
    for (;;) {
      auto *group = contention.claim_gc_group();
      if (group == nullptr)
        break;
      if (group->versions.empty())
        continue;
      auto &version = group->versions.front();
      if (partitioner.has_master_partition(version.table->partitionID()))
        version.table->garbage_collect(version.key);
    }
  }

  void dispatch_writes(TransactionType &transaction)
  {
    for (auto &writeKey : transaction.writeSet) {
      auto partition_id = writeKey.get_partition_id();
      if (!partitioner.has_master_partition(partition_id)) {
        protocol.send_remote_write(transaction, writeKey, messages);
        continue;
      }

      auto *table       = db.find_table(writeKey.get_table_id(), partition_id);
      auto  fingerprint = contention.key_fingerprint(db, writeKey);
      if (!contention.is_hot(fingerprint)) {
        protocol.apply_local_write(transaction, writeKey);
        inline_writes++;
        continue;
      }

      transaction.pendingPieces.fetch_add(1);
      auto target = contention.worker_for(fingerprint, transaction.id, piece_scheduler.worker_count());
      piece_scheduler.enqueue(target, CaracalPiece::local(transaction.id, fingerprint, table, &transaction, &writeKey));
    }
  }

  bool process_one_piece(uint64_t serial_id_limit = 0)
  {
    CaracalPiece piece;
    if (!piece_scheduler.try_pop(id, piece, serial_id_limit))
      return false;

    if (piece.kind == CaracalPiece::Kind::LocalWrite) {
      CHECK(piece.transaction != nullptr);
      CHECK(piece.write_key != nullptr);
      protocol.apply_local_write(*piece.transaction, *piece.write_key);
      local_pieces_executed++;
      piece_scheduler.complete();
      auto previous = piece.transaction->pendingPieces.fetch_sub(1);
      CHECK(previous > 0);
    } else {
      CHECK(piece.table != nullptr);
      CHECK(piece.key.size() == piece.table->key_size());
      CHECK(piece.value.size() == piece.table->field_size());
      auto &placeholder = piece.table->search_metadata(piece.key.data(), piece.serial_id);
      CHECK(!CaracalHelper::is_placeholder_ready(placeholder));
      piece.table->deserialize_value(
          piece.key.data(), StringPiece(piece.value.data(), piece.value.size()), piece.serial_id);
      CaracalHelper::set_placeholder_to_ready(placeholder);
      remote_pieces_executed++;
      piece_scheduler.complete();
      send_piece_write_response(piece);
    }
    return true;
  }

  void send_piece_write_response(const CaracalPiece &piece)
  {
    auto response = std::make_unique<Message>();
    response->set_source_node_id(coordinator_id);
    response->set_dest_node_id(piece.response_node);
    response->set_worker_id(piece.response_worker);
    MessageFactoryType::new_write_response_message(*response, *piece.table, piece.serial_id);
    out_queue.push(response.release());
  }

  bool enqueue_remote_write_piece(
      MessagePiece inputPiece, ITable &table, std::size_t response_node, std::size_t response_worker)
  {
    auto key_size   = table.key_size();
    auto field_size = table.field_size();
    DCHECK(
        inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size + field_size + sizeof(uint64_t));

    auto              bytes = inputPiece.toStringPiece();
    std::vector<char> key(bytes.data(), bytes.data() + key_size);
    bytes.remove_prefix(key_size);
    std::vector<char> value(bytes.data(), bytes.data() + field_size);
    bytes.remove_prefix(field_size);
    uint64_t transaction_id;
    Decoder  decoder(bytes);
    decoder >> transaction_id;
    DCHECK(decoder.size() == 0);

    auto fingerprint = CaracalContention::fingerprint(table.tableID(), table.partitionID(), key.data(), key.size());
    if (!contention.is_hot(fingerprint))
      return false;

    auto target = contention.worker_for(fingerprint, transaction_id, piece_scheduler.worker_count());
    piece_scheduler.enqueue(target,
        CaracalPiece::remote(
            transaction_id, fingerprint, &table, std::move(key), std::move(value), response_node, response_worker));
    return true;
  }

  bool defer_unready_read(MessagePiece inputPiece, ITable &table, std::size_t response_node)
  {
    auto key_size = table.key_size();
    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(uint32_t) + sizeof(uint64_t));

    auto        bytes = inputPiece.toStringPiece();
    const void *key   = bytes.data();
    bytes.remove_prefix(key_size);
    uint32_t key_offset;
    uint64_t transaction_id;
    Decoder decoder(bytes);
    decoder >> key_offset >> transaction_id;
    DCHECK(decoder.size() == 0);

    auto row = table.search_prev(key, transaction_id);
    if (CaracalHelper::is_placeholder_ready(*std::get<0>(row)))
      return false;

    deferred_reads.push_back(
        {&table, std::get<0>(row), std::get<1>(row), key_offset, transaction_id, response_node});
    deferred_read_requests++;
    return true;
  }

  std::size_t process_deferred_reads()
  {
    constexpr std::size_t scan_budget = 64;
    auto                  to_scan     = std::min(scan_budget, deferred_reads.size());
    std::size_t           completed   = 0;
    for (auto i = 0u; i < to_scan; i++) {
      auto read = deferred_reads.front();
      deferred_reads.pop_front();
      if (!CaracalHelper::is_placeholder_ready(*read.placeholder)) {
        deferred_reads.push_back(read);
        continue;
      }

      auto &response   = *messages[read.response_node];
      auto  value_size = read.table->value_size();
      auto  message_size = MessagePiece::get_header_size() + sizeof(bool) + sizeof(read.key_offset) +
                          sizeof(read.transaction_id) + value_size;
      auto message_piece_header = MessagePiece::construct_message_piece_header(
          static_cast<uint32_t>(CaracalMessage::READ_RESPONSE), message_size, read.table->tableID(),
          read.table->partitionID());
      bool    success = true;
      Encoder encoder(response.data);
      encoder << message_piece_header << success << read.key_offset << read.transaction_id;
      response.data.append(value_size, 0);
      void *destination = &response.data[0] + response.data.size() - value_size;
      CaracalHelper::read(std::make_tuple(read.placeholder, read.value), destination, value_size);
      response.flush();
      completed++;
    }
    if (completed > 0)
      flush_messages();
    return completed;
  }

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

        if (type == static_cast<uint32_t>(CaracalMessage::READ_REQUEST) &&
            defer_unready_read(messagePiece, *table, message->get_source_node_id())) {
          continue;
        }
        if (type == static_cast<uint32_t>(CaracalMessage::WRITE_REQUEST) &&
            enqueue_remote_write_piece(messagePiece, *table, message->get_source_node_id(), message->get_worker_id())) {
          continue;
        }
        messageHandlers[type](messagePiece, *messages[message->get_source_node_id()], *table, transactions);
      }

      size += message->get_message_count();
      flush_messages();
    }
    size += process_deferred_reads();
    return size;
  }

private:
  struct DeferredRead
  {
    ITable                *table;
    std::atomic<uint64_t> *placeholder;
    void                  *value;
    uint32_t               key_offset;
    uint64_t               transaction_id;
    std::size_t            response_node;
  };

  DatabaseType                                  &db;
  const ContextType                             &context;
  std::vector<std::unique_ptr<TransactionType>> &transactions;
  std::vector<StorageType>                      &storages;
  std::atomic<uint32_t>                         &epoch, &worker_status;
  std::atomic<uint32_t>                         &n_complete_workers, &n_started_workers;
  CaracalContention                             &contention;
  CaracalPieceScheduler                         &piece_scheduler;
  CaracalPartitioner                             partitioner;
  WorkloadType                                   workload;
  bool                                           init_transaction;
  RandomType                                     random, sleep_random;
  ProtocolType                                   protocol;
  std::unique_ptr<Delay>                         delay;
  Percentile<int64_t>                            percentile;
  std::vector<std::unique_ptr<Message>>          messages;
  std::vector<std::function<void(MessagePiece, Message &, ITable &, std::vector<std::unique_ptr<TransactionType>> &)>>
                           messageHandlers;
  LockfreeQueue<Message *> in_queue, out_queue;
  std::deque<DeferredRead> deferred_reads;
  uint64_t                 local_pieces_executed  = 0;
  uint64_t                 remote_pieces_executed = 0;
  uint64_t                 inline_writes          = 0;
  uint64_t                 remote_read_suspensions = 0;
  uint64_t                 deferred_read_requests  = 0;
};
}  // namespace aria
