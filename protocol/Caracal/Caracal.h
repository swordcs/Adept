//
// Created by Yi Lu on 2019-09-05.
//

#pragma once

#include "core/Table.h"
#include "protocol/Caracal/CaracalHelper.h"
#include "protocol/Caracal/CaracalMessage.h"
#include "protocol/Caracal/CaracalPartitioner.h"
#include "protocol/Caracal/CaracalTransaction.h"

#include <string>

namespace aria {

template <class Database>
class Caracal
{
public:
  using DatabaseType    = Database;
  using MetaDataType    = std::atomic<uint64_t>;
  using ContextType     = typename DatabaseType::ContextType;
  using MessageType     = CaracalMessage;
  using TransactionType = CaracalTransaction;

  using MessageFactoryType = CaracalMessageFactory;
  using MessageHandlerType = CaracalMessageHandler;

  Caracal(DatabaseType &db, CaracalPartitioner &partitioner) : db(db), partitioner(partitioner) {}

  void abort(TransactionType &txn, std::vector<std::unique_ptr<Message>> &messages)
  {
    txn.load_read_count();
    txn.clear_execution_bit();
    txn.abort_read_not_ready = false;
    txn.begin_execution_attempt();
  }

  void apply_local_write(TransactionType &txn, CaracalRWKey &writeKey)
  {
    auto *table       = db.find_table(writeKey.get_table_id(), writeKey.get_partition_id());
    auto *placeholder = writeKey.get_tid();
    if (placeholder == nullptr)
      placeholder = &table->search_metadata(writeKey.get_key(), txn.id);
    CHECK(!CaracalHelper::is_placeholder_ready(*placeholder));

    // ITable::update publishes the ready marker before assigning the value in
    // the legacy MVCC table. Serialize/deserialize lets Caracal publish only
    // after the complete value is visible.
    thread_local std::string encoded;
    encoded.clear();
    Encoder encoder(encoded);
    table->serialize_value(encoder, writeKey.get_value());
    table->deserialize_value(writeKey.get_key(), StringPiece(encoded), txn.id);
    CaracalHelper::set_placeholder_to_ready(*placeholder);
  }

  void send_remote_write(TransactionType &txn, CaracalRWKey &writeKey, std::vector<std::unique_ptr<Message>> &messages)
  {
    auto  partitionId   = writeKey.get_partition_id();
    auto *table         = db.find_table(writeKey.get_table_id(), partitionId);
    auto  coordinatorID = partitioner.master_coordinator(partitionId);
    txn.network_size += MessageFactoryType::new_write_message(
        *messages[coordinatorID], *table, txn.id, writeKey.get_key(), writeKey.get_value());
    txn.distributed_transaction = true;
    txn.pendingResponses++;
  }

private:
  DatabaseType       &db;
  CaracalPartitioner &partitioner;
};
}  // namespace aria
