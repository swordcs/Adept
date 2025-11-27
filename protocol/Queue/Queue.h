

#pragma once

#include "core/Table.h"
#include "protocol/Queue/QueueHelper.h"
#include "protocol/Queue/QueueMessage.h"
#include "protocol/Queue/QueuePartitioner.h"
#include "protocol/Queue/QueueTransaction.h"

namespace aria {

template <class Database>
class Queue
{
public:
  using DatabaseType    = Database;
  using MetaDataType    = std::atomic<uint64_t>;
  using ContextType     = typename DatabaseType::ContextType;
  using MessageType     = QueueMessage;
  using TransactionType = QueueTransaction;

  using MessageFactoryType = QueueMessageFactory;
  using MessageHandlerType = QueueMessageHandler;

  Queue(DatabaseType &db, QueuePartitioner &partitioner) : db(db), partitioner(partitioner) {}

  void abort(TransactionType &txn) {}

  bool commit(TransactionType &txn)
  {
    // write to db
    write(txn);
    return true;
  }

  void write(TransactionType &txn)
  {

    auto &writeSet = txn.writeSet;
    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey    = writeSet[i];
      auto  tableId     = writeKey.get_table_id();
      auto  partitionId = writeKey.get_partition_id();
      auto  table       = db.find_table(tableId, partitionId);

      if (!partitioner.has_master_partition(partitionId)) {
        continue;
      }

      auto key   = writeKey.get_key();
      auto value = writeKey.get_value();
      table->update(key, value);
    }
  }

private:
  DatabaseType     &db;
  QueuePartitioner &partitioner;
};
}  // namespace aria