//
// Created by Yi Lu on 9/13/18.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "core/ControlMessage.h"
#include "core/Table.h"
#include "protocol/Adept/AdeptRWKey.h"
#include "protocol/Adept/AdeptTransaction.h"

namespace aria {

enum class AdeptMessage
{
  READ_VALUE = static_cast<int>(ControlMessage::NFIELDS),
  NFIELDS
};

class AdeptMessageFactory
{

public:
  static std::size_t new_read_value_message(
      Message &message, ITable &table, uint64_t tid, uint32_t key_offset, const void *value)
  {

    auto value_size = table.value_size();

    auto message_size = MessagePiece::get_header_size() + sizeof(tid) + sizeof(key_offset) + value_size;

    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(AdeptMessage::READ_VALUE), message_size, table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << tid << key_offset;
    encoder.write_n_bytes(value, value_size);
    message.flush();
    return message_size;
  }
};

class AdeptMessageHandler
{
  using Transaction = AdeptTransaction;

public:
  static void read_value_handler(
      MessagePiece inputPiece, Message &responseMessage, ITable &table, std::vector<std::unique_ptr<Transaction>> &txns)
  {
    CHECK(inputPiece.get_message_type() == static_cast<uint32_t>(AdeptMessage::READ_VALUE));
    auto table_id     = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    CHECK(table_id == table.tableID());
    CHECK(partition_id == table.partitionID());
    auto value_size = table.value_size();

    uint64_t tid;
    uint32_t key_offset;

    CHECK(inputPiece.get_message_length() ==
          MessagePiece::get_header_size() + sizeof(tid) + sizeof(key_offset) + value_size);

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder     dec(stringPiece);
    dec >> tid >> key_offset;

    uint32_t tid_offset = AdeptHelper::get_tid_offset(tid);
    CHECK(tid_offset < txns.size());
    CHECK(txns[tid_offset] != nullptr);
    CHECK(txns[tid_offset]->id == tid);
    CHECK(key_offset < txns[tid_offset]->readSet.size());
    AdeptRWKey &readKey = txns[tid_offset]->readSet[key_offset];
    CHECK(readKey.get_table_id() == table_id);
    CHECK(readKey.get_partition_id() == partition_id);
    CHECK(!readKey.get_blind_bit());
    dec.read_n_bytes(readKey.get_value(), value_size);
    txns[tid_offset]->complete_remote_read();
  }

  static std::vector<
      std::function<void(MessagePiece, Message &, ITable &, std::vector<std::unique_ptr<Transaction>> &)>>
      get_message_handlers()
  {
    std::vector<std::function<void(MessagePiece, Message &, ITable &, std::vector<std::unique_ptr<Transaction>> &)>> v;
    v.resize(static_cast<int>(ControlMessage::NFIELDS));
    v.push_back(read_value_handler);
    return v;
  }
};

}  // namespace aria
