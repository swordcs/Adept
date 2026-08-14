#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "core/ControlMessage.h"
#include "core/Table.h"
#include "protocol/Pwv/PwvTransaction.h"

namespace aria {

enum class PwvMessage
{
  PIECE_RESULT = static_cast<int>(ControlMessage::NFIELDS),
  NFIELDS
};

class PwvMessageFactory
{
public:
  static std::size_t new_piece_result(Message &message, ITable &table, uint32_t txn_id, uint32_t piece_id,
      uint32_t read_offset, uint32_t stage, const void *value)
  {
    auto value_size  = table.value_size();
    auto message_size = MessagePiece::get_header_size() + sizeof(txn_id) + sizeof(piece_id) + sizeof(read_offset) +
                        sizeof(stage) + value_size;
    auto header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(PwvMessage::PIECE_RESULT), message_size, table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << header << txn_id << piece_id << read_offset << stage;
    encoder.write_n_bytes(value, value_size);
    message.flush();
    return message_size;
  }
};

class PwvMessageHandler
{
public:
  static void piece_result(MessagePiece input, ITable &table, std::vector<std::unique_ptr<PwvTransaction>> &txns)
  {
    CHECK(input.get_message_type() == static_cast<uint32_t>(PwvMessage::PIECE_RESULT));
    auto value_size = table.value_size();
    CHECK(input.get_message_length() == MessagePiece::get_header_size() + 4 * sizeof(uint32_t) + value_size);

    uint32_t txn_id, piece_id, read_offset, stage;
    Decoder dec(input.toStringPiece());
    dec >> txn_id >> piece_id >> read_offset >> stage;
    CHECK(txn_id < txns.size());
    CHECK(piece_id < txns[txn_id]->pieces.size());
    CHECK(read_offset < txns[txn_id]->pieces[piece_id]->readSet.size());
    CHECK(stage < txns[txn_id]->pending_results.size());
    dec.read_n_bytes(txns[txn_id]->pieces[piece_id]->readSet[read_offset].get_value(), value_size);
    CHECK(dec.size() == 0);

    auto previous = txns[txn_id]->pending_results[stage].fetch_sub(1);
    CHECK(previous > 0);
  }
};

}  // namespace aria
