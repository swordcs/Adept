#pragma once

#include "core/Table.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace aria {

class CaracalRWKey;
class CaracalTransaction;

struct CaracalPiece
{
  enum class Kind
  {
    LocalWrite,
    RemoteWrite
  };

  static CaracalPiece local(
      uint64_t serial_id, uint64_t fingerprint, ITable *table, CaracalTransaction *transaction, CaracalRWKey *write_key)
  {
    CaracalPiece piece;
    piece.kind        = Kind::LocalWrite;
    piece.serial_id   = serial_id;
    piece.fingerprint = fingerprint;
    piece.table       = table;
    piece.transaction = transaction;
    piece.write_key   = write_key;
    return piece;
  }

  static CaracalPiece remote(uint64_t serial_id, uint64_t fingerprint, ITable *table, std::vector<char> key,
      std::vector<char> value, std::size_t response_node, std::size_t response_worker)
  {
    CaracalPiece piece;
    piece.kind            = Kind::RemoteWrite;
    piece.serial_id       = serial_id;
    piece.fingerprint     = fingerprint;
    piece.table           = table;
    piece.key             = std::move(key);
    piece.value           = std::move(value);
    piece.response_node   = response_node;
    piece.response_worker = response_worker;
    return piece;
  }

  Kind                kind        = Kind::LocalWrite;
  uint64_t            serial_id   = 0;
  uint64_t            fingerprint = 0;
  ITable             *table       = nullptr;
  CaracalTransaction *transaction = nullptr;
  CaracalRWKey       *write_key   = nullptr;
  std::vector<char>   key;
  std::vector<char>   value;
  std::size_t         response_node   = 0;
  std::size_t         response_worker = 0;
};

class CaracalPieceQueue
{
public:
  void push(CaracalPiece piece)
  {
    std::lock_guard<std::mutex> guard(mutex_);
    pieces_.emplace(std::make_pair(piece.serial_id, next_sequence_++), std::move(piece));
  }

  bool try_pop(CaracalPiece &piece, uint64_t serial_id_limit = 0)
  {
    std::lock_guard<std::mutex> guard(mutex_);
    if (pieces_.empty())
      return false;

    auto it = pieces_.begin();
    if (serial_id_limit != 0 && it->first.first >= serial_id_limit)
      return false;

    piece = std::move(it->second);
    pieces_.erase(it);
    return true;
  }

  void clear()
  {
    std::lock_guard<std::mutex> guard(mutex_);
    pieces_.clear();
    next_sequence_ = 0;
  }

private:
  std::mutex                                            mutex_;
  uint64_t                                              next_sequence_ = 0;
  std::map<std::pair<uint64_t, uint64_t>, CaracalPiece> pieces_;
};

class CaracalPieceScheduler
{
public:
  explicit CaracalPieceScheduler(std::size_t worker_count) : queues_(worker_count)
  {
    for (auto &queue : queues_)
      queue = std::make_unique<CaracalPieceQueue>();
  }

  void reset()
  {
    CHECK(outstanding_.load() == 0);
    for (auto &queue : queues_)
      queue->clear();
  }

  void enqueue(std::size_t worker_id, CaracalPiece piece)
  {
    CHECK(worker_id < queues_.size());
    outstanding_.fetch_add(1);
    queues_[worker_id]->push(std::move(piece));
  }

  bool try_pop(std::size_t worker_id, CaracalPiece &piece, uint64_t serial_id_limit = 0)
  {
    CHECK(worker_id < queues_.size());
    return queues_[worker_id]->try_pop(piece, serial_id_limit);
  }

  void complete()
  {
    auto previous = outstanding_.fetch_sub(1);
    CHECK(previous > 0);
  }

  std::size_t worker_count() const { return queues_.size(); }

  uint64_t outstanding() const { return outstanding_.load(); }

private:
  std::vector<std::unique_ptr<CaracalPieceQueue>> queues_;
  std::atomic<uint64_t>                           outstanding_{0};
};

}  // namespace aria
