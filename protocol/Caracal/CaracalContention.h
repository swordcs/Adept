#pragma once

#include "core/Table.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace aria {

class CaracalRWKey;

class CaracalContention
{
public:
  struct PendingVersion
  {
    ITable       *table          = nullptr;
    const void   *key            = nullptr;
    const void   *value          = nullptr;
    uint64_t      transaction_id = 0;
    CaracalRWKey *write_key      = nullptr;
  };

  struct InitializationGroup
  {
    uint64_t                    fingerprint = 0;
    std::vector<unsigned char>  key_bytes;
    std::vector<PendingVersion> versions;
  };

  struct Placement
  {
    bool        hot          = false;
    std::size_t partition_id = 0;
    uint64_t    fingerprint  = 0;
    std::size_t frequency    = 0;
  };

  template <class Database, class Transaction>
  void analyze(Database &db, const std::vector<std::unique_ptr<Transaction>> &transactions, std::size_t threshold)
  {
    threshold_ = threshold;
    frequencies_.clear();
    ranges_.clear();
    initialization_groups_.clear();

    std::unordered_map<std::string, std::size_t> group_by_key;
    for (const auto &txn : transactions) {
      if (!txn || txn->abort_no_retry)
        continue;
      for (auto &write_key : txn->writeSet) {
        auto *table = db.find_table(write_key.get_table_id(), write_key.get_partition_id());
        auto  key =
            fingerprint(write_key.get_table_id(), write_key.get_partition_id(), write_key.get_key(), table->key_size());
        frequencies_[key]++;

        auto identity = key_identity(
            write_key.get_table_id(), write_key.get_partition_id(), write_key.get_key(), table->key_size());
        auto [it, inserted] = group_by_key.emplace(identity, initialization_groups_.size());
        if (inserted) {
          InitializationGroup group;
          group.fingerprint = key;
          auto *bytes       = static_cast<const unsigned char *>(write_key.get_key());
          group.key_bytes.assign(bytes, bytes + table->key_size());
          initialization_groups_.push_back(std::move(group));
        }
        initialization_groups_[it->second].versions.push_back(
            {table, write_key.get_key(), write_key.get_value(), txn->id, &write_key});
      }
    }

    for (auto &group : initialization_groups_) {
      std::stable_sort(group.versions.begin(), group.versions.end(), [](const auto &left, const auto &right) {
        return left.transaction_id < right.transaction_id;
      });
    }
    std::stable_sort(
        initialization_groups_.begin(), initialization_groups_.end(), [](const auto &left, const auto &right) {
          if (left.fingerprint != right.fingerprint)
            return left.fingerprint < right.fingerprint;
          return left.key_bytes < right.key_bytes;
        });

    total_hot_weight_ = 0;
    for (const auto &group : initialization_groups_) {
      auto count = group.versions.size();
      if (threshold_ > 0 && count >= threshold_) {
        ranges_[group.fingerprint] = {total_hot_weight_, count};
        total_hot_weight_ += count;
      }
    }
    initialization_cursor_.store(0);
    gc_cursor_.store(0);
  }

  template <class Database, class RWKey>
  uint64_t key_fingerprint(Database &db, const RWKey &key) const
  {
    auto *table = db.find_table(key.get_table_id(), key.get_partition_id());
    return fingerprint(key.get_table_id(), key.get_partition_id(), key.get_key(), table->key_size());
  }

  std::size_t frequency(uint64_t key) const
  {
    auto it = frequencies_.find(key);
    return it == frequencies_.end() ? 0 : it->second;
  }

  bool is_hot(uint64_t key) const { return threshold_ > 0 && frequency(key) >= threshold_; }

  std::size_t worker_for(uint64_t key, uint64_t transaction_id, std::size_t worker_count) const
  {
    CHECK(worker_count > 0);
    auto range = ranges_.find(key);
    if (range == ranges_.end() || total_hot_weight_ == 0)
      return mix(key ^ transaction_id) % worker_count;

    auto point = mix(key ^ transaction_id) % range->second.weight;
    auto slot  = range->second.start + point;
    return std::min<std::size_t>(worker_count - 1, static_cast<std::size_t>((slot * worker_count) / total_hot_weight_));
  }

  InitializationGroup *claim_initialization_group()
  {
    auto index = initialization_cursor_.fetch_add(1);
    if (index >= initialization_groups_.size())
      return nullptr;
    return &initialization_groups_[index];
  }

  void reset_gc_cursor() { gc_cursor_.store(0); }

  InitializationGroup *claim_gc_group()
  {
    auto index = gc_cursor_.fetch_add(1);
    if (index >= initialization_groups_.size())
      return nullptr;
    return &initialization_groups_[index];
  }

  template <class Database, class Transaction>
  Placement placement(Database &db, const Transaction &txn) const
  {
    Placement result;
    for (const auto &write_key : txn.writeSet) {
      auto key   = key_fingerprint(db, write_key);
      auto count = frequency(key);
      if (count > result.frequency) {
        result.partition_id = write_key.get_partition_id();
        result.fingerprint  = key;
        result.frequency    = count;
      }
    }
    result.hot = threshold_ > 0 && result.frequency >= threshold_;
    return result;
  }

  static uint64_t fingerprint(std::size_t table_id, std::size_t partition_id, const void *key, std::size_t key_size)
  {
    constexpr uint64_t offset    = 1469598103934665603ull;
    constexpr uint64_t prime     = 1099511628211ull;
    uint64_t           hash      = offset;
    auto               mix_bytes = [&hash](const unsigned char *bytes, std::size_t size) {
      for (std::size_t i = 0; i < size; i++) {
        hash ^= bytes[i];
        hash *= prime;
      }
    };
    mix_bytes(reinterpret_cast<const unsigned char *>(&table_id), sizeof(table_id));
    mix_bytes(reinterpret_cast<const unsigned char *>(&partition_id), sizeof(partition_id));
    mix_bytes(static_cast<const unsigned char *>(key), key_size);
    return hash;
  }

private:
  struct Range
  {
    std::size_t start  = 0;
    std::size_t weight = 0;
  };

  static uint64_t mix(uint64_t value)
  {
    value += 0x9e3779b97f4a7c15ull;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ull;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebull;
    return value ^ (value >> 31);
  }

  static std::string key_identity(std::size_t table_id, std::size_t partition_id, const void *key, std::size_t key_size)
  {
    std::string identity;
    identity.reserve(sizeof(table_id) + sizeof(partition_id) + key_size);
    identity.append(reinterpret_cast<const char *>(&table_id), sizeof(table_id));
    identity.append(reinterpret_cast<const char *>(&partition_id), sizeof(partition_id));
    identity.append(static_cast<const char *>(key), key_size);
    return identity;
  }

  std::size_t                               threshold_ = 0;
  std::unordered_map<uint64_t, std::size_t> frequencies_;
  std::unordered_map<uint64_t, Range>       ranges_;
  std::vector<InitializationGroup>          initialization_groups_;
  std::size_t                               total_hot_weight_ = 0;
  std::atomic<std::size_t>                  initialization_cursor_{0};
  std::atomic<std::size_t>                  gc_cursor_{0};
};

}  // namespace aria
