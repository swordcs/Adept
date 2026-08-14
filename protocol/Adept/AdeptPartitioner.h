//
// Created by Yi Lu on 2019-09-05.
//

#pragma once

#include "core/Partitioner.h"

#include <algorithm>

namespace aria {
class AdeptPartitioner : public Partitioner
{

public:
  AdeptPartitioner(std::size_t coordinator_id, std::size_t coordinator_num, std::vector<std::size_t> replica_group_sizes)
      : Partitioner(coordinator_id, coordinator_num)
  {
    CHECK(coordinator_num > 0);
    CHECK(coordinator_id < coordinator_num);
    CHECK(!replica_group_sizes.empty());
    CHECK(std::all_of(replica_group_sizes.begin(), replica_group_sizes.end(), [](std::size_t size) { return size > 0; }));
    CHECK(std::accumulate(replica_group_sizes.begin(), replica_group_sizes.end(), std::size_t{0}) == coordinator_num);

    std::size_t end = 0;
    for (auto i = 0u; i < replica_group_sizes.size(); i++) {
      end += replica_group_sizes[i];

      if (coordinator_id < end) {
        coordinator_start_id = end - replica_group_sizes[i];
        replica_group_id     = i;
        replica_group_size   = replica_group_sizes[i];
        break;
      }
    }
  }

  ~AdeptPartitioner() override = default;

  std::size_t replica_num() const override { return replica_group_size; }

  bool is_replicated() const override
  {
    // replica group in Adept is independent
    return false;
  }

  bool has_master_partition(std::size_t partition_id) const override
  {
    return master_coordinator(partition_id) == coordinator_id;
  }

  std::size_t master_coordinator(std::size_t partition_id) const override
  {
    return partition_id % replica_group_size + coordinator_start_id;
  }

  bool is_partition_replicated_on(std::size_t partition_id, std::size_t coordinator_id) const override
  {
    // replica group in Adept is independent
    return false;
  }

  bool is_backup() const override { return false; }

public:
  std::size_t replica_group_id;
  std::size_t replica_group_size;

private:
  // the first coordinator in this replica group
  std::size_t coordinator_start_id;
};
}  // namespace aria
