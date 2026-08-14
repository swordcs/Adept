#pragma once

#include "core/Table.h"
#include "protocol/Adept/AdeptHelper.h"
#include "protocol/Adept/AdeptPartitioner.h"
#include "protocol/Adept/AdeptTransaction.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <condition_variable>
#include <deque>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace aria {

struct AdeptMirrorKey
{
  uint32_t                    table_id;
  uint32_t                    partition_id;
  ITable                     *table;
  std::shared_ptr<const void> key;

  bool operator<(const AdeptMirrorKey &other) const
  {
    if (table_id != other.table_id) {
      return table_id < other.table_id;
    }
    if (partition_id != other.partition_id) {
      return partition_id < other.partition_id;
    }
    return table->key_less(key.get(), other.key.get());
  }
};

struct AdeptMirrorKeyView
{
  uint32_t    table_id;
  uint32_t    partition_id;
  ITable     *table;
  const void *key;
};

struct AdeptMirrorKeyHash
{
  using is_transparent = void;

  std::size_t operator()(const AdeptMirrorKey &key) const
  {
    return hash(key.table_id, key.partition_id, key.table->hash_key(key.key.get()));
  }

  std::size_t operator()(const AdeptMirrorKeyView &key) const
  {
    return hash(key.table_id, key.partition_id, key.table->hash_key(key.key));
  }

private:
  static std::size_t hash(uint32_t table_id, uint32_t partition_id, std::size_t key_hash)
  {
    std::size_t seed = key_hash;
    seed ^= std::hash<uint32_t>{}(table_id) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    seed ^= std::hash<uint32_t>{}(partition_id) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    return seed;
  }
};

struct AdeptMirrorKeyEqual
{
  using is_transparent = void;

  bool operator()(const AdeptMirrorKey &lhs, const AdeptMirrorKey &rhs) const
  {
    return lhs.table_id == rhs.table_id && lhs.partition_id == rhs.partition_id && lhs.table == rhs.table &&
           lhs.table->key_equal(lhs.key.get(), rhs.key.get());
  }

  bool operator()(const AdeptMirrorKey &lhs, const AdeptMirrorKeyView &rhs) const
  {
    return lhs.table_id == rhs.table_id && lhs.partition_id == rhs.partition_id && lhs.table == rhs.table &&
           lhs.table->key_equal(lhs.key.get(), rhs.key);
  }

  bool operator()(const AdeptMirrorKeyView &lhs, const AdeptMirrorKey &rhs) const { return operator()(rhs, lhs); }
};

struct AdeptMirrorTrainingStat
{
  uint64_t remote_reads = 0;
  uint64_t writes       = 0;
};

class AdeptMirrorFrequencyEstimator
{
public:
  using FrequencyMap =
      std::unordered_map<AdeptMirrorKey, AdeptMirrorTrainingStat, AdeptMirrorKeyHash, AdeptMirrorKeyEqual>;

  explicit AdeptMirrorFrequencyEstimator(std::size_t capacity)
      : summary_capacity(scale_capacity(capacity, 4, 64)),
        sketch_width(scale_capacity(summary_capacity, 8, 1024)),
        queue_capacity(scale_capacity(summary_capacity, 2, 128)),
        sketch(4, std::vector<Counters>(sketch_width))
  {
    if (capacity > 0) {
      worker = std::thread(&AdeptMirrorFrequencyEstimator::run, this);
    }
  }

  ~AdeptMirrorFrequencyEstimator() { close(); }

  AdeptMirrorFrequencyEstimator(const AdeptMirrorFrequencyEstimator &) = delete;
  AdeptMirrorFrequencyEstimator &operator=(const AdeptMirrorFrequencyEstimator &) = delete;

  void observe(AdeptMirrorKey key, uint64_t remote_reads, uint64_t writes)
  {
    if (remote_reads == 0 && writes == 0) {
      return;
    }
    std::unique_lock<std::mutex> lock(mutex);
    space_available.wait(lock, [this] { return closed || samples.size() < queue_capacity; });
    CHECK(!closed);
    samples.push_back(Sample{std::move(key), remote_reads, writes});
    lock.unlock();
    work_available.notify_one();
  }

  FrequencyMap finish()
  {
    close();
    return std::move(summary);
  }

private:
  using Counters = std::array<uint64_t, 2>;

  static std::size_t scale_capacity(std::size_t value, std::size_t multiplier, std::size_t minimum)
  {
    const auto maximum = std::numeric_limits<std::size_t>::max();
    const auto scaled  = value > maximum / multiplier ? maximum : value * multiplier;
    return std::max(scaled, minimum);
  }

  struct Sample
  {
    AdeptMirrorKey key;
    uint64_t       remote_reads;
    uint64_t       writes;
  };

  static uint64_t saturating_add(uint64_t value, uint64_t increment)
  {
    const auto maximum = std::numeric_limits<uint64_t>::max();
    return increment > maximum - value ? maximum : value + increment;
  }

  static uint64_t mix(uint64_t value)
  {
    value ^= value >> 30;
    value *= 0xbf58476d1ce4e5b9ULL;
    value ^= value >> 27;
    value *= 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
  }

  AdeptMirrorTrainingStat update_sketch(const Sample &sample)
  {
    const auto base = static_cast<uint64_t>(AdeptMirrorKeyHash{}(sample.key));
    AdeptMirrorTrainingStat estimate{std::numeric_limits<uint64_t>::max(),
        std::numeric_limits<uint64_t>::max()};
    for (std::size_t row = 0; row < sketch.size(); row++) {
      const auto offset = mix(base + (row + 1) * 0x9e3779b97f4a7c15ULL) % sketch_width;
      auto      &count  = sketch[row][offset];
      count[0] = saturating_add(count[0], sample.remote_reads);
      count[1] = saturating_add(count[1], sample.writes);
      estimate.remote_reads = std::min(estimate.remote_reads, count[0]);
      estimate.writes       = std::min(estimate.writes, count[1]);
    }
    return estimate;
  }

  static bool more_promising(const AdeptMirrorKey &lhs_key, const AdeptMirrorTrainingStat &lhs,
      const AdeptMirrorKey &rhs_key, const AdeptMirrorTrainingStat &rhs)
  {
    if (lhs.remote_reads != rhs.remote_reads) {
      return lhs.remote_reads > rhs.remote_reads;
    }
    if (lhs.writes != rhs.writes) {
      return lhs.writes < rhs.writes;
    }
    return lhs_key < rhs_key;
  }

  void update_summary(const Sample &sample)
  {
    auto estimate = update_sketch(sample);
    auto found    = summary.find(sample.key);
    if (found != summary.end()) {
      found->second = estimate;
      return;
    }
    if (estimate.remote_reads == 0) {
      return;
    }
    if (summary.size() < summary_capacity) {
      summary.emplace(sample.key, estimate);
      return;
    }

    auto victim = summary.begin();
    for (auto it = std::next(summary.begin()); it != summary.end(); ++it) {
      if (more_promising(victim->first, victim->second, it->first, it->second)) {
        victim = it;
      }
    }
    if (more_promising(sample.key, estimate, victim->first, victim->second)) {
      summary.erase(victim);
      summary.emplace(sample.key, estimate);
    }
  }

  void run()
  {
    for (;;) {
      Sample sample{};
      {
        std::unique_lock<std::mutex> lock(mutex);
        work_available.wait(lock, [this] { return closed || !samples.empty(); });
        if (samples.empty()) {
          break;
        }
        sample = std::move(samples.front());
        samples.pop_front();
      }
      space_available.notify_all();
      update_summary(sample);
    }
  }

  void close()
  {
    if (!worker.joinable()) {
      return;
    }
    {
      std::lock_guard<std::mutex> lock(mutex);
      closed = true;
    }
    work_available.notify_all();
    space_available.notify_all();
    worker.join();
  }

  std::size_t                            summary_capacity;
  std::size_t                            sketch_width;
  std::size_t                            queue_capacity;
  std::vector<std::vector<Counters>>     sketch;
  FrequencyMap                           summary;
  std::deque<Sample>                     samples;
  std::mutex                             mutex;
  std::condition_variable                work_available;
  std::condition_variable                space_available;
  bool                                   closed = false;
  std::thread                            worker;
};

class AdeptMirrorCache
{
public:
  AdeptMirrorCache(std::size_t capacity, std::size_t warmup_batches, std::size_t coordinator_num)
      : capacity(capacity),
        warmup_batches(std::max<std::size_t>(1, warmup_batches)),
        coordinator_num(coordinator_num),
        coordinator_mask(make_coordinator_mask(coordinator_num)),
        frequency_estimator(capacity)
  {}

  bool enabled() const { return capacity > 0; }

  bool ready() const { return cache_ready.load(std::memory_order_acquire); }

  bool ready_for_measurement() const { return ready() && planned_batches > 0; }

  template <class Database>
  void annotate_transaction(AdeptTransaction &transaction, Database &db) const
  {
    if (!ready()) {
      return;
    }
    for (auto &read_key : transaction.readSet) {
      if (read_key.get_local_index_read_bit()) {
        continue;
      }
      auto      *table = db.find_table(read_key.get_table_id(), read_key.get_partition_id());
      const auto found = entries.find(make_key_view(read_key, *table));
      if (found != entries.end()) {
        read_key.set_mirror_cache_entry(&found->second);
      }
    }
  }

  template <class Database>
  void analyze_and_plan(std::vector<std::unique_ptr<AdeptTransaction>> &transactions, Database &db,
      AdeptPartitioner &partitioner, std::size_t coordinator_id)
  {
    if (!enabled()) {
      return;
    }
    CHECK(coordinator_id < coordinator_num);

    if (!ready()) {
      collect_training_batch(transactions, db, partitioner);
      training_batches++;
      if (training_batches >= warmup_batches) {
        finalize_candidates();
      }
      return;
    }

    const uint64_t local_mask = uint64_t{1} << coordinator_id;
    const uint64_t generation = planned_batches + 1;
    touched_entries.clear();

    for (auto &transaction_ptr : transactions) {
      auto &transaction = *transaction_ptr;
      if (transaction.abort_no_retry) {
        continue;
      }

      const auto active_mask                             = make_active_mask(transaction.active_coordinators);
      uint64_t   outgoing_read_destinations              = 0;
      uint64_t   outgoing_uncached_destinations          = 0;
      uint64_t   transaction_remote_tuple_waits          = 0;
      uint64_t   transaction_uncached_remote_tuple_waits = 0;
      std::unordered_set<Entry *> local_fills;

      for (auto &read_key : transaction.readSet) {
        read_key.clear_mirror_cache_decisions();
        if (read_key.get_local_index_read_bit()) {
          continue;
        }

        const auto master      = partitioner.master_coordinator(read_key.get_partition_id());
        const auto master_mask = uint64_t{1} << master;
        const auto remote_mask = coordinator_mask & ~master_mask;
        const bool needs_value = !read_key.get_blind_bit();
        const auto consumers   = needs_value ? active_mask & remote_mask : uint64_t{0};
        transaction_remote_tuple_waits += std::popcount(consumers);
        if (consumers & local_mask) {
          remote_read_opportunities++;
        }
        if (coordinator_id == master) {
          outgoing_read_destinations |= consumers;
        }

        auto *entry_ptr = static_cast<const Entry *>(read_key.get_mirror_cache_entry());
        if (entry_ptr == nullptr) {
          transaction_uncached_remote_tuple_waits += std::popcount(consumers);
          if (coordinator_id == master) {
            outgoing_uncached_destinations |= consumers;
          }
          continue;
        }

        auto &entry = *const_cast<Entry *>(entry_ptr);
        candidate_accesses++;
        if (read_key.get_write_lock_bit()) {
          candidate_writes++;
        }

        begin_batch(entry, generation);

        const auto materialized_copies = read_key.get_write_lock_bit()
                                             ? (needs_value ? active_mask & remote_mask
                                                            : active_mask & remote_mask & entry.usable_mask)
                                             : consumers;

        auto hit_mask = entry.usable_mask & consumers;
        for (std::size_t destination = 0; destination < coordinator_num; destination++) {
          if (entry.producers[destination] == &transaction) {
            hit_mask &= ~(uint64_t{1} << destination);
          }
        }
        const auto miss_mask = consumers & ~hit_mask;
        transaction_uncached_remote_tuple_waits += std::popcount(miss_mask);
        read_key.add_mirror_skip_mask(hit_mask);

        if (hit_mask & local_mask) {
          CHECK(coordinator_id != master) << "MirrorCache must only serve non-local tuple copies";
          read_key.set_mirror_cache_read_bit();
          cache_hits++;
          if (entry.producers[coordinator_id] != nullptr && entry.producers[coordinator_id] != &transaction) {
            add_dependency(*entry.producers[coordinator_id], transaction);
          }
        } else if (miss_mask & local_mask) {
          cache_misses++;
        }
        if (coordinator_id == master) {
          skipped_messages += std::popcount(hit_mask);
          outgoing_uncached_destinations |= consumers & ~hit_mask;
        }

        if (coordinator_id != master && (active_mask & local_mask) &&
            ((miss_mask & local_mask) || (materialized_copies & local_mask)) &&
            local_fills.insert(&entry).second) {
          auto *table    = db.find_table(read_key.get_table_id(), read_key.get_partition_id());
          auto &metadata = table->insert_pure_holder(read_key.get_key(), transaction.id);
          AdeptHelper::write_lock(metadata);
          read_key.set_mirror_cache_fill_bit();
          mark_for_gc(entry, local_mask);
          cache_fills++;
        }

        auto newly_materialized = miss_mask;
        while (newly_materialized) {
          auto destination             = std::countr_zero(newly_materialized);
          entry.producers[destination] = &transaction;
          newly_materialized &= newly_materialized - 1;
        }
        entry.usable_mask |= miss_mask;

        if (read_key.get_write_lock_bit()) {
          const auto inactive_copies = entry.usable_mask & remote_mask & ~active_mask;
          if (inactive_copies & local_mask) {
            cache_invalidations++;
          }
          entry.usable_mask = (entry.usable_mask & ~remote_mask) | materialized_copies;
          for (std::size_t destination = 0; destination < coordinator_num; destination++) {
            const auto destination_mask = uint64_t{1} << destination;
            if (!(remote_mask & destination_mask)) {
              continue;
            }
            entry.producers[destination] =
                (materialized_copies & destination_mask) ? &transaction : nullptr;
          }
        }
      }

      if (outgoing_read_destinations) {
        read_message_opportunities += std::popcount(outgoing_read_destinations);
        fully_cached_read_messages += std::popcount(outgoing_read_destinations & ~outgoing_uncached_destinations);
      }
      if (coordinator_id == 0) {
        logical_transactions++;
        record_wait_histogram(remote_wait_histogram, transaction_remote_tuple_waits);
        record_wait_histogram(uncached_remote_wait_histogram, transaction_uncached_remote_tuple_waits);
        if (transaction_remote_tuple_waits > 0) {
          remote_waiting_transactions++;
        }
        if (transaction_uncached_remote_tuple_waits > 0) {
          uncached_remote_waiting_transactions++;
        }
      }
    }

    for (auto *entry : touched_entries) {
      entry->valid_mask = entry->usable_mask;
    }
    planned_batches++;
  }

  template <class Database>
  void garbage_collect(Database &db, std::size_t coordinator_id)
  {
    if (!ready() || pending_gc_entries.empty()) {
      return;
    }

    const auto local_mask = uint64_t{1} << coordinator_id;
    for (auto *entry : pending_gc_entries) {
      if (!(entry->pending_gc_mask & local_mask)) {
        continue;
      }
      auto *table = db.find_table(entry->key->table_id, entry->key->partition_id);
      table->garbage_collect(entry->key->key.get());
      entry->pending_gc_mask &= ~local_mask;
    }
    pending_gc_entries.clear();
  }

  void log_summary(std::size_t coordinator_id) const
  {
    if (!enabled()) {
      return;
    }
    LOG(INFO) << "Adept MirrorCache node " << coordinator_id << ": ready=" << ready() << " candidates=" << entries.size()
              << " training_batches=" << training_batches << " planned_batches=" << planned_batches
              << " hits=" << cache_hits << " misses=" << cache_misses << " fills=" << cache_fills
              << " invalidations=" << cache_invalidations << " skipped_messages=" << skipped_messages
              << " candidate_accesses=" << candidate_accesses
              << " remote_read_opportunities=" << remote_read_opportunities << " candidate_writes=" << candidate_writes
              << " read_message_opportunities=" << read_message_opportunities
              << " fully_cached_read_messages=" << fully_cached_read_messages
              << " logical_transactions=" << logical_transactions
              << " remote_waiting_transactions=" << remote_waiting_transactions
              << " uncached_remote_waiting_transactions=" << uncached_remote_waiting_transactions
              << " remote_wait_zero=" << remote_wait_histogram[0] << " remote_wait_one=" << remote_wait_histogram[1]
              << " remote_wait_two=" << remote_wait_histogram[2] << " remote_wait_three=" << remote_wait_histogram[3]
              << " remote_wait_four=" << remote_wait_histogram[4] << " remote_wait_five=" << remote_wait_histogram[5]
              << " remote_wait_six_to_ten=" << remote_wait_histogram[6]
              << " remote_wait_over_ten=" << remote_wait_histogram[7]
              << " uncached_wait_zero=" << uncached_remote_wait_histogram[0]
              << " uncached_wait_one=" << uncached_remote_wait_histogram[1]
              << " uncached_wait_two=" << uncached_remote_wait_histogram[2]
              << " uncached_wait_three=" << uncached_remote_wait_histogram[3]
              << " uncached_wait_four=" << uncached_remote_wait_histogram[4]
              << " uncached_wait_five=" << uncached_remote_wait_histogram[5]
              << " uncached_wait_six_to_ten=" << uncached_remote_wait_histogram[6]
              << " uncached_wait_over_ten=" << uncached_remote_wait_histogram[7]
              << " selected_training_reads=" << selected_training_reads
              << " selected_training_writes=" << selected_training_writes;
  }

private:
  struct Entry
  {
    const AdeptMirrorKey                *key                  = nullptr;
    uint64_t                            valid_mask           = 0;
    uint64_t                            usable_mask          = 0;
    uint64_t                            pending_gc_mask      = 0;
    uint64_t                            initialized_batch_id = 0;
    std::array<AdeptTransaction *, 64>   producers{};
  };

  struct Candidate
  {
    AdeptMirrorKey          key;
    AdeptMirrorTrainingStat stat;
  };

  using EntryMap = std::unordered_map<AdeptMirrorKey, Entry, AdeptMirrorKeyHash, AdeptMirrorKeyEqual>;

  static uint64_t make_coordinator_mask(std::size_t count)
  {
    CHECK(count > 0 && count <= 64) << "MirrorCache destination masks support between 1 and 64 coordinators";
    return count == 64 ? ~uint64_t{0} : (uint64_t{1} << count) - 1;
  }

  static uint64_t cache_score(const AdeptMirrorTrainingStat &stat, std::size_t coordinator_num)
  {
    if (stat.writes > std::numeric_limits<uint64_t>::max() / coordinator_num) {
      return 0;
    }
    const auto write_cost = stat.writes * coordinator_num;
    return stat.remote_reads > write_cost ? stat.remote_reads - write_cost : 0;
  }

  static AdeptMirrorKeyView make_key_view(const AdeptRWKey &read_key, ITable &table)
  {
    return AdeptMirrorKeyView{static_cast<uint32_t>(read_key.get_table_id()),
        static_cast<uint32_t>(read_key.get_partition_id()),
        &table,
        read_key.get_key()};
  }

  static AdeptMirrorKey copy_key(const AdeptMirrorKeyView &view)
  {
    return AdeptMirrorKey{view.table_id, view.partition_id, view.table, view.table->clone_key(view.key)};
  }

  uint64_t make_active_mask(const std::vector<bool> &active) const
  {
    CHECK(active.size() == coordinator_num);
    uint64_t mask = 0;
    for (std::size_t coordinator = 0; coordinator < active.size(); coordinator++) {
      if (active[coordinator]) {
        mask |= uint64_t{1} << coordinator;
      }
    }
    return mask;
  }

  void begin_batch(Entry &entry, uint64_t generation)
  {
    if (entry.initialized_batch_id == generation) {
      return;
    }
    entry.initialized_batch_id = generation;
    entry.usable_mask          = entry.valid_mask;
    entry.producers.fill(nullptr);
    touched_entries.push_back(&entry);
  }

  static void add_dependency(AdeptTransaction &producer, AdeptTransaction &consumer)
  {
    CHECK(producer.id < consumer.id);
    consumer.blocked_counter.fetch_add(1, std::memory_order_relaxed);
    producer.mirror_dependents.push_back(&consumer);
  }

  static void record_wait_histogram(std::array<uint64_t, 8> &histogram, uint64_t tuple_count)
  {
    std::size_t bucket = tuple_count <= 5 ? tuple_count : tuple_count <= 10 ? 6 : 7;
    histogram[bucket]++;
  }

  void mark_for_gc(Entry &entry, uint64_t local_mask)
  {
    if (!(entry.pending_gc_mask & local_mask)) {
      entry.pending_gc_mask |= local_mask;
      pending_gc_entries.push_back(&entry);
    }
  }

  template <class Database>
  void collect_training_batch(
      std::vector<std::unique_ptr<AdeptTransaction>> &transactions, Database &db, AdeptPartitioner &partitioner)
  {
    for (const auto &transaction_ptr : transactions) {
      const auto &transaction = *transaction_ptr;
      if (transaction.abort_no_retry) {
        continue;
      }

      const auto active_mask = make_active_mask(transaction.active_coordinators);
      for (const auto &read_key : transaction.readSet) {
        if (read_key.get_local_index_read_bit()) {
          continue;
        }

        const auto master           = partitioner.master_coordinator(read_key.get_partition_id());
        const auto remote_consumers = std::popcount(active_mask & ~(uint64_t{1} << master));
        const uint64_t remote_reads = read_key.get_blind_bit() ? 0 : remote_consumers;
        const uint64_t writes       = read_key.get_write_lock_bit() ? 1 : 0;
        if (remote_reads == 0 && writes == 0) {
          continue;
        }

        auto *table = db.find_table(read_key.get_table_id(), read_key.get_partition_id());
        auto  view  = make_key_view(read_key, *table);
        frequency_estimator.observe(copy_key(view), remote_reads, writes);
      }
    }
  }

  void finalize_candidates()
  {
    auto frequencies = frequency_estimator.finish();
    std::vector<Candidate> candidates;
    candidates.reserve(frequencies.size());
    for (const auto &[key, stat] : frequencies) {
      if (cache_score(stat, coordinator_num) > 0) {
        candidates.push_back(Candidate{key, stat});
      }
    }

    std::sort(candidates.begin(), candidates.end(), [this](const auto &lhs, const auto &rhs) {
      // A write invalidates remote copies and can add a local dependency. Rank
      // by estimated net messages saved, retaining enough popularity to repay
      // lookup and materialization costs.
      const auto lhs_score = cache_score(lhs.stat, coordinator_num);
      const auto rhs_score = cache_score(rhs.stat, coordinator_num);
      if (lhs_score != rhs_score) {
        return lhs_score > rhs_score;
      }
      if (lhs.stat.remote_reads != rhs.stat.remote_reads) {
        return lhs.stat.remote_reads > rhs.stat.remote_reads;
      }
      if (lhs.stat.writes != rhs.stat.writes) {
        return lhs.stat.writes < rhs.stat.writes;
      }
      return lhs.key < rhs.key;
    });
    if (candidates.size() > capacity) {
      candidates.resize(capacity);
    }

    entries.reserve(candidates.size());
    for (auto &candidate : candidates) {
      selected_training_reads += candidate.stat.remote_reads;
      selected_training_writes += candidate.stat.writes;
      auto [it, inserted] = entries.emplace(std::move(candidate.key), Entry{});
      CHECK(inserted);
      it->second.key = &it->first;
    }
    touched_entries.reserve(entries.size());
    pending_gc_entries.reserve(entries.size());
    cache_ready.store(true, std::memory_order_release);
  }

private:
  std::size_t       capacity;
  std::size_t       warmup_batches;
  std::size_t       coordinator_num;
  uint64_t          coordinator_mask;
  AdeptMirrorFrequencyEstimator frequency_estimator;
  std::size_t       training_batches = 0;
  uint64_t          planned_batches  = 0;
  std::atomic<bool> cache_ready{false};

  EntryMap             entries;
  std::vector<Entry *> touched_entries;
  std::vector<Entry *> pending_gc_entries;

  uint64_t                cache_hits                           = 0;
  uint64_t                cache_misses                         = 0;
  uint64_t                cache_fills                          = 0;
  uint64_t                cache_invalidations                  = 0;
  uint64_t                skipped_messages                     = 0;
  uint64_t                remote_read_opportunities            = 0;
  uint64_t                candidate_accesses                   = 0;
  uint64_t                candidate_writes                     = 0;
  uint64_t                read_message_opportunities           = 0;
  uint64_t                fully_cached_read_messages           = 0;
  uint64_t                logical_transactions                 = 0;
  uint64_t                remote_waiting_transactions          = 0;
  uint64_t                uncached_remote_waiting_transactions = 0;
  std::array<uint64_t, 8> remote_wait_histogram{};
  std::array<uint64_t, 8> uncached_remote_wait_histogram{};
  uint64_t                selected_training_reads  = 0;
  uint64_t                selected_training_writes = 0;
};

}  // namespace aria
