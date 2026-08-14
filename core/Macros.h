//
// Created by Yi Lu on 3/18/19.
//

#pragma once

#define GLOG_USE_GLOG_EXPORT
#include "glog/logging.h"
#include <boost/algorithm/string/split.hpp>
#include <cstdint>
#include <gflags/gflags.h>
DEFINE_string(servers, "127.0.0.1:10010", "semicolon-separated list of servers");
DEFINE_int32(id, 0, "coordinator id");
DEFINE_int32(threads, 1, "the number of threads");
DEFINE_int32(io, 1, "the number of i/o threads");
DEFINE_int32(partition_num, 1, "the number of partitions");
DEFINE_string(partitioner, "hash", "database partitioner (hash, hash2, pb)");
DEFINE_bool(sleep_on_retry, true, "sleep when retry aborted transactions");
DEFINE_int32(batch_size, 100, "star or calvin batch size");
DEFINE_int32(group_time, 10, "group commit frequency");
DEFINE_int32(batch_flush, 50, "batch flush");
DEFINE_int32(sleep_time, 1000, "retry sleep time");
DEFINE_string(protocol, "Aria", "transaction protocol");
DEFINE_string(replica_group, "1", "calvin replica group");
DEFINE_string(lock_manager, "1", "calvin lock manager");
DEFINE_bool(read_on_replica, false, "read from replicas");
DEFINE_bool(local_validation, false, "local validation");
DEFINE_bool(rts_sync, false, "rts sync");
DEFINE_bool(star_sync, false, "synchronous write in the single-master phase");
DEFINE_bool(star_dynamic_batch_size, true, "dynamic batch size");
DEFINE_bool(plv, true, "parallel locking and validation");
DEFINE_bool(same_batch, false, "always run the same batch of txns in calvin and bohm.");
DEFINE_bool(aria_read_only, true, "aria read only optimization");
DEFINE_bool(aria_reordering, true, "aria reordering optimization");
DEFINE_bool(aria_si, false, "aria snapshot isolation");
DEFINE_int32(delay, 0, "delay time in us.");
DEFINE_int32(epoch_delay, 1, "epoch delay parameter.");
DEFINE_string(cdf_path, "", "path to cdf");
DEFINE_string(log_dir, "", "dir to disk logging.");
DEFINE_bool(tcp_no_delay, true, "TCP Nagle algorithm, true: disable nagle");
DEFINE_bool(tcp_quick_ack, false, "TCP quick ack mode, true: enable quick ack");
DEFINE_bool(cpu_affinity, true, "pinning each thread to a separate core");
DEFINE_int32(cpu_core_id, 0, "cpu core id");
DEFINE_int32(durable_write_cost, 0, "the cost of durable write in microseconds");
DEFINE_bool(exact_group_commit, false, "dynamically adjust group time.");
DEFINE_bool(mvcc, false, "use MVCC storage.");
DEFINE_bool(bohm_local, false, "locality optimization for Bohm.");
DEFINE_bool(bohm_single_spin, false, "spin optimization for Bohm.");
DEFINE_int32(ariaFB_lock_manager, 1, "# of lock managers in Aria's fallback mode.");
DEFINE_int32(txn_generator_num, 1, "# of threads to generate transactions.");
DEFINE_bool(blocked_optimize, true, "optimize blocked transaction handling for Adept.");
DEFINE_bool(adept_pipelined_shipping, true, "ship acquired Adept reads while a transaction waits for other locks.");
DEFINE_int32(runtime, 25, "benchmark runtime in seconds.");
DEFINE_int32(warmup, 10, "warmup interval in seconds.");
DEFINE_int32(cooldown, 5, "cooldown interval in seconds.");
DEFINE_int32(caracal_threshold, 16, "pending-version threshold for Caracal split-on-demand.");
DEFINE_int32(mirror_cache_size, 0, "number of deterministic remote keys cached by Adept.");
DEFINE_int32(mirror_cache_warmup_batches, 1, "number of deterministic batches used to select MirrorCache keys.");

#define SETUP_CONTEXT(context)                                                           \
  boost::algorithm::split(context.peers, FLAGS_servers, boost::is_any_of(";"));          \
  CHECK(FLAGS_id >= 0 && static_cast<std::size_t>(FLAGS_id) < context.peers.size());     \
  CHECK(FLAGS_threads > 0);                                                              \
  CHECK(FLAGS_io > 0);                                                                   \
  CHECK(FLAGS_partition_num > 0);                                                        \
  CHECK(FLAGS_batch_size > 0);                                                           \
  CHECK(FLAGS_runtime > 0);                                                              \
  CHECK(FLAGS_warmup >= 0);                                                              \
  CHECK(FLAGS_cooldown >= 0);                                                            \
  CHECK(static_cast<int64_t>(FLAGS_runtime) >                                            \
        static_cast<int64_t>(FLAGS_warmup) + static_cast<int64_t>(FLAGS_cooldown));      \
  CHECK(FLAGS_ariaFB_lock_manager >= 0);                                                 \
  CHECK(FLAGS_mirror_cache_size >= 0);                                                   \
  CHECK(FLAGS_mirror_cache_warmup_batches > 0);                                          \
  context.coordinator_num                  = context.peers.size();                       \
  context.coordinator_id                   = FLAGS_id;                                   \
  context.worker_num                       = FLAGS_threads;                              \
  context.io_thread_num                    = FLAGS_io;                                   \
  context.partition_num                    = FLAGS_partition_num;                        \
  context.partitioner                      = FLAGS_partitioner;                          \
  context.sleep_on_retry                   = FLAGS_sleep_on_retry;                       \
  context.batch_size                       = FLAGS_batch_size;                           \
  context.group_time                       = FLAGS_group_time;                           \
  context.batch_flush                      = FLAGS_batch_flush;                          \
  context.sleep_time                       = FLAGS_sleep_time;                           \
  context.protocol                         = FLAGS_protocol;                             \
  context.replica_group                    = FLAGS_replica_group;                        \
  context.lock_manager                     = FLAGS_lock_manager;                         \
  context.read_on_replica                  = FLAGS_read_on_replica;                      \
  context.local_validation                 = FLAGS_local_validation;                     \
  context.rts_sync                         = FLAGS_rts_sync;                             \
  context.star_sync_in_single_master_phase = FLAGS_star_sync;                            \
  context.star_dynamic_batch_size          = FLAGS_star_dynamic_batch_size;              \
  context.parallel_locking_and_validation  = FLAGS_plv;                                  \
  context.same_batch                       = FLAGS_same_batch;                           \
  context.aria_read_only_optmization       = FLAGS_aria_read_only;                       \
  context.aria_reordering_optmization      = FLAGS_aria_reordering;                      \
  context.aria_snapshot_isolation          = FLAGS_aria_si;                              \
  context.delay_time                       = FLAGS_delay;                                \
  context.epoch_delay                      = FLAGS_epoch_delay;                          \
  context.log_dir                          = FLAGS_log_dir;                              \
  context.cdf_path                         = FLAGS_cdf_path;                             \
  context.tcp_no_delay                     = FLAGS_tcp_no_delay;                         \
  context.tcp_quick_ack                    = FLAGS_tcp_quick_ack;                        \
  context.cpu_affinity                     = FLAGS_cpu_affinity;                         \
  context.cpu_core_id                      = FLAGS_cpu_core_id;                          \
  context.durable_write_cost               = FLAGS_durable_write_cost;                   \
  context.exact_group_commit               = FLAGS_exact_group_commit;                   \
  context.mvcc                             = FLAGS_mvcc;                                 \
  context.bohm_local                       = FLAGS_bohm_local;                           \
  context.bohm_single_spin                 = FLAGS_bohm_single_spin;                     \
  context.ariaFB_lock_manager              = FLAGS_ariaFB_lock_manager;                  \
  context.txn_generator_num                = FLAGS_txn_generator_num;                    \
  context.blocked_optimize                 = FLAGS_blocked_optimize;                     \
  context.adept_pipelined_shipping         = FLAGS_adept_pipelined_shipping;             \
  context.runtime_seconds                  = FLAGS_runtime;                              \
  context.warmup_seconds                   = FLAGS_warmup;                               \
  context.cooldown_seconds                 = FLAGS_cooldown;                             \
  context.caracal_threshold                = FLAGS_caracal_threshold;                    \
  context.mirror_cache_size                = FLAGS_mirror_cache_size;                    \
  context.mirror_cache_warmup_batches      = FLAGS_mirror_cache_warmup_batches;          \
  if (context.protocol == "Bohm" || context.protocol == "Caracal" ||                   \
      context.protocol == "Adept")                                                       \
    context.mvcc = true;                                                                 \
  if (context.protocol == "AriaFB")                                                      \
    CHECK(context.ariaFB_lock_manager > 0);                                              \
  CHECK(context.coordinator_num == 1 || context.bohm_single_spin == false)               \
      << "bohm_single_spin must be used in single-node mode.";                           \
  context.set_star_partitioner();
