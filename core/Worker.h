//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include "common/LockfreeQueue.h"
#include "common/Message.h"
#include <atomic>
#include <chrono>
#include <glog/logging.h>
#include <queue>

namespace aria {

class Worker
{
public:
  Worker(std::size_t coordinator_id, std::size_t id) : coordinator_id(coordinator_id), id(id)
  {
    n_commit.store(0);
    n_abort_no_retry.store(0);
    n_abort_lock.store(0);
    n_abort_read_validation.store(0);
    n_local.store(0);
    n_si_in_serializable.store(0);
    n_network_size.store(0);
    n_phase_schedule_us.store(0);
    n_phase_execute_us.store(0);
    n_phase_network_wait_us.store(0);
    n_phase_embedded_network_wait_us.store(0);
  }

  virtual ~Worker() = default;

  virtual void start() = 0;

  virtual bool measurement_ready() const { return true; }

  virtual void onExit() {}

  virtual void push_message(Message *message) = 0;

  virtual Message *pop_message() = 0;

public:
  using Clock = std::chrono::steady_clock;

  static uint64_t elapsed_us(Clock::time_point start, Clock::time_point end)
  {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  }

  void add_schedule_time(Clock::time_point start) { n_phase_schedule_us.fetch_add(elapsed_us(start, Clock::now())); }

  void add_execute_time(Clock::time_point start) { n_phase_execute_us.fetch_add(elapsed_us(start, Clock::now())); }

  void add_network_wait_time(Clock::time_point start)
  {
    n_phase_network_wait_us.fetch_add(elapsed_us(start, Clock::now()));
  }

  void add_network_wait_time_us(uint64_t elapsed) { n_phase_network_wait_us.fetch_add(elapsed); }

  void add_embedded_network_wait_time(Clock::time_point start)
  {
    auto elapsed = elapsed_us(start, Clock::now());
    n_phase_network_wait_us.fetch_add(elapsed);
    n_phase_embedded_network_wait_us.fetch_add(elapsed);
  }

  std::size_t           coordinator_id;
  std::size_t           id;
  std::atomic<uint64_t> n_commit, n_abort_no_retry, n_abort_lock, n_abort_read_validation, n_local,
      n_si_in_serializable, n_network_size;
  std::atomic<uint64_t> n_phase_schedule_us, n_phase_execute_us, n_phase_network_wait_us,
      n_phase_embedded_network_wait_us;
};

}  // namespace aria
