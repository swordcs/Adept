//
// Created by Yi Lu on 7/24/18.
//

#pragma once

#include "common/LockfreeQueue.h"
#include "common/Message.h"
#include "common/Socket.h"
#include "core/ControlMessage.h"
#include "core/Dispatcher.h"
#include "core/Executor.h"
#include "core/Worker.h"
#include "core/factory/WorkerFactory.h"
#include "core/factory/TxnGeneratorFactory.h"
#include "core/TxnGenerator.h"
#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <glog/logging.h>
#include <thread>
#include <vector>

namespace aria {

class Coordinator
{
public:
  template <class Database, class Context>
  Coordinator(std::size_t id, Database &db, const Context &context)
      : id(id), coordinator_num(context.peers.size()), peers(context.peers), context(context)
  {
    workerStopFlag.store(false);
    ioStopFlag.store(false);
    txnStopFlag.store(false);
    LOG(INFO) << "Coordinator initializes " << context.worker_num << " workers.";

    workers = WorkerFactory::create_workers(id, db, context, workerStopFlag);

    if (check_txn_generator_needed()) {
      txn_generators = TxnGeneratorFactory::create_generators(
          id, db, context, static_cast<Manager *>(workers.back().get()), txnStopFlag);
    }

    // init sockets vector
    inSockets.resize(context.io_thread_num);
    outSockets.resize(context.io_thread_num);

    for (auto i = 0u; i < context.io_thread_num; i++) {
      inSockets[i].resize(peers.size());
      outSockets[i].resize(peers.size());
    }
  }

  ~Coordinator() = default;

  void start()
  {

    // init dispatcher vector
    iDispatchers.resize(context.io_thread_num);
    oDispatchers.resize(context.io_thread_num);

    // start dispatcher threads

    std::vector<std::thread> iDispatcherThreads, oDispatcherThreads;

    for (auto i = 0u; i < context.io_thread_num; i++) {

      iDispatchers[i] = std::make_unique<IncomingDispatcher>(
          id, i, context.io_thread_num, inSockets[i], workers, in_queue, ioStopFlag);
      oDispatchers[i] = std::make_unique<OutgoingDispatcher>(
          id, i, context.io_thread_num, outSockets[i], workers, out_queue, ioStopFlag);

      iDispatcherThreads.emplace_back(&IncomingDispatcher::start, iDispatchers[i].get());
      oDispatcherThreads.emplace_back(&OutgoingDispatcher::start, oDispatchers[i].get());
      if (context.cpu_affinity) {
        pin_thread_to_core(iDispatcherThreads[i]);
        pin_thread_to_core(oDispatcherThreads[i]);
      }
    }

    std::vector<std::thread> generatorThreads;

    for (auto i = 0u; i < txn_generators.size(); i++) {
      generatorThreads.emplace_back(&TxnGenerator::start, txn_generators[i].get());
      if (context.cpu_affinity) {
        pin_thread_to_core(generatorThreads[i]);
      }
    }

    std::vector<std::thread> threads;

    LOG(INFO) << "Coordinator starts to run " << workers.size() << " workers.";

    for (auto i = 0u; i < workers.size(); i++) {
      threads.emplace_back(&Worker::start, workers[i].get());
      if (context.cpu_affinity) {
        pin_thread_to_core(threads[i]);
      }
    }

    // Keep the historical defaults, but make short, reproducible rebuttal
    // experiments possible from the command line.
    auto timeToRun = static_cast<int64_t>(context.runtime_seconds);
    auto warmup    = static_cast<int64_t>(context.warmup_seconds);
    auto cooldown  = static_cast<int64_t>(context.cooldown_seconds);

    if (context.mirror_cache_size > 0) {
      while (!std::all_of(workers.begin(), workers.end(),
          [](const auto &worker) { return worker->measurement_ready(); })) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
      for (const auto &worker : workers) {
        worker->n_commit.exchange(0);
        worker->n_abort_no_retry.exchange(0);
        worker->n_abort_lock.exchange(0);
        worker->n_abort_read_validation.exchange(0);
        worker->n_local.exchange(0);
        worker->n_si_in_serializable.exchange(0);
        worker->n_network_size.exchange(0);
        worker->n_phase_schedule_us.exchange(0);
        worker->n_phase_execute_us.exchange(0);
        worker->n_phase_network_wait_us.exchange(0);
        worker->n_phase_embedded_network_wait_us.exchange(0);
      }
      LOG(INFO) << "Measurement window starts after MirrorCache training and priming.";
    }
    auto startTime = std::chrono::steady_clock::now();

    uint64_t total_commit = 0, total_abort_no_retry = 0, total_abort_lock = 0, total_abort_read_validation = 0,
             total_local = 0, total_si_in_serializable = 0, total_network_size = 0, total_phase_schedule_us = 0,
             total_phase_execute_us = 0, total_phase_network_wait_us = 0,
             total_phase_embedded_network_wait_us = 0;
    int64_t count = 0;

    do {
      std::this_thread::sleep_for(std::chrono::seconds(1));

      uint64_t n_commit = 0, n_abort_no_retry = 0, n_abort_lock = 0, n_abort_read_validation = 0, n_local = 0,
               n_si_in_serializable = 0, n_network_size = 0, n_phase_schedule_us = 0, n_phase_execute_us = 0,
               n_phase_network_wait_us = 0, n_phase_embedded_network_wait_us = 0;

      for (auto i = 0u; i < workers.size(); i++) {

        n_commit += workers[i]->n_commit.exchange(0);

        n_abort_no_retry += workers[i]->n_abort_no_retry.exchange(0);

        n_abort_lock += workers[i]->n_abort_lock.exchange(0);

        n_abort_read_validation += workers[i]->n_abort_read_validation.exchange(0);

        n_local += workers[i]->n_local.exchange(0);

        n_si_in_serializable += workers[i]->n_si_in_serializable.exchange(0);

        n_network_size += workers[i]->n_network_size.exchange(0);

        n_phase_schedule_us += workers[i]->n_phase_schedule_us.exchange(0);

        n_phase_execute_us += workers[i]->n_phase_execute_us.exchange(0);

        n_phase_network_wait_us += workers[i]->n_phase_network_wait_us.exchange(0);

        n_phase_embedded_network_wait_us += workers[i]->n_phase_embedded_network_wait_us.exchange(0);
      }

      const auto avg_network_size = n_commit == 0 ? 0.0 : 1.0 * n_network_size / n_commit;
      const auto si_percent       = n_commit == 0 ? 0.0 : 100.0 * n_si_in_serializable / n_commit;
      const auto local_percent    = n_commit == 0 ? 0.0 : 100.0 * n_local / n_commit;
      LOG(INFO) << "commit: " << n_commit << " abort: " << n_abort_no_retry + n_abort_lock + n_abort_read_validation
                << " (" << n_abort_no_retry << "/" << n_abort_lock << "/" << n_abort_read_validation
                << "), network size: " << n_network_size << ", avg network size: " << avg_network_size
                << ", si_in_serializable: " << n_si_in_serializable << " " << si_percent
                << " %"
                << ", local: " << local_percent << " %"
                << ", phase profile us: schedule: " << n_phase_schedule_us << ", execute: " << n_phase_execute_us
                << ", network_wait: " << n_phase_network_wait_us
                << ", embedded_network_wait: " << n_phase_embedded_network_wait_us;
      count++;
      if (count > warmup && count <= timeToRun - cooldown) {
        total_commit += n_commit;
        total_abort_no_retry += n_abort_no_retry;
        total_abort_lock += n_abort_lock;
        total_abort_read_validation += n_abort_read_validation;
        total_local += n_local;
        total_si_in_serializable += n_si_in_serializable;
        total_network_size += n_network_size;
        total_phase_schedule_us += n_phase_schedule_us;
        total_phase_execute_us += n_phase_execute_us;
        total_phase_network_wait_us += n_phase_network_wait_us;
        total_phase_embedded_network_wait_us += n_phase_embedded_network_wait_us;
      }

    } while (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - startTime).count() <
             timeToRun);

    count = timeToRun - warmup - cooldown;

    const auto avg_total_network_size = total_commit == 0 ? 0.0 : 1.0 * total_network_size / total_commit;
    const auto total_si_percent = total_commit == 0 ? 0.0 : 100.0 * total_si_in_serializable / total_commit;
    const auto total_local_percent = total_commit == 0 ? 0.0 : 100.0 * total_local / total_commit;

    LOG(INFO) << "average commit: " << 1.0 * total_commit / count
              << " abort: " << 1.0 * (total_abort_no_retry + total_abort_lock + total_abort_read_validation) / count
              << " (" << 1.0 * total_abort_no_retry / count << "/" << 1.0 * total_abort_lock / count << "/"
              << 1.0 * total_abort_read_validation / count << "), network size: " << total_network_size
              << ", avg network size: " << avg_total_network_size
              << ", si_in_serializable: " << total_si_in_serializable << " "
              << total_si_percent << " %"
              << ", local: " << total_local_percent << " %"
              << ", phase profile total us: schedule: " << total_phase_schedule_us
              << ", execute: " << total_phase_execute_us << ", network_wait: " << total_phase_network_wait_us
              << ", embedded_network_wait: " << total_phase_embedded_network_wait_us;

    workerStopFlag.store(true);

    for (auto i = 0u; i < threads.size(); i++) {
      workers[i]->onExit();
      threads[i].join();
    }

    // gather throughput
    double sum_commit = gather(1.0 * total_commit / count);
    if (id == 0) {
      LOG(INFO) << "total commit: " << sum_commit;
    }

    // make sure all messages are sent
    std::this_thread::sleep_for(std::chrono::seconds(1));

    ioStopFlag.store(true);

    for (auto i = 0u; i < context.io_thread_num; i++) {
      iDispatcherThreads[i].join();
      oDispatcherThreads[i].join();
    }

    txnStopFlag.store(true);

    for (auto i = 0u; i < generatorThreads.size(); i++) {
      generatorThreads[i].join();
    }

    close_sockets();

    LOG(INFO) << "Coordinator exits.";
  }

  void connectToPeers()
  {

    // single node test mode
    if (peers.size() == 1) {
      return;
    }

    auto getAddressPort = [](const std::string &addressPort) {
      std::vector<std::string> result;
      boost::algorithm::split(result, addressPort, boost::is_any_of(":"));
      return result;
    };

    // start some listener threads

    std::vector<std::thread> listenerThreads;

    for (auto i = 0u; i < context.io_thread_num; i++) {

      listenerThreads.emplace_back(
          [id            = this->id,
              peers      = this->peers,
              &inSockets = this->inSockets[i],
              &getAddressPort,
              tcp_quick_ack = context.tcp_quick_ack](std::size_t listener_id) {
            std::vector<std::string> addressPort = getAddressPort(peers[id]);

            Listener l(addressPort[0].c_str(), atoi(addressPort[1].c_str()) + listener_id, 100);
            LOG(INFO) << "Listener " << listener_id << " on coordinator " << id << " listening on " << peers[id];

            auto n = peers.size();

            for (std::size_t i = 0; i < n - 1; i++) {
              Socket      socket = l.accept();
              std::size_t c_id;
              socket.read_number(c_id);
              // set quick ack flag
              socket.set_quick_ack_flag(tcp_quick_ack);
              inSockets[c_id] = std::move(socket);
            }

            LOG(INFO) << "Listener " << listener_id << " on coordinator " << id << " exits.";
          },
          i);
    }

    // connect to peers
    auto                  n          = peers.size();
    constexpr std::size_t retryLimit = 200;

    // connect to multiple remote coordinators
    for (auto i = 0u; i < n; i++) {
      if (i == id)
        continue;
      std::vector<std::string> addressPort = getAddressPort(peers[i]);
      // connnect to multiple remote listeners
      for (auto listener_id = 0u; listener_id < context.io_thread_num; listener_id++) {
        for (auto k = 0u; k < retryLimit; k++) {
          Socket socket;

          int ret = socket.connect(addressPort[0].c_str(), atoi(addressPort[1].c_str()) + listener_id);
          if (ret == -1) {
            socket.close();
            if (k == retryLimit - 1) {
              LOG(FATAL) << "failed to connect to peers, exiting ...";
              exit(1);
            }

            // listener on the other side has not been set up.
            LOG(INFO) << "Coordinator " << id << " failed to connect " << i << "(" << peers[i] << ")'s listener "
                      << listener_id << ", retry in 50 milliseconds.";
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            continue;
          }
          if (context.tcp_no_delay) {
            socket.disable_nagle_algorithm();
          }

          LOG(INFO) << "Coordinator " << id << " connected to " << i;
          socket.write_number(id);
          outSockets[listener_id][i] = std::move(socket);
          break;
        }
      }
    }

    for (auto i = 0u; i < listenerThreads.size(); i++) {
      listenerThreads[i].join();
    }

    LOG(INFO) << "Coordinator " << id << " connected to all peers.";
  }

  double gather(double value)
  {

    auto init_message = [](Message *message, std::size_t coordinator_id, std::size_t dest_node_id) {
      message->set_source_node_id(coordinator_id);
      message->set_dest_node_id(dest_node_id);
      message->set_worker_id(0);
    };

    double sum = value;

    if (id == 0) {
      for (std::size_t i = 0; i < coordinator_num - 1; i++) {

        in_queue.wait_till_non_empty();
        std::unique_ptr<Message> message(in_queue.front());
        bool                     ok = in_queue.pop();
        CHECK(ok);
        CHECK(message->get_message_count() == 1);

        MessagePiece messagePiece = *(message->begin());

        CHECK(messagePiece.get_message_type() == static_cast<uint32_t>(ControlMessage::STATISTICS));
        CHECK(messagePiece.get_message_length() == MessagePiece::get_header_size() + sizeof(double));
        Decoder dec(messagePiece.toStringPiece());
        double  v;
        dec >> v;
        sum += v;
      }

    } else {
      auto message = std::make_unique<Message>();
      init_message(message.get(), id, 0);
      ControlMessageFactory::new_statistics_message(*message, value);
      out_queue.push(message.release());
    }
    return sum;
  }

private:
  void close_sockets()
  {
    for (auto i = 0u; i < inSockets.size(); i++) {
      for (auto j = 0u; j < inSockets[i].size(); j++) {
        inSockets[i][j].close();
      }
    }
    for (auto i = 0u; i < outSockets.size(); i++) {
      for (auto j = 0u; j < outSockets[i].size(); j++) {
        outSockets[i][j].close();
      }
    }
  }

  void pin_thread_to_core(std::thread &t)
  {
#ifndef __APPLE__
    static std::size_t core_id = context.cpu_core_id;
    cpu_set_t          cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id++, &cpuset);
    int rc = pthread_setaffinity_np(t.native_handle(), sizeof(cpu_set_t), &cpuset);
    CHECK(rc == 0);
#endif
  }

  bool check_txn_generator_needed()
  {
    std::unordered_set<std::string> protocols = {"Calvin", "AsyncCalvin", "Adept", "Queue", "Q-Store"};
    return protocols.count(context.protocol) == 1;
  }

private:
  /*
   * A coordinator may have multilpe inSockets and outSockets, connected to one
   * remote coordinator to fully utilize the network
   *
   * inSockets[0][i] receives w_id % io_threads from coordinator i
   */

  std::size_t                                      id, coordinator_num;
  const std::vector<std::string>                  &peers;
  const Context                                   &context;
  std::vector<std::vector<Socket>>                 inSockets, outSockets;
  std::atomic<bool>                                workerStopFlag, ioStopFlag;
  std::vector<std::shared_ptr<Worker>>             workers;
  std::vector<std::unique_ptr<IncomingDispatcher>> iDispatchers;
  std::vector<std::unique_ptr<OutgoingDispatcher>> oDispatchers;
  LockfreeQueue<Message *>                         in_queue, out_queue;

  std::vector<std::shared_ptr<TxnGenerator>> txn_generators;
  std::atomic<bool>                          txnStopFlag;
};
}  // namespace aria
