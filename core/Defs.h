//
// Created by Yi Lu on 9/10/18.
//

#pragma once

namespace aria {

enum class ExecutorStatus
{
  START,
  CLEANUP,
  C_PHASE,
  S_PHASE,
  Analysis,
  Execute,
  Aria_READ,
  Aria_COMMIT,
  Docc_READ,
  Docc_COMMIT,
  STOP,
  EXIT
};

enum class TransactionResult
{
  COMMIT,
  READY_TO_COMMIT,
  ABORT,
  ABORT_NORETRY
};

}  // namespace aria
