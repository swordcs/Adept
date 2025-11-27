/*
 * @Description: Base class for transaction generators
 * @Date: 2025-09-16
 */

#pragma once

#include <atomic>
#include <memory>
#include <vector>

namespace aria {

class TxnGenerator
{
public:
  TxnGenerator() = default;

  virtual ~TxnGenerator() = default;

  virtual void start() = 0;

protected:
  std::atomic<std::size_t> generated_count{0};
  std::atomic<std::size_t> processed_count{0};
  std::size_t              batch_size    = 0;
  std::int64_t             current_epoch = 0;
};

}  // namespace aria