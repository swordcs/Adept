//
// Created by Yi Lu on 8/29/18.
//

#pragma once

#include <algorithm>
#include <cmath>
#include <fstream>
#include <glog/logging.h>
#include <numeric>  // for accumulate

// The nearest-rank method
// https://en.wikipedia.org/wiki/Percentile

namespace aria {

template <class T>
class Percentile
{
public:
  using element_type = T;

  void add(const element_type &value)
  {
    isSorted_ = false;
    data_.push_back(value);
  }

  void add(const std::vector<element_type> &v)
  {
    isSorted_ = false;
    std::copy(v.begin(), v.end(), std::back_inserter(data_));
  }

  void clear()
  {
    isSorted_ = true;
    data_.clear();
  }

  auto size() { return data_.size(); }

  element_type nth(double n)
  {
    if (data_.size() == 0) {
      return 0;
    }
    checkSort();
    DCHECK(n > 0 && n <= 100);
    auto sz = size();
    auto i  = static_cast<decltype(sz)>(ceil(n / 100 * sz)) - 1;
    DCHECK(i >= 0 && i < size());
    return data_[i];
  }

  element_type min()
  {
    if (data_.size() == 0) {
      return 0;
    }
    checkSort();
    return data_.front();
  }

  element_type max()
  {
    if (data_.size() == 0) {
      return 0;
    }
    checkSort();
    return data_.back();
  }

  void save_cdf(const std::string &path)
  {
    if (data_.size() == 0) {
      return;
    }
    checkSort();

    if (path.empty()) {
      return;
    }

    std::ofstream cdf;
    cdf.open(path);

    cdf << "value\tcdf" << std::endl;

    // output ~ 1k rows
    auto step_size = std::max(1, int(data_.size() * 0.99 / 1000));

    std::vector<element_type> cdf_result;

    for (auto i = 0u; i < 0.99 * data_.size(); i += step_size) {
      cdf_result.push_back(data_[i]);
    }

    for (auto i = 0u; i < cdf_result.size(); i++) {
      cdf << cdf_result[i] << "\t" << 1.0 * (i + 1) / cdf_result.size() << std::endl;
    }

    cdf.close();
  }

  void box_print()
  {
    if (data_.empty()) {
      LOG(INFO) << "BoxPlot: empty dataset";
      return;
    }
    checkSort();

    auto   sz         = data_.size();
    auto   mn         = min();
    auto   mx         = max();
    auto   q1         = nth(25);
    auto   median     = nth(50);
    auto   q3         = nth(75);
    double iqr        = static_cast<double>(q3) - static_cast<double>(q1);
    double lowerFence = static_cast<double>(q1) - 1.5 * iqr;
    double upperFence = static_cast<double>(q3) + 1.5 * iqr;

    double mean = std::accumulate(data_.begin(), data_.end(), 0.0, [](double acc, const element_type &v) {
      return acc + static_cast<double>(v);
    }) / static_cast<double>(sz);

    std::size_t outliersLow = 0, outliersHigh = 0;
    for (const auto &v : data_) {
      if (static_cast<double>(v) < lowerFence) {
        ++outliersLow;
      } else if (static_cast<double>(v) > upperFence) {
        ++outliersHigh;
      }
    }

    LOG(INFO) << "BoxPlot size=" << sz << " min=" << mn << " Q1=" << q1 << " median=" << median << " Q3=" << q3
              << " max=" << mx << " IQR=" << iqr << " lowerFence=" << lowerFence << " upperFence=" << upperFence
              << " mean=" << mean << " outliersLow=" << outliersLow << " outliersHigh=" << outliersHigh;
  }

private:
  void checkSort()
  {
    if (!isSorted_) {
      std::sort(data_.begin(), data_.end());
      isSorted_ = true;
    }
  }

private:
  bool isSorted_ = true;

public:
  std::vector<element_type> data_;
};
}  // namespace aria