#pragma once

#include <coroutine>
#include <exception>
#include <functional>
#include <memory>
#include <stdexcept>
#include <utility>

namespace aria {

template <typename T>
struct Task
{
  struct promise_type
  {
    T                  value{};
    std::exception_ptr exception;

    Task get_return_object() { return Task{std::coroutine_handle<promise_type>::from_promise(*this)}; }

    std::suspend_never initial_suspend() { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; }

    void return_value(T val) { value = std::move(val); }

    void unhandled_exception() { exception = std::current_exception(); }
  };

  std::coroutine_handle<promise_type> _h;

  Task(std::coroutine_handle<promise_type> h) : _h(h) {}
  ~Task()
  {
    if (_h) 
      _h.destroy();
  }

  Task(const Task &)            = delete;
  Task &operator=(const Task &) = delete;

  Task(Task &&other) noexcept : _h(std::exchange(other._h, {})) {}

  Task &operator=(Task &&other) noexcept
  {
    if (this != &other) {
      if (_h)
        _h.destroy();
      _h = std::exchange(other._h, {});
    }
    return *this;
  }

  bool done() const { return !_h || _h.done(); }
  void resume()
  {
    if (_h && !_h.done())
      _h.resume();
  }

  T get_value() 
  {
    if (!_h || !_h.done()) {
      throw std::runtime_error("Coroutine not completed");
    }
    if (_h.promise().exception) {
      std::rethrow_exception(_h.promise().exception);
    }
    return _h.promise().value;
  }
};

template <>
struct Task<void>
{
  struct promise_type
  {
    std::exception_ptr exception;

    Task get_return_object() { return Task{std::coroutine_handle<promise_type>::from_promise(*this)}; }

    std::suspend_never initial_suspend() { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; }

    void return_void() {}
    void unhandled_exception() { exception = std::current_exception(); }
  };

  std::coroutine_handle<promise_type> _h;

  Task(std::coroutine_handle<promise_type> h) : _h(h) {}
  ~Task()
  {
    if (_h)
      _h.destroy();
  }

  Task(const Task &)            = delete;
  Task &operator=(const Task &) = delete;

  Task(Task &&other) noexcept : _h(std::exchange(other._h, {})) {}

  Task &operator=(Task &&other) noexcept
  {
    if (this != &other) {
      if (_h)
        _h.destroy();
      _h = std::exchange(other._h, {});
    }
    return *this;
  }

  bool done() const { return !_h || _h.done(); }
  void resume()
  {
    if (_h && !_h.done())
      _h.resume();
  }

  void get_value()
  {
    if (!_h || !_h.done()) {
      throw std::runtime_error("Coroutine not completed");
    }
    if (_h.promise().exception) {
      std::rethrow_exception(_h.promise().exception);
    }
  }
};

}  // namespace aria
