#pragma once

#include <coroutine>
#include <functional>
#include <memory>

namespace aria {

// 协程任务类型 - 非void特化版本
template <typename T>
struct Task
{
  struct promise_type
  {
    T value;

    Task get_return_object() { return Task{std::coroutine_handle<promise_type>::from_promise(*this)}; }

    std::suspend_never initial_suspend() { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; }

    void return_value(T val) { value = std::move(val); }

    void unhandled_exception() {}
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

  bool done() const { return _h.done(); }
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
    return _h.promise().value;
  }

  bool await_ready() const { return false; }
  void await_resume() {}
  void await_suspend(std::coroutine_handle<> h) {}
};

template <>
struct Task<void>
{
  struct promise_type
  {
    Task get_return_object() { return Task{std::coroutine_handle<promise_type>::from_promise(*this)}; }

    std::suspend_never initial_suspend() { return {}; }
    std::suspend_never final_suspend() noexcept { return {}; }

    void return_void() {}
    void unhandled_exception() {}
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

  bool done() const { return _h.done(); }
  void resume()
  {
    if (_h && !_h.done())
      _h.resume();
  }

  bool await_ready() const { return false; }
  void await_resume() {}
  void await_suspend(std::coroutine_handle<> h) {}
};

}  // namespace aria