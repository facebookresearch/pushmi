#pragma once

// Copyright (c) 2018-present, Facebook, Inc.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

#include <cstdio>

#include <experimental/thread_pool>

#include <pushmi/executor.h>
#include <pushmi/trampoline.h>
#include <pushmi/concepts.h>

#if __cpp_deduction_guides >= 201703
#define MAKE(x) x MAKE_
#define MAKE_(...) {__VA_ARGS__}
#else
#define MAKE(x) make_ ## x
#endif

namespace pushmi {

using std::experimental::static_thread_pool;
namespace execution = std::experimental::execution;

template<class Executor>
class pool_executor {
  Executor e;

  struct task {
    using properties =
      property_set<
        is_sender<>, is_never_blocking<>, is_single<>>;

    explicit task(pool_executor e)
      : pool_ex_(std::move(e))
    {}

    PUSHMI_TEMPLATE(class Out)
      (requires ReceiveValue<Out, pool_executor&>)
    void submit(Out out) && {
      pool_ex_.e.execute([e = pool_ex_, out = std::move(out)]() mutable {
        ::pushmi::set_value(out, e);
        ::pushmi::set_done(out);
      });
    }

  private:
    pool_executor pool_ex_;
  };

public:
  using properties = property_set<is_executor<>, is_concurrent_sequence<>>;

  explicit pool_executor(Executor e)
    : e(std::move(e))
  {}

  task schedule() {
    return task{*this};
  }
};

class pool {
  static_thread_pool p;

public:
  explicit pool(std::size_t threads)
    : p(threads)
  {}

  auto executor() {
    auto exec = execution::require(p.executor(), execution::never_blocking, execution::oneway);
    return pool_executor<decltype(exec)>{exec};
  }

  void stop() { p.stop(); }
  void wait() { p.wait(); }
};

} // namespace pushmi
