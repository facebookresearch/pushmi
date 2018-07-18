#include "catch.hpp"

#include <type_traits>

#include <chrono>
#include <condition_variable>
#include <set>

using namespace std::literals;

#include "pushmi/flow_single_deferred.h"
#include "pushmi/o/async.h"
#include "pushmi/o/empty.h"
#include "pushmi/o/just.h"
#include "pushmi/o/on.h"
#include "pushmi/o/transform.h"
#include "pushmi/o/tap.h"
#include "pushmi/o/via.h"
#include "pushmi/o/submit.h"
#include "pushmi/o/extension_operators.h"

#include "pushmi/trampoline.h"
#include "pushmi/new_thread.h"

// Pause ensures that the tasks take long enough that the enqueue task
// runs ahead and multiple threads are launched
void pause() {
  std::this_thread::sleep_for(100ms);
}

using namespace pushmi::aliases;

struct __inline_submit {

  template<class TP, class Out>
  void operator()(TP at, Out out) const {
    auto tr = ::pushmi::trampoline();
    ::pushmi::submit(tr, std::move(at), std::move(out));
  }
};

inline auto inline_executor() {
  return ::pushmi::make_time_single_deferred(__inline_submit{});
}

SCENARIO( "async", "[async]" ) {
  #if 1
  GIVEN( "A new_thread time_single_deferred" ) {
    auto nt = v::new_thread();
    using NT = decltype(nt);

    std::mutex threads_mutex;

    WHEN( "async task chain used with via" ) {
      {
        std::vector<std::string> values;
        std::set<std::thread::id> threads;
        auto workerTask = [&threads, &threads_mutex](auto v) mutable {
          std::lock_guard<std::mutex> lck(threads_mutex);
          threads.insert(std::this_thread::get_id());
          pause();
          return v + 1;
        };

        auto comparablething = op::just(2.0) |
          op::via([&](){return nt;}) |
          op::transform([exec = nt](auto v){
            auto token = pushmi::detail::NewThreadAsyncToken<
                std::decay_t<decltype(v)>, std::decay_t<decltype(exec)>>{
              exec};
            token.dataPtr_->v_ = std::forward<decltype(v)>(v);
            return token;
          }) |
          op::transform([workerTask](auto v) mutable {
            v.dataPtr_->v_ = workerTask(v.dataPtr_->v_);
            return v;
          }) |
          op::transform([workerTask](auto v) mutable {
            v.dataPtr_->v_ = workerTask(v.dataPtr_->v_);
            return v;
          }) |
          op::transform([workerTask](auto v) mutable {
            v.dataPtr_->v_ = workerTask(v.dataPtr_->v_);
            return v;
          }) |
          op::transform([workerTask](auto v) mutable {
            v.dataPtr_->v_ = workerTask(v.dataPtr_->v_);
            return v;
          }) |
          op::transform([workerTask](auto v) mutable {
            v.dataPtr_->v_ = workerTask(v.dataPtr_->v_);
            return v;
          }) |
          op::transform([workerTask](auto v) mutable {
            v.dataPtr_->v_ = workerTask(v.dataPtr_->v_);
            return v;
          }) |
          op::transform([](auto v){return v.dataPtr_->v_;}) |
          op::blocking_submit(v::on_value([&](auto v) { values.push_back(std::to_string(v)); }));

        THEN( "only the first item was pushed" ) {
          REQUIRE(values == std::vector<std::string>{"8.000000"});
          // Should be inline on a single thread as transform is literal
          REQUIRE(threads.size() == 1);
        }
      }

      {
        std::vector<std::string> values;
        std::set<std::thread::id> threads;
        auto workerTask = [&threads, &threads_mutex](auto v) mutable {
          std::lock_guard<std::mutex> lck(threads_mutex);
          threads.insert(std::this_thread::get_id());
          std::this_thread::sleep_for(100ms);
          pause();
          return v + 1;
        };

        auto realthing = op::just(2.0) |
          op::async_fork([&](){return nt;}) |
          op::async_transform(workerTask) |
          op::async_transform(workerTask) |
          op::async_transform(workerTask) |
          op::async_transform(workerTask) |
          op::async_transform(workerTask) |
          op::async_transform(workerTask) |
          op::async_join() |
          op::blocking_submit(v::on_value([&](auto v) { values.push_back(std::to_string(v)); }));

        THEN( "only the first item was pushed" ) {
          REQUIRE(values == std::vector<std::string>{"8.000000"});
          // Should be asynchronous on multiple threads
          REQUIRE(threads.size() != 1);
        }
      }
    }
  }
  #endif

  GIVEN( "An inline time_single_deferred" ) {
    auto nt = inline_executor();
    using NT = decltype(nt);
    std::mutex threads_mutex;

    WHEN( "async task chain used with via" ) {
      {
        std::vector<std::string> values;
        std::set<std::thread::id> threads;
        auto workerTask = [&threads, &threads_mutex](auto v) mutable {
          std::lock_guard<std::mutex> lck(threads_mutex);
          threads.insert(std::this_thread::get_id());
          pause();
          return v + 1;
        };

        auto realthing = op::just(2.0) |
          op::async_fork([&](){return nt;}) |
          op::async_transform(workerTask) |
          op::async_transform(workerTask) |
          op::async_transform(workerTask) |
          op::async_transform(workerTask) |
          op::async_transform(workerTask) |
          op::async_transform(workerTask) |
          op::async_join() |
          op::blocking_submit(v::on_value([&](auto v) { values.push_back(std::to_string(v)); }));

        THEN( "only the first item was pushed" ) {
          REQUIRE(values == std::vector<std::string>{"8.000000"});
          // Should have all been inline on a single thread
          REQUIRE(threads.size() == 1);
        }
      }
    }
  }
}

/*
v::bulk_on_value(
  [](size_t idx, auto& shared){shared += idx;},
  []() -> size_t { return 10; },
  [](size_t shape){ return 0; },
  [](auto& shared){return shared;})
*/
