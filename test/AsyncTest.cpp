#include "catch.hpp"

#include <type_traits>

#include <chrono>
#include <condition_variable>

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

using namespace pushmi::aliases;

SCENARIO( "async", "[async]" ) {

  GIVEN( "A new_thread time_single_deferred" ) {
    auto nt = v::new_thread();
    using NT = decltype(nt);

    auto workerTask = [](auto v) mutable {return v + 1;};

    WHEN( "async task chain used with via" ) {
      {
        std::vector<std::string> values;

        auto comparablething = op::just(2.0) |
          op::via([&](){return nt;}) |
          op::transform([exec = nt](auto v){
            auto token = pushmi::detail::AsyncToken<
                std::decay_t<decltype(v)>, std::decay_t<decltype(exec)>>{
              exec};
            token.dataPtr_->v_ = std::forward<decltype(v)>(v);
            return token;
          }) |
          op::transform([workerTask](auto v) mutable {
            v.dataPtr_->v_ = workerTask(v.dataPtr_->v_);
            return v;
          }) |
          op::transform([](auto v){return v.dataPtr_->v_;}) |
          op::blocking_submit(v::on_value([&](auto v) { values.push_back(std::to_string(v)); }));

        THEN( "only the first item was pushed" ) {
          REQUIRE(values == std::vector<std::string>{"3.000000"});
        }
      }

      {
        std::vector<std::string> values;
        auto realthing = op::just(2.0) |
          op::async_fork([&](){return nt;}) |
          op::async_transform(workerTask) |
          op::async_join() |
          op::blocking_submit(v::on_value([&](auto v) { values.push_back(std::to_string(v)); }));

        THEN( "only the first item was pushed" ) {
          REQUIRE(values == std::vector<std::string>{"3.000000"});
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
