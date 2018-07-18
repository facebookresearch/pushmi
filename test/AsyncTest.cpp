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

// This should specialise using customisation points
// For now it just assumes that this executor deals with threads and creates a
// new thread for each node.
/*
auto async_join() {
   return pushmi::make_single_deferred(
     [](auto out){
       return v::on_value([out = std::move(out)](auto pushmi::detail::AsyncToken){
         std::thread t([
            pushmi::detail::AsyncToken = std::move(pushmi::detail::AsyncToken),
            out = std::move(out)](){
           std::unique_lock<std::mutex> lk(pushmi::detail::AsyncToken.data_->cvm_);
           pushmi::detail::AsyncToken.data_->cv_.wait(lk);
           op::via([](){return pushmi::detail::AsyncToken.e_;}) |
              v::on_value([pushmi::detail::AsyncToken, out = std::move(out)](auto&){
                ::pushmi::set_value(out, std::move(pushmi::detail::AsyncToken.data_->v_));
              });
         });
         t.detach();
       });
     });
}*/

SCENARIO( "async", "[async]" ) {

  GIVEN( "A new_thread time_single_deferred" ) {
    auto nt = v::new_thread();
    using NT = decltype(nt);

    WHEN( "async task chain used with via" ) {
      {
        std::vector<std::string> values;

        auto comparablething = op::just(2.0) |
          op::via([&](){return nt;}) |
          op::transform([exec = nt](auto v){
            return pushmi::detail::AsyncToken<
                std::decay_t<decltype(v)>, std::decay_t<decltype(exec)>>{
              exec, std::forward<decltype(v)>(v)};
          }) |
          op::transform([](auto v){auto ptr = v.dataPtr_; return v;}) |
          op::transform([](auto v){return v.dataPtr_->v_;}) |
          op::blocking_submit(v::on_value([&](auto v) { values.push_back(std::to_string(v)); }));

        THEN( "only the first item was pushed" ) {
          REQUIRE(values == std::vector<std::string>{"2.000000"});
        }
      }

      {
        std::vector<std::string> values;
        auto realthing = op::just(2.0) |
          op::async_fork([&](){return nt;}) |
          op::transform([](auto v){auto ptr = v.dataPtr_; return v;}) |
          op::async_join() |
          op::blocking_submit(v::on_value([&](auto v) { values.push_back(std::to_string(v)); }));

        THEN( "only the first item was pushed" ) {
          REQUIRE(values == std::vector<std::string>{"2.000000"});
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
