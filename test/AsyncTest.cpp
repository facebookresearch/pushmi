#include "catch.hpp"

#include <type_traits>

#include <chrono>
#include <condition_variable>

using namespace std::literals;

#include "pushmi/flow_single_deferred.h"
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

template<class ValueType, class ExecutorType>
struct AsyncToken {
public:
  struct Data {
    Data(ValueType v) : v_(std::move(v)) {}
    ValueType v_;
    std::condition_variable cv_;
    std::mutex cvm_;
    bool flag_ = false;
  };

  AsyncToken(ExecutorType e, ValueType v) :
    e_{std::move(e)}, dataPtr_{std::make_shared<Data>(std::move(v))} {}

  ExecutorType e_;
  std::shared_ptr<Data> dataPtr_;
};


namespace pushmi {
namespace detail {
  template<class Executor, class Out>
  struct async_fork_fn_data : public Out {
    Executor exec;

    async_fork_fn_data(Out out, Executor exec) :
      Out(std::move(out)), exec(std::move(exec)) {}
  };

  template<class Out, class Executor>
  auto make_async_fork_fn_data(Out out, Executor ex) -> async_fork_fn_data<Executor, Out> {
    return {std::move(out), std::move(ex)};
  }

  struct async_fork_fn {
    PUSHMI_TEMPLATE(class ExecutorFactory)
      (requires Invocable<ExecutorFactory&>)
    auto operator()(ExecutorFactory ef) const {
      return constrain(lazy::Sender<_1>, [ef = std::move(ef)](auto in) {
        using In = decltype(in);
        return ::pushmi::detail::deferred_from<In, single<>>(
          std::move(in),
          ::pushmi::detail::submit_transform_out<In>(
            constrain(lazy::Receiver<_1>, [ef](auto out) {
              using Out = decltype(out);
              auto exec = ef();
              return ::pushmi::detail::out_from_fn<In>()(
                make_async_fork_fn_data(std::move(out), std::move(exec)),
                // copy 'f' to allow multiple calls to submit
                ::pushmi::on_value([](auto& data, auto&& v) {
                  using V = decltype(v);
                  auto exec = data.exec;
                  ::pushmi::submit(
                    exec,
                    ::pushmi::now(exec),
                    ::pushmi::make_single(
                      [v = (V&&)v, out = std::move(static_cast<Out&>(data)), exec](auto) mutable {
                        // Token hard coded for this executor type at the moment
                        auto token = AsyncToken<
                            std::decay_t<decltype(v)>, std::decay_t<decltype(exec)>>{
                          exec, std::forward<decltype(v)>(v)};
                        token.dataPtr_->flag_ = true;
                        ::pushmi::set_value(out, std::move(token));
                      }
                    )
                  );
                }),
                ::pushmi::on_error([](auto& data, auto e) noexcept {
                  ::pushmi::submit(
                    data.exec,
                    ::pushmi::now(data.exec),
                    ::pushmi::make_single(
                      [e = std::move(e), out = std::move(static_cast<Out&>(data))](auto) mutable {
                        ::pushmi::set_error(out, std::move(e));
                      }
                    )
                  );
                }),
                ::pushmi::on_done([](auto& data){
                  ::pushmi::submit(
                    data.exec,
                    ::pushmi::now(data.exec),
                    ::pushmi::make_single(
                      [out = std::move(static_cast<Out&>(data))](auto) mutable {
                        ::pushmi::set_done(out);
                      }
                    )
                  );
                })
              );
            })
          )
        );
      });
    }
  };

} // namespace detail

namespace operators {
PUSHMI_INLINE_VAR constexpr detail::async_fork_fn async_fork{};
} // namespace operators
} // namespace pushmi


namespace pushmi {
namespace detail {
  template<class Out>
  struct async_join_fn_data : public Out {
    async_join_fn_data(Out out) :
      Out(std::move(out)) {}
  };

  template<class Out>
  auto make_async_join_fn_data(Out out) -> async_join_fn_data<Out> {
    return {std::move(out)};
  }

  struct async_join_fn {
    auto operator()() const {
      return constrain(lazy::Sender<_1>, [](auto in) {
        using In = decltype(in);
        return ::pushmi::detail::deferred_from<In, single<>>(
          std::move(in),
          ::pushmi::detail::submit_transform_out<In>(
            constrain(lazy::Receiver<_1>, [](auto out) {
              using Out = decltype(out);
              return ::pushmi::detail::out_from_fn<In>()(
                make_async_join_fn_data(std::move(out)),
                // copy 'f' to allow multiple calls to submit
                ::pushmi::on_value([](auto& data, auto&& asyncToken) {
                  // Async version that does not - why not?
                  using V = decltype(asyncToken);
                  auto exec = asyncToken.e_;
                  ::pushmi::submit(
                    exec,
                    ::pushmi::now(exec),
                    ::pushmi::make_single(
                      [asyncToken, out = std::move(static_cast<Out&>(data)), exec](auto) mutable {
                        // Token hard coded for this executor type at the moment
                        std::thread t([
                           exec,
                           asyncToken,
                           out]() mutable {
                          std::unique_lock<std::mutex> lk(asyncToken.dataPtr_->cvm_);
                          if(!asyncToken.dataPtr_->flag_) {
                            asyncToken.dataPtr_->cv_.wait(
                              lk, [&](){return asyncToken.dataPtr_->flag_;});
                          }
                          ::pushmi::submit(
                            exec,
                            ::pushmi::now(exec),
                            ::pushmi::make_single(
                              [asyncToken, out, exec](auto) mutable {
                                // Token hard coded for this executor type at the moment
                                ::pushmi::set_value(out, std::move(asyncToken.dataPtr_->v_));
                              }
                            ));
                        });
                        t.detach();
                      }
                    )
                  );
                }),
                ::pushmi::on_error([](auto& data, auto e) noexcept {
                  auto out = std::move(static_cast<Out&>(data));
                  ::pushmi::set_error(out, std::move(e));
                }),
                ::pushmi::on_done([](auto& data){
                  auto out = std::move(static_cast<Out&>(data));
                  ::pushmi::set_done(out);
                })
              );
            })
          )
        );
      });
    }
  };

} // namespace detail

namespace operators {
PUSHMI_INLINE_VAR constexpr detail::async_join_fn async_join{};
} // namespace operators
} // namespace pushmi

// This should specialise using customisation points
// For now it just assumes that this executor deals with threads and creates a
// new thread for each node.
/*
auto async_join() {
   return pushmi::make_single_deferred(
     [](auto out){
       return v::on_value([out = std::move(out)](auto asyncToken){
         std::thread t([
            asyncToken = std::move(asyncToken),
            out = std::move(out)](){
           std::unique_lock<std::mutex> lk(asyncToken.data_->cvm_);
           asyncToken.data_->cv_.wait(lk);
           op::via([](){return asyncToken.e_;}) |
              v::on_value([asyncToken, out = std::move(out)](auto&){
                ::pushmi::set_value(out, std::move(asyncToken.data_->v_));
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
            return AsyncToken<
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
