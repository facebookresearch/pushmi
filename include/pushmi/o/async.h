#pragma once
// Copyright (c) 2018-present, Facebook, Inc.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

#include "../piping.h"
#include "../executor.h"
#include "extension_operators.h"

namespace pushmi {
namespace detail {

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
PUSHMI_INLINE_VAR constexpr detail::async_fork_fn async_fork{};
} // namespace operators
} // namespace pushmi
