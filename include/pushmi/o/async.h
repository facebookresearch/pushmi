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

  template<class ValueType_, class ExecutorType_>
  struct AsyncToken {
  public:
    using ValueType = ValueType_;
    using ExecutorType = ExecutorType_;
    struct Data {
      ValueType v_;
      std::condition_variable cv_;
      std::mutex cvm_;
      bool flag_ = false;
    };

    AsyncToken(ExecutorType e) :
      e_{std::move(e)}, dataPtr_{std::make_shared<Data>()} {}

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
                          exec};
                        token.dataPtr_->v_ = std::forward<decltype(v)>(v);
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


  // extracted this to workaround cuda compiler failure to compute the static_asserts in the nested lambda context
  template<class F>
  struct async_transform_on_value {
    F f_;
    async_transform_on_value() = default;
    constexpr explicit async_transform_on_value(F f)
      : f_(std::move(f)) {}
    template<class Out, class V>
    auto operator()(Out& out, V&& inputToken) {
      using Result = decltype(f_(std::declval<typename V::ValueType>()));
      using Executor = typename V::ExecutorType;
      static_assert(::pushmi::SemiMovable<AsyncToken<Result, Executor>>,
        "none of the functions supplied to transform can convert this value");
      static_assert(::pushmi::SingleReceiver<Out, AsyncToken<Result, Executor>>,
        "Result of value transform cannot be delivered to Out");

      AsyncToken<Result, Executor> outputToken{inputToken.e_};
      std::thread t([
         inputToken,
         outputToken,
         out,
         func = this->f_]() mutable {
        std::unique_lock<std::mutex> inlk(inputToken.dataPtr_->cvm_);
        // Wait for input value
        if(!inputToken.dataPtr_->flag_) {
          inputToken.dataPtr_->cv_.wait(
            inlk, [&](){return inputToken.dataPtr_->flag_;});
        }
        // Compute
        auto result = func(inputToken.dataPtr_->v_);
        // Move output and notify
        std::unique_lock<std::mutex> outlk(outputToken.dataPtr_->cvm_);
        outputToken.dataPtr_->v_ = std::move(result);
        outputToken.dataPtr_->flag_ = true;
        outputToken.dataPtr_->cv_.notify_all();
      });

      t.detach();
      ::pushmi::set_value(out, outputToken);
    }
  };

  struct async_transform_fn {
    template <class... FN>
    auto operator()(FN... fn) const;
  };

  template <class... FN>
  auto async_transform_fn::operator()(FN... fn) const {
    auto f = ::pushmi::overload(std::move(fn)...);
    return ::pushmi::constrain(::pushmi::lazy::Sender<::pushmi::_1>, [f = std::move(f)](auto in) {
      using In = decltype(in);
      // copy 'f' to allow multiple calls to connect to multiple 'in'
      using F = decltype(f);
      return ::pushmi::detail::deferred_from<In, ::pushmi::single<>>(
        std::move(in),
        ::pushmi::detail::submit_transform_out<In>(
          ::pushmi::constrain(::pushmi::lazy::Receiver<::pushmi::_1>, [f](auto out) {
            using Out = decltype(out);
            return ::pushmi::detail::out_from_fn<In>()(
              std::move(out),
              // copy 'f' to allow multiple calls to submit
              ::pushmi::on_value(
                async_transform_on_value<F>(f)
                // [f](Out& out, auto&& v) {
                //   using V = decltype(v);
                //   using Result = decltype(f((V&&) v));
                //   static_assert(::pushmi::SemiMovable<Result>,
                //     "none of the functions supplied to transform can convert this value");
                //   static_assert(::pushmi::SingleReceiver<Out, Result>,
                //     "Result of value transform cannot be delivered to Out");
                //   ::pushmi::set_value(out, f((V&&) v));
                // }
              )
            );
          })
        )
      );
    });
  }

} // namespace detail

namespace operators {
PUSHMI_INLINE_VAR constexpr detail::async_join_fn async_join{};
PUSHMI_INLINE_VAR constexpr detail::async_fork_fn async_fork{};
PUSHMI_INLINE_VAR constexpr detail::async_transform_fn async_transform{};
} // namespace operators
} // namespace pushmi
