#pragma once
// Copyright (c) 2018-present, Facebook, Inc.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

#include "../piping.h"
#include "../executor.h"
#include "../new_thread.h"
#include "extension_operators.h"

namespace pushmi {
namespace detail {

  template<class ValueType_, class ExecutorType_>
  struct NewThreadAsyncToken {
  public:
    using ValueType = ValueType_;
    using ExecutorType = ExecutorType_;
    struct Data {
      ValueType v_;
      std::condition_variable cv_;
      std::mutex cvm_;
      bool flag_ = false;
    };

    NewThreadAsyncToken(ExecutorType e) :
      e_{std::move(e)}, dataPtr_{std::make_shared<Data>()} {}

    ExecutorType e_;
    std::shared_ptr<Data> dataPtr_;
  };

  template<class ValueType_, class ExecutorType_>
  struct InlineAsyncToken {
  public:
    using ValueType = ValueType_;
    using ExecutorType = ExecutorType_;

    InlineAsyncToken(ExecutorType e) :
      e_{std::move(e)} {}

    ExecutorType e_;
    ValueType value_;
  };

  template<class Executor, class Out>
  struct async_fork_fn_data : public Out {
    using out_t = Out;
    Executor exec;

    async_fork_fn_data(Out out, Executor exec) :
      Out(std::move(out)), exec(std::move(exec)) {}
  };

  template<class Out, class Executor>
  auto make_async_fork_fn_data(Out out, Executor ex) -> async_fork_fn_data<Executor, Out> {
    return {std::move(out), std::move(ex)};
  }

  // Generic version
  template<class Executor, class Data, class Value>
  struct async_fork_on_value_impl {
    void operator()(Executor exec, Data& data, Value&& value) {
      static_assert(std::is_same<Executor, Executor>::value, "Inline not yet implemented for fork");
    }
  };


  // Customisation for NewThreadAsyncToken
  template<class Data, class Value>
  struct async_fork_on_value_impl<decltype(new_thread()), Data, Value> {
    void operator()(decltype(new_thread()) exec, Data& data, Value&& value) {

      ::pushmi::submit(
        exec,
        ::pushmi::now(exec),
        ::pushmi::make_single(
          [value = (Value&&)value,
           out = std::move(static_cast<typename std::decay_t<Data>::out_t&>(data)),
           exec](auto) mutable {
            // Token hard coded for this executor type at the moment
            auto token = NewThreadAsyncToken<
                std::decay_t<decltype(value)>, std::decay_t<decltype(exec)>>{
              exec};
            token.dataPtr_->v_ = std::forward<Value>(value);
            token.dataPtr_->flag_ = true;
            ::pushmi::set_value(out, std::move(token));
          }
        )
      );
    }
  };

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
                  async_fork_on_value_impl<decltype(exec), decltype(data), V>{}(
                    exec, data, std::forward<decltype(v)>(v));
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
    using out_t = Out;
    async_join_fn_data(Out out) :
      Out(std::move(out)) {}
  };

  template<class Out>
  auto make_async_join_fn_data(Out out) -> async_join_fn_data<Out> {
    return {std::move(out)};
  }

  // Generic version
  template<class Token, class Data>
  struct async_join_on_value_impl {
    void operator()(Data& data, Token&& token) {
      static_assert(std::is_same<Token, Token>::value, "Inline not yet implemented for join");
    }
  };

  // Customisation for NewThreadAsyncToken
  template<class Data, class Value>
  struct async_join_on_value_impl<
      NewThreadAsyncToken<Value, decltype(new_thread())>, Data> {
    using token_t = NewThreadAsyncToken<Value, decltype(new_thread())>;
    void operator()(Data& data, token_t&& asyncToken) {
      auto exec = asyncToken.e_;
      ::pushmi::submit(
        exec,
        ::pushmi::now(exec),
        ::pushmi::make_single(
          [asyncToken,
           out = std::move(static_cast<typename std::decay_t<Data>::out_t&>(data)),
           exec](auto) mutable {
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
    }
  };

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
                  async_join_on_value_impl<
                    std::decay_t<decltype(asyncToken)>,
                    std::decay_t<decltype(data)>>{}(
                      data, std::move(asyncToken));
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

  // Generic version
  template<class F, class Token, class Data>
  struct async_transform_on_value_impl {
    F f_;
    async_transform_on_value_impl() = default;
    constexpr explicit async_transform_on_value_impl(F f)
      : f_(std::move(f)) {}
    template<class Out, class V>
    auto operator()(Out& out, V&& inputToken) {
      static_assert(std::is_same<Token, Token>::value, "Inline not yet implemented for transform");
    }
  };

  // Customisation for NewThreadAsyncToken
  template<class F, class Data, class Value>
  struct async_transform_on_value_impl<
      F, NewThreadAsyncToken<Value, decltype(new_thread())>, Data> {

    using token_t = NewThreadAsyncToken<Value, decltype(new_thread())>;
    F f_;

    async_transform_on_value_impl() = default;
    constexpr explicit async_transform_on_value_impl(F f)
      : f_(std::move(f)) {}

    template<class Out>
    auto operator()(Out& out, token_t&& inputToken) {
      using Result = decltype(f_(std::declval<typename token_t::ValueType>()));
      using Executor = typename token_t::ExecutorType;
      static_assert(::pushmi::SemiMovable<NewThreadAsyncToken<Result, Executor>>,
        "none of the functions supplied to transform can convert this value");
      static_assert(::pushmi::SingleReceiver<Out, NewThreadAsyncToken<Result, Executor>>,
        "Result of value transform cannot be delivered to Out");

      NewThreadAsyncToken<Result, Executor> outputToken{inputToken.e_};
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
              ::pushmi::on_value([f](auto& data, auto&& asyncToken) mutable {
                async_transform_on_value_impl<
                  F,
                  std::decay_t<decltype(asyncToken)>,
                  std::decay_t<decltype(data)>>(std::move(f))(
                    data, std::move(asyncToken));
              })
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
