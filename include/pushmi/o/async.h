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
  using new_thread_t = decltype(new_thread());

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
  auto make_async_fork_fn_data(Out out, Executor ex) {
    return async_fork_fn_data<Executor, Out>{std::move(out), std::move(ex)};
  }


  // Generic version
  template<class Executor, class MakeReceiver>
  struct async_fork_customization_generic {
  private:
    struct value_fn {
      template <class Data, class Value>
      void operator()(Data& data, Value&& value) const {
        ::pushmi::submit(
          data.exec,
          ::pushmi::now(data.exec),
          ::pushmi::make_single(
            [value = (Value&&) value,
             out = std::move(static_cast<typename Data::out_t&>(data)),
             exec = data.exec](auto) mutable {
              // Token hard coded for this executor type at the moment
              auto token = InlineAsyncToken<
                  std::decay_t<decltype(value)>, std::decay_t<Executor>>{exec};
              token.value_ = std::forward<Value>(value);
              ::pushmi::set_value(out, std::move(token));
            }
          )
        );
      }
    };
    template <class Out>
    struct error_fn {
    private:
      template <class E>
      struct on_value_impl {
        E e_;
        Out out_;
        void operator()(any) {
          ::pushmi::set_error(out_, std::move(e_));
        }
      };
    public:
      template <class Data, class E>
      void operator()(Data& data, E e) const noexcept {
        ::pushmi::submit(
          data.exec,
          ::pushmi::now(data.exec),
          ::pushmi::make_single(
            on_value_impl<E>{std::move(e), std::move(static_cast<Out&>(data))}
          )
        );
      }
    };
    template <class Out>
    struct done_fn {
      struct on_value_impl {
        Out out_;
        void operator()(any) {
          ::pushmi::set_done(out_);
        }
      };
      template <class Data>
      void operator()(Data& data) const {
        ::pushmi::submit(
          data.exec,
          ::pushmi::now(data.exec),
          ::pushmi::make_single(
            on_value_impl{std::move(static_cast<Out&>(data))}
          )
        );
      }
    };

  public:
    template<class Out>
    auto operator()(Out out, Executor exec) {
      return MakeReceiver{}(
        make_async_fork_fn_data(std::move(out), std::move(exec)),
        // copy 'f' to allow multiple calls to submit
        value_fn{},
        error_fn<Out>{},
        done_fn<Out>{}
      );
    }
  };

  // Customisation for NewThreadAsyncToken
  template<class MakeReceiver>
  struct async_fork_customization_new_thread_t {
  private:
    using Executor = new_thread_t;
    struct value_fn {
      template <class Data, class Value>
      void operator()(Data& data, Value&& value) const {
        ::pushmi::submit(
          data.exec,
          ::pushmi::now(data.exec),
          ::pushmi::make_single(
            [value = (Value&&) value,
             out = std::move(static_cast<typename Data::out_t&>(data)),
             exec = data.exec](auto) mutable {
              // Token hard coded for this executor type at the moment
              auto token = NewThreadAsyncToken<
                  std::decay_t<decltype(value)>, std::decay_t<decltype(exec)>>{
                std::move(exec)};
              token.dataPtr_->v_ = std::move(value);
              token.dataPtr_->flag_ = true;
              ::pushmi::set_value(out, std::move(token));
            }
          )
        );
      }
    };
    template <class Out>
    struct error_fn {
    private:
      template <class E>
      struct on_value_impl {
        E e_;
        Out out_;
        void operator()(any) {
          ::pushmi::set_error(out_, std::move(e_));
        }
      };
    public:
      template <class Data, class E>
      void operator()(Data& data, E e) const noexcept {
        ::pushmi::submit(
          data.exec,
          ::pushmi::now(data.exec),
          ::pushmi::make_single(
            on_value_impl<E>{std::move(e), std::move(static_cast<Out&>(data))}
          )
        );
      }
    };
    template <class Out>
    struct done_fn {
      struct on_value_impl {
        Out out_;
        void operator()(any) {
          ::pushmi::set_done(out_);
        }
      };
      template <class Data>
      void operator()(Data& data) const {
        ::pushmi::submit(
          data.exec,
          ::pushmi::now(data.exec),
          ::pushmi::make_single(
            on_value_impl{std::move(static_cast<Out&>(data))}
          )
        );
      }
    };

  public:
    template<class Out>
    auto operator()(Out out, Executor exec) {
      return MakeReceiver{}(
        make_async_fork_fn_data(std::move(out), std::move(exec)),
        // copy 'f' to allow multiple calls to submit
        value_fn{},
        error_fn<Out>{},
        done_fn<Out>{}
      );
    }
  };

  // Generalisation to customise the entire protocol
  template<class MakeReceiver, class Executor, class Out>
  auto async_fork_customization(Executor exec, Out out) {
    return async_fork_customization_generic<Executor, MakeReceiver>{}(out, exec);
  }

  template<class MakeReceiver, class Out>
  auto async_fork_customization(new_thread_t exec, Out out) {
    return async_fork_customization_new_thread_t<MakeReceiver>{}(out, exec);
  }

  struct async_fork_fn {
  private:
    template <class ExecutorFactory, class MakeReceiver>
    struct out_impl {
      ExecutorFactory ef_;
      PUSHMI_TEMPLATE(class Out)
        (requires Receiver<Out>)
      auto operator()(Out out) const {
        auto exec = ef_();
        // Call customization point for fork
        // TODO: how should this actually customise.
        return async_fork_customization<MakeReceiver>(exec, out);
      }
    };
    template <class ExecutorFactory>
    struct in_impl {
      ExecutorFactory ef_;
      PUSHMI_TEMPLATE(class In)
        (requires Sender<In>)
      auto operator()(In in) const {
        return ::pushmi::detail::deferred_from<In, single<>>(
          std::move(in),
          ::pushmi::detail::submit_transform_out<In>(
            out_impl<ExecutorFactory, out_from_fn<In>>{ef_}
          )
        );
      }
    };
  public:
    PUSHMI_TEMPLATE(class ExecutorFactory)
      (requires Invocable<ExecutorFactory&>)
    auto operator()(ExecutorFactory ef) const {
      return in_impl<ExecutorFactory>{std::move(ef)};
    }
  };

  template<class Out>
  struct async_join_fn_data : public Out {
    using out_t = Out;
    async_join_fn_data(Out out) :
      Out(std::move(out)) {}
  };

  template<class Out>
  auto make_async_join_fn_data(Out out) {
    return async_join_fn_data<Out>{std::move(out)};
  }

  // Generic version, using inline execution
  template<class Token, class Data>
  struct async_join_on_value_impl {
    void operator()(Data& data, Token&& token) {
      ::pushmi::set_value(
        std::move(static_cast<typename Data::out_t&>(data)),
        std::move(token.value_));
    }
  };

  struct condition {
    bool* flag_;
    bool operator()() const {
      return *flag_;
    }
  };

  // Customisation for NewThreadAsyncToken
  template<class Data, class Value>
  struct async_join_on_value_impl<NewThreadAsyncToken<Value, new_thread_t>, Data> {
    using token_t = NewThreadAsyncToken<Value, new_thread_t>;
  private:
    using out_t = typename Data::out_t;
    struct thread_fn {
      struct on_value_fn {
        token_t asyncToken_;
        out_t out_;
        void operator()(any) {
          ::pushmi::set_value(out_, std::move(asyncToken_.dataPtr_->v_));
        }
      };
      token_t asyncToken_;
      out_t out_;
      void operator()() {
        std::unique_lock<std::mutex> lk(asyncToken_.dataPtr_->cvm_);
        if(!asyncToken_.dataPtr_->flag_) {
          asyncToken_.dataPtr_->cv_.wait(
            lk, condition{&asyncToken_.dataPtr_->flag_}
          );
        }
        ::pushmi::submit(
          asyncToken_.e_,
          ::pushmi::now(asyncToken_.e_),
          ::pushmi::make_single(on_value_fn{asyncToken_, out_})
        );
      }
    };
    struct on_value_fn {
      token_t asyncToken_;
      out_t out_;
      void operator()(any) {
        // Token hard coded for this executor type at the moment
        std::thread t(thread_fn{asyncToken_, out_});
        t.detach();
      }
    };
  public:
    void operator()(Data& data, token_t&& asyncToken) {
      ::pushmi::submit(
        asyncToken.e_,
        ::pushmi::now(asyncToken.e_),
        ::pushmi::make_single(
          on_value_fn{asyncToken, std::move(static_cast<out_t&>(data))}
        )
      );
    }
  };

  // TODO: This should be transformed to use a single customisation point as for
  // fork. To do this we need to get the executor consistently rather than
  // getting it from the value method.
  struct async_join_fn {
  private:
    struct value_fn {
      template <class Data, class Token>
      void operator()(Data& data, Token&& asyncToken) const {
        async_join_on_value_impl<std::decay_t<Token>, Data>{}(
          data,
          std::move(asyncToken) // BUGBUG this is suspect
        );
      }
    };
    template <class Out>
    struct error_fn {
      template <class Data, class E>
      void operator()(Data& data, E e) const noexcept {
        auto out = std::move(static_cast<Out&>(data));
        ::pushmi::set_error(out, std::move(e));
      }
    };
    template <class Out>
    struct done_fn {
      template <class Data>
      void operator()(Data& data) const noexcept {
        auto out = std::move(static_cast<Out&>(data));
        ::pushmi::set_done(out);
      }
    };
    template <class MakeReceiver>
    struct out_impl {
      PUSHMI_TEMPLATE (class Out)
        (requires Receiver<Out>)
      auto operator()(Out out) const {
        return MakeReceiver{}(
          make_async_join_fn_data(std::move(out)),
          // copy 'f' to allow multiple calls to submit
          ::pushmi::on_value(value_fn{}),
          ::pushmi::on_error(error_fn<Out>{}),
          ::pushmi::on_done(done_fn<Out>{})
        );
      }
    };
    struct in_impl {
      PUSHMI_TEMPLATE(class In)
        (requires Sender<In>)
      auto operator()(In in) const {
        return ::pushmi::detail::deferred_from<In, single<>>(
          std::move(in),
          ::pushmi::detail::submit_transform_out<In>(
              out_impl<out_from_fn<In>>{}
          )
        );
      }
    };
  public:
    auto operator()() const {
      return in_impl{};
    }
  };

  // Generic version implemented as inline
  template<class F, class Token, class Data>
  struct async_transform_on_value_impl {
    F f_;
    async_transform_on_value_impl() = default;
    constexpr explicit async_transform_on_value_impl(F f)
      : f_(std::move(f)) {}

    template<class Out, class V>
    auto operator()(Out& out, V&& inputToken) {
      auto outputToken = inputToken;
      outputToken.value_ = f_(std::move(inputToken.value_));
      ::pushmi::set_value(out, outputToken);
    }
  };

  // Customisation for NewThreadAsyncToken
  template<class F, class Data, class Value>
  struct async_transform_on_value_impl<
      F, NewThreadAsyncToken<Value, new_thread_t>, Data> {

    using token_t = NewThreadAsyncToken<Value, new_thread_t>;
  private:
    using Result = invoke_result_t<F&, typename token_t::ValueType&>;
    using Executor = typename token_t::ExecutorType;
    static_assert(::pushmi::SemiMovable<NewThreadAsyncToken<Result, Executor>>,
      "none of the functions supplied to transform can convert this value");
    using OutputToken = NewThreadAsyncToken<Result, Executor>;

    struct thread_fn {
      token_t inputToken_;
      OutputToken outputToken_;
      F func_;
      void operator()() {
        std::unique_lock<std::mutex> inlk(inputToken_.dataPtr_->cvm_);
        // Wait for input value
        if(!inputToken_.dataPtr_->flag_) {
          inputToken_.dataPtr_->cv_.wait(
            inlk, condition{&inputToken_.dataPtr_->flag_}
          );
        }
        // Compute
        auto result = func_(inputToken_.dataPtr_->v_);
        // Move output and notify
        std::unique_lock<std::mutex> outlk(outputToken_.dataPtr_->cvm_);
        outputToken_.dataPtr_->v_ = std::move(result);
        outputToken_.dataPtr_->flag_ = true;
        outputToken_.dataPtr_->cv_.notify_all();
      }
    };

    F f_;

  public:
    async_transform_on_value_impl() = default;
    constexpr explicit async_transform_on_value_impl(F f)
      : f_(std::move(f)) {}

    template<class Out>
    auto operator()(Out& out, token_t&& inputToken) {
      static_assert(::pushmi::SingleReceiver<Out, NewThreadAsyncToken<Result, Executor>>,
        "Result of value transform cannot be delivered to Out");
      OutputToken outputToken{inputToken.e_};
      std::thread t(thread_fn{inputToken, outputToken, f_});
      t.detach();
      ::pushmi::set_value(out, outputToken);
    }
  };

  // TODO: This should be transformed to use a single customisation point as for
  // fork. To do this we need to get the executor consistently rather than
  // getting it from the value method.
  struct async_transform_fn {
  private:
    template <class F>
    struct on_value_fn {
      F f_;
      template <class Data, class Token>
      void operator()(Data& data, Token&& asyncToken) {
        async_transform_on_value_impl<F, std::decay_t<Token>, Data>(
            std::move(f_))(data, std::move(asyncToken)
        );
      }
    };
    template <class F, class MakeReceiver>
    struct out_impl {
      F f_;
      PUSHMI_TEMPLATE(class Out)
        (requires Receiver<Out>)
      auto operator()(Out out) const {
        return MakeReceiver{}(
          std::move(out),
          // copy 'f' to allow multiple calls to submit
          ::pushmi::on_value(on_value_fn<F>{f_})
        );
      }
    };
    template <class F>
    struct in_impl {
      F f_;
      PUSHMI_TEMPLATE(class In)
        (requires Sender<In>)
      auto operator()(In in) {
        return ::pushmi::detail::deferred_from<In, ::pushmi::single<>>(
          std::move(in),
          ::pushmi::detail::submit_transform_out<In>(
              out_impl<F, out_from_fn<In>>{f_}
          )
        );
      }
    };
    template <class F>
    static auto make_in_impl(F f) {
      return in_impl<F>{std::move(f)};
    }
  public:
    template <class... FN>
    auto operator()(FN... fn) const {
      return make_in_impl(overload(std::move(fn)...));
    }
  };

  // Generic version implemented as inline
  template<
    class ValueFunction,
    class ShapeF,
    class SharedF,
    class ResultS,
    class Token,
    class Data>
  struct async_bulk_on_value_impl {
    ValueFunction f_;
    ShapeF shapeF_;
    SharedF sharedF_;
    ResultS resultS_;

    async_bulk_on_value_impl() = default;
    constexpr explicit async_bulk_on_value_impl(
      ValueFunction f,
      ShapeF shapeF,
      SharedF sharedF,
      ResultS resultS)
      : f_(std::move(f)),
        shapeF_(std::move(shapeF)),
        sharedF_(std::move(sharedF)),
        resultS_(std::move(resultS)) {}

    template<class Out, class V>
    auto operator()(Out& out, V&& inputToken) {
      auto shape = shapeF_(inputToken.value_);
      auto shared = sharedF_(inputToken.value_, shape);
      using ShapeType = decltype(shape);
      for(ShapeType i{}; i <= shape; ++i) {
        f_(inputToken.value_, i, shared);
      }
      auto outputToken = inputToken;
      outputToken.value_ = resultS_(shared);
      ::pushmi::set_value(out, std::move(outputToken));
    }
  };


  // TODO: This should be transformed to use a single customisation point as for
  // fork. To do this we need to get the executor consistently rather than
  // getting it from the value method.
  struct async_bulk_fn {
  private:
    template <class ValueFunction, class ShapeF, class SharedF, class ResultS>
    struct on_value_fn {
      ValueFunction vfn_;
      ShapeF shapeF_;
      SharedF sharedF_;
      ResultS resultS_;
      template <class Data, class Token>
      void operator()(Data& data, Token&& asyncToken) {
        async_bulk_on_value_impl<
          ValueFunction,
          ShapeF,
          SharedF,
          ResultS,
          std::decay_t<Token>,
          Data>(
            std::move(vfn_),
            std::move(shapeF_),
            std::move(sharedF_),
            std::move(resultS_))(data, std::move(asyncToken));
      }
    };
    template <class ValueFunction, class ShapeF, class SharedF, class ResultS, class MakeReceiver>
    struct out_impl {
      ValueFunction vfn_;
      ShapeF shapeF_;
      SharedF sharedF_;
      ResultS resultS_;
      PUSHMI_TEMPLATE (class Out)
        (requires Receiver<Out>)
      auto operator()(Out out) const {
        return MakeReceiver{}(
          std::move(out),
          // copy 'f' to allow multiple calls to submit
          ::pushmi::on_value(
            on_value_fn<ValueFunction, ShapeF, SharedF, ResultS>{
                vfn_, shapeF_, sharedF_, resultS_}
          )
        );
      }
    };
    template <class ValueFunction, class ShapeF, class SharedF, class ResultS>
    struct in_impl {
      ValueFunction vfn_;
      ShapeF shapeF_;
      SharedF sharedF_;
      ResultS resultS_;
      PUSHMI_TEMPLATE (class In)
        (requires Sender<In>)
      auto operator()(In in) const {
        return ::pushmi::detail::deferred_from<In, ::pushmi::single<>>(
          std::move(in),
          ::pushmi::detail::submit_transform_out<In>(
            out_impl<ValueFunction, ShapeF, SharedF, ResultS, out_from_fn<In>>{
              vfn_, shapeF_, sharedF_, resultS_}
          )
        );
      }
    };
  public:
    template <class ValueFunction, class ShapeF, class SharedF, class ResultS>
    auto operator()(ValueFunction, ShapeF, SharedF, ResultS) const;
  };

  template <class ValueFunction, class ShapeF, class SharedF, class ResultS>
  auto async_bulk_fn::operator()(
      ValueFunction vfn, ShapeF shapeF, SharedF sharedF, ResultS resultS)
      const {
    return in_impl<ValueFunction, ShapeF, SharedF, ResultS>{
      std::move(vfn), std::move(shapeF), std::move(sharedF), std::move(resultS)};
  }
} // namespace detail

namespace operators {
PUSHMI_INLINE_VAR constexpr detail::async_join_fn async_join{};
PUSHMI_INLINE_VAR constexpr detail::async_fork_fn async_fork{};
PUSHMI_INLINE_VAR constexpr detail::async_transform_fn async_transform{};
PUSHMI_INLINE_VAR constexpr detail::async_bulk_fn async_bulk{};
} // namespace operators
} // namespace pushmi
