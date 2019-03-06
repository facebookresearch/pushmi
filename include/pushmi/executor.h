/*
 * Copyright 2018-present Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <chrono>
#include <functional>

#include <pushmi/concepts.h>
#include <pushmi/extension_points.h>
#include <pushmi/forwards.h>
#include <pushmi/receiver.h>
#include <pushmi/traits.h>

namespace pushmi {
namespace detail {
template <class T, template <class...> class C>
using not_is_t = std::enable_if_t<!is_v<std::decay_t<T>, C>, std::decay_t<T>>;
} // namespace detail

//
// define types for executors

template <class E, class... VN>
class any_executor {
  union data {
    void* pobj_ = nullptr;
    char buffer_[sizeof(std::tuple<VN...>)]; // can hold a V in-situ
  } data_{};
  template <class Wrapped>
  static constexpr bool insitu() {
    return sizeof(Wrapped) <= sizeof(data::buffer_) &&
        std::is_nothrow_move_constructible<Wrapped>::value;
  }
  struct vtable {
    static void s_op(data&, data*) {}
    static any_single_sender<E, any_executor_ref<E>> s_schedule(data&, VN...) { return {}; }
    void (*op_)(data&, data*) = vtable::s_op;
    any_single_sender<E, any_executor_ref<E>> (*schedule_)(data&, VN...) = vtable::s_schedule;
  };
  static constexpr vtable const noop_{};
  vtable const* vptr_ = &noop_;
  template <class Wrapped>
  any_executor(Wrapped obj, std::false_type) : any_executor() {
    struct s {
      static void op(data& src, data* dst) {
        if (dst)
          dst->pobj_ = std::exchange(src.pobj_, nullptr);
        delete static_cast<Wrapped const*>(src.pobj_);
      }
      static any_single_sender<E, any_executor_ref<E>> schedule(data& src, VN... vn) {
        return any_single_sender<E, any_executor_ref<E>>{::pushmi::schedule(
            *static_cast<Wrapped*>(src.pobj_), vn...)};
      }
    };
    static const vtable vtbl{s::op, s::schedule};
    data_.pobj_ = new Wrapped(std::move(obj));
    vptr_ = &vtbl;
  }
  template <class Wrapped>
  any_executor(Wrapped obj, std::true_type) noexcept
      : any_executor() {
    struct s {
      static void op(data& src, data* dst) {
        if (dst)
          new (dst->buffer_)
              Wrapped(std::move(*static_cast<Wrapped*>((void*)src.buffer_)));
        static_cast<Wrapped const*>((void*)src.buffer_)->~Wrapped();
      }
      static any_single_sender<E, any_executor_ref<E>> schedule(data& src, VN... vn) {
        return any_single_sender<E, any_executor_ref<E>>{::pushmi::schedule(
            *static_cast<Wrapped*>((void*)src.buffer_), vn...)};
      }
    };
    static const vtable vtbl{s::op, s::schedule};
    new (data_.buffer_) Wrapped(std::move(obj));
    vptr_ = &vtbl;
  }
  template <class T, class U = std::decay_t<T>>
  using wrapped_t =
      std::enable_if_t<!std::is_same<U, any_executor>::value, U>;

 public:
  using properties = property_set<is_executor<>>;

  any_executor() = default;
  any_executor(any_executor&& that) noexcept : any_executor() {
    that.vptr_->op_(that.data_, &data_);
    std::swap(that.vptr_, vptr_);
  }

  PUSHMI_TEMPLATE(class Wrapped)
  (requires Executor<wrapped_t<Wrapped>>) //
      explicit any_executor(Wrapped obj) noexcept(insitu<Wrapped>())
      : any_executor{std::move(obj), bool_<insitu<Wrapped>()>{}} {}
  ~any_executor() {
    vptr_->op_(data_, nullptr);
  }
  any_executor& operator=(any_executor&& that) noexcept {
    this->~any_executor();
    new ((void*)this) any_executor(std::move(that));
    return *this;
  }
  PUSHMI_TEMPLATE(class... AN)
  (requires sizeof...(VN) == sizeof...(AN)) //
  any_single_sender<E, any_executor_ref<E>> schedule(AN&&... an) {
    // moved this check out of the requires due to a
    // mismached pack size between VN and AN on gcc.
    static_assert(And<Constructible<VN, AN>...>, "arguments must be convertible");
    return vptr_->schedule_(data_, VN{(AN &&) an}...);
  }
};

// Class static definitions:
template <class E, class... VN>
constexpr typename any_executor<E, VN...>::vtable const
    any_executor<E, VN...>::noop_;

template <class SF>
class executor<SF> {
  SF sf_;

 public:
  using properties = property_set<is_executor<>>;

  constexpr executor() = default;
  constexpr explicit executor(SF sf) : sf_(std::move(sf)) {}

  PUSHMI_TEMPLATE(class... AN)
  (requires PUSHMI_EXP(
      lazy::Invocable<SF&, AN...>)) //
      auto schedule(AN&&... an) {
    return sf_((AN&&)an...);
  }
};

template <
    PUSHMI_TYPE_CONSTRAINT(Executor) Data,
    class DSF>
class executor<Data, DSF> {
  Data data_;
  DSF sf_;

 public:
  using properties = property_set_insert_t<
      properties_t<Data>,
      property_set<is_executor<>>>;

  constexpr executor() = default;
  constexpr explicit executor(Data data) : data_(std::move(data)) {}
  constexpr executor(Data data, DSF sf)
      : data_(std::move(data)), sf_(std::move(sf)) {}

  PUSHMI_TEMPLATE(class... AN)
  (requires PUSHMI_EXP(
      lazy::Invocable<DSF&, Data&, AN...>)) //
      auto schedule(AN&&... an) {
    return sf_(data_, (AN&&)an...);
  }
};

template <>
class executor<> : public executor<ignoreSF> {
 public:
  executor() = default;
};

////////////////////////////////////////////////////////////////////////////////
// make_executor
PUSHMI_INLINE_VAR constexpr struct make_executor_fn {
  inline auto operator()() const {
    return executor<ignoreSF>{};
  }
  PUSHMI_TEMPLATE(class SF)
  (requires not Executor<SF>) //
      auto
      operator()(SF sf) const {
    return executor<SF>{std::move(sf)};
  }
  PUSHMI_TEMPLATE(class Data)
  (requires True<>&& Executor<Data>) //
      auto
      operator()(Data d) const {
    return executor<Data, passDSF>{std::move(d)};
  }
  PUSHMI_TEMPLATE(class Data, class DSF)
  (requires Executor<Data>) //
      auto
      operator()(Data d, DSF sf) const {
    return executor<Data, DSF>{std::move(d), std::move(sf)};
  }
} const make_executor{};

////////////////////////////////////////////////////////////////////////////////
// deduction guides
#if __cpp_deduction_guides >= 201703
executor()->executor<ignoreSF>;

PUSHMI_TEMPLATE(class SF)
(requires True<> PUSHMI_BROKEN_SUBSUMPTION(&&not Executor<SF>)) //
    executor(SF)
        ->executor<SF>;

PUSHMI_TEMPLATE(class Data)
(requires True<>&& Executor<Data>) //
    executor(Data)
        ->executor<Data, passDSF>;

PUSHMI_TEMPLATE(class Data, class DSF)
(requires Executor<Data>) //
    executor(Data, DSF)
        ->executor<Data, DSF>;
#endif

template <>
struct construct_deduced<executor> : make_executor_fn {};


namespace detail {
template <class T>
using not_any_executor_ref_t = not_is_t<T, any_executor_ref>;
} // namespace detail

template <class E, class... VN>
struct any_executor_ref {
 private:
  using This = any_executor_ref;
  void* pobj_;
  struct vtable {
    any_single_sender<E, any_executor_ref<E>> (*schedule_)(void*, VN...);
  } const* vptr_;
  template <class T>
  using wrapped_t = detail::not_any_executor_ref_t<T>;

 public:
  using properties = property_set<is_executor<>>;

  any_executor_ref() = delete;
  any_executor_ref(const any_executor_ref&) = default;

  PUSHMI_TEMPLATE(class Wrapped)
  (requires Executor<wrapped_t<Wrapped>>) //
      any_executor_ref(Wrapped& w) {
    struct s {
      static any_single_sender<E, any_executor_ref<E>> schedule(void* pobj) {
        return any_single_sender<E, any_executor_ref<E>>{
            ::pushmi::schedule(*static_cast<Wrapped*>(pobj))};
      }
    };
    static const vtable vtbl{s::schedule};
    pobj_ = std::addressof(w);
    vptr_ = &vtbl;
  }
  template<class... AN>
  any_single_sender<E, any_executor_ref<E>> schedule(AN&&... an) {
    return vptr_->schedule_(pobj_, VN{(AN&&)an}...);
  }
};

////////////////////////////////////////////////////////////////////////////////
// make_any_executor_ref
template <class E = std::exception_ptr>
auto make_any_executor_ref() {
  return any_executor_ref<E>{};
}

PUSHMI_TEMPLATE(class E = std::exception_ptr, class Wrapped)
(requires Executor<detail::not_any_executor_ref_t<Wrapped>>) //
    auto make_any_executor_ref(Wrapped& w) {
  return any_executor_ref<E>{w};
}

////////////////////////////////////////////////////////////////////////////////
// deduction guides
#if __cpp_deduction_guides >= 201703
any_executor_ref()->any_executor_ref<std::exception_ptr>;

PUSHMI_TEMPLATE(class Wrapped)
(requires Executor<detail::not_any_executor_ref_t<Wrapped>>) //
    any_executor_ref(Wrapped&)
        ->any_executor_ref<std::exception_ptr>;
#endif

//
// define types for constrained executors


template <class E, class CV, class... VN>
class any_constrained_executor {
  union data {
    void* pobj_ = nullptr;
    char buffer_[sizeof(std::tuple<CV, VN...>)]; // can hold a V in-situ
  } data_{};
  template <class Wrapped>
  static constexpr bool insitu() {
    return sizeof(Wrapped) <= sizeof(data::buffer_) &&
        std::is_nothrow_move_constructible<Wrapped>::value;
  }
  struct vtable {
    static void s_op(data&, data*) {}
    static any_single_sender<E, any_constrained_executor_ref<E>> s_schedule(data&, CV, VN...) { return {}; }
    static CV s_top(data&) { return {}; }
    void (*op_)(data&, data*) = vtable::s_op;
    CV (*top_)(data&) = vtable::s_top;
    any_single_sender<E, any_constrained_executor_ref<E>> (*schedule_)(data&, CV, VN...) = vtable::s_schedule;
  };
  static constexpr vtable const noop_{};
  vtable const* vptr_ = &noop_;
  template <class Wrapped>
  any_constrained_executor(Wrapped obj, std::false_type) : any_constrained_executor() {
    struct s {
      static void op(data& src, data* dst) {
        if (dst)
          dst->pobj_ = std::exchange(src.pobj_, nullptr);
        delete static_cast<Wrapped const*>(src.pobj_);
      }
      static CV top(data& src) {
        return ::pushmi::top(*static_cast<Wrapped*>(src.pobj_));
      }
      static any_single_sender<E, any_constrained_executor_ref<E>> schedule(data& src, CV cv, VN... vn) {
        return any_single_sender<E, any_constrained_executor_ref<E>>{::pushmi::schedule(
            *static_cast<Wrapped*>(src.pobj_), cv, vn...)};
      }
    };
    static const vtable vtbl{s::op, s::top, s::schedule};
    data_.pobj_ = new Wrapped(std::move(obj));
    vptr_ = &vtbl;
  }
  template <class Wrapped>
  any_constrained_executor(Wrapped obj, std::true_type) noexcept
      : any_constrained_executor() {
    struct s {
      static void op(data& src, data* dst) {
        if (dst)
          new (dst->buffer_)
              Wrapped(std::move(*static_cast<Wrapped*>((void*)src.buffer_)));
        static_cast<Wrapped const*>((void*)src.buffer_)->~Wrapped();
      }
      static CV top(data& src) {
        return ::pushmi::top(*static_cast<Wrapped*>((void*)src.buffer_));
      }
      static any_single_sender<E, any_constrained_executor_ref<E>> schedule(data& src, CV cv, VN... vn) {
        return any_single_sender<E, any_constrained_executor_ref<E>>{::pushmi::schedule(
            *static_cast<Wrapped*>((void*)src.buffer_), cv, vn...)};
      }
    };
    static const vtable vtbl{s::op, s::top, s::schedule};
    new (data_.buffer_) Wrapped(std::move(obj));
    vptr_ = &vtbl;
  }
  template <class T, class U = std::decay_t<T>>
  using wrapped_t =
      std::enable_if_t<!std::is_same<U, any_constrained_executor>::value, U>;

 public:
  using properties = property_set<is_constrained<>>;

  any_constrained_executor() = default;
  any_constrained_executor(any_constrained_executor&& that) noexcept : any_constrained_executor() {
    that.vptr_->op_(that.data_, &data_);
    std::swap(that.vptr_, vptr_);
  }

  PUSHMI_TEMPLATE(class Wrapped)
  (requires ConstrainedExecutor<wrapped_t<Wrapped>>) //
      explicit any_constrained_executor(Wrapped obj) noexcept(insitu<Wrapped>())
      : any_constrained_executor{std::move(obj), bool_<insitu<Wrapped>()>{}} {}
  ~any_constrained_executor() {
    vptr_->op_(data_, nullptr);
  }
  any_constrained_executor& operator=(any_constrained_executor&& that) noexcept {
    this->~any_constrained_executor();
    new ((void*)this) any_constrained_executor(std::move(that));
    return *this;
  }
  CV top() {
    return vptr_->top_(data_);
  }
  PUSHMI_TEMPLATE(class... AN)
  (requires And<Constructible<VN, AN>...>)
  any_single_sender<E, any_constrained_executor_ref<E>> schedule(AN&&... an) {
    return vptr_->schedule_(data_, top(), VN{(AN &&) an}...);
  }
  PUSHMI_TEMPLATE(class... AN)
  (requires And<Constructible<VN, AN>...>)
  any_single_sender<E, any_constrained_executor_ref<E>> schedule(CV cv, AN&&... an) {
    return vptr_->schedule_(data_, cv, VN{(AN &&) an}...);
  }
};

// Class static definitions:
template <class E, class CV, class... VN>
constexpr typename any_constrained_executor<E, CV, VN...>::vtable const
    any_constrained_executor<E, CV, VN...>::noop_;

template <class SF, class ZF>
class constrained_executor<SF, ZF> {
  SF sf_;
  ZF zf_;

 public:
  using properties = property_set<is_constrained<>>;

  constexpr constrained_executor() = default;
  constexpr explicit constrained_executor(SF sf) : sf_(std::move(sf)) {}
  constexpr constrained_executor(SF sf, ZF zf) : sf_(std::move(sf)), zf_(std::move(zf)) {}

  invoke_result_t<ZF&> top() {
    return zf_();
  }

  PUSHMI_TEMPLATE(class... AN)
  (requires PUSHMI_EXP(
      lazy::Invocable<SF&, AN...>)) //
      auto schedule(AN&&... an) {
    return sf_((AN&&)an...);
  }
};

template <
    PUSHMI_TYPE_CONSTRAINT(ConstrainedExecutor) Data,
    class DSF,
    class DZF>
class constrained_executor<Data, DSF, DZF> {
  Data data_;
  DSF sf_;
  DZF zf_;

 public:
  using properties = property_set_insert_t<
      properties_t<Data>,
      property_set<is_constrained<>>>;

  constexpr constrained_executor() = default;
  constexpr explicit constrained_executor(Data data) : data_(std::move(data)) {}
  constexpr constrained_executor(Data data, DSF sf)
      : data_(std::move(data)), sf_(std::move(sf)) {}
  constexpr constrained_executor(Data data, DSF sf, DZF zf)
      : data_(std::move(data)), sf_(std::move(sf)), zf_(std::move(zf)) {}

  invoke_result_t<DZF&, Data&> top() {
    return zf_(data_);
  }

  PUSHMI_TEMPLATE(class... AN)
  (requires PUSHMI_EXP(
      lazy::Invocable<DSF&, Data&, AN...>)) //
      auto schedule(AN&&... an) {
    return sf_(data_, (AN&&)an...);
  }
};

template <>
class constrained_executor<> : public constrained_executor<ignoreSF, priorityZeroF> {
 public:
  constrained_executor() = default;
};

////////////////////////////////////////////////////////////////////////////////
// make_constrained_executor
PUSHMI_INLINE_VAR constexpr struct make_constrained_executor_fn {
  inline auto operator()() const {
    return constrained_executor<ignoreSF, priorityZeroF>{};
  }
  PUSHMI_TEMPLATE(class SF)
  (requires not ConstrainedExecutor<SF>) //
      auto
      operator()(SF sf) const {
    return constrained_executor<SF, priorityZeroF>{std::move(sf)};
  }
  PUSHMI_TEMPLATE(class SF, class ZF)
  (requires not ConstrainedExecutor<SF>) //
      auto
      operator()(SF sf, ZF zf) const {
    return constrained_executor<SF, ZF>{std::move(sf), std::move(zf)};
  }
  PUSHMI_TEMPLATE(class Data)
  (requires True<>&& ConstrainedExecutor<Data>) //
      auto
      operator()(Data d) const {
    return constrained_executor<Data, passDSF, passDZF>{std::move(d)};
  }
  PUSHMI_TEMPLATE(class Data, class DSF)
  (requires ConstrainedExecutor<Data>) //
      auto
      operator()(Data d, DSF sf) const {
    return constrained_executor<Data, DSF, passDZF>{std::move(d), std::move(sf)};
  }
  PUSHMI_TEMPLATE(class Data, class DSF, class DZF)
  (requires ConstrainedExecutor<Data>) //
      auto
      operator()(Data d, DSF sf, DZF zf) const {
    return constrained_executor<Data, DSF, DZF>{std::move(d), std::move(sf), std::move(zf)};
  }
} const make_constrained_executor{};

////////////////////////////////////////////////////////////////////////////////
// deduction guides
#if __cpp_deduction_guides >= 201703
constrained_executor()->constrained_executor<ignoreSF, priorityZeroF>;

PUSHMI_TEMPLATE(class SF)
(requires True<> PUSHMI_BROKEN_SUBSUMPTION(&&not ConstrainedExecutor<SF>)) //
    constrained_executor(SF)
        ->constrained_executor<SF, priorityZeroF>;

PUSHMI_TEMPLATE(class SF, class ZF)
(requires True<> PUSHMI_BROKEN_SUBSUMPTION(&&not ConstrainedExecutor<SF>)) //
    constrained_executor(SF, ZF)
        ->constrained_executor<SF, ZF>;

PUSHMI_TEMPLATE(class Data)
(requires True<>&& ConstrainedExecutor<Data>) //
    constrained_executor(Data)
        ->constrained_executor<Data, passDSF, passDZF>;

PUSHMI_TEMPLATE(class Data, class DSF)
(requires ConstrainedExecutor<Data>) //
    constrained_executor(Data, DSF)
        ->constrained_executor<Data, DSF, passDZF>;

PUSHMI_TEMPLATE(class Data, class DSF, class DZF)
(requires ConstrainedExecutor<Data>) //
    constrained_executor(Data, DSF, DZF)
        ->constrained_executor<Data, DSF, DZF>;
#endif

template <>
struct construct_deduced<constrained_executor> : make_constrained_executor_fn {};

namespace detail {
template <class T>
using not_any_constrained_executor_ref_t =
    not_is_t<T, any_constrained_executor_ref>;
} // namespace detail

template <class E, class CV, class... VN>
struct any_constrained_executor_ref {
 private:
  using This = any_constrained_executor_ref;
  void* pobj_;
  struct vtable {
    CV (*top_)(void*);
    any_single_sender<E, CV, any_constrained_executor_ref<E, CV>> (*schedule_)(
        void*, VN...);
    any_single_sender<E, CV, any_constrained_executor_ref<E, CV>> (
        *schedule_cv_)(void*, CV, VN...);
  } const* vptr_;
  template <class T>
  using wrapped_t = detail::not_any_constrained_executor_ref_t<T>;

 public:
  using properties = property_set<is_constrained<>>;

  any_constrained_executor_ref() = delete;
  any_constrained_executor_ref(const any_constrained_executor_ref&) = default;

  PUSHMI_TEMPLATE(class Wrapped)
  (requires ConstrainedExecutor<wrapped_t<Wrapped>>)
      any_constrained_executor_ref(Wrapped& w) {
    struct s {
      static CV top(void* pobj) {
        return ::pushmi::top(*static_cast<Wrapped*>(pobj));
      }
      static any_single_sender<E, CV, any_constrained_executor_ref<E, CV>>
      schedule(void* pobj, VN... vn) {
        return ::pushmi::schedule(
            *static_cast<Wrapped*>(pobj),
            ::pushmi::top(*static_cast<Wrapped*>(pobj)), vn...);
      }
      static any_single_sender<E, CV, any_constrained_executor_ref<E, CV>>
      schedule_cv(void* pobj, CV cv, VN... vn) {
        return ::pushmi::schedule(*static_cast<Wrapped*>(pobj), cv, vn...);
      }
    };
    static const vtable vtbl{s::top, s::schedule, s::schedule_cv};
    pobj_ = std::addressof(w);
    vptr_ = &vtbl;
  }
  CV top() {
    return vptr_->top_(pobj_);
  }
  PUSHMI_TEMPLATE(class... AN)
  (requires sizeof...(VN) == sizeof...(AN)) //
  any_single_sender<E, any_constrained_executor_ref<E>> schedule(AN&&... an) {
    // moved this check out of the requires due to a
    // mismached pack size between VN and AN on gcc.
    static_assert(And<Constructible<VN, AN>...>, "arguments must be convertible");
    return vptr_->schedule_(pobj_, VN{(AN&&)an}...);
  }
  PUSHMI_TEMPLATE(class... AN)
  (requires sizeof...(VN) == sizeof...(AN)) //
  any_single_sender<E, any_constrained_executor_ref<E>> schedule(CV cv, AN&&... an) {
    // moved this check out of the requires due to a
    // mismached pack size between VN and AN on gcc.
    static_assert(And<Constructible<VN, AN>...>, "arguments must be convertible");
    return vptr_->schedule_cv_(pobj_, cv, VN{(AN&&)an}...);
  }
};

////////////////////////////////////////////////////////////////////////////////
// make_any_constrained_executor_ref
template <class E = std::exception_ptr, class CV = std::ptrdiff_t>
auto make_any_constrained_executor_ref() {
  return any_constrained_executor_ref<E, CV>{};
}

PUSHMI_TEMPLATE(class E = std::exception_ptr, class Wrapped)
(requires ConstrainedExecutor<
    detail::not_any_constrained_executor_ref_t<Wrapped>>) //
    auto make_any_constrained_executor_ref(Wrapped& w) {
  return any_constrained_executor_ref<E, constraint_t<Wrapped>>{w};
}

////////////////////////////////////////////////////////////////////////////////
// deduction guides
#if __cpp_deduction_guides >= 201703
any_constrained_executor_ref()
    ->any_constrained_executor_ref<std::exception_ptr, std::ptrdiff_t>;

PUSHMI_TEMPLATE(class Wrapped)
(requires ConstrainedExecutor<
    detail::not_any_constrained_executor_ref_t<Wrapped>>) //
    any_constrained_executor_ref(Wrapped&)
        ->any_constrained_executor_ref<
            std::exception_ptr,
            constraint_t<Wrapped>>;
#endif

//
// define types for time executors

template <class E, class TP, class... VN>
class any_time_executor {
  union data {
    void* pobj_ = nullptr;
    char buffer_[sizeof(std::tuple<TP, VN...>)]; // can hold a V in-situ
  } data_{};
  template <class Wrapped>
  static constexpr bool insitu() {
    return sizeof(Wrapped) <= sizeof(data::buffer_) &&
        std::is_nothrow_move_constructible<Wrapped>::value;
  }
  struct vtable {
    static void s_op(data&, data*) {}
    static any_single_sender<E, any_time_executor_ref<E>> s_schedule(data&, TP, VN...) { return {}; }
    static TP s_now(data&) { return {}; }
    void (*op_)(data&, data*) = vtable::s_op;
    TP (*now_)(data&) = vtable::s_now;
    any_single_sender<E, any_time_executor_ref<E>> (*schedule_)(data&, TP, VN...) = vtable::s_schedule;
  };
  static constexpr vtable const noop_{};
  vtable const* vptr_ = &noop_;
  template <class Wrapped>
  any_time_executor(Wrapped obj, std::false_type) : any_time_executor() {
    struct s {
      static void op(data& src, data* dst) {
        if (dst)
          dst->pobj_ = std::exchange(src.pobj_, nullptr);
        delete static_cast<Wrapped const*>(src.pobj_);
      }
      static TP now(data& src) {
        return ::pushmi::now(*static_cast<Wrapped*>(src.pobj_));
      }
      static any_single_sender<E, any_time_executor_ref<E>> schedule(data& src, TP tp, VN... vn) {
        return any_single_sender<E, any_time_executor_ref<E>>{::pushmi::schedule(
            *static_cast<Wrapped*>(src.pobj_), tp, vn...)};
      }
    };
    static const vtable vtbl{s::op, s::now, s::schedule};
    data_.pobj_ = new Wrapped(std::move(obj));
    vptr_ = &vtbl;
  }
  template <class Wrapped>
  any_time_executor(Wrapped obj, std::true_type) noexcept
      : any_time_executor() {
    struct s {
      static void op(data& src, data* dst) {
        if (dst)
          new (dst->buffer_)
              Wrapped(std::move(*static_cast<Wrapped*>((void*)src.buffer_)));
        static_cast<Wrapped const*>((void*)src.buffer_)->~Wrapped();
      }
      static TP now(data& src) {
        return ::pushmi::now(*static_cast<Wrapped*>((void*)src.buffer_));
      }
      static any_single_sender<E, any_time_executor_ref<E>> schedule(data& src, TP tp, VN... vn) {
        return any_single_sender<E, any_time_executor_ref<E>>{::pushmi::schedule(
            *static_cast<Wrapped*>((void*)src.buffer_), tp, vn...)};
      }
    };
    static const vtable vtbl{s::op, s::now, s::schedule};
    new (data_.buffer_) Wrapped(std::move(obj));
    vptr_ = &vtbl;
  }
  template <class T, class U = std::decay_t<T>>
  using wrapped_t =
      std::enable_if_t<!std::is_same<U, any_time_executor>::value, U>;

 public:
  using properties = property_set<is_time<>>;

  any_time_executor() = default;
  any_time_executor(any_time_executor&& that) noexcept : any_time_executor() {
    that.vptr_->op_(that.data_, &data_);
    std::swap(that.vptr_, vptr_);
  }

  PUSHMI_TEMPLATE(class Wrapped)
  (requires TimeExecutor<wrapped_t<Wrapped>>) //
      explicit any_time_executor(Wrapped obj) noexcept(insitu<Wrapped>())
      : any_time_executor{std::move(obj), bool_<insitu<Wrapped>()>{}} {}
  ~any_time_executor() {
    vptr_->op_(data_, nullptr);
  }
  any_time_executor& operator=(any_time_executor&& that) noexcept {
    this->~any_time_executor();
    new ((void*)this) any_time_executor(std::move(that));
    return *this;
  }
  TP top() {
    return vptr_->now_(data_);
  }
  PUSHMI_TEMPLATE(class... AN)
  (requires And<Constructible<VN, AN>...>)
  any_single_sender<E, any_time_executor_ref<E>> schedule(AN&&... an) {
    return vptr_->schedule_(data_, vptr_->now_(data_), VN{(AN &&) an}...);
  }
  PUSHMI_TEMPLATE(class... AN)
  (requires And<Constructible<VN, AN>...>)
  any_single_sender<E, any_time_executor_ref<E>> schedule(TP tp, AN&&... an) {
    return vptr_->schedule_(data_, tp, VN{(AN &&) an}...);
  }
};

// Class static definitions:
template <class E, class TP, class... VN>
constexpr typename any_time_executor<E, TP, VN...>::vtable const
    any_time_executor<E, TP, VN...>::noop_;

template <class SF, class NF>
class time_executor<SF, NF> {
  SF sf_;
  NF nf_;

 public:
  using properties = property_set<is_time<>>;

  constexpr time_executor() = default;
  constexpr explicit time_executor(SF sf) : sf_(std::move(sf)) {}
  constexpr time_executor(SF sf, NF nf) : sf_(std::move(sf)), nf_(std::move(nf)) {}

  invoke_result_t<NF&> top() {
    return nf_();
  }

  PUSHMI_TEMPLATE(class... AN)
  (requires PUSHMI_EXP(
      lazy::Invocable<SF&, AN...>)) //
      auto schedule(AN&&... an) {
    return sf_((AN&&)an...);
  }
};

template <
    PUSHMI_TYPE_CONSTRAINT(TimeExecutor) Data,
    class DSF,
    class DNF>
class time_executor<Data, DSF, DNF> {
  Data data_;
  DSF sf_;
  DNF nf_;

 public:
  using properties = property_set_insert_t<
      properties_t<Data>,
      property_set<is_time<>>>;

  constexpr time_executor() = default;
  constexpr explicit time_executor(Data data) : data_(std::move(data)) {}
  constexpr time_executor(Data data, DSF sf)
      : data_(std::move(data)), sf_(std::move(sf)) {}
  constexpr time_executor(Data data, DSF sf, DNF nf)
      : data_(std::move(data)), sf_(std::move(sf)), nf_(std::move(nf)) {}

  invoke_result_t<DNF&, Data&> top() {
    return nf_(data_);
  }

  PUSHMI_TEMPLATE(class... AN)
  (requires PUSHMI_EXP(
      lazy::Invocable<DSF&, Data&, AN...>)) //
      auto schedule(AN&&... an) {
    return sf_(data_, (AN&&)an...);
  }
};

template <>
class time_executor<> : public time_executor<ignoreSF, systemNowF> {
 public:
  time_executor() = default;
};

////////////////////////////////////////////////////////////////////////////////
// make_time_executor
PUSHMI_INLINE_VAR constexpr struct make_time_executor_fn {
  inline auto operator()() const {
    return time_executor<ignoreSF, systemNowF>{};
  }
  PUSHMI_TEMPLATE(class SF)
  (requires not TimeExecutor<SF>) //
      auto
      operator()(SF sf) const {
    return time_executor<SF, systemNowF>{std::move(sf)};
  }
  PUSHMI_TEMPLATE(class SF, class NF)
  (requires not TimeExecutor<SF>) //
      auto
      operator()(SF sf, NF nf) const {
    return time_executor<SF, NF>{std::move(sf), std::move(nf)};
  }
  PUSHMI_TEMPLATE(class Data)
  (requires True<>&& TimeExecutor<Data>) //
      auto
      operator()(Data d) const {
    return time_executor<Data, passDSF, passDNF>{std::move(d)};
  }
  PUSHMI_TEMPLATE(class Data, class DSF)
  (requires TimeExecutor<Data>) //
      auto
      operator()(Data d, DSF sf) const {
    return time_executor<Data, DSF, passDNF>{std::move(d), std::move(sf)};
  }
  PUSHMI_TEMPLATE(class Data, class DSF, class DNF)
  (requires TimeExecutor<Data>) //
      auto
      operator()(Data d, DSF sf, DNF nf) const {
    return time_executor<Data, DSF, DNF>{std::move(d), std::move(sf), std::move(nf)};
  }
} const make_time_executor{};

////////////////////////////////////////////////////////////////////////////////
// deduction guides
#if __cpp_deduction_guides >= 201703
time_executor()->time_executor<ignoreSF, systemNowF>;

PUSHMI_TEMPLATE(class SF)
(requires True<> PUSHMI_BROKEN_SUBSUMPTION(&&not TimeExecutor<SF>)) //
    time_executor(SF)
        ->time_executor<SF, systemNowF>;

PUSHMI_TEMPLATE(class SF, class NF)
(requires True<> PUSHMI_BROKEN_SUBSUMPTION(&&not TimeExecutor<SF>)) //
    time_executor(SF, NF)
        ->time_executor<SF, NF>;

PUSHMI_TEMPLATE(class Data)
(requires True<>&& TimeExecutor<Data>) //
    time_executor(Data)
        ->time_executor<Data, passDSF, passDNF>;

PUSHMI_TEMPLATE(class Data, class DSF)
(requires TimeExecutor<Data>) //
    time_executor(Data, DSF)
        ->time_executor<Data, DSF, passDNF>;

PUSHMI_TEMPLATE(class Data, class DSF, class DNF)
(requires TimeExecutor<Data>) //
    time_executor(Data, DSF, DNF)
        ->time_executor<Data, DSF, DNF>;
#endif

template <>
struct construct_deduced<time_executor> : make_time_executor_fn {};


namespace detail {
template <class T>
using not_any_time_executor_ref_t = not_is_t<T, any_time_executor_ref>;
} // namespace detail

template <class E, class TP, class... VN>
struct any_time_executor_ref {
 private:
  using This = any_time_executor_ref;
  void* pobj_;
  struct vtable {
    TP (*now_)(void*);
    any_single_sender<E, any_time_executor_ref<E, TP>> (*schedule_)(void*, VN...);
    any_single_sender<E, any_time_executor_ref<E, TP>> (
        *schedule_tp_)(void*, TP, VN...);
  } const* vptr_;
  template <class T>
  using wrapped_t = detail::not_any_time_executor_ref_t<T>;

 public:
  using properties = property_set<is_time<>>;

  any_time_executor_ref() = delete;
  any_time_executor_ref(const any_time_executor_ref&) = default;

  PUSHMI_TEMPLATE(class Wrapped)
  (requires TimeExecutor<wrapped_t<Wrapped>>)
      any_time_executor_ref(Wrapped& w) {
    struct s {
      static TP now(void* pobj) {
        return ::pushmi::now(*static_cast<Wrapped*>(pobj));
      }
      static any_single_sender<E, any_time_executor_ref<E, TP>> schedule(
          void* pobj, VN... vn) {
        return any_single_sender<E, any_time_executor_ref<E, TP>>{::pushmi::schedule(
            *static_cast<Wrapped*>(pobj),
            ::pushmi::now(*static_cast<Wrapped*>(pobj)), vn...)};
      }
      static any_single_sender<E, any_time_executor_ref<E, TP>> schedule_tp(
          void* pobj,
          TP tp,
          VN... vn) {
        return any_single_sender<E, any_time_executor_ref<E, TP>>{::pushmi::schedule(*static_cast<Wrapped*>(pobj), tp, vn...)};
      }
    };
    static const vtable vtbl{s::now, s::schedule, s::schedule_tp};
    pobj_ = std::addressof(w);
    vptr_ = &vtbl;
  }
  PUSHMI_TEMPLATE(class Wrapped)
  (requires TimeExecutor<wrapped_t<Wrapped>> && std::is_rvalue_reference<Wrapped>::value)
      any_time_executor_ref(Wrapped&& w) {
    struct s {
      static TP now(void* pobj) {
        return ::pushmi::now(*static_cast<Wrapped*>(pobj));
      }
      static any_single_sender<E, any_time_executor_ref<E, TP>> schedule(
          void* pobj, VN... vn) {
        return any_single_sender<E, any_time_executor_ref<E, TP>>{::pushmi::schedule(
            std::move(*static_cast<Wrapped*>(pobj)),
            ::pushmi::now(*static_cast<Wrapped*>(pobj)), vn...)};
      }
      static any_single_sender<E, any_time_executor_ref<E, TP>> schedule_tp(
          void* pobj,
          TP tp,
          VN... vn) {
        return any_single_sender<E, any_time_executor_ref<E, TP>>{::pushmi::schedule(
          std::move(*static_cast<Wrapped*>(pobj)), tp, vn...)};
      }
    };
    static const vtable vtbl{s::now, s::schedule, s::schedule_tp};
    pobj_ = std::addressof(w);
    vptr_ = &vtbl;
  }
  TP top() {
    return vptr_->now_(pobj_);
  }
  PUSHMI_TEMPLATE(class... AN)
  (requires sizeof...(VN) == sizeof...(AN)) //
  any_single_sender<E, any_time_executor_ref<E, TP>> schedule(AN&&... an) {
    // moved this check out of the requires due to a
    // mismached pack size between VN and AN on gcc.
    static_assert(And<Constructible<VN, AN>...>, "arguments must be convertible");
    return vptr_->schedule_(pobj_, VN{(AN&&)an}...);
  }
  PUSHMI_TEMPLATE(class... AN)
  (requires sizeof...(VN) == sizeof...(AN)) //
  any_single_sender<E, any_time_executor_ref<E, TP>> schedule(TP tp, AN&&... an) {
    // moved this check out of the requires due to a
    // mismached pack size between VN and AN on gcc.
    static_assert(And<Constructible<VN, AN>...>, "arguments must be convertible");
    return vptr_->schedule_tp_(pobj_, tp, VN{(AN&&)an}...);
  }
};

////////////////////////////////////////////////////////////////////////////////
// make_any_time_executor_ref
template <
    class E = std::exception_ptr,
    class TP = std::chrono::system_clock::time_point>
auto make_any_time_executor_ref() {
  return any_time_executor_ref<E, TP>{};
}

PUSHMI_TEMPLATE(class E = std::exception_ptr, class Wrapped)
(requires TimeExecutor<detail::not_any_time_executor_ref_t<Wrapped>>) //
    auto make_any_time_executor_ref(Wrapped& w) {
  return any_time_executor_ref<E, time_point_t<Wrapped>>{w};
}

////////////////////////////////////////////////////////////////////////////////
// deduction guides
#if __cpp_deduction_guides >= 201703
any_time_executor_ref()
    ->any_time_executor_ref<
        std::exception_ptr,
        std::chrono::system_clock::time_point>;

PUSHMI_TEMPLATE(class Wrapped)
(requires TimeExecutor<detail::not_any_time_executor_ref_t<Wrapped>>) //
    any_time_executor_ref(Wrapped&)
        ->any_time_executor_ref<std::exception_ptr, time_point_t<Wrapped>>;
#endif

} // namespace pushmi
