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

#include <chrono>
#include <type_traits>
#include <string>

using namespace std::literals;

#include <pushmi/flow_single_sender.h>
#include <pushmi/o/empty.h>
#include <pushmi/o/extension_operators.h>
#include <pushmi/o/just.h>
#include <pushmi/o/on.h>
#include <pushmi/o/submit.h>
#include <pushmi/o/tap.h>
#include <pushmi/o/transform.h>
#include <pushmi/o/via.h>

#include <pushmi/inline.h>
#include <pushmi/trampoline.h>

using namespace pushmi::aliases;

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace testing;

struct countdownsingle {
  explicit countdownsingle(int& c) : counter(&c) {}

  int* counter;

  template <class ExecutorRef>
  void operator()(ExecutorRef exec) {
    if (--*counter > 0) {
      exec | op::schedule() | op::submit(*this);
    }
  }
};

using TR = decltype(mi::trampoline());

class TrampolineExecutor : public Test {
 protected:
  TR tr_{mi::trampoline()};
};

TEST_F(TrampolineExecutor, TransformAndSubmit) {
  auto signals = 0;
  tr_ | op::schedule() | op::transform([](auto) { return 42; }) |
      op::submit(
          [&](auto) { signals += 100; },
          [&](auto) noexcept { signals += 1000; },
          [&]() { signals += 10; });

  EXPECT_THAT(signals, Eq(110))
      << "expected that the value and done signals are each recorded once";
}

TEST_F(TrampolineExecutor, BlockingGet) {
  auto v = tr_ | op::schedule() | op::transform([](auto) { return 42; }) | op::get<int>;

  EXPECT_THAT(v, Eq(42)) << "expected that the result would be different";
}

TEST_F(TrampolineExecutor, VirtualDerecursion) {
  int counter = 100'000;
  std::function<void(::pushmi::any_executor_ref<> exec)> recurse;
  recurse = [&](::pushmi::any_executor_ref<> tr) {
    if (--counter <= 0)
      return;
    tr | op::schedule() | op::submit(recurse);
  };
  tr_ | op::schedule() | op::submit([&](auto exec) { recurse(exec); });

  EXPECT_THAT(counter, Eq(0))
      << "expected that all nested submissions complete";
}

TEST_F(TrampolineExecutor, StaticDerecursion) {
  int counter = 100'000;
  countdownsingle single{counter};
  tr_ | op::schedule() | op::submit(single);

  EXPECT_THAT(counter, Eq(0))
      << "expected that all nested submissions complete";
}

TEST_F(TrampolineExecutor, UsedWithOn) {
  std::vector<std::string> values;
  auto sender = ::pushmi::make_single_sender([](auto out) {
    ::pushmi::set_value(out, 2.0);
    ::pushmi::set_done(out);
    // ignored
    ::pushmi::set_value(out, 1);
    ::pushmi::set_value(out, std::numeric_limits<int8_t>::min());
    ::pushmi::set_value(out, std::numeric_limits<int8_t>::max());
  });
  auto inlineon = sender | op::on([&]() { return mi::inline_executor(); });
  inlineon | op::submit(v::on_value([&](auto v) {
    values.push_back(std::to_string(v));
  }));

  EXPECT_THAT(values, ElementsAre(std::to_string(2.0)))
      << "expected that only the first item was pushed";
}

TEST_F(TrampolineExecutor, UsedWithVia) {
  std::vector<std::string> values;
  auto sender = ::pushmi::make_single_sender([](auto out) {
    ::pushmi::set_value(out, 2.0);
    ::pushmi::set_done(out);
    // ignored
    ::pushmi::set_value(out, 1);
    ::pushmi::set_value(out, std::numeric_limits<int8_t>::min());
    ::pushmi::set_value(out, std::numeric_limits<int8_t>::max());
  });
  auto inlinevia = sender | op::via([&]() { return mi::inline_executor(); });
  inlinevia | op::submit(v::on_value([&](auto v) {
    values.push_back(std::to_string(v));
  }));

  EXPECT_THAT(values, ElementsAre(std::to_string(2.0)))
      << "expected that only the first item was pushed";
}
