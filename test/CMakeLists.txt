
if(NOT ${CMAKE_VERSION} VERSION_LESS "3.10.0" AND NOT PUSHMI_ONE_TEST_BINARY)

add_executable(FlowTest FlowTest.cpp)
target_link_libraries(FlowTest pushmi gtest_main gmock_main Threads::Threads)
add_test(NAME FlowTest COMMAND FlowTest)

add_executable(FlowManyTest FlowManyTest.cpp)
target_link_libraries(FlowManyTest pushmi gtest_main gmock_main Threads::Threads)
add_test(NAME FlowManyTest COMMAND FlowManyTest)

add_executable(CompileTest CompileTest.cpp)
target_link_libraries(CompileTest pushmi gtest_main gmock_main Threads::Threads)
add_test(NAME CompileTest COMMAND CompileTest)

add_executable(NewThreadTest NewThreadTest.cpp)
target_link_libraries(NewThreadTest pushmi gtest_main gmock_main Threads::Threads)
add_test(NAME NewThreadTest COMMAND NewThreadTest)

add_executable(TrampolineTest TrampolineTest.cpp)
target_link_libraries(TrampolineTest pushmi gtest_main gmock_main Threads::Threads)
add_test(NAME TrampolineTest COMMAND TrampolineTest)

add_executable(PushmiTest PushmiTest.cpp)
target_link_libraries(PushmiTest pushmi gtest_main gmock_main Threads::Threads)
add_test(NAME PushmiTest COMMAND PushmiTest)

else()

add_executable(PushmiTest PushmiTest.cpp
  CompileTest.cpp
  TrampolineTest.cpp
  NewThreadTest.cpp
  FlowTest.cpp
  FlowManyTest.cpp
  )

target_link_libraries(PushmiTest pushmi gtest_main gmock_main Threads::Threads)
add_test(NAME PushmiTest COMMAND PushmiTest)

endif()
