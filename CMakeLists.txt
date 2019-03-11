# Copyright (c) 2018-present, Facebook, Inc.
#
# This source code is licensed under the Apache License found in the
# LICENSE file in the root directory of this source tree.

cmake_minimum_required(VERSION 3.7)

project(pushmi-project CXX)

option(PUSHMI_USE_CONCEPTS_EMULATION "Use C++14 Concepts Emulation" ON)
option(PUSHMI_USE_CPP_2A "Use C++2a with concepts emulation" OFF)
option(PUSHMI_USE_CPP_17 "Use C++17 with concepts emulation" OFF)
option(PUSHMI_ONE_TEST_BINARY "Compile all the tests into one binary" OFF)

FIND_PACKAGE (Threads REQUIRED)

add_library(pushmi INTERFACE)

target_include_directories(pushmi INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/external/executors-impl/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/external/futures-impl/future-executor-interaction/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/external/networking-ts-impl/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/external/nonius/include>
    $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include>
    $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include/executors>
    $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include/futures>
    $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include/net>
    $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include/nonius>
)

if (PUSHMI_USE_CONCEPTS_EMULATION)

message("Using concepts emulation!")

if(${CMAKE_VERSION} VERSION_LESS "3.8.0" OR PUSHMI_USE_CPP_17 OR PUSHMI_USE_CPP_2A)

if (PUSHMI_USE_CPP_17)
message("Using c++17!")
target_compile_options(pushmi INTERFACE
    $<$<CXX_COMPILER_ID:GNU>:-std=c++17>
    $<$<CXX_COMPILER_ID:AppleClang>:-std=c++17>
    $<$<CXX_COMPILER_ID:Clang>:-std=c++17>
    )
elseif (PUSHMI_USE_CPP_2A)
message("Using c++2a!")
target_compile_options(pushmi INTERFACE
    $<$<CXX_COMPILER_ID:GNU>:-std=c++2a>
    $<$<CXX_COMPILER_ID:AppleClang>:-std=c++2a>
    $<$<CXX_COMPILER_ID:Clang>:-std=c++2a>
    )
else()
message("Using c++14!")
target_compile_options(pushmi INTERFACE
    $<$<CXX_COMPILER_ID:GNU>:-std=c++14>
    $<$<CXX_COMPILER_ID:AppleClang>:-std=c++14>
    $<$<CXX_COMPILER_ID:Clang>:-std=c++14>
    )
endif()

else()

target_compile_features(pushmi INTERFACE cxx_std_14)

endif()

else(PUSHMI_USE_CONCEPTS_EMULATION)

message("Using real concepts!")
message("Using c++2a!")

target_compile_options(pushmi INTERFACE
    $<$<CXX_COMPILER_ID:GNU>:-std=c++2a>
    $<$<CXX_COMPILER_ID:GNU>:-fconcepts>)

endif(PUSHMI_USE_CONCEPTS_EMULATION)

target_compile_options(pushmi INTERFACE
    $<$<CXX_COMPILER_ID:GNU>:-ftemplate-backtrace-limit=0>
    $<$<CXX_COMPILER_ID:Clang>:-ftemplate-backtrace-limit=0>
    $<$<CXX_COMPILER_ID:AppleClang>:-ftemplate-backtrace-limit=0>)

target_compile_options(pushmi INTERFACE
    $<$<CXX_COMPILER_ID:Clang>:-stdlib=libc++>
    $<$<CXX_COMPILER_ID:AppleClang>:-stdlib=libc++>)

# target_compile_options(pushmi INTERFACE
#     $<$<CXX_COMPILER_ID:Clang>:-fsanitize=thread -fno-omit-frame-pointer>)
# target_link_options(pushmi INTERFACE
#     $<$<CXX_COMPILER_ID:Clang>:-fsanitize=thread>)

if (PUSHMI_CONCEPTS)
    target_compile_options(pushmi INTERFACE $<$<CXX_COMPILER_ID:GNU>:-fconcepts>)
endif ()

add_custom_target(buildSingleHeader
    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_SOURCE_DIR}/buildSingleHeader.cmake
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
add_dependencies(pushmi buildSingleHeader)

install(
    DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/
    DESTINATION include)
install(
    DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/external/executors-impl/include/
    DESTINATION include/executors)
install(
    DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/external/futures-impl/future-executor-interaction/include/
    DESTINATION include/futures)
install(
    DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/external/networking-ts-impl/include/
    DESTINATION include/net)
install(
    DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/external/Catch2/
    DESTINATION include/Catch2)
install(
    DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/external/nonius/include/
    DESTINATION include/nonius)
install(TARGETS pushmi EXPORT pushmi-project)
install(EXPORT pushmi-project DESTINATION pushmi-project)

add_subdirectory(examples)
add_subdirectory(benchmarks)

enable_testing()

# Download and unpack googletest at configure time
configure_file(cmake/CMakeLists.txt.in googletest-download/CMakeLists.txt)
execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download )
if(result)
  message(FATAL_ERROR "CMake step for googletest failed: ${result}")
endif()
execute_process(COMMAND ${CMAKE_COMMAND} --build .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/googletest-download )
if(result)
  message(FATAL_ERROR "Build step for googletest failed: ${result}")
endif()

# Prevent overriding the parent project's compiler/linker
# settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add googletest directly to our build. This defines
# the gtest and gtest_main targets.
add_subdirectory(${CMAKE_CURRENT_BINARY_DIR}/googletest-src
                 ${CMAKE_CURRENT_BINARY_DIR}/googletest-build
                 EXCLUDE_FROM_ALL)

# The gtest/gtest_main targets carry header search path
# dependencies automatically when using CMake 2.8.11 or
# later. Otherwise we have to add them here ourselves.
if (CMAKE_VERSION VERSION_LESS 2.8.11)
  include_directories("${gtest_SOURCE_DIR}/include")
endif()

include(CTest)
add_subdirectory(test)
