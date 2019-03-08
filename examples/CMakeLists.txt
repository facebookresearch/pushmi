# Copyright (c) 2018-present, Facebook, Inc.
#
# This source code is licensed under the Apache License found in the
# LICENSE file in the root directory of this source tree.

add_library(examples INTERFACE)

target_include_directories(examples INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)

add_subdirectory(twoway_execute)
add_subdirectory(then_execute)
add_subdirectory(composition)
add_subdirectory(for_each)
add_subdirectory(reduce)
add_subdirectory(set_done)
add_subdirectory(set_error)
