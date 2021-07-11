cmake_minimum_required(VERSION 3.0.0)

project(liburingpp)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS on)

include(cmake/wall.cmake)

if (LIBURINGPP_ENABLE_ASAN)
  include(cmake/asan.cmake)
endif()

add_library(liburingpp src/iouring.cpp)
set_wall(liburingpp)

add_executable(ucat src/cat.cpp)
target_link_libraries(ucat liburingpp)
set_wall(ucat)
