cmake_minimum_required(VERSION 3.0.0)

project(ioucat)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS on)

include(cmake/wall.cmake)

if (IOUCAT_ENABLE_ASAN)
  include(cmake/asan.cmake)
endif()

set(SRC
  fd.cpp
  main.cpp
)
list(TRANSFORM SRC PREPEND src/)

add_executable(ioucat ${SRC})
target_include_directories(ioucat PUBLIC include)
set_wall(ioucat)
