cmake_minimum_required(VERSION 3.0.2)

PROJECT(MyDB)

set(CMAKE_CXX_FLAGS "--std=c++11 -O3 -Wall -Wextra ${CMAKE_CXX_FLAGS}")

include_directories(${CMAKE_SOURCE_DIR}/src)
include_directories(${CMAKE_SOURCE_DIR}/include)

add_subdirectory(src)
add_subdirectory(test)