cmake_minimum_required(VERSION 3.10)

set(CORNET cornet)
project(${CORNET})

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-O2 -Ofast -ffast-math -funroll-loops -march=native)
endif()

if(NOT UNIX)
    message(FATAL_ERROR "only support to be build on Unix system")
endif()

find_package(spdlog CONFIG REQUIRED)
find_package(tomlplusplus CONFIG REQUIRED)
find_package(asio CONFIG REQUIRED)

file(GLOB_RECURSE CORNET_SRC CONFIGURE_DEPENDS src/*.cc src/*.hpp)

#add_library(${CORNET} SHARED ${CORNET_SRC})
#target_link_libraries(${CORNET} uring)

add_executable(${CORNET} main.cc ${CORNET_SRC})
target_link_libraries(${CORNET} uring spdlog::spdlog tomlplusplus::tomlplusplus)
target_include_directories(${CORNET} PUBLIC include)


add_executable(bench test.cc ${CORNET_SRC})

target_link_libraries(bench PRIVATE uring spdlog::spdlog tomlplusplus::tomlplusplus asio::asio)
target_include_directories(bench PUBLIC include)