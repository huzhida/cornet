cmake_minimum_required(VERSION 3.10)

set(CORNET cornet)
project(${CORNET})

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-O2 -Ofast -ffast-math -funroll-loops -march=native)
else()
    add_compile_options(-fsanitize=address -g)
    add_link_options(-fsanitize=address)
endif()

if(NOT UNIX)
    message(FATAL_ERROR "only support to be build on Unix system")
endif()

find_package(spdlog CONFIG REQUIRED)
find_package(tomlplusplus CONFIG REQUIRED)
find_package(asio CONFIG REQUIRED)
find_package(concurrentqueue CONFIG REQUIRED)
find_package(libuv CONFIG REQUIRED)

file(GLOB_RECURSE CORNET_SRC CONFIGURE_DEPENDS src/*.cc src/*.hpp)

add_executable(${CORNET} main.cc ${CORNET_SRC})
target_link_libraries(${CORNET} uring spdlog::spdlog tomlplusplus::tomlplusplus)
target_include_directories(${CORNET} PUBLIC include)

add_executable(bench bench/benchmark.cc ${CORNET_SRC})
target_link_libraries(bench PRIVATE uring spdlog::spdlog tomlplusplus::tomlplusplus asio::asio concurrentqueue::concurrentqueue $<IF:$<TARGET_EXISTS:libuv::uv_a>,libuv::uv_a,libuv::uv>)
target_include_directories(bench PUBLIC include bench)

file(GLOB_RECURSE CORNET_TEST_SRC CONFIGURE_DEPENDS tests/*.cc)
enable_testing()
find_package(GTest CONFIG REQUIRED)
include(GoogleTest)
add_executable(unit_test ${CORNET_TEST_SRC} ${CORNET_SRC})
target_include_directories(unit_test PUBLIC include)
target_link_libraries(unit_test PRIVATE uring spdlog::spdlog tomlplusplus::tomlplusplus asio::asio concurrentqueue::concurrentqueue
        GTest::gtest GTest::gtest_main GTest::gmock GTest::gmock_main)
