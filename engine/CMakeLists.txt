cmake_minimum_required(VERSION 3.16)
project(seatmesh_engine CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(SQLite3 REQUIRED)

# 引擎核心：contracts + 状态机 + 三套存储实现（依赖反转的装配由 main.cpp 完成）
add_library(engine_core STATIC
  src/engine.cpp
  src/storage/file_event_log.cpp
  src/storage/db_event_log.cpp
  src/storage/projections.cpp)
target_include_directories(engine_core PUBLIC include src)
target_link_libraries(engine_core PUBLIC SQLite::SQLite3)
target_compile_options(engine_core PRIVATE -Wall -Wextra)

add_executable(engine_d src/main.cpp)
target_link_libraries(engine_d PRIVATE engine_core)

# 契约一致性测试：三实现行为必须一致（阶段一 AC5 的载体之一）
add_executable(test_log_contract tests/test_log_contract.cpp)
target_link_libraries(test_log_contract PRIVATE engine_core)

enable_testing()
add_test(NAME log_contract COMMAND test_log_contract)
