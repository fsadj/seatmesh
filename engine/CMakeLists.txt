cmake_minimum_required(VERSION 3.16)
project(seatmesh_engine CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(SQLite3 REQUIRED)

# ZMQ 请求源/发布器（T1 阶段一）：cppzmq 未必安装，默认 OFF，默认构建不依赖。
# 打开方式：cmake -DSEATMESH_HAS_ZMQ=ON ..（需要 cppzmq + libzmq）
option(SEATMESH_HAS_ZMQ "Enable ZmqRouterSource/ZmqPublisher (requires cppzmq)" OFF)
if(SEATMESH_HAS_ZMQ)
  find_package(cppzmq REQUIRED)
endif()

# 引擎核心：contracts + 状态机 + 三套存储实现（依赖反转的装配由 main.cpp 完成）
add_library(engine_core STATIC
  src/engine.cpp
  src/storage/file_event_log.cpp
  src/storage/db_event_log.cpp
  src/storage/projections.cpp)
target_include_directories(engine_core PUBLIC include src)
target_link_libraries(engine_core PUBLIC SQLite::SQLite3)
target_compile_options(engine_core PRIVATE -Wall -Wextra)
if(SEATMESH_HAS_ZMQ)
  target_compile_definitions(engine_core PUBLIC SEATMESH_HAS_ZMQ)
  target_link_libraries(engine_core PUBLIC cppzmq)
endif()

add_executable(engine_d src/main.cpp)
target_link_libraries(engine_d PRIVATE engine_core)

# 契约一致性测试：三实现行为必须一致（阶段一 AC5 的载体之一）
add_executable(test_log_contract tests/test_log_contract.cpp)
target_link_libraries(test_log_contract PRIVATE engine_core)

# 引擎单测（FakeClock 确定性驱动，T1 阶段一/二）与 fuzz（阶段三，种子参数化）
add_executable(test_engine tests/test_engine.cpp)
target_link_libraries(test_engine PRIVATE engine_core)

add_executable(test_fuzz tests/test_fuzz.cpp)
target_link_libraries(test_fuzz PRIVATE engine_core)

enable_testing()
add_test(NAME log_contract COMMAND test_log_contract)
add_test(NAME engine COMMAND test_engine)
add_test(NAME fuzz COMMAND test_fuzz)
add_test(NAME fuzz_seeded COMMAND test_fuzz 42 300)
