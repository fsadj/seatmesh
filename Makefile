# SeatMesh 根构建（macOS/Linux，clang++/g++ 均可）
CXX ?= clang++
CXXFLAGS ?= -std=c++17 -Wall -Wextra
ENGINE_SRC = src/engine.cpp \
             src/storage/file_event_log.cpp \
             src/storage/db_event_log.cpp \
             src/storage/projections.cpp

.PHONY: build-engine test-engine demo-engine demo-python all

build-engine:
	cd engine && $(CXX) $(CXXFLAGS) -Iinclude -Isrc src/main.cpp $(ENGINE_SRC) -lsqlite3 -o /tmp/engine_d

test-engine:
	cd engine && $(CXX) $(CXXFLAGS) -Iinclude -Isrc tests/test_log_contract.cpp $(ENGINE_SRC) -lsqlite3 -o /tmp/seatmesh_test_bin && /tmp/seatmesh_test_bin

demo-engine: build-engine
	rm -rf /tmp/seatmesh && mkdir -p /tmp/seatmesh
	printf 'reserve user=1001 seat=3 day=1 start=120 end=240 rid=r1 trace=t001\nreserve user=1002 seat=3 day=1 start=180 end=300 rid=r2 trace=t002\nreserve user=1002 seat=5 day=1 start=120 end=180 rid=r3 trace=t003\ncancel booking=B1\nquit\n' > /tmp/seatmesh_demo.txt
	/tmp/engine_d --storage=file --source=replay --replay=/tmp/seatmesh_demo.txt --projection=csv
	cat /tmp/seatmesh/events.csv

demo-python:
	python3 gateway/demo_contract.py

all: build-engine demo-python
