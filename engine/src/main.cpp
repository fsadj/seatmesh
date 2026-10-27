// 组合根（依赖反转的装配点）：所有具体技术在这里选择并注入。
// 用法示例：
//   engine_d --storage=file  --source=replay --replay=scripts/demo_day1.txt
//   engine_d --storage=sqlite --source=stdin
//   engine_d --storage=memory --projection=csv --seats=10
#include "engine/engine.hpp"
#include "engine/timing_wheel.hpp"
#include "storage/db_event_log.hpp"
#include "storage/file_event_log.hpp"
#include "storage/in_memory_log.hpp"
#include "storage/projections.hpp"

#include "adapters/publisher.hpp"
#include "adapters/request_source.hpp"

#include <sys/stat.h>

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

#if defined(__unix__) || defined(__APPLE__)
#include <unistd.h>
#endif

using namespace seatmesh;

namespace {
std::string arg_of(int argc, char** argv, const std::string& key,
                   const std::string& def) {
  const std::string prefix = key + "=";
  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    if (a == key && i + 1 < argc) return argv[i + 1];      // --key value
    if (a.rfind(prefix, 0) == 0) return a.substr(prefix.size());  // --key=value
  }
  return def;
}
void ensure_dir(const std::string& path) { ::mkdir(path.c_str(), 0755); }
}  // namespace

int main(int argc, char** argv) {
  const std::string storage    = arg_of(argc, argv, "--storage", "file");
  const std::string wal        = arg_of(argc, argv, "--wal", "/tmp/seatmesh/wal.log");
  const std::string snap       = arg_of(argc, argv, "--snapshot", "/tmp/seatmesh/snap");
  const std::string db         = arg_of(argc, argv, "--db", "/tmp/seatmesh/engine.db");
  const std::string projection_kind = arg_of(argc, argv, "--projection", "none");
  const std::string csv        = arg_of(argc, argv, "--csv", "/tmp/seatmesh/events.csv");
  const std::string pdb        = arg_of(argc, argv, "--pdb", "/tmp/seatmesh/projection.db");
  const int seats              = std::stoi(arg_of(argc, argv, "--seats", "200"));
  const std::string source     = arg_of(argc, argv, "--source", "stdin");
  const std::string replay     = arg_of(argc, argv, "--replay", "");

  ensure_dir("/tmp/seatmesh");

  // ---- 依赖注入：核心只认契约，具体实现全部在这里选择 ----
  std::unique_ptr<IEventLog> log;
  if (storage == "file")
    log = std::make_unique<FileEventLog>(wal, snap);
  else if (storage == "sqlite")
    log = std::make_unique<DbEventLog>(db);
  else if (storage == "memory")
    log = std::make_unique<InMemoryEventLog>();
  else { std::cerr << "unknown --storage " << storage << "\n"; return 2; }

  std::unique_ptr<IProjectionSink> projection;
  if (projection_kind == "csv")
    projection = std::make_unique<CsvProjection>(csv);
  else if (projection_kind == "sqlite")
    projection.reset(make_sqlite_projection(pdb));
  else
    projection = std::make_unique<NullProjection>();

  std::unique_ptr<IEventPublisher> publisher;
  try {
    publisher = make_publisher("memory");  // 正式拓扑改 "tcp://*:5556"（需 cppzmq）
  } catch (const std::exception& e) {
    std::cerr << "publisher: " << e.what() << "\n";
    return 2;
  }

  EngineConfig cfg;
  cfg.n_seats = seats;
  Engine engine(cfg, std::make_shared<SystemClock>(), std::move(log),
                std::move(projection), std::move(publisher),
                std::make_unique<TimerHeap>());

  std::cout << "[engine] recover: replayed to index "
            << engine.recover() << "\n";

  std::unique_ptr<IRequestSource> src =
      (source == "replay" && !replay.empty())
          ? std::unique_ptr<IRequestSource>(
                std::make_unique<ReplaySource>(replay))
          : std::make_unique<StdinSource>();

  int idle_ms = 0;
  while (true) {
    if (auto req = src->poll()) {
      idle_ms = 0;
      if (req->kind == "quit") break;
      const Outcome out = engine.dispatch(*req);
      char conflict_hex[24] = "";
      if (out.conflict_mask)
        std::snprintf(conflict_hex, sizeof conflict_hex, " conflict=0x%llx",
                      static_cast<unsigned long long>(out.conflict_mask));
      std::cout << (out.ok ? "OK   " : "REJECT ")
                << (out.booking_id.empty() ? "-" : out.booking_id)
                << conflict_hex
                << (out.detail.empty() ? "" : " " + out.detail) << "\n";
    }
    engine.tick();
#if defined(__unix__) || defined(__APPLE__)
    ::usleep(20000);
#endif
    // 回放源耗尽后短暂排空定时器即退出；stdin 模式由 quit/Ctrl-C 结束
    if (src->exhausted() && ++idle_ms > 100) break;
  }

  std::cout << "[engine] snapshot written: "
            << (engine.log().write_snapshot() ? "yes" : "no") << "\n";
  return 0;
}
