// 契约一致性测试：File / SQLite / InMemory 三实现必须行为完全一致。
// 这是"依赖反转"的价值证明——上层一次编写，底层随意替换。
#include "engine/contracts.hpp"
#include "engine/event_codec.hpp"
#include "engine/seat_board.hpp"
#include "storage/db_event_log.hpp"
#include "storage/file_event_log.hpp"
#include "storage/in_memory_log.hpp"

#include <cstdio>
#include <iostream>
#include <vector>

using namespace seatmesh;

namespace {

std::vector<Event> sample_events() {
  std::vector<Event> es;
  auto base = [] {
    Event e;
    e.user_id = 1001;
    e.seat_id = 3;
    e.day_off = 0;
    e.request_id = "r";
    e.trace_id = "tr0123456789abcd";
    return e;
  };
  Event e1 = base();
  e1.type = EventType::RESERVE;
  e1.start_min = 120;
  e1.end_min = 240;
  auto m = mask_for(120, 240);  // 相对 07:00：120 = 09:00
  if (!m) {
    std::cerr << "FATAL: mask_for invalid\n";
    std::exit(2);
  }
  e1.mask = *m;
  Event e2 = base();
  e2.type = EventType::CHECKIN;
  e2.booking_id = "B2";
  Event e3 = base();
  e3.type = EventType::CHECKOUT;
  e3.booking_id = "B2";
  e3.released_mask = e1.mask >> 4;
  Event e4 = base();
  e4.type = EventType::EXPIRE;
  e4.booking_id = "B9";
  Event e5 = base();
  e5.type = EventType::CANCEL;
  e5.booking_id = "B5";
  return {e1, e2, e3, e4, e5};
}

using LogPtr = std::unique_ptr<IEventLog>;
using Factory = std::function<LogPtr()>;

// 追加全部事件并收集 apply 序列；新建实例重放，返回两端是否逐字节一致
bool roundtrip(const Factory& make, std::vector<std::string>& applied_out) {
  const auto es = sample_events();
  std::vector<std::string> live;
  {
    LogPtr log = make();
    log->set_apply([&live](const Event& e, uint64_t) {
      live.push_back(encode_event(e));
    });
    for (const Event& e : es) (void)log->append(e);
    log->barrier();
  }
  {
    LogPtr log = make();
    log->set_apply([&applied_out](const Event& e, uint64_t) {
      applied_out.push_back(encode_event(e));
    });
    log->replay();
  }
  return live == applied_out;
}

// 快照路径：前 4 条入快照，第 5 条只存在于日志尾部
bool snapshot_path(const Factory& make) {
  const auto es = sample_events();
  constexpr char kBlob[] = "STATE_BLOB_V1";
  std::string restored;
  {
    LogPtr log = make();
    log->set_apply([](const Event&, uint64_t) {});
    log->set_snapshot([] { return std::string("STATE_BLOB_V1"); },
                      [&restored](const std::string& s) { restored = s; });
    for (int i = 0; i < 4; ++i) (void)log->append(es[i]);
    if (!log->write_snapshot()) return false;
    (void)log->append(es[4]);
  }
  std::vector<std::string> replayed;
  LogPtr log = make();
  log->set_apply([&replayed](const Event& e, uint64_t) {
    replayed.push_back(encode_event(e));
  });
  log->set_snapshot([] { return std::string(); },
                    [&restored](const std::string& s) { restored = s; });
  const uint64_t idx = log->replay();
  return idx == 5 && restored == kBlob && replayed.size() == 1 &&
         replayed[0] == encode_event(es[4]);
}

void remove_file(const char* path) { std::remove(path); }

int failures = 0;
void expect(bool ok, const char* what) {
  std::cout << (ok ? "PASS " : "FAIL ") << what << "\n";
  if (!ok) ++failures;
}

}  // namespace

int main() {
  std::vector<std::string> reference;
  const auto es = sample_events();

  // 内存实现没有跨实例持久化：同一实例上 append 与 replay 的结果必须一致
  {
    auto mem = std::make_unique<InMemoryEventLog>();
    std::vector<std::string> live, replayed;
    mem->set_apply([&live](const Event& e, uint64_t) {
      live.push_back(encode_event(e));
    });
    for (const Event& e : es) (void)mem->append(e);
    mem->set_apply([&replayed](const Event& e, uint64_t) {
      replayed.push_back(encode_event(e));
    });
    mem->replay();
    reference = replayed;
    expect(live == replayed, "inmemory roundtrip");
  }

  std::vector<std::string> out_file;
  remove_file("/tmp/seatmesh_test/wal.log");
  expect(roundtrip(
             [] {
               return std::make_unique<FileEventLog>(
                   "/tmp/seatmesh_test/wal.log", "/tmp/seatmesh_test/snap");
             },
             out_file),
         "file roundtrip");
  expect(out_file == reference, "file == inmemory (字节级一致)");
  if (out_file != reference) {
    std::cout << "  [diag] sizes ref=" << reference.size()
              << " file=" << out_file.size() << "\n";
    for (size_t i = 0; i < reference.size() && i < out_file.size(); ++i)
      if (reference[i] != out_file[i]) {
        std::cout << "  [diag] first diff at index " << i << "\n";
        break;
      }
  }

  std::vector<std::string> out_db;
  remove_file("/tmp/seatmesh_test/engine.db");
  expect(roundtrip(
             [] {
               return std::make_unique<DbEventLog>("/tmp/seatmesh_test/engine.db");
             },
             out_db),
         "sqlite roundtrip");
  expect(out_db == reference, "sqlite == inmemory (字节级一致)");

  remove_file("/tmp/seatmesh_test/wal2.log");
  remove_file("/tmp/seatmesh_test/snap2.bin");
  remove_file("/tmp/seatmesh_test/snap2.meta");
  expect(snapshot_path([] {
           return std::make_unique<FileEventLog>("/tmp/seatmesh_test/wal2.log",
                                                 "/tmp/seatmesh_test/snap2");
         }),
         "file snapshot + tail replay");
  remove_file("/tmp/seatmesh_test/engine2.db");
  expect(snapshot_path([] {
           return std::make_unique<DbEventLog>("/tmp/seatmesh_test/engine2.db");
         }),
         "sqlite snapshot + tail replay");

  // 损坏检测：翻转 WAL 中部一个字节，重放必须在损坏处截断
  remove_file("/tmp/seatmesh_test/wal3.log");
  {
    LogPtr log = std::make_unique<FileEventLog>("/tmp/seatmesh_test/wal3.log",
                                                "/tmp/seatmesh_test/snap3");
    log->set_apply([](const Event&, uint64_t) {});
    for (int i = 0; i < 3; ++i) (void)log->append(sample_events()[0]);
  }
  if (std::FILE* f = std::fopen("/tmp/seatmesh_test/wal3.log", "r+b")) {
    std::fseek(f, 20, SEEK_SET);
    int c = std::fgetc(f);
    std::fseek(f, 20, SEEK_SET);
    std::fputc(c ^ 0xFF, f);
    std::fclose(f);
  }
  {
    size_t applied = 0;
    LogPtr log = std::make_unique<FileEventLog>("/tmp/seatmesh_test/wal3.log",
                                                "/tmp/seatmesh_test/snap3");
    log->set_apply([&applied](const Event&, uint64_t) { ++applied; });
    log->replay();
    expect(applied < 3, "file 损坏尾部检测（crc 截断）");
  }

  std::cout << (failures ? "RESULT: FAIL\n" : "RESULT: ALL PASS\n");
  return failures ? 1 : 0;
}
