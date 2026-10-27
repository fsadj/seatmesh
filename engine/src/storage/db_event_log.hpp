#pragma once
// 数据库日志实现（SQLite，嵌入式，同一 IEventLog 契约）。
// 语义与 FileEventLog 完全一致：events 表是事实，payload 字节级同构
// （event_codec 编码）。快照存 meta 表。MySQL 版只需替换本文件的连接层。
#include "engine/contracts.hpp"

#include <memory>
#include <string>

struct sqlite3;
struct sqlite3_stmt;

namespace seatmesh {

class DbEventLog final : public IEventLog {
 public:
  explicit DbEventLog(std::string db_path);
  ~DbEventLog() override;

  uint64_t append(const Event& e) override;
  void barrier() override {}  // autocommit + synchronous=FULL，逐条持久
  void set_apply(ApplyFn fn) override { apply_ = std::move(fn); }
  void set_snapshot(SnapshotOut out, SnapshotIn in) override;
  bool write_snapshot() override;
  uint64_t replay() override;
  uint64_t applied_index() const override { return idx_; }

 private:
  std::string db_path_;
  sqlite3* db_ = nullptr;
  sqlite3_stmt* insert_ = nullptr;
  uint64_t idx_ = 0;
  ApplyFn apply_;
  SnapshotOut snap_out_;
  SnapshotIn snap_in_;
};

}  // namespace seatmesh
