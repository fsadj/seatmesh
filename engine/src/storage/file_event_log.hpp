#pragma once
// 文件日志实现（课程交付物：C++ 文件操作）。
// 记录格式（docs/01 §5）：
//   | magic 'S''M' (2B) | type (1B) | ver (1B) | len (4B) |
//   | crc32 (4B, 覆盖 payload) | payload (len B, event_codec 编码) |
// 快照：snapshot.bin（引擎状态）+ snapshot.meta（文本，首行 = 快照时的 log index）。
// WAL 轮转（T1 阶段一）：write_snapshot() 成功后当前 WAL 原子 rename 为 wal.1
//（保留一代），新开 wal.log；replay() 按 wal.1 → wal.log 顺序读，
// 快照 index 前的记录跳过；撕裂尾/CRC 错在损坏处停止（后续段不再可信）。
#include "engine/contracts.hpp"

#include <cstdio>
#include <string>

namespace seatmesh {

class FileEventLog final : public IEventLog {
 public:
  FileEventLog(std::string wal_path, std::string snapshot_path,
               bool auto_barrier = true);
  ~FileEventLog() override;

  uint64_t append(const Event& e) override;
  void barrier() override;
  void set_apply(ApplyFn fn) override { apply_ = std::move(fn); }
  void set_snapshot(SnapshotOut out, SnapshotIn in) override;
  bool write_snapshot() override;
  uint64_t replay() override;
  uint64_t applied_index() const override { return idx_; }

 private:
  void open_wal();
  void durable();
  void rotate_wal();

  std::string wal_path_;
  std::string snap_path_;   // 基名：snap_path_ + ".bin" / ".meta"
  bool auto_barrier_;
  std::FILE* wal_ = nullptr;
  uint64_t idx_ = 0;
  ApplyFn apply_;
  SnapshotOut snap_out_;
  SnapshotIn snap_in_;
};

}  // namespace seatmesh
