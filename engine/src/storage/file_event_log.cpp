#include "storage/file_event_log.hpp"

#include "engine/event_codec.hpp"

#include <cstring>
#include <stdexcept>

#if defined(__unix__) || defined(__APPLE__)
#include <unistd.h>
#endif

namespace seatmesh {

FileEventLog::FileEventLog(std::string wal_path, std::string snapshot_path,
                           bool auto_barrier)
    : wal_path_(std::move(wal_path)),
      snap_path_(std::move(snapshot_path)),
      auto_barrier_(auto_barrier) {
  open_wal();
}

FileEventLog::~FileEventLog() {
  if (wal_) std::fclose(wal_);
}

void FileEventLog::open_wal() {
  wal_ = std::fopen(wal_path_.c_str(), "ab");
  if (!wal_) throw std::runtime_error("cannot open WAL: " + wal_path_);
}

void FileEventLog::durable() {
  if (!wal_) return;
  std::fflush(wal_);
#if defined(__unix__) || defined(__APPLE__)
  ::fsync(::fileno(wal_));
#endif
}

uint64_t FileEventLog::append(const Event& e) {
  const std::string payload = encode_event(e);
  const uint32_t len = static_cast<uint32_t>(payload.size());
  const uint32_t crc = crc32(payload.data(), payload.size());
  const uint8_t header[12] = {
      'S', 'M', static_cast<uint8_t>(e.type), 1,
      static_cast<uint8_t>(len), static_cast<uint8_t>(len >> 8),
      static_cast<uint8_t>(len >> 16), static_cast<uint8_t>(len >> 24),
      static_cast<uint8_t>(crc), static_cast<uint8_t>(crc >> 8),
      static_cast<uint8_t>(crc >> 16), static_cast<uint8_t>(crc >> 24)};
  if (std::fwrite(header, 1, sizeof(header), wal_) != sizeof(header) ||
      std::fwrite(payload.data(), 1, len, wal_) != len)
    throw std::runtime_error("WAL write failed");
  if (auto_barrier_) durable();
  const uint64_t idx = ++idx_;
  if (apply_) apply_(e, idx);  // 契约：持久后 apply
  return idx;
}

void FileEventLog::barrier() { durable(); }

void FileEventLog::set_snapshot(SnapshotOut out, SnapshotIn in) {
  snap_out_ = std::move(out);
  snap_in_ = std::move(in);
}

bool FileEventLog::write_snapshot() {
  if (!snap_out_) return false;
  const std::string blob = snap_out_();
  const std::string bin = snap_path_ + ".bin.tmp";
  std::FILE* f = std::fopen(bin.c_str(), "wb");
  if (!f) return false;
  const bool ok =
      std::fwrite(blob.data(), 1, blob.size(), f) == blob.size();
  std::fclose(f);
  if (!ok) return false;
  const std::string meta = snap_path_ + ".meta.tmp";
  f = std::fopen(meta.c_str(), "w");
  if (!f) return false;
  std::fprintf(f, "%llu\n", static_cast<unsigned long long>(idx_));
  std::fclose(f);
  std::rename(bin.c_str(), (snap_path_ + ".bin").c_str());   // 原子替换
  std::rename(meta.c_str(), (snap_path_ + ".meta").c_str());
  return true;
}

uint64_t FileEventLog::replay() {
  uint64_t snap_idx = 0;
  if (std::FILE* f = std::fopen((snap_path_ + ".meta").c_str(), "r")) {
    unsigned long long v = 0;
    if (std::fscanf(f, "%llu", &v) == 1) snap_idx = v;
    std::fclose(f);
  }
  if (snap_idx > 0 && snap_in_) {
    std::FILE* f = std::fopen((snap_path_ + ".bin").c_str(), "rb");
    if (!f) throw std::runtime_error("snapshot meta exists but bin missing");
    std::string blob;
    char buf[65536];
    size_t n;
    while ((n = std::fread(buf, 1, sizeof(buf), f)) > 0) blob.append(buf, n);
    std::fclose(f);
    snap_in_(blob);
    idx_ = snap_idx;
  }

  // 回放 WAL：校验 magic/crc，遇到损坏尾部即停（半条记录 = 崩溃时未完成写入）
  std::FILE* f = std::fopen(wal_path_.c_str(), "rb");
  if (!f) return idx_;
  uint64_t cur = 0;
  Event e;
  for (;;) {
    uint8_t header[12];
    if (std::fread(header, 1, sizeof(header), f) != sizeof(header)) break;
    if (header[0] != 'S' || header[1] != 'M') break;  // 损坏，截断
    const uint32_t len = header[4] | (header[5] << 8) | (header[6] << 16) |
                         (header[7] << 24);
    const uint32_t crc = header[8] | (header[9] << 8) | (header[10] << 16) |
                         (header[11] << 24);
    std::string payload(len, '\0');
    if (len && std::fread(payload.data(), 1, len, f) != len) break;  // 撕裂尾
    if (crc32(payload.data(), payload.size()) != crc) break;         // 翻转字节
    ++cur;
    if (cur <= snap_idx) continue;  // 快照已含
    if (decode_event(payload.data(), payload.size(), e) && apply_) apply_(e, cur);
    idx_ = cur;
  }
  std::fclose(f);
  return idx_;
}

}  // namespace seatmesh
