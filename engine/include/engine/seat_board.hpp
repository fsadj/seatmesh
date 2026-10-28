#pragma once
// 座位网格：一个座位一天 = 一个 uint64，冲突检测与抢占在同一原子字上完成。
// 语义见 docs/01 §1；本类是纯数据面，不含任何 IO。
#include "engine/contracts.hpp"

#include <atomic>
#include <optional>
#include <vector>

namespace seatmesh {

// [start_min, end_min) → 位掩码。start_min 为距 06:00 的分钟（与 proto 一致），
// 要求 15 的倍数、落在营业时段内、≤16 片。
inline std::optional<uint64_t> mask_for(int start_min, int end_min) {
  const int total = kSlotsPerDay * kSlotMinutes;  // 960 = 16h
  if (start_min < 0 || end_min > total || start_min >= end_min)
    return std::nullopt;
  if (start_min % kSlotMinutes != 0) return std::nullopt;
  if ((end_min - start_min) % kSlotMinutes != 0) return std::nullopt;
  const int s = start_min / kSlotMinutes;
  const int n = (end_min - start_min) / kSlotMinutes;
  if (n > kMaxSlotsPerReq) return std::nullopt;
  const uint64_t low = (n >= 64) ? ~0ULL : ((1ULL << n) - 1);
  return low << s;
}

class SeatBoard {
 public:
  SeatBoard(int n_seats, int rolling_days)
      : n_seats_(n_seats), rolling_days_(rolling_days),
        grid_(static_cast<size_t>(n_seats) * rolling_days) {}

  int n_seats() const { return n_seats_; }
  int rolling_days() const { return rolling_days_; }

  // day_off → 网格下标。注意：date 标签防 ABA 属 M1 的 P2 加固项（docs/01）。
  int day_index(int day_off) const {
    int d = day_off % rolling_days_;
    if (d < 0) d += rolling_days_;
    return d;
  }

  // 抢占：成功返回 true（CAS 即线性化点）；失败时 conflict=重叠已占片。
  bool reserve(int seat, int day_off, uint64_t mask, uint64_t& conflict) {
    auto& cell = grid_[grid_index(seat, day_off)].bits;
    uint64_t cur = cell.load(std::memory_order_relaxed);
    while (!(cur & mask)) {
      if (cell.compare_exchange_weak(cur, cur | mask,
              std::memory_order_acq_rel, std::memory_order_relaxed))
        return true;
    }
    conflict = cur & mask;
    return false;
  }

  void release(int seat, int day_off, uint64_t mask) {
    grid_[grid_index(seat, day_off)].bits.fetch_and(~mask,
        std::memory_order_acq_rel);
  }

  bool has_conflict(int seat, int day_off, uint64_t mask) const {
    return (bits(seat, day_off) & mask) != 0;
  }

  uint64_t bits(int seat, int day_off) const {
    return grid_[grid_index(seat, day_off)].bits.load(std::memory_order_acquire);
  }

  // 快照/恢复用：按网格顺序导出/覆盖（引擎单写者线程内调用）
  void snapshot_raw(std::vector<uint64_t>& out) const {
    out.clear();
    for (const auto& c : grid_) out.push_back(c.bits.load(std::memory_order_relaxed));
  }
  void restore_raw(const std::vector<uint64_t>& in) {
    for (size_t i = 0; i < grid_.size() && i < in.size(); ++i)
      grid_[i].bits.store(in[i], std::memory_order_relaxed);
  }

 private:
  size_t grid_index(int seat, int day_off) const {
    return static_cast<size_t>(day_index(day_off)) * n_seats_ + seat;
  }
  struct SeatDay { std::atomic<uint64_t> bits{0}; };
  int n_seats_;
  int rolling_days_;
  std::vector<SeatDay> grid_;
};

}  // namespace seatmesh
