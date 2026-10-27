#pragma once
// 预约单状态与信用规则（语义见 docs/01 §2 生命周期图）
#include "engine/contracts.hpp"

#include <string>

namespace seatmesh {

enum class BookingState : uint8_t {
  HELD = 1,      // 已预约，等待签到窗口
  GRANTED = 2,   // 候补授权期（位图保持置位，60s 内认领）
  ACTIVE = 3,    // 已签到在场
  EXPIRED = 4,   // 超时未签到（信用 -10）
  CANCELLED = 5, // 开始前取消（信用 0）
  DONE = 6       // 完成 / 提前签退（信用 +1 / 0）
};

inline const char* state_name(BookingState s) {
  switch (s) {
    case BookingState::HELD: return "HELD";
    case BookingState::GRANTED: return "GRANTED";
    case BookingState::ACTIVE: return "ACTIVE";
    case BookingState::EXPIRED: return "EXPIRED";
    case BookingState::CANCELLED: return "CANCELLED";
    case BookingState::DONE: return "DONE";
  }
  return "?";
}

// 信用规则（docs/00 常量区）：按时完成 +1 / 提前签退 0 / 开始前取消 0 / 爽约 -10
inline int credit_delta_for(BookingState from, BookingState to) {
  if (from == BookingState::HELD && to == BookingState::EXPIRED) return -10;
  if (from == BookingState::ACTIVE && to == BookingState::DONE) return +1;
  return 0;
}

struct Booking {
  std::string  id;
  int64_t      user_id = 0;
  int32_t      seat_id = -1;
  int32_t      day_off = -1;
  uint64_t     mask = 0;
  BookingState state = BookingState::HELD;
  int64_t      start_ms = 0;        // 时段开始（UNIX ms）
  int64_t      deadline_ms = 0;     // HELD=签到截止 / GRANTED=授权截止
  uint64_t     seq = 0;             // 创建事件 seq（确定性 ID 的来源）
};

}  // namespace seatmesh
