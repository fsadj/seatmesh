#pragma once
// 预约单状态与信用规则。
// 状态机与真实校园图书馆选座系统六态对齐（docs/compat-notes.md）：
//   PENDING_ENTRY 待入馆 → IN_LIBRARY 已入馆 → LEFT 已离馆
//                        ↘ BREAKAWAY 中途离开(保留期内) → IN_LIBRARY / LEFT
//   PENDING_ENTRY →(超时) NO_SHOW 超时未入馆；PENDING_ENTRY →(取消) CANCELLED
//   GRANTED 候补授权为本系统扩展态（位图保持置位，60s 内认领）。
#include "engine/contracts.hpp"

#include <string>

namespace seatmesh {

enum class BookingState : uint8_t {
  PENDING_ENTRY = 1,  // 待入馆（对齐真实"待入馆"）
  GRANTED = 2,        // 候补授权期（扩展）
  IN_LIBRARY = 3,     // 已入馆（对齐真实"已入馆"）
  BREAKAWAY = 4,      // 中途离开，保留期内（对齐真实"中途离开"）
  LEFT = 5,           // 已离馆（对齐真实"已离馆"；含正式离馆与保留期超时）
  NO_SHOW = 6,        // 超时未入馆（对齐真实"超时未入馆"；当日禁约，信用 -10）
  CANCELLED = 7       // 已取消（对齐真实"已取消"；触发 30min 冷却）
};

inline const char* state_name(BookingState s) {
  switch (s) {
    case BookingState::PENDING_ENTRY: return "PENDING_ENTRY";
    case BookingState::GRANTED: return "GRANTED";
    case BookingState::IN_LIBRARY: return "IN_LIBRARY";
    case BookingState::BREAKAWAY: return "BREAKAWAY";
    case BookingState::LEFT: return "LEFT";
    case BookingState::NO_SHOW: return "NO_SHOW";
    case BookingState::CANCELLED: return "CANCELLED";
  }
  return "?";
}

// 信用规则：按时完成 +1 / 超时未入馆 -10 / 其余（取消、提前或保留期离馆）0。
// 另有网关硬规则：NO_SHOW 当日不可再线上申请；取消/爽约后 30min 冷却。
inline int credit_delta_for(BookingState from, BookingState to) {
  if (from == BookingState::PENDING_ENTRY && to == BookingState::NO_SHOW) return -10;
  if (from == BookingState::IN_LIBRARY && to == BookingState::LEFT) return +1;
  return 0;
}

struct Booking {
  std::string  id;
  int64_t      user_id = 0;
  int32_t      seat_id = -1;
  int32_t      day_off = -1;
  uint64_t     mask = 0;
  BookingState state = BookingState::PENDING_ENTRY;
  int64_t      start_ms = 0;        // 时段开始（UNIX ms）
  int64_t      deadline_ms = 0;     // PENDING_ENTRY=入馆截止 / GRANTED=授权截止
  uint64_t     seq = 0;             // 创建事件 seq（确定性 ID 的来源）
};

}  // namespace seatmesh
