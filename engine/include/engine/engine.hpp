#pragma once
// 引擎：依赖反转的组合点。核心只认识 contracts.hpp 里的抽象，
// 具体的日志/投影/发布/定时器/时钟由 src/main.cpp 注入。
//
// 线性化模型（docs/01 §4）：
//   handle_*  = 预检（只读，冲突快速失败，省一次日志写入）+ append
//   apply     = 唯一线性化点（由 IEventLog 回调驱动，重放共用同一路径）
//   仲裁永远以 apply 为准——预检通过后 apply 时仍可能被并发抢先，拒绝也是事件。
#include "engine/booking.hpp"
#include "engine/contracts.hpp"
#include "engine/seat_board.hpp"

#include <unordered_map>
#include <vector>

namespace seatmesh {

struct EngineConfig {
  int n_seats = 200;
  // 营业日开始时刻（UNIX ms，即 day_off=0 的 06:00）。
  // <=0 时由时钟推断：UTC 当日 00:00 + 06:00（演示环境够用，正式版配置注入）。
  int64_t day0_start_ms = 0;
};

class Engine {
 public:
  Engine(EngineConfig cfg, std::shared_ptr<IClock> clock,
         std::unique_ptr<IEventLog> log,
         std::unique_ptr<IProjectionSink> projection,
         std::unique_ptr<IEventPublisher> publisher,
         std::unique_ptr<ITimerWheel> timers);

  // 启动：重放日志重建全部状态（幂等；必须在处理请求前调用一次）
  uint64_t recover();

  // 入口（预检 + append）。应答由 apply 同步填写（单线程事件循环）。
  Outcome handle_reserve(const Event& req);
  Outcome handle_cancel(const Event& req);
  Outcome handle_checkin(const Event& req);
  Outcome handle_checkout(const Event& req);

  // 周期驱动：到期定时器 → 产生 EXPIRE 事件走日志仲裁
  void tick();

  // 入站分发（main 循环调用）
  Outcome dispatch(const IncomingRequest& req);

  // 无锁读（单写者架构下读线程可直接调；v2 分片时改为每分片读）
  uint64_t bits(int seat, int day_off) const { return board_.bits(seat, day_off); }

  // ---- 快照（供 FileEventLog/DbEventLog 的 write_snapshot 使用） ----
  std::string serialize_state() const;
  void restore_state(const std::string& blob);

  IEventLog& log() { return *log_; }

 private:
  // 唯一线性化点：改状态机 → 写投影 → 发事件流（恢复期间不发布）
  void apply(const Event& e, uint64_t index);

  // 预检失败时的快速拒绝同样落日志（拒绝也是事件：可审计、可重放）
  Outcome append_and_apply(Event e);

  // 状态重建期由 restore_state 调用：为 HELD 单重挂签到超时定时器
  void arm_timers_for(const Booking& b);
  int64_t start_ms_of(int day_off, int start_min) const {
    return day0_start_ms_ +
           static_cast<int64_t>(day_off) * 86400000LL +
           static_cast<int64_t>(start_min) * 60000LL;
  }
  // 入馆资格截止（对齐馆方规则）：当日 08:00 前的申请（仅 day_off==0）
  // 保留至当日 08:30；其余 = max(申请时刻, 时段开始) + 30min。
  int64_t entry_deadline_for(int day_off, int64_t start_ms, int64_t now_ms) const;
  Booking* find_booking(const std::string& id);

  EngineConfig cfg_;
  std::shared_ptr<IClock> clock_;
  std::unique_ptr<IEventLog> log_;
  std::unique_ptr<IProjectionSink> projection_;
  std::unique_ptr<IEventPublisher> publisher_;
  std::unique_ptr<ITimerWheel> timers_;

  SeatBoard board_;
  int64_t day0_start_ms_ = 0;
  uint64_t seq_ = 0;              // 事件 seq，apply 分配，单调递增
  bool recovering_ = false;       // 重放期间不对外发布

  std::unordered_map<std::string, Booking> bookings_;       // id → 单
  std::unordered_map<std::string, std::string> idempotent_; // request_id → booking_id（LRU 化是 M2 的 P2 项）
  std::unordered_map<uint64_t, std::string> timer_tag_to_booking_;

  Outcome last_outcome_;         // apply 同步填写的应答（单线程事件循环语义）
};

}  // namespace seatmesh
