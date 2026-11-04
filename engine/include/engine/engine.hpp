#pragma once
// 引擎：依赖反转的组合点。核心只认识 contracts.hpp 里的抽象，
// 具体的日志/投影/发布/定时器/时钟由 src/main.cpp 注入。
//
// 线性化模型（docs/01 §4）：
//   handle_*  = 预检（只读，冲突快速失败，省一次日志写入）+ append
//   apply     = 唯一线性化点（由 IEventLog 回调驱动，重放共用同一路径）
//   仲裁永远以 apply 为准——预检通过后 apply 时仍可能被并发抢先，拒绝也是事件。
//
// epoch 2.2（T1 包授权）新增：
//   - 候补授权：队列 = 引擎内存态（键 (day_off, seat)，FIFO，不落日志——
//     重启清空为已知限制）；释放路径优先转授权，队空才真正释放位图。
//   - BREAKAWAY 中途离开：IN_LIBRARY ↔ BREAKAWAY，保留期挂时间轮。
//   - 读己之写：Outcome.applied_seq；内存有界：终态且过日的单移出内存。
//   - SHED 准入：in-flight 上限（EngineConfig::max_inflight，默认 1024）。
#include "engine/booking.hpp"
#include "engine/contracts.hpp"
#include "engine/seat_board.hpp"

#include <cstdint>
#include <deque>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace seatmesh {

struct EngineConfig {
  int n_seats = 200;
  // 营业日开始时刻（UNIX ms，即 day_off=0 的 06:00）。
  // <=0 时由时钟推断：UTC 当日 00:00 + 06:00（演示环境够用，正式版配置注入）。
  int64_t day0_start_ms = 0;
  // ---- epoch 2.2 增量（T1 包授权）----
  int      region_id = kDefaultRegionId;  // 多区部署区标识（--region=N）
  int      max_inflight = kMaxInflight;   // SHED 准入上限（dispatch 入口判定）
  uint32_t idempotent_cap = kIdempotentCap;  // 幂等表 LRU 上限
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
  Outcome handle_reserve(const Event& req, bool enqueue_if_conflict = false);
  Outcome handle_cancel(const Event& req);
  Outcome handle_checkin(const Event& req);   // 含 BREAKAWAY 期内再入馆（RETURN）
  Outcome handle_breakaway(const Event& req); // 中途离开（leave=true）
  Outcome handle_checkout(const Event& req);
  Outcome handle_claim(const Event& req);     // 候补授权认领（kind=claim_ticket）

  // 周期驱动：到期定时器 → 产生事件走日志仲裁
  void tick();

  // 入站分发（main 循环调用）
  Outcome dispatch(const IncomingRequest& req);

  // 无锁读（单写者架构下读线程可直接调；v2 分片时改为每分片读）
  uint64_t bits(int seat, int day_off) const { return board_.bits(seat, day_off); }

  // ---- 快照（供 FileEventLog/DbEventLog 的 write_snapshot 使用） ----
  std::string serialize_state() const;
  void restore_state(const std::string& blob);

  IEventLog& log() { return *log_; }

  // ---- 诊断/审计接口（审计器与 fuzz 共用；不参与仲裁） ----
  size_t bookings_in_memory() const { return bookings_.size(); }
  size_t waitq_size() const;                    // 候补队列总长（跨全部 key）
  const char* state_of(const std::string& booking_id) const;  // 不存在 → nullptr
  int64_t in_flight() const { return in_flight_; }
  // I1（docs/01 §8）：任意 (seat, day) 置位片对应至多一个有效 booking。
  // 违例时返回 false 并在 *why 写入首个违例坐标。
  bool check_invariant_i1(std::string* why = nullptr) const;

 private:
  // 唯一线性化点：改状态机 → 写投影 → 发事件流（恢复期间不发布）
  void apply(const Event& e, uint64_t index);

  // 预检失败时的快速拒绝同样落日志（拒绝也是事件：可审计、可重放）
  Outcome append_and_apply(Event e);

  // 释放路径：候补队列队首匹配则转授权（位图经"释放+授予"原子换手），
  // 队空才真正释放。返回位图是否发生变更。
  bool release_path(int seat, int day_off, uint64_t freeable);
  // [now, 当日 22:00) 已过去的片数掩码（前 consumed 片）
  uint64_t consumed_mask_at(int64_t now_ms, int day_off) const;
  // 内存有界：终态且过日的 booking 移出内存（快照/日志仍保留事实）
  void sweep_bound_memory();
  int  current_day_off() const;

  // 幂等表 LRU（request_id → booking_id；容量 EngineConfig::idempotent_cap）
  void idem_put(const std::string& rid, const std::string& bid);
  const std::string* idem_get(const std::string& rid);  // 命中即触碰
  void idem_clear();

  // 状态重建期由 restore_state 调用：为各在途状态重挂定时器
  void arm_timers_for(Booking& b);
  int64_t start_ms_of(int day_off, int start_min) const {
    return day0_start_ms_ +
           static_cast<int64_t>(day_off) * 86400000LL +
           static_cast<int64_t>(start_min) * 60000LL;
  }
  int64_t end_ms_of(const Booking& b) const {
    return b.start_ms + static_cast<int64_t>(b.end_min - b.start_min) * 60000LL;
  }
  // 中途离开保留期：11:00–13:00 / 17:00–19:00 离开 60min，其余 30min
  int64_t breakaway_keep_ms(int64_t now_ms, int day_off) const;
  // 入馆资格截止（对齐馆方规则 + 兜底夹取）：
  //   base = 时段开始 + 30min；
  //   当日 08:00 前申请且时段开始 ≤ 08:30 → 至少保留至当日 08:30；
  //   最终 min(base, 时段结束)——截止永不越过时段本身（防"必然爽约"死锁）。
  int64_t entry_deadline_for(int day_off, int start_min, int end_min,
                             int64_t start_ms, int64_t now_ms) const;
  // 单用户活跃预约唯一（区域内硬约束；跨区由 Saga+对账兜底）
  bool   has_active_booking(int64_t user_id) const;
  void   cancel_timer(uint64_t& timer_id);
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
  int64_t in_flight_ = 0;         // 准入计数（dispatch RAII 增减）

  std::unordered_map<std::string, Booking> bookings_;       // id → 单

  // 幂等表 LRU：list 头 = 最旧；index 提供O(1)定位
  using IdemList = std::list<std::pair<std::string, std::string>>;
  IdemList idem_lru_;
  std::unordered_map<std::string, IdemList::iterator> idem_index_;

  // 候补队列（内存态，键 (day_off, seat)，FIFO；重启清空为已知限制）
  struct QueueEntry {
    std::string ticket;
    uint64_t seq = 0;   // 票据序号（"T<seq>"，也用于授权单确定性 ID "G<seq>"）
    int64_t user_id = 0;
    int seat = -1, day_off = -1, start_min = -1, end_min = -1;
    uint64_t mask = 0;
  };
  std::map<std::pair<int, int>, std::deque<QueueEntry>> waitq_;
  uint64_t queue_seq_ = 0;   // 票据序号（"T<seq>"），内存态
  std::unordered_map<std::string, std::string> ticket_to_booking_;
  std::vector<Booking> pending_grant_offers_;  // apply 期间暂存的派生授权（apply 尾统一发布）

  std::unordered_map<uint64_t, std::string> timer_tag_to_booking_;

  Outcome last_outcome_;         // apply 同步填写的应答（单线程事件循环语义）
};

}  // namespace seatmesh
