#include "engine/engine.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <stdexcept>

namespace seatmesh {

int64_t SystemClock::now_ms() const {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

static int64_t infer_day0(int64_t now_ms) {
  // 演示环境：UTC 当日 00:00 + 06:00（营业开始）。正式版由配置注入（docs/09）。
  return (now_ms / 86400000LL) * 86400000LL + kDayStartMin * 60000LL;
}

// Outcome 字段较多（epoch 2.2 增量后 9 个），统一用工厂避免逐字段聚合初始化告警。
inline Outcome reject(const char* code, uint64_t conflict = 0,
                      const std::string& bid = "") {
  Outcome o;
  o.ok = false;
  o.booking_id = bid;
  o.conflict_mask = conflict;
  o.detail = code;
  return o;
}
inline Outcome accept(const std::string& bid, const std::string& detail = "") {
  Outcome o;
  o.ok = true;
  o.booking_id = bid;
  o.detail = detail;
  return o;
}

Engine::Engine(EngineConfig cfg, std::shared_ptr<IClock> clock,
               std::unique_ptr<IEventLog> log,
               std::unique_ptr<IProjectionSink> projection,
               std::unique_ptr<IEventPublisher> publisher,
               std::unique_ptr<ITimerWheel> timers)
    : cfg_(cfg),
      clock_(std::move(clock)),
      log_(std::move(log)),
      projection_(std::move(projection)),
      publisher_(std::move(publisher)),
      timers_(std::move(timers)),
      board_(cfg_.n_seats, kRollingDays) {
  day0_start_ms_ = cfg_.day0_start_ms > 0
                       ? cfg_.day0_start_ms
                       : infer_day0(clock_->now_ms());
  log_->set_apply(
      [this](const Event& e, uint64_t idx) { this->apply(e, idx); });
  SnapshotOut out = [this]() { return this->serialize_state(); };
  SnapshotIn in = [this](const std::string& s) { this->restore_state(s); };
  log_->set_snapshot(out, in);
}

uint64_t Engine::recover() {
  seq_ = 0;
  bookings_.clear();
  idem_clear();
  waitq_.clear();
  queue_seq_ = 0;
  ticket_to_booking_.clear();
  in_flight_ = 0;
  recovering_ = true;  // 重放期间只重建状态与投影，不对外发布
  const uint64_t idx = log_->replay();
  recovering_ = false;
  sweep_bound_memory();   // 恢复完成即做一次内存有界清扫
  return idx;
}

// ---------- 预检 + append（预检只读，仲裁在 apply） ----------

Outcome Engine::append_and_apply(Event e) {
  e.ts_ms = clock_->now_ms();
  e.region_id = cfg_.region_id;   // epoch 2.2：事件携带区标识
  last_outcome_ = Outcome{};
  (void)log_->append(e);  // 持久后同步回调 apply → last_outcome_
  log_->barrier();
  return last_outcome_;
}

Outcome Engine::handle_reserve(const Event& req, bool enqueue_if_conflict) {
  // 参数校验（非仲裁，不落日志）
  auto mask = mask_for(req.start_min, req.end_min);
  if (!mask || req.seat_id < 0 || req.seat_id >= board_.n_seats() ||
      req.day_off < 0 || req.day_off >= kRollingDays)
    return reject("E_PARAM");

  // 幂等重放
  if (!req.request_id.empty()) {
    if (const std::string* bid = idem_get(req.request_id)) {
      Outcome o = reject("E_IDEMPOTENT_REPLAY");
      o.ok = !bid->empty();
      o.booking_id = *bid;
      return o;
    }
  }

  // 时段已整体过去 → 拒绝（堵"预约过去时段刷信用"漏洞）
  if (start_ms_of(req.day_off, req.end_min) <= clock_->now_ms())
    return reject("E_PARAM");

  // 单用户活跃预约唯一（区域内硬约束；跨区由 Saga+对账兜底）
  if (has_active_booking(req.user_id))
    return reject("E_QUOTA");

  // 预检：冲突直接拒绝，省一次日志写入（最终仲裁仍在 apply）
  if (board_.has_conflict(req.seat_id, req.day_off, *mask)) {
    const uint64_t conflict = board_.bits(req.seat_id, req.day_off) & *mask;
    if (!enqueue_if_conflict)
      return reject("E_CONFLICT", conflict);
    // 候补入队（内存态，不落日志）：同用户同 key 已在队 → 返回原票据
    auto& q = waitq_[{req.day_off, req.seat_id}];
    for (size_t i = 0; i < q.size(); ++i)
      if (q[i].user_id == req.user_id) {
        Outcome o = reject("E_QUEUED", conflict);
        o.queue_ticket = q[i].ticket;
        o.queue_pos = static_cast<int32_t>(i + 1);
        return o;
      }
    QueueEntry e;
    e.seq = ++queue_seq_;
    e.ticket = "T" + std::to_string(e.seq);
    e.user_id = req.user_id;
    e.seat = req.seat_id;
    e.day_off = req.day_off;
    e.start_min = req.start_min;
    e.end_min = req.end_min;
    e.mask = *mask;
    q.push_back(e);
    Outcome o = reject("E_QUEUED", conflict);
    o.queue_ticket = e.ticket;
    o.queue_pos = static_cast<int32_t>(q.size());
    return o;
  }

  Event e = req;
  e.type = EventType::RESERVE;
  e.mask = *mask;
  return append_and_apply(e);
}

Outcome Engine::handle_cancel(const Event& req) {
  const Booking* b = find_booking(req.booking_id);
  if (!b) return reject("E_NOT_FOUND");
  if (b->state != BookingState::PENDING_ENTRY) return reject("E_WINDOW");
  if (clock_->now_ms() >= b->start_ms)
    return reject("E_WINDOW", 0, req.booking_id);  // 开始后只能签退
  Event e = req;
  e.type = EventType::CANCEL;
  e.user_id = b->user_id;
  return append_and_apply(e);
}

Outcome Engine::handle_checkin(const Event& req) {
  const Booking* b = find_booking(req.booking_id);
  if (!b) return reject("E_NOT_FOUND");
  // BREAKAWAY 期内再入馆 → RETURN（回到 IN_LIBRARY）
  if (b->state == BookingState::BREAKAWAY) {
    Event e = req;
    e.type = EventType::RETURN;
    e.user_id = b->user_id;
    return append_and_apply(e);
  }
  const int64_t now = clock_->now_ms();
  // 签到窗口 = [时段开始, 入馆资格截止]；提前/迟到都不落日志
  if (b->state != BookingState::PENDING_ENTRY || now < b->start_ms ||
      now > b->deadline_ms)
    return reject("E_WINDOW", 0, req.booking_id);
  Event e = req;
  e.type = EventType::CHECKIN;
  e.user_id = b->user_id;
  return append_and_apply(e);
}

Outcome Engine::handle_breakaway(const Event& req) {
  const Booking* b = find_booking(req.booking_id);
  if (!b) return reject("E_NOT_FOUND");
  if (b->state != BookingState::IN_LIBRARY)
    return reject("E_WINDOW", 0, req.booking_id);  // 仅已入馆可中途离开
  Event e = req;
  e.type = EventType::BREAKAWAY;
  e.user_id = b->user_id;
  return append_and_apply(e);
}

Outcome Engine::handle_claim(const Event& req) {
  if (req.queue_ticket.empty()) return reject("E_PARAM");
  auto it = ticket_to_booking_.find(req.queue_ticket);
  if (it == ticket_to_booking_.end())
    return reject("E_NOT_FOUND");
  const Booking* b = find_booking(it->second);
  if (!b || b->state != BookingState::GRANTED)
    return reject("E_WINDOW");
  if (end_ms_of(*b) <= clock_->now_ms())
    return reject("E_WINDOW", 0, b->id);  // 时段已整体过去
  Event e = req;
  e.type = EventType::GRANT_CLAIM;
  e.booking_id = b->id;
  e.user_id = b->user_id;
  return append_and_apply(e);
}

void Engine::tick() {
  sweep_bound_memory();
  const int64_t now = clock_->now_ms();
  for (const Timer& t : timers_->expire(now)) {
    auto it = timer_tag_to_booking_.find(t.id);
    if (it == timer_tag_to_booking_.end()) continue;
    const std::string booking_id = it->second;
    timer_tag_to_booking_.erase(it);
    Event e;
    e.booking_id = booking_id;
    if (t.tag.rfind("expire:", 0) == 0) {
      e.type = EventType::NO_SHOW;
      append_and_apply(e);  // 超时也走日志仲裁：apply 幂等复核
    } else if (t.tag.rfind("end:", 0) == 0) {
      e.type = EventType::DONE;
      append_and_apply(e);  // 自然离馆：LEFT(+1)
    } else if (t.tag.rfind("grant:", 0) == 0) {
      e.type = EventType::GRANT_EXPIRE;
      if (const Booking* b = find_booking(booking_id))
        e.queue_ticket = b->ticket;
      append_and_apply(e);  // 授权期超时：顺延下一位 / 队空真正释放
    } else if (t.tag.rfind("breakaway:", 0) == 0) {
      e.type = EventType::CHECKOUT;
      e.detail = "BREAKAWAY_EXPIRE";  // 保留期超时：按签退路径出账（LEFT, 信用 0）
      append_and_apply(e);
    }
  }
}

Outcome Engine::dispatch(const IncomingRequest& req) {
  // SHED 准入控制：in-flight 上限（默认 1024）。拒绝不落日志，
  // 客户端按 backoff_hint_ms 回退后重试。
  if (in_flight_ >= cfg_.max_inflight) {
    Outcome o{};
    o.detail = "E_SHED";
    o.shed = true;
    o.backoff_hint_ms = kShedBackoffHintMs;
    return o;
  }
  ++in_flight_;
  struct InflightGuard {
    Engine* e;
    ~InflightGuard() { --e->in_flight_; }
  } guard{this};

  sweep_bound_memory();   // 终态且过日的单移出内存（读路径仍有日志事实）
  if (req.kind == "reserve") return handle_reserve(req.ev, req.enqueue_if_conflict);
  if (req.kind == "cancel") return handle_cancel(req.ev);
  if (req.kind == "checkin")
    return req.leave ? handle_breakaway(req.ev) : handle_checkin(req.ev);
  if (req.kind == "checkout") return handle_checkout(req.ev);
  if (req.kind == "claim_ticket") return handle_claim(req.ev);
  return reject("E_PARAM");
}

Outcome Engine::handle_checkout(const Event& req) {
  const Booking* b = find_booking(req.booking_id);
  if (!b) return reject("E_NOT_FOUND");
  if (b->state != BookingState::IN_LIBRARY)
    return reject("E_WINDOW", 0, req.booking_id);
  Event e = req;
  e.type = EventType::CHECKOUT;
  e.user_id = b->user_id;
  return append_and_apply(e);
}

// ---------- 时间/期限计算 ----------

int Engine::current_day_off() const {
  const int64_t d = (clock_->now_ms() - day0_start_ms_) / 86400000LL;
  return d < 0 ? 0 : static_cast<int>(d);
}

uint64_t Engine::consumed_mask_at(int64_t now_ms, int day_off) const {
  const int64_t day_abs = day0_start_ms_ +
      static_cast<int64_t>(day_off) * 86400000LL;
  const int64_t m = (now_ms - day_abs) / 60000;   // 距当日 06:00 的分钟（勿二次减 kDayStartMin）
  const int consumed =
      (m <= 0) ? 0
               : (m >= kSlotsPerDay * kSlotMinutes
                      ? kSlotsPerDay
                      : static_cast<int>((m + kSlotMinutes - 1) / kSlotMinutes));
  return (consumed >= 64) ? ~0ULL : ((1ULL << consumed) - 1);
}

int64_t Engine::breakaway_keep_ms(int64_t now_ms, int day_off) const {
  const int64_t day_abs = day0_start_ms_ +
      static_cast<int64_t>(day_off) * 86400000LL;
  const int wall_min =
      kDayStartMin + static_cast<int>((now_ms - day_abs) / 60000);
  const bool peak =
      (wall_min >= kBreakawayPeakOpenMin && wall_min < kBreakawayPeakCloseMin) ||
      (wall_min >= 17 * 60 && wall_min < 19 * 60);   // 11:00–13:00 / 17:00–19:00
  return peak ? kBreakawayKeepPeakMs : kBreakawayKeepMs;
}

int64_t Engine::entry_deadline_for(int day_off, int start_min, int end_min,
                                   int64_t start_ms, int64_t now_ms) const {
  const int64_t day_anchor = day0_start_ms_ +
      static_cast<int64_t>(day_off) * 86400000LL;                  // 当日 06:00
  const int64_t end_ms = start_ms +
      static_cast<int64_t>(end_min - start_min) * 60000LL;
  int64_t base = start_ms + kEntryGraceMs;                         // 开始+30min
  // 早鸟保留：当日 08:00 前申请、且时段开始 ≤ 08:30 → 至少保留至 08:30
  const int64_t morning_before = kMorningApplyBeforeMs - kDayStartMin * 60000LL;
  const int64_t hold_until    = kMorningHoldUntilMs  - kDayStartMin * 60000LL;
  if (day_off == 0 && now_ms - day_anchor < morning_before &&
      start_ms <= day_anchor + hold_until)
    base = std::max(base, day_anchor + hold_until);
  return std::min(base, end_ms);   // 截止永不越过时段结束（防必然爽约死锁）
}

// ---------- 释放路径：候补授权优先，队空才真正释放 ----------

bool Engine::release_path(int seat, int day_off, uint64_t freeable) {
  if (!freeable) return false;
  auto it = waitq_.find({day_off, seat});
  if (it == waitq_.end() || it->second.empty()) {
    board_.release(seat, day_off, freeable);
    return true;
  }
  auto& q = it->second;
  const int64_t now = clock_->now_ms();
  while (!q.empty()) {
    const QueueEntry head = q.front();
    // 按真正空闲的片授予（队首期望窗口 ∩ 本次释放）：部分重合也授予剩余时段
    const uint64_t gm = head.mask & freeable;
    if (gm) {
      const int first_slot = __builtin_ctzll(gm);
      const int last_slot = 63 - __builtin_clzll(gm);
      const int64_t g_end =
          start_ms_of(day_off, (last_slot + 1) * kSlotMinutes);
      if (g_end > now) {
        q.pop_front();
        // 位图原子换手：先释放空闲片、立即授予队首——观察上座位从未公开空闲。
        board_.release(seat, day_off, freeable);
        uint64_t conflict = 0;
        if (!board_.reserve(seat, day_off, gm, conflict))
          continue;  // 理论不可达（gm ⊆ freeable，单线程 apply）
        Booking g;
        g.id = "G" + std::to_string(head.seq);
        g.user_id = head.user_id;
        g.seat_id = seat;
        g.day_off = day_off;
        g.mask = gm;
        g.state = BookingState::GRANTED;
        g.start_min = first_slot * kSlotMinutes;
        g.end_min = (last_slot + 1) * kSlotMinutes;
        g.start_ms = start_ms_of(day_off, g.start_min);
        g.deadline_ms = now + kGrantTtlMs;   // 授权期 60s
        g.ticket = head.ticket;
        bookings_[g.id] = g;
        ticket_to_booking_[g.ticket] = g.id;
        arm_timers_for(bookings_[g.id]);     // GRANTED → "grant:<ticket>" 定时器
        pending_grant_offers_.push_back(g);
        return true;
      }
    }
    // 队首无可授时段（窗口不重叠/已过去）→ 放弃该票据，顺延下一位
    q.pop_front();
  }
  board_.release(seat, day_off, freeable);   // 队空才真正释放
  return true;
}

// ---------- apply：唯一线性化点 ----------

void Engine::apply(const Event& raw, uint64_t index) {
  Event ev = raw;           // apply 需补全 seq / booking_id 后再投影与发布
  ev.seq = ++seq_;
  last_outcome_ = Outcome{};
  bool changed = false;       // 是否发生真实状态迁移（发布守卫）
  bool seat_mutated = false;  // 位图是否变更（决定是否追发 SeatChanged）
  pending_grant_offers_.clear();

  switch (ev.type) {
    case EventType::RESERVE: {
      uint64_t conflict = 0;
      if (board_.reserve(ev.seat_id, ev.day_off, ev.mask, conflict)) {
        Booking b;
        b.id = "B" + std::to_string(ev.seq);  // 确定性 ID：跨副本一致
        b.user_id = ev.user_id;
        b.seat_id = ev.seat_id;
        b.day_off = ev.day_off;
        b.mask = ev.mask;
        b.state = BookingState::PENDING_ENTRY;
        b.start_min = ev.start_min;
        b.end_min = ev.end_min;
        b.start_ms = start_ms_of(ev.day_off, ev.start_min);
        b.deadline_ms = entry_deadline_for(ev.day_off, ev.start_min,
                                           ev.end_min, b.start_ms, ev.ts_ms);
        b.seq = ev.seq;
        bookings_[b.id] = b;
        if (!ev.request_id.empty()) idem_put(ev.request_id, b.id);
        ev.booking_id = b.id;
        arm_timers_for(bookings_[b.id]);
        changed = true;
        seat_mutated = true;
        last_outcome_ = accept(b.id);
      } else {
        // 拒绝也是事件（BookingRejected 语义）：进入事件流供审计与对账
        changed = true;
        last_outcome_ = reject("E_CONFLICT", conflict);
        if (!ev.request_id.empty()) idem_put(ev.request_id, "");
      }
      break;
    }
    case EventType::CANCEL: {
      Booking* b = find_booking(ev.booking_id);
      if (b && b->state == BookingState::PENDING_ENTRY &&
          ev.ts_ms < b->start_ms) {
        cancel_timer(b->expire_timer);   // 撤销幽灵 NO_SHOW 定时器
        b->state = BookingState::CANCELLED;  // 信用 0
        changed = true;
        seat_mutated = release_path(b->seat_id, b->day_off, b->mask);
        last_outcome_ = accept(b->id);
      } else {
        last_outcome_ = reject("E_WINDOW", 0, ev.booking_id);
      }
      break;
    }
    case EventType::CHECKIN: {
      // 入馆刷卡：窗口 = [时段开始, 入馆资格截止]
      Booking* b = find_booking(ev.booking_id);
      if (b && b->state == BookingState::PENDING_ENTRY && ev.ts_ms >= b->start_ms &&
          ev.ts_ms <= b->deadline_ms) {
        cancel_timer(b->expire_timer);   // 已入馆，撤销 NO_SHOW 定时器（防幽灵扣分）
        b->state = BookingState::IN_LIBRARY;
        b->end_timer = timers_->schedule(end_ms_of(*b), "end:" + b->id);
        timer_tag_to_booking_[b->end_timer] = b->id;   // 自然离馆：LEFT(+1)
        changed = true;
        last_outcome_ = accept(b->id);
      } else {
        last_outcome_ = reject("E_WINDOW", 0, ev.booking_id);
      }
      break;
    }
    case EventType::CHECKOUT: {
      Booking* b = find_booking(ev.booking_id);
      // 提前/正式离馆：释放剩余时段（含当前片的剩余部分），交给候补授权流程。
      // BREAKAWAY 保留期超时（定时器产生）走同一出账路径：LEFT，信用 0。
      const bool expire_from_breakaway =
          ev.detail == "BREAKAWAY_EXPIRE";
      if (b && (b->state == BookingState::IN_LIBRARY ||
                (b->state == BookingState::BREAKAWAY && expire_from_breakaway))) {
        cancel_timer(b->end_timer);
        cancel_timer(b->breakaway_timer);
        const uint64_t consumed_mask = consumed_mask_at(ev.ts_ms, b->day_off);
        board_.release(b->seat_id, b->day_off, b->mask & consumed_mask);  // 已过片直接清
        const bool past_freed = (b->mask & consumed_mask) != 0;
        const uint64_t released = b->mask & ~consumed_mask;
        ev.released_mask = released;
        b->state = BookingState::LEFT;
        changed = true;
        int credit = 0;
        if (!expire_from_breakaway) {
          const int64_t dur_ms =
              static_cast<int64_t>(b->mask ? __builtin_popcountll(b->mask) : 0) *
              kSlotMinutes * 60000LL;
          credit = (ev.ts_ms >= b->start_ms + dur_ms) ? +1 : 0;
          ev.detail = "credit=" + std::to_string(credit);  // 信用进事件流
        }
        seat_mutated = release_path(b->seat_id, b->day_off, released) || past_freed;
        last_outcome_ = accept(b->id, "credit=" + std::to_string(credit));
      } else {
        last_outcome_ = reject("E_WINDOW", 0, ev.booking_id);
      }
      break;
    }
    case EventType::NO_SHOW: {
      Booking* b = find_booking(ev.booking_id);
      if (b && b->state == BookingState::PENDING_ENTRY && ev.ts_ms >= b->deadline_ms) {
        cancel_timer(b->expire_timer);
        const uint64_t consumed_mask = consumed_mask_at(ev.ts_ms, b->day_off);
        board_.release(b->seat_id, b->day_off, b->mask & consumed_mask);  // 已过片直接清
        const bool past_freed = (b->mask & consumed_mask) != 0;
        b->state = BookingState::NO_SHOW;  // 信用 -10（控制面结算）
        ev.detail = "credit=-10";          // 信用进事件流（T5 结算消费）
        changed = true;
        seat_mutated =
            release_path(b->seat_id, b->day_off, b->mask & ~consumed_mask) ||
            past_freed;
        last_outcome_ = accept(b->id, "credit=-10");
      }  // 否则幂等跳过：定时器竞态/重放下的空 NO_SHOW **不发布**（防幽灵扣分）
      break;
    }
    case EventType::DONE: {
      // 自然离馆：时段结束仍未刷卡离馆 → LEFT(+1)，无剩余片需释放
      Booking* b = find_booking(ev.booking_id);
      const int64_t end_ms = b ? end_ms_of(*b) : 0;
      if (b && b->state == BookingState::IN_LIBRARY && ev.ts_ms >= end_ms) {
        cancel_timer(b->end_timer);
        b->state = BookingState::LEFT;
        ev.detail = "credit=+1";           // 信用进事件流（T5 结算消费）
        changed = true;
        last_outcome_ = accept(b->id, "credit=+1");
      } else {
        changed = false;   // 空转不发布
      }
      break;
    }
    case EventType::BREAKAWAY: {
      // 中途离开：IN_LIBRARY → BREAKAWAY，保留期挂时间轮（座位仍保留）
      Booking* b = find_booking(ev.booking_id);
      if (b && b->state == BookingState::IN_LIBRARY) {
        cancel_timer(b->end_timer);          // 保留期内暂不确定时离馆
        b->state = BookingState::BREAKAWAY;
        b->deadline_ms = ev.ts_ms + breakaway_keep_ms(ev.ts_ms, b->day_off);
        b->breakaway_timer =
            timers_->schedule(b->deadline_ms, "breakaway:" + b->id);
        timer_tag_to_booking_[b->breakaway_timer] = b->id;
        changed = true;
        last_outcome_ = accept(b->id);
      } else {
        last_outcome_ = reject("E_WINDOW", 0, ev.booking_id);
      }
      break;
    }
    case EventType::RETURN: {
      // 保留期内再入馆：BREAKAWAY → IN_LIBRARY，重挂自然离馆定时器
      Booking* b = find_booking(ev.booking_id);
      if (b && b->state == BookingState::BREAKAWAY) {
        cancel_timer(b->breakaway_timer);
        b->state = BookingState::IN_LIBRARY;
        b->end_timer = timers_->schedule(end_ms_of(*b), "end:" + b->id);
        timer_tag_to_booking_[b->end_timer] = b->id;
        changed = true;
        last_outcome_ = accept(b->id);
      } else {
        last_outcome_ = reject("E_WINDOW", 0, ev.booking_id);
      }
      break;
    }
    case EventType::GRANT_CLAIM: {
      // 期内认领：GRANTED → PENDING_ENTRY（入馆资格重新起算，夹到时段结束）
      Booking* b = find_booking(ev.booking_id);
      if (b && b->state == BookingState::GRANTED && b->ticket == ev.queue_ticket &&
          end_ms_of(*b) > ev.ts_ms) {
        cancel_timer(b->expire_timer);   // 撤销授权期定时器
        b->state = BookingState::PENDING_ENTRY;
        const int64_t base = entry_deadline_for(b->day_off, b->start_min,
                                                b->end_min, b->start_ms, ev.ts_ms);
        b->deadline_ms = std::min(
            std::max(base, ev.ts_ms + kEntryGraceMs), end_ms_of(*b));
        arm_timers_for(*b);              // PENDING_ENTRY → "expire:<id>" 定时器
        changed = true;
        last_outcome_ = accept(b->id);
      } else {
        last_outcome_ = reject("E_WINDOW", 0, ev.booking_id);
      }
      break;
    }
    case EventType::GRANT_EXPIRE: {
      // 授权期超时顺延：放弃票据 → 剩余片继续走释放路径（下一位 / 队空真正释放）。
      // 重放安全：GRANTED 单缺失（队列内存态未重建）时幂等跳过。
      Booking* b = find_booking(ev.booking_id);
      if (b && b->state == BookingState::GRANTED && b->ticket == ev.queue_ticket) {
        cancel_timer(b->expire_timer);
        ticket_to_booking_.erase(b->ticket);
        b->state = BookingState::CANCELLED;   // 授权超时不扣分（信用 0）
        b->ticket.clear();
        const uint64_t consumed_mask = consumed_mask_at(ev.ts_ms, b->day_off);
        board_.release(b->seat_id, b->day_off, b->mask & consumed_mask);
        changed = true;
        seat_mutated =
            release_path(b->seat_id, b->day_off, b->mask & ~consumed_mask) ||
            (b->mask & consumed_mask) != 0;
        last_outcome_ = accept(b->id, "grant_expired");
      }
      break;
    }
    case EventType::GRANT_OFFER:
      // 派生事件：只发布不落日志（见 contracts.hpp）；防御性 no-op。
      break;
    case EventType::FREEZE:
    case EventType::SAGA:
      // TODO(T1/T4, P2)：封楼 / Saga 跨区配额确认
      last_outcome_ = reject("NOT_IMPLEMENTED");
      break;
    case EventType::SYSTEM:
    case EventType::SEAT_CHANGED:
      break;
  }

  last_outcome_.applied_seq = ev.seq;   // 读己之写：落日志事件的 apply seq

  if (projection_) projection_->on_apply(ev, index);
  // 发布守卫：只有真实发生状态迁移的事件才进入事件流——
  // 定时器竞态/重放产生的空 NO_SHOW/DONE 若被发布，信用消费者会误扣分。
  if (recovering_ || !changed) return;
  publisher_->publish(ev);
  if (seat_mutated) {   // 位图变更 → 追发 SeatChanged（同 seq，副本整字覆盖）
    Event sc = ev;
    sc.type = EventType::SEAT_CHANGED;
    sc.user_id = 0;
    sc.booking_id.clear();
    if (const Booking* b = find_booking(ev.booking_id)) {
      // CANCEL/NO_SHOW/CHECKOUT 等请求事件可不带 seat/day：以单据坐标为准
      // （否则副本会按 (-1,-1) 键整字覆盖，副本位图错位）
      sc.seat_id = b->seat_id;
      sc.day_off = b->day_off;
    }
    sc.mask = board_.bits(sc.seat_id, sc.day_off);
    publisher_->publish(sc);
  }
  for (const Booking& g : pending_grant_offers_) {   // 派生 GRANT_OFFER（同 seq 发布）
    Event go = ev;
    go.type = EventType::GRANT_OFFER;
    go.user_id = g.user_id;
    go.seat_id = g.seat_id;
    go.day_off = g.day_off;
    go.mask = g.mask;
    go.booking_id = g.id;
    go.queue_ticket = g.ticket;
    go.detail.clear();
    publisher_->publish(go);
  }
  pending_grant_offers_.clear();
}

// ---------- 辅助 ----------

Booking* Engine::find_booking(const std::string& id) {
  auto it = bookings_.find(id);
  return it == bookings_.end() ? nullptr : &it->second;
}

void Engine::arm_timers_for(Booking& b) {
  switch (b.state) {
    case BookingState::PENDING_ENTRY:
      b.expire_timer = timers_->schedule(b.deadline_ms, "expire:" + b.id);
      timer_tag_to_booking_[b.expire_timer] = b.id;
      break;
    case BookingState::GRANTED:   // 授权期定时器（超时 → GRANT_EXPIRE 顺延）
      b.expire_timer = timers_->schedule(b.deadline_ms, "grant:" + b.ticket);
      timer_tag_to_booking_[b.expire_timer] = b.id;
      break;
    case BookingState::BREAKAWAY: // 保留期定时器（超时 → 强制 LEFT）
      b.breakaway_timer = timers_->schedule(b.deadline_ms, "breakaway:" + b.id);
      timer_tag_to_booking_[b.breakaway_timer] = b.id;
      break;
    case BookingState::IN_LIBRARY: // 自然离馆定时器（时段结束 → LEFT+1）
      b.end_timer = timers_->schedule(end_ms_of(b), "end:" + b.id);
      timer_tag_to_booking_[b.end_timer] = b.id;
      break;
    default:
      break;
  }
}

bool Engine::has_active_booking(int64_t user_id) const {
  for (const auto& [id, b] : bookings_)
    if (b.user_id == user_id &&
        (b.state == BookingState::PENDING_ENTRY ||
         b.state == BookingState::GRANTED ||
         b.state == BookingState::IN_LIBRARY ||
         b.state == BookingState::BREAKAWAY))
      return true;
  return false;
}

void Engine::cancel_timer(uint64_t& timer_id) {
  if (!timer_id) return;
  timers_->cancel(timer_id);
  timer_tag_to_booking_.erase(timer_id);
  timer_id = 0;
}

void Engine::sweep_bound_memory() {
  const int cd = current_day_off();
  for (auto it = bookings_.begin(); it != bookings_.end();) {
    const Booking& b = it->second;
    const bool terminal = b.state == BookingState::LEFT ||
                          b.state == BookingState::NO_SHOW ||
                          b.state == BookingState::CANCELLED;
    if (terminal && b.day_off < cd) {   // 终态且过日 → 移出内存（日志/快照仍保留事实）
      if (!b.ticket.empty()) ticket_to_booking_.erase(b.ticket);
      it = bookings_.erase(it);
    } else {
      ++it;
    }
  }
}

size_t Engine::waitq_size() const {
  size_t n = 0;
  for (const auto& [key, q] : waitq_) n += q.size();
  return n;
}

const char* Engine::state_of(const std::string& booking_id) const {
  auto it = bookings_.find(booking_id);
  return it == bookings_.end() ? nullptr : state_name(it->second.state);
}

bool Engine::check_invariant_i1(std::string* why) const {
  // I1（docs/01 §8）：任意 (seat, day) 置位片对应至多一个有效 booking。
  std::vector<uint8_t> owner(static_cast<size_t>(board_.n_seats()) *
                             kRollingDays * kSlotsPerDay, 0);
  auto report = [](std::string* why, std::string msg) {
    if (why) *why = std::move(msg);
    return false;
  };
  for (const auto& [id, b] : bookings_) {
    if (b.state == BookingState::LEFT || b.state == BookingState::NO_SHOW ||
        b.state == BookingState::CANCELLED)
      continue;   // 终态单不再持有片（历史位图残留由席位字自然衰减）
    if (b.seat_id < 0 || b.seat_id >= board_.n_seats() || b.day_off < 0 ||
        b.day_off >= kRollingDays)
      return report(why, "I1 booking out of range: " + id);
    const int base = b.day_off * board_.n_seats() + b.seat_id;
    for (int s = 0; s < kSlotsPerDay; ++s) {
      if (!((b.mask >> s) & 1)) continue;
      if (owner[static_cast<size_t>(base) * kSlotsPerDay + s]++)
        return report(why, "I1 double-owned slot seat=" +
                               std::to_string(b.seat_id) + " day=" +
                               std::to_string(b.day_off) + " slot=" +
                               std::to_string(s));
    }
  }
  // 有效单的每一片必须确实置位（防"授予后丢片"）
  for (const auto& [id, b] : bookings_) {
    if (b.state == BookingState::LEFT || b.state == BookingState::NO_SHOW ||
        b.state == BookingState::CANCELLED)
      continue;
    if ((board_.bits(b.seat_id, b.day_off) & b.mask) != b.mask)
      return report(why, "I1 active booking bit missing: " + id);
  }
  return true;
}

// ---------- 幂等表 LRU ----------

void Engine::idem_put(const std::string& rid, const std::string& bid) {
  auto it = idem_index_.find(rid);
  if (it != idem_index_.end()) {           // 已存在：更新并触碰（移到最新端）
    it->second->second = bid;
    idem_lru_.splice(idem_lru_.end(), idem_lru_, it->second);
    return;
  }
  idem_lru_.emplace_back(rid, bid);
  idem_index_[rid] = std::prev(idem_lru_.end());
  while (idem_lru_.size() > cfg_.idempotent_cap) {   // 超限淘汰最旧
    idem_index_.erase(idem_lru_.front().first);
    idem_lru_.pop_front();
  }
}

const std::string* Engine::idem_get(const std::string& rid) {
  auto it = idem_index_.find(rid);
  if (it == idem_index_.end()) return nullptr;
  idem_lru_.splice(idem_lru_.end(), idem_lru_, it->second);  // 命中触碰
  return &it->second->second;
}

void Engine::idem_clear() {
  idem_lru_.clear();
  idem_index_.clear();
}

// ---------- 快照（v2：booking 扩展字段 + 幂等表） ----------

std::string Engine::serialize_state() const {
  std::string s;
  auto put = [&s](const void* p, size_t n) {
    s.append(static_cast<const char*>(p), n);
  };
  auto put_str16 = [&](const std::string& v) {
    const uint16_t len = static_cast<uint16_t>(v.size());
    put(&len, 2);
    put(v.data(), len);
  };
  const uint8_t ver = 2;
  put(&ver, 1);
  const int n_seats = board_.n_seats();
  put(&n_seats, 4);
  put(&seq_, 8);
  put(&day0_start_ms_, 8);
  std::vector<uint64_t> raw;
  board_.snapshot_raw(raw);
  const uint32_t cells = static_cast<uint32_t>(raw.size());
  put(&cells, 4);
  for (uint64_t v : raw) put(&v, 8);
  const uint32_t nb = static_cast<uint32_t>(bookings_.size());
  put(&nb, 4);
  // 规范序（按 booking id 排序）序列化：跨副本/往返字节级全等，不依赖
  // unordered_map 的迭代序（restore 后再次 serialize 仍相等）。
  std::vector<const std::pair<const std::string, Booking>*> ordered;
  ordered.reserve(bookings_.size());
  for (const auto& kv : bookings_) ordered.push_back(&kv);
  std::sort(ordered.begin(), ordered.end(),
            [](const auto* a, const auto* b) { return a->first < b->first; });
  for (const auto* kv : ordered) {
    const std::string& id = kv->first;
    const Booking& b = kv->second;
    put_str16(id);
    put(&b.user_id, 8); put(&b.seat_id, 4); put(&b.day_off, 4);
    put(&b.mask, 8);
    const uint8_t st = static_cast<uint8_t>(b.state);
    put(&st, 1);
    put(&b.start_ms, 8); put(&b.deadline_ms, 8); put(&b.seq, 8);
    put(&b.start_min, 4); put(&b.end_min, 4);   // epoch 2.2
    put_str16(b.ticket);                        // epoch 2.2
  }
  const uint32_t ni = static_cast<uint32_t>(idem_lru_.size());
  put(&ni, 4);                                  // 幂等表（最旧→最新，保持 LRU 序）
  for (const auto& [rid, bid] : idem_lru_) {
    put_str16(rid);
    put_str16(bid);
  }
  return s;
}

void Engine::restore_state(const std::string& s) {
  size_t off = 0;
  auto get = [&](void* p, size_t n) {
    if (off + n > s.size()) throw std::runtime_error("snapshot truncated");
    std::memcpy(p, s.data() + off, n);
    off += n;
  };
  auto get_str16 = [&](std::string& v) {
    uint16_t len = 0;
    get(&len, 2);
    if (off + len > s.size()) throw std::runtime_error("snapshot truncated");
    v.assign(s.data() + off, len);
    off += len;
  };
  uint8_t ver = 0;
  get(&ver, 1);
  if (ver != 2) throw std::runtime_error("snapshot version unsupported");
  int n_seats = 0;
  get(&n_seats, 4);
  get(&seq_, 8);
  get(&day0_start_ms_, 8);
  uint32_t cells = 0;
  get(&cells, 4);
  std::vector<uint64_t> raw(cells);
  for (auto& v : raw) get(&v, 8);
  board_.restore_raw(raw);
  uint32_t nb = 0;
  get(&nb, 4);
  bookings_.clear();
  ticket_to_booking_.clear();
  for (uint32_t i = 0; i < nb; ++i) {
    Booking b;
    get_str16(b.id);
    get(&b.user_id, 8); get(&b.seat_id, 4); get(&b.day_off, 4);
    get(&b.mask, 8);
    uint8_t st = 0;
    get(&st, 1);
    b.state = static_cast<BookingState>(st);
    get(&b.start_ms, 8); get(&b.deadline_ms, 8); get(&b.seq, 8);
    get(&b.start_min, 4); get(&b.end_min, 4);
    get_str16(b.ticket);
    bookings_[b.id] = b;
    if (!b.ticket.empty()) ticket_to_booking_[b.ticket] = b.id;
    arm_timers_for(bookings_[b.id]);  // 在途单重挂定时器（expire/end/grant/breakaway）
  }
  uint32_t ni = 0;
  get(&ni, 4);
  idem_clear();
  for (uint32_t i = 0; i < ni; ++i) {   // 恢复后重复 request_id 仍拒
    std::string rid, bid;
    get_str16(rid);
    get_str16(bid);
    idem_put(rid, bid);   // 按最旧→最新顺序重建，LRU 序不变
  }
  waitq_.clear();   // 候补队列为内存态，不进快照（已知限制，docs/T1 阶段二）
  queue_seq_ = 0;
  (void)n_seats;
}

}  // namespace seatmesh
