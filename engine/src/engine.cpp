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
  idempotent_.clear();
  recovering_ = true;  // 重放期间只重建状态与投影，不对外发布
  const uint64_t idx = log_->replay();
  recovering_ = false;
  return idx;
}

// ---------- 预检 + append（预检只读，仲裁在 apply） ----------

Outcome Engine::append_and_apply(Event e) {
  e.ts_ms = clock_->now_ms();
  last_outcome_ = Outcome{};
  (void)log_->append(e);  // 持久后同步回调 apply → last_outcome_
  log_->barrier();
  return last_outcome_;
}

Outcome Engine::handle_reserve(const Event& req) {
  // 参数校验（非仲裁，不落日志）
  auto mask = mask_for(req.start_min, req.end_min);
  if (!mask || req.seat_id < 0 || req.seat_id >= board_.n_seats() ||
      req.day_off < 0 || req.day_off >= kRollingDays)
    return Outcome{false, "", 0, "E_PARAM"};

  // 幂等重放
  auto it = idempotent_.find(req.request_id);
  if (!req.request_id.empty() && it != idempotent_.end())
    return Outcome{!it->second.empty(), it->second, 0, "E_IDEMPOTENT_REPLAY"};

  // 时段已整体过去 → 拒绝（堵"预约过去时段刷信用"漏洞）
  if (start_ms_of(req.day_off, req.end_min) <= clock_->now_ms())
    return Outcome{false, "", 0, "E_PARAM"};

  // 单用户活跃预约唯一（区域内硬约束；跨区由 Saga+对账兜底）
  if (has_active_booking(req.user_id))
    return Outcome{false, "", 0, "E_QUOTA"};

  // 预检：冲突直接拒绝，省一次日志写入（最终仲裁仍在 apply）
  if (board_.has_conflict(req.seat_id, req.day_off, *mask))
    return Outcome{false, "", board_.bits(req.seat_id, req.day_off) & *mask,
                   "E_CONFLICT"};

  Event e = req;
  e.type = EventType::RESERVE;
  e.mask = *mask;
  return append_and_apply(e);
}

Outcome Engine::handle_cancel(const Event& req) {
  const Booking* b = find_booking(req.booking_id);
  if (!b) return Outcome{false, "", 0, "E_NOT_FOUND"};
  if (b->state != BookingState::PENDING_ENTRY) return Outcome{false, "", 0, "E_WINDOW"};
  if (clock_->now_ms() >= b->start_ms)
    return Outcome{false, req.booking_id, 0, "E_WINDOW"};  // 开始后只能签退
  Event e = req;
  e.type = EventType::CANCEL;
  e.user_id = b->user_id;
  return append_and_apply(e);
}

Outcome Engine::handle_checkin(const Event& req) {
  const Booking* b = find_booking(req.booking_id);
  if (!b) return Outcome{false, "", 0, "E_NOT_FOUND"};
  const int64_t now = clock_->now_ms();
  // 签到窗口 = [时段开始, 开始+15min)；提前/迟到都不落日志
  if (b->state != BookingState::PENDING_ENTRY || now < b->start_ms ||
      now > b->deadline_ms)
    return Outcome{false, req.booking_id, 0, "E_WINDOW"};
  Event e = req;
  e.type = EventType::CHECKIN;
  e.user_id = b->user_id;
  return append_and_apply(e);
}

Outcome Engine::handle_checkout(const Event& req) {
  const Booking* b = find_booking(req.booking_id);
  if (!b) return Outcome{false, "", 0, "E_NOT_FOUND"};
  if (b->state != BookingState::IN_LIBRARY)
    return Outcome{false, req.booking_id, 0, "E_WINDOW"};
  Event e = req;
  e.type = EventType::CHECKOUT;
  e.user_id = b->user_id;
  return append_and_apply(e);
}

void Engine::tick() {
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
    }
  }
}

Outcome Engine::dispatch(const IncomingRequest& req) {
  if (req.kind == "reserve") return handle_reserve(req.ev);
  if (req.kind == "cancel") return handle_cancel(req.ev);
  if (req.kind == "checkin") return handle_checkin(req.ev);
  if (req.kind == "checkout") return handle_checkout(req.ev);
  return Outcome{false, "", 0, "E_PARAM"};
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

// ---------- apply：唯一线性化点 ----------

void Engine::apply(const Event& raw, uint64_t index) {
  Event ev = raw;           // apply 需补全 seq / booking_id 后再投影与发布
  ev.seq = ++seq_;
  last_outcome_ = Outcome{};
  bool changed = false;       // 是否发生真实状态迁移（发布守卫）
  bool seat_mutated = false;  // 位图是否变更（决定是否追发 SeatChanged）

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
        b.start_ms = start_ms_of(ev.day_off, ev.start_min);
        b.deadline_ms = entry_deadline_for(ev.day_off, ev.start_min,
                                           ev.end_min, b.start_ms, ev.ts_ms);
        b.seq = ev.seq;
        bookings_[b.id] = b;
        if (!ev.request_id.empty()) idempotent_[ev.request_id] = b.id;
        ev.booking_id = b.id;
        arm_timers_for(b);
        changed = true;
        seat_mutated = true;
        last_outcome_ = Outcome{true, b.id, 0, ""};
      } else {
        // 拒绝也是事件（BookingRejected 语义）：进入事件流供审计与对账
        changed = true;
        last_outcome_ = Outcome{false, "", conflict, "E_CONFLICT"};
        if (!ev.request_id.empty()) idempotent_[ev.request_id] = "";
      }
      break;
    }
    case EventType::CANCEL: {
      Booking* b = find_booking(ev.booking_id);
      if (b && b->state == BookingState::PENDING_ENTRY &&
          ev.ts_ms < b->start_ms) {
        board_.release(b->seat_id, b->day_off, b->mask);
        cancel_timer(b->expire_timer);   // 撤销幽灵 NO_SHOW 定时器
        b->state = BookingState::CANCELLED;  // 信用 0
        changed = true;
        seat_mutated = true;
        last_outcome_ = Outcome{true, b->id, 0, ""};
      } else {
        last_outcome_ = Outcome{false, ev.booking_id, 0, "E_WINDOW"};
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
        const int64_t end_ms = b->start_ms +
            static_cast<int64_t>(__builtin_popcountll(b->mask)) * 900000LL;
        b->end_timer = timers_->schedule(end_ms, "end:" + b->id);
        timer_tag_to_booking_[b->end_timer] = b->id;   // 自然离馆：LEFT(+1)
        changed = true;
        last_outcome_ = Outcome{true, b->id, 0, ""};
      } else {
        last_outcome_ = Outcome{false, ev.booking_id, 0, "E_WINDOW"};
      }
      break;
    }
    case EventType::CHECKOUT: {
      Booking* b = find_booking(ev.booking_id);
      // 提前/正式离馆：释放剩余时段（含当前片的剩余部分），交给候补授权流程
      if (b && b->state == BookingState::IN_LIBRARY) {
        cancel_timer(b->end_timer);
        const int64_t day_abs = day0_start_ms_ +
                                static_cast<int64_t>(b->day_off) * 86400000LL;
        const int64_t m = (ev.ts_ms - day_abs) / 60000;   // 距当日 06:00 的分钟
        const int consumed = (m <= 0) ? 0
                                      : (m >= kSlotsPerDay * kSlotMinutes
                                             ? kSlotsPerDay
                                             : (static_cast<int>(m) + kSlotMinutes - 1) / kSlotMinutes);
        const uint64_t consumed_mask =
            (consumed >= 64) ? ~0ULL : ((1ULL << consumed) - 1);
        const uint64_t released = b->mask & ~consumed_mask;
        board_.release(b->seat_id, b->day_off, released);
        ev.released_mask = released;
        const int64_t dur_ms =
            static_cast<int64_t>(b->mask ? __builtin_popcountll(b->mask) : 0) *
            kSlotMinutes * 60000LL;
        const int credit = (ev.ts_ms >= b->start_ms + dur_ms) ? +1 : 0;
        (void)credit;  // 信用结算由控制面消费事件完成；detail 携带 delta
        b->state = BookingState::LEFT;
        changed = true;
        seat_mutated = (released != 0);
        last_outcome_ = Outcome{true, b->id, 0, "credit=" + std::to_string(credit)};
      } else {
        last_outcome_ = Outcome{false, ev.booking_id, 0, "E_WINDOW"};
      }
      break;
    }
    case EventType::NO_SHOW: {
      Booking* b = find_booking(ev.booking_id);
      if (b && b->state == BookingState::PENDING_ENTRY && ev.ts_ms >= b->deadline_ms) {
        board_.release(b->seat_id, b->day_off, b->mask);
        cancel_timer(b->expire_timer);
        b->state = BookingState::NO_SHOW;  // 信用 -10（控制面结算）
        changed = true;
        seat_mutated = true;
        last_outcome_ = Outcome{true, b->id, 0, "credit=-10"};
      }  // 否则幂等跳过：定时器竞态/重放下的空 NO_SHOW **不发布**（防幽灵扣分）
      break;
    }
    case EventType::DONE: {
      // 自然离馆：时段结束仍未刷卡离馆 → LEFT(+1)，无剩余片需释放
      Booking* b = find_booking(ev.booking_id);
      const int64_t end_ms = b ? b->start_ms +
          static_cast<int64_t>(__builtin_popcountll(b->mask)) * 900000LL : 0;
      if (b && b->state == BookingState::IN_LIBRARY && ev.ts_ms >= end_ms) {
        cancel_timer(b->end_timer);
        b->state = BookingState::LEFT;
        changed = true;
        last_outcome_ = Outcome{true, b->id, 0, "credit=+1"};
      } else {
        changed = false;   // 空转不发布
      }
      break;
    }
    case EventType::GRANT_OFFER:
    case EventType::GRANT_CLAIM:
    case EventType::GRANT_EXPIRE:
    case EventType::BREAKAWAY:
    case EventType::RETURN:
    case EventType::FREEZE:
    case EventType::SAGA:
      // TODO(T1/T4, P2)：等待队列授权 / 中途离开保留期 / 封楼 / Saga
      last_outcome_ = Outcome{false, "", 0, "NOT_IMPLEMENTED"};
      break;
    case EventType::SYSTEM:
    case EventType::SEAT_CHANGED:
      break;
  }

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
    sc.mask = board_.bits(ev.seat_id, ev.day_off);
    publisher_->publish(sc);
  }
}

// ---------- 辅助 ----------

Booking* Engine::find_booking(const std::string& id) {
  auto it = bookings_.find(id);
  return it == bookings_.end() ? nullptr : &it->second;
}

void Engine::arm_timers_for(const Booking& b) {
  if (b.state == BookingState::PENDING_ENTRY) {
    const uint64_t id =
        timers_->schedule(b.deadline_ms, "expire:" + b.id);
    timer_tag_to_booking_[id] = b.id;
  } else if (b.state == BookingState::IN_LIBRARY) {
    const int64_t end_ms = b.start_ms +
        static_cast<int64_t>(__builtin_popcountll(b.mask)) * 900000LL;
    const uint64_t id = timers_->schedule(end_ms, "end:" + b.id);
    timer_tag_to_booking_[id] = b.id;
  }
  // TODO(T1, P2)：GRANTED 状态挂授权期定时器 "grant:<id>"
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

std::string Engine::serialize_state() const {
  std::string s;
  auto put = [&s](const void* p, size_t n) {
    s.append(static_cast<const char*>(p), n);
  };
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
  for (const auto& [id, b] : bookings_) {
    const uint16_t len = static_cast<uint16_t>(id.size());
    put(&len, 2);
    put(id.data(), len);
    put(&b.user_id, 8); put(&b.seat_id, 4); put(&b.day_off, 4);
    put(&b.mask, 8);
    const uint8_t st = static_cast<uint8_t>(b.state);
    put(&st, 1);
    put(&b.start_ms, 8); put(&b.deadline_ms, 8); put(&b.seq, 8);
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
  for (uint32_t i = 0; i < nb; ++i) {
    Booking b;
    uint16_t len = 0;
    get(&len, 2);
    b.id.resize(len);
    get(b.id.data(), len);
    get(&b.user_id, 8); get(&b.seat_id, 4); get(&b.day_off, 4);
    get(&b.mask, 8);
    uint8_t st = 0;
    get(&st, 1);
    b.state = static_cast<BookingState>(st);
    get(&b.start_ms, 8); get(&b.deadline_ms, 8); get(&b.seq, 8);
    bookings_[b.id] = b;
    arm_timers_for(b);  // HELD 单恢复后重挂签到超时
  }
  (void)n_seats;
  idempotent_.clear();  // TODO(M2, P2)：幂等表入快照，避免重启后重复键失忆
}

}  // namespace seatmesh
