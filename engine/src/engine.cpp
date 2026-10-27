#include "engine/engine.hpp"

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
  // 演示环境：UTC 当日 00:00 + 07:00。正式版由配置注入（docs/09）。
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
  if (b->state != BookingState::HELD) return Outcome{false, "", 0, "E_WINDOW"};
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
  if (b->state != BookingState::HELD || now < b->start_ms ||
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
  if (b->state != BookingState::ACTIVE)
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
    if (t.tag.rfind("expire:", 0) == 0) {
      Event e;
      e.type = EventType::EXPIRE;
      e.booking_id = booking_id;
      append_and_apply(e);  // 超时也走日志仲裁：apply 幂等复核
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

// ---------- apply：唯一线性化点 ----------

void Engine::apply(const Event& raw, uint64_t index) {
  Event ev = raw;           // apply 需补全 seq / booking_id 后再投影与发布
  ev.seq = ++seq_;
  last_outcome_ = Outcome{};

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
        b.state = BookingState::HELD;
        b.start_ms = start_ms_of(ev.day_off, ev.start_min);
        b.deadline_ms = b.start_ms + kCheckinGraceMs;  // 窗口=[开始,开始+15min)
        b.seq = ev.seq;
        bookings_[b.id] = b;
        if (!ev.request_id.empty()) idempotent_[ev.request_id] = b.id;
        ev.booking_id = b.id;
        arm_timers_for(b);
        last_outcome_ = Outcome{true, b.id, 0, ""};
      } else {
        last_outcome_ = Outcome{false, "", conflict, "E_CONFLICT"};
        if (!ev.request_id.empty()) idempotent_[ev.request_id] = "";
      }
      break;
    }
    case EventType::CANCEL: {
      Booking* b = find_booking(ev.booking_id);
      if (b && b->state == BookingState::HELD &&
          ev.ts_ms < b->start_ms) {
        board_.release(b->seat_id, b->day_off, b->mask);
        b->state = BookingState::CANCELLED;  // 信用 0
        last_outcome_ = Outcome{true, b->id, 0, ""};
      } else {
        last_outcome_ = Outcome{false, ev.booking_id, 0, "E_WINDOW"};
      }
      break;
    }
    case EventType::CHECKIN: {
      Booking* b = find_booking(ev.booking_id);
      if (b && b->state == BookingState::HELD && ev.ts_ms >= b->start_ms &&
          ev.ts_ms <= b->deadline_ms) {
        b->state = BookingState::ACTIVE;
        last_outcome_ = Outcome{true, b->id, 0, ""};
      } else {
        last_outcome_ = Outcome{false, ev.booking_id, 0, "E_WINDOW"};
      }
      break;
    }
    case EventType::CHECKOUT: {
      Booking* b = find_booking(ev.booking_id);
      // 提前签退：释放剩余时段（含当前片的剩余部分），交给候补授权流程
      if (b && b->state == BookingState::ACTIVE) {
        const int64_t day_abs = day0_start_ms_ +
                                static_cast<int64_t>(b->day_off) * 86400000LL;
        const int64_t m = (ev.ts_ms - day_abs) / 60000 - kDayStartMin;
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
        b->state = BookingState::DONE;
        last_outcome_ = Outcome{true, b->id, 0, "credit=" + std::to_string(credit)};
      } else {
        last_outcome_ = Outcome{false, ev.booking_id, 0, "E_WINDOW"};
      }
      break;
    }
    case EventType::EXPIRE: {
      Booking* b = find_booking(ev.booking_id);
      if (b && b->state == BookingState::HELD && ev.ts_ms >= b->deadline_ms) {
        board_.release(b->seat_id, b->day_off, b->mask);
        b->state = BookingState::EXPIRED;  // 信用 -10（控制面结算）
        last_outcome_ = Outcome{true, b->id, 0, "credit=-10"};
      }  // 否则幂等跳过（tick 与重放竞争时安全）
      break;
    }
    case EventType::GRANT_OFFER:
    case EventType::GRANT_CLAIM:
    case EventType::GRANT_EXPIRE:
    case EventType::FREEZE:
    case EventType::SAGA:
      // TODO(M1, P2)：等待队列授权 / 封楼 / Saga（docs/01 §2、docs/03 §3）
      last_outcome_ = Outcome{false, "", 0, "NOT_IMPLEMENTED"};
      break;
    case EventType::SYSTEM:
      break;
  }

  if (projection_) projection_->on_apply(ev, index);
  if (!recovering_) publisher_->publish(ev);  // 拒绝与超时同样是事件流的一部分
}

// ---------- 辅助 ----------

Booking* Engine::find_booking(const std::string& id) {
  auto it = bookings_.find(id);
  return it == bookings_.end() ? nullptr : &it->second;
}

void Engine::arm_timers_for(const Booking& b) {
  if (b.state == BookingState::HELD) {
    const uint64_t id =
        timers_->schedule(b.deadline_ms, "expire:" + b.id);
    timer_tag_to_booking_[id] = b.id;
  }
  // TODO(M1, P2)：GRANTED 状态挂授权期定时器 "grant:<id>"
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
