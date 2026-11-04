// 引擎单测（T1 阶段一/二）：FakeClock 确定性驱动，覆盖
//   - 业务逻辑演练修订 8 条（D1..D8，task-packs/T1 §业务逻辑演练修订）
//   - 生命周期 / 幂等 / 冲突 / 快照往返（L1..L9）
//   - 候补授权状态机（W1..W14，≥10 条）
//   - BREAKAWAY 中途离开（B1..B9，≥6 条）
//   - 幂等 LRU / 幂等入快照 / applied_seq / 内存有界 / SHED / region / JSON 转义（X*）
// 所有时间走 FakeClock；候选补队列内存态、GRANT_OFFER 派生发布的限制均有断言。
#include "engine/engine.hpp"
#include "engine/timing_wheel.hpp"
#include "storage/file_event_log.hpp"
#include "storage/in_memory_log.hpp"
#include "storage/projections.hpp"
#include "adapters/publisher.hpp"
#include "adapters/request_source.hpp"

#include <sys/stat.h>

#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

using namespace seatmesh;

namespace {

int failures = 0;
int passes = 0;
void expect(bool ok, const std::string& what) {
  std::cout << (ok ? "PASS " : "FAIL ") << what << "\n";
  ok ? ++passes : ++failures;
}

// ---- 时间锚点：day0 = 第 1000 天的 06:00（营业开始）；参数为距当日 06:00 的分钟 ----
constexpr int64_t kBase = 86400000LL * 1000;
constexpr int64_t kDay0 = kBase;                       // day_off=0 的 06:00
int64_t at(int day, int start_min) {                   // UNIX ms
  return kDay0 + static_cast<int64_t>(day) * 86400000LL +
         static_cast<int64_t>(start_min) * 60000LL;
}

Event reserve_ev(int64_t user, int seat, int day, int s, int e,
                 const std::string& rid = "") {
  Event ev;
  ev.user_id = user;
  ev.seat_id = seat;
  ev.day_off = day;
  ev.start_min = s;
  ev.end_min = e;
  ev.request_id = rid;
  return ev;
}
Event booking_ev(const std::string& booking_id) {
  Event ev;
  ev.booking_id = booking_id;
  return ev;
}
IncomingRequest req_of(const std::string& kind, const Event& ev,
                       bool leave = false, bool enqueue = false) {
  IncomingRequest r;
  r.kind = kind;
  r.ev = ev;
  r.leave = leave;
  r.enqueue_if_conflict = enqueue;
  return r;
}

int count_type(const std::vector<Event>& es, EventType t) {
  int n = 0;
  for (const auto& e : es)
    if (e.type == t) ++n;
  return n;
}
const Event* last_of(const std::vector<Event>& es, EventType t) {
  const Event* found = nullptr;
  for (const auto& e : es)
    if (e.type == t) found = &e;
  return found;
}

// 内存版脚手架（绝大多数用例）
struct Rig {
  EngineConfig cfg;
  std::shared_ptr<FakeClock> clock;
  InMemoryEventLog* log = nullptr;
  TimerHeap* timers = nullptr;
  std::unique_ptr<Engine> engine;
  std::vector<Event> events;

  explicit Rig(int64_t now_ms = kDay0 + 90 * 60000LL,  // 默认 07:30（当日 08:00 前申请）
               int seats = 20, EngineConfig over = EngineConfig{}) {
    cfg = over;
    cfg.n_seats = seats;
    cfg.day0_start_ms = kDay0;
    clock = std::make_shared<FakeClock>(now_ms);
    auto l = std::make_unique<InMemoryEventLog>();
    log = l.get();
    auto t = std::make_unique<TimerHeap>();
    timers = t.get();
    auto p = std::make_unique<InMemoryPublisher>();
    p->add_subscriber([this](const Event& e) { events.push_back(e); });
    engine = std::make_unique<Engine>(cfg, clock, std::move(l),
                                      std::make_unique<NullProjection>(),
                                      std::move(p), std::move(t));
    engine->recover();
  }
  Outcome dispatch(const std::string& kind, const Event& ev,
                   bool leave = false, bool enqueue = false) {
    return engine->dispatch(req_of(kind, ev, leave, enqueue));
  }
};

// 文件版脚手架（快照/轮转/重启恢复用例）
struct FileRig {
  std::string dir;
  std::shared_ptr<FakeClock> clock;
  std::unique_ptr<Engine> engine;
  std::vector<Event> events;
  int64_t now_ms;

  FileRig(const std::string& d, int64_t now) : dir(d), now_ms(now) {
    ::mkdir("/tmp/seatmesh_test", 0755);
    ::mkdir(dir.c_str(), 0755);
    open();
  }
  void open() {
    EngineConfig cfg;
    cfg.n_seats = 20;
    cfg.day0_start_ms = kDay0;
    clock = std::make_shared<FakeClock>(now_ms);
    auto log = std::make_unique<FileEventLog>(dir + "/wal.log", dir + "/snap");
    auto p = std::make_unique<InMemoryPublisher>();
    p->add_subscriber([this](const Event& e) { events.push_back(e); });
    engine = std::make_unique<Engine>(cfg, clock, std::move(log),
                                      std::make_unique<NullProjection>(),
                                      std::move(p),
                                      std::make_unique<TimerHeap>());
    engine->recover();
  }
  // 落盘快照并关闭（触发 WAL 轮转），下次 start 重启恢复
  void close() {
    (void)engine->log().write_snapshot();
    engine.reset();
  }
  // 优雅重启：先快照（模拟正常退出），再以新时钟恢复
  void restart(int64_t now) {
    if (engine) (void)engine->log().write_snapshot();
    now_ms = now;
    events.clear();
    open();
  }
  Outcome dispatch(const std::string& kind, const Event& ev,
                   bool leave = false, bool enqueue = false) {
    return engine->dispatch(req_of(kind, ev, leave, enqueue));
  }
};

}  // namespace

int main() {
  ::mkdir("/tmp/seatmesh_test", 0755);

  // ================= 业务逻辑演练修订（契约 epoch 2.1） =================

  // D1 过去时段拒绝：end 对应时刻 ≤ 当前 → E_PARAM，且不落日志
  {
    Rig rig(at(0, 30));  // 06:30
    Outcome o = rig.dispatch(
        "reserve", reserve_ev(1, 3, 0, 0, 30));  // 06:00-06:30，end==now
    expect(!o.ok && o.detail == "E_PARAM", "D1 过去时段(截止==now)拒绝 E_PARAM");
    Outcome o2 = rig.dispatch("reserve", reserve_ev(1, 3, 0, 0, 15));
    expect(!o2.ok && o2.detail == "E_PARAM" && rig.log->applied_index() == 0,
           "D1 过去时段拒绝不落日志");
  }

  // D2 入馆截止夹取：早鸟保留至 08:30 / 非早鸟 start+30 / 夹到时段结束
  {
    Rig rig;  // 06:30 申请（当日 08:00 前）
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 0, 90, 210, "r-a"));
    expect(a.ok, "D2 早鸟预约成功");
    // 截止 = max(07:30+30, 08:30) = 08:30：08:29 可入馆、08:31 不可
    rig.clock->advance(at(0, 149) - at(0, 90));
    Outcome c1 = rig.dispatch("checkin", booking_ev(a.booking_id));
    expect(c1.ok, "D2 早鸟截止 08:30 内可入馆");
    Rig rig2;
    Outcome b = rig2.dispatch("reserve", reserve_ev(2, 2, 0, 90, 210, "r-b"));
    rig2.clock->advance(at(0, 151) - at(0, 90));
    Outcome c2 = rig2.dispatch("checkin", booking_ev(b.booking_id));
    expect(!c2.ok && c2.detail == "E_WINDOW", "D2 早鸟截止=08:30 越线 E_WINDOW");

    // 非早鸟（开始 10:00 > 08:30）→ 截止 start+30 = 10:30
    Rig rig3;
    Outcome d = rig3.dispatch("reserve", reserve_ev(3, 3, 0, 240, 360, "r-c"));
    rig3.clock->advance(at(0, 270) - at(0, 90));  // 10:30
    Outcome c3 = rig3.dispatch("checkin", booking_ev(d.booking_id));
    expect(c3.ok, "D3 非早鸟截止=start+30 边界内入馆");
    Rig rig4;
    Outcome e = rig4.dispatch("reserve", reserve_ev(4, 4, 0, 240, 360, "r-d"));
    rig4.clock->advance(at(0, 271) - at(0, 90));  // 10:31
    Outcome c4 = rig4.dispatch("checkin", booking_ev(e.booking_id));
    expect(!c4.ok && c4.detail == "E_WINDOW", "D2 非早鸟截止越线 E_WINDOW");

    // 夹取到时段结束：07:30-08:00 短时段，早鸟保底 08:30 被夹到 08:00
    Rig rig5;
    Outcome f = rig5.dispatch("reserve", reserve_ev(5, 5, 0, 90, 120, "r-e"));
    rig5.clock->advance(at(0, 120) - at(0, 90));  // 08:00 = 时段结束
    Outcome c5 = rig5.dispatch("checkin", booking_ev(f.booking_id));
    expect(c5.ok, "D2 截止夹到时段结束（防必然爽约死锁）");
    Rig rig6;
    Outcome g = rig6.dispatch("reserve", reserve_ev(6, 6, 0, 90, 120, "r-f"));
    rig6.clock->advance(at(0, 121) - at(0, 90));
    Outcome c6 = rig6.dispatch("checkin", booking_ev(g.booking_id));
    expect(!c6.ok, "D2 时段结束后入馆 E_WINDOW");
  }

  // D3 CHECKOUT 释放公式：m = 距当日 06:00 分钟（勿二次减 kDayStartMin）
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 0, 120, 240, "r"));
    rig.clock->advance(at(0, 120) - at(0, 90));  // 08:00 入馆
    expect(rig.dispatch("checkin", booking_ev(a.booking_id)).ok,
           "D3 入馆");
    rig.clock->advance(at(0, 180) - at(0, 120));  // 09:00 签退（距 06:00 180min）
    Outcome o = rig.dispatch("checkout", booking_ev(a.booking_id));
    const Event* co = last_of(rig.events, EventType::CHECKOUT);
    const uint64_t mask = *mask_for(120, 240);        // bits8..15
    const uint64_t released = mask & ~((1ULL << 12) - 1);  // bits12..15
    expect(o.ok && co && co->released_mask == released,
           "D3 released_mask=剩余片（bits12..15）");
    expect(rig.engine->bits(1, 0) == 0, "D3 位图含已过片一并清空");
  }

  // D4 定时器生命周期：签到撤 expire 挂 end；取消撤 expire；checkout 撤 end；
  //   空转定时器事件不发布
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 0, 120, 240, "r"));
    rig.clock->advance(at(0, 120) - at(0, 90));
    rig.engine->tick();
    rig.dispatch("checkin", booking_ev(a.booking_id));
    rig.clock->advance(at(0, 151) - at(0, 120));  // 越过原入馆截止
    rig.engine->tick();
    expect(count_type(rig.events, EventType::NO_SHOW) == 0,
           "D4 签到后 expire 定时器已撤销（无幽灵 NO_SHOW）");
    rig.clock->advance(at(0, 240) - at(0, 151));
    rig.engine->tick();
    expect(count_type(rig.events, EventType::DONE) == 1,
           "D4 end 定时器到期 → DONE");
    expect(std::string(rig.engine->state_of(a.booking_id)) == "LEFT",
           "D4 自然离馆 LEFT");
    const int n_events = static_cast<int>(rig.events.size());
    rig.clock->advance(3600000);
    rig.engine->tick();
    rig.engine->tick();
    expect(static_cast<int>(rig.events.size()) == n_events,
           "D4 空转 tick 不发布任何事件");
  }
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r"));
    expect(rig.dispatch("cancel", booking_ev(a.booking_id)).ok,
           "D4 开始前取消成功");
    rig.clock->advance(at(1, 151) - at(0, 90) + 60000);  // 越过入馆截止
    rig.engine->tick();
    expect(count_type(rig.events, EventType::NO_SHOW) == 0,
           "D4 取消后 expire 定时器已撤销");
    expect(rig.engine->bits(1, 1) == 0, "D4 取消释放位图");
  }
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r"));
    rig.clock->advance(at(1, 120) - at(0, 90));
    rig.dispatch("checkin", booking_ev(a.booking_id));
    rig.dispatch("checkout", booking_ev(a.booking_id));
    rig.clock->advance(at(1, 240) - at(1, 120) + 60000);
    rig.engine->tick();
    expect(count_type(rig.events, EventType::DONE) == 0,
           "D4 checkout 后 end 定时器已撤销");
  }

  // D5 自然离馆：IN_LIBRARY 挂 end 定时器 → DONE → LEFT(+1)；快照恢复重挂
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 0, 120, 240, "r"));
    rig.clock->advance(at(0, 120) - at(0, 90));
    rig.dispatch("checkin", booking_ev(a.booking_id));
    rig.clock->advance(at(0, 240) - at(0, 120));
    rig.engine->tick();
    const Event* done = last_of(rig.events, EventType::DONE);
    expect(done && done->detail == "credit=+1" &&
               std::string(rig.engine->state_of(a.booking_id)) == "LEFT",
           "D5 自然离馆 DONE → LEFT(+1)");
  }
  {
    FileRig rig("/tmp/seatmesh_test/d5", at(1, 100));
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r"));
    rig.clock->advance(at(1, 120) - at(1, 100));  // 08:00 入馆
    rig.dispatch("checkin", booking_ev(a.booking_id));
    rig.restart(at(1, 130));  // 快照（IN_LIBRARY）+ 轮转 + 重启恢复
    expect(std::string(rig.engine->state_of(a.booking_id)) == "IN_LIBRARY",
           "D5 快照恢复 IN_LIBRARY");
    rig.restart(at(1, 240));  // 到时段结束
    rig.engine->tick();
    expect(count_type(rig.events, EventType::DONE) == 1,
           "D5 恢复后重挂 end 定时器 → DONE");
  }

  // D6 活跃预约唯一：E_QUOTA；取消后解除
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r1"));
    Outcome b = rig.dispatch("reserve", reserve_ev(1, 2, 2, 120, 240, "r2"));
    expect(a.ok && !b.ok && b.detail == "E_QUOTA", "D6 活跃唯一 E_QUOTA");
    rig.dispatch("cancel", booking_ev(a.booking_id));
    Outcome c = rig.dispatch("reserve", reserve_ev(1, 2, 2, 120, 240, "r3"));
    expect(c.ok, "D6 取消后可再次申请");
  }

  // D7 SeatChanged：位图变更后追发、与业务事件同 seq、整字覆盖
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r"));
    const Event* sc = last_of(rig.events, EventType::SEAT_CHANGED);
    const Event* rs = last_of(rig.events, EventType::RESERVE);
    expect(sc && rs && sc->seq == rs->seq &&
               sc->mask == rig.engine->bits(1, 1) &&
               sc->mask == *mask_for(120, 240),
           "D7 SeatChanged 同 seq 且整字覆盖");
    rig.dispatch("cancel", booking_ev(a.booking_id));
    const Event* sc2 = last_of(rig.events, EventType::SEAT_CHANGED);
    expect(sc2 && sc2->mask == 0 && sc2->seat_id == 1 && sc2->day_off == 1,
           "D7 释放后 SeatChanged mask=0 且坐标取自单据（非请求残缺字段）");
  }

  // D8 准入控制：in-flight 超限 → Shed{E_SHED, backoff}
  {
    EngineConfig over;
    over.max_inflight = 0;  // 测试用极小上限：任何 dispatch 即超限
    Rig rig(at(0, 90), 20, over);
    const uint64_t idx_before = rig.log->applied_index();
    Outcome o = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r"));
    expect(!o.ok && o.detail == "E_SHED" && o.shed &&
               o.backoff_hint_ms == kShedBackoffHintMs && o.applied_seq == 0,
           "D8 SHED: E_SHED + backoff_hint_ms=20 + 未落日志");
    expect(rig.log->applied_index() == idx_before, "D8 SHED 不落日志");
  }

  // ================= 生命周期 / 幂等 / 冲突 / 快照（阶段一钉死） =================

  // L1 预约成功置位
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 3, 1, 120, 240, "r"));
    expect(a.ok && rig.engine->bits(3, 1) == *mask_for(120, 240),
           "L1 预约成功置位");
  }

  // L2 重叠拒绝且 conflict_mask 恰为交集
  {
    Rig rig;
    rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r1"));
    Outcome b = rig.dispatch("reserve", reserve_ev(2, 1, 1, 180, 300, "r2"));
    const uint64_t want = *mask_for(120, 240) & *mask_for(180, 300);
    expect(!b.ok && b.detail == "E_CONFLICT" && b.conflict_mask == want,
           "L2 conflict_mask 恰为交集");
  }

  // L3 幂等重放返回原结果、不重复落日志
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "rid"));
    const uint64_t idx = rig.log->applied_index();
    Outcome b = rig.dispatch("reserve", reserve_ev(9, 9, 1, 60, 120, "rid"));
    expect(b.detail == "E_IDEMPOTENT_REPLAY" && b.booking_id == a.booking_id &&
               b.ok && rig.log->applied_index() == idx,
           "L3 幂等重放返回原结果且不落日志");
  }

  // L4 参数非法 → E_PARAM 不落日志
  {
    Rig rig;
    const uint64_t idx0 = rig.log->applied_index();
    expect(rig.dispatch("reserve", reserve_ev(1, -1, 1, 120, 240)).detail ==
               "E_PARAM",
           "L4 负座位 E_PARAM");
    expect(rig.dispatch("reserve", reserve_ev(1, 1, 1, 121, 240)).detail ==
               "E_PARAM",
           "L4 非倍数时间 E_PARAM");
    expect(rig.dispatch("reserve", reserve_ev(1, 1, 1, 240, 120)).detail ==
               "E_PARAM",
           "L4 end<start E_PARAM");
    expect(rig.dispatch("reserve", reserve_ev(1, 1, 14, 120, 240)).detail ==
               "E_PARAM",
           "L4 day 越界 E_PARAM");
    expect(rig.dispatch("reserve", reserve_ev(1, 20, 1, 120, 240)).detail ==
               "E_PARAM",
           "L4 seat==n_seats E_PARAM");
    expect(rig.log->applied_index() == idx0, "L4 E_PARAM 均不落日志");
  }

  // L5 入馆刷卡窗 [时段开始, 截止]：窗外 E_WINDOW
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r"));
    Outcome early = rig.dispatch("checkin", booking_ev(a.booking_id));
    expect(!early.ok && early.detail == "E_WINDOW", "L5 开始前入馆 E_WINDOW");
    rig.clock->advance(at(1, 120) - at(0, 90) + 60000);  // 开始+1min
    expect(rig.dispatch("checkin", booking_ev(a.booking_id)).ok,
           "L5 窗内入馆成功");
    Rig rig2;
    Outcome b = rig2.dispatch("reserve", reserve_ev(2, 2, 1, 120, 240, "r"));
    rig2.clock->advance(at(1, 150) - at(0, 90) + 60000);  // 截止+1min
    expect(rig2.dispatch("checkin", booking_ev(b.booking_id))
                   .detail == "E_WINDOW",
           "L5 越过截止入馆 E_WINDOW");
  }

  // L6 越过截止 tick → NO_SHOW + 位图释放 + credit=-10
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r"));
    rig.clock->advance(at(1, 150) - at(0, 90) + 60000);
    rig.engine->tick();
    const Event* ns = last_of(rig.events, EventType::NO_SHOW);
    expect(ns && ns->detail == "credit=-10" && rig.engine->bits(1, 1) == 0 &&
               std::string(rig.engine->state_of(a.booking_id)) == "NO_SHOW",
           "L6 NO_SHOW：位图释放 + credit=-10");
  }

  // L7 开始前 cancel 释放；开始后 cancel E_WINDOW
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r"));
    rig.clock->advance(at(1, 120) - at(0, 90));  // 恰到开始时刻
    Outcome late = rig.dispatch("cancel", booking_ev(a.booking_id));
    expect(!late.ok && late.detail == "E_WINDOW", "L7 开始后 cancel E_WINDOW");
    Rig rig2;
    Outcome b = rig2.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r"));
    expect(rig2.dispatch("cancel", booking_ev(b.booking_id)).ok &&
               rig2.engine->bits(1, 1) == 0,
           "L7 开始前 cancel 释放");
  }

  // L8/L9 serialize/restore 往返全等 + 恢复后行为一致（含幂等表）
  {
    Rig rig;
    rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "rid-a"));
    Outcome b = rig.dispatch("reserve", reserve_ev(2, 2, 1, 120, 240, "rid-b"));
    rig.clock->advance(at(1, 120) - at(0, 90));
    rig.dispatch("checkin", booking_ev(b.booking_id));
    const std::string blob = rig.engine->serialize_state();

    Rig rig2;   // 同一时钟起点，空日志
    rig2.engine->restore_state(blob);
    expect(rig2.engine->serialize_state() == blob, "L9 快照往返全等");
    expect(rig2.engine->state_of(b.booking_id) &&
               std::string(rig2.engine->state_of(b.booking_id)) == "IN_LIBRARY",
           "L9 恢复后状态一致（IN_LIBRARY）");
    Outcome c = rig2.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "rid-a"));
    expect(c.detail == "E_IDEMPOTENT_REPLAY" && c.booking_id ==
               rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "rid-a"))
                   .booking_id,
           "L9 幂等表入快照：恢复后重复 rid 仍拒");
  }

  // ================= 候补授权状态机（阶段二，≥10 条） =================

  // W1 冲突入队：ticket=T1、queue_pos=1、不落日志
  {
    Rig rig;
    rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r1"));
    const uint64_t idx = rig.log->applied_index();
    Outcome q = rig.dispatch("reserve", reserve_ev(2, 1, 1, 120, 240, "r2"),
                             false, true);
    expect(!q.ok && q.detail == "E_QUEUED" && q.queue_ticket == "T1" &&
               q.queue_pos == 1 && rig.log->applied_index() == idx,
           "W1 冲突入队返 T1/pos=1 且不落日志");
    Outcome q2 = rig.dispatch("reserve", reserve_ev(3, 1, 1, 120, 240, "r3"),
                              false, true);
    expect(q2.queue_ticket == "T2" && q2.queue_pos == 2,
           "W8 FIFO 第二位 pos=2");
  }

  // W2 释放 → 队首 GRANT_OFFER（派生发布）+ GRANTED 单 + 位图换手不落日志
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r1"));
    rig.dispatch("reserve", reserve_ev(2, 1, 1, 120, 240, "r2"), false, true);
    const uint64_t idx = rig.log->applied_index();
    expect(rig.dispatch("cancel", booking_ev(a.booking_id)).ok,
           "W2 持有者取消");
    const Event* go = last_of(rig.events, EventType::GRANT_OFFER);
    expect(go && go->queue_ticket == "T1" && go->booking_id == "G1" &&
               go->user_id == 2,
           "W2 GRANT_OFFER 派发 T1→G1");
    expect(std::string(rig.engine->state_of("G1")) == "GRANTED",
           "W2 授权单 GRANTED");
    expect(rig.engine->bits(1, 1) == *mask_for(120, 240),
           "W2 位图换手未公开空闲");
    expect(rig.log->applied_index() == idx + 1,
           "W2 GRANT_OFFER 不落日志（仅 CANCEL 一条）");
  }

  // W3 期内认领 → PENDING_ENTRY（落日志）+ 读己之写 applied_seq
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r1"));
    rig.dispatch("reserve", reserve_ev(2, 1, 1, 120, 240, "r2"), false, true);
    rig.dispatch("cancel", booking_ev(a.booking_id));
    IncomingRequest cr;
    cr.kind = "claim_ticket";
    cr.ev.queue_ticket = "T1";
    Outcome c = rig.engine->dispatch(cr);
    expect(c.ok && c.booking_id == "G1" &&
               std::string(rig.engine->state_of("G1")) == "PENDING_ENTRY" &&
               c.applied_seq > 0,
           "W3 认领转 PENDING_ENTRY 且 applied_seq>0");
  }

  // W4 认领后旧授权定时器撤销（到期不再顺延/释放）
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r1"));
    rig.dispatch("reserve", reserve_ev(2, 1, 1, 120, 240, "r2"), false, true);
    rig.dispatch("cancel", booking_ev(a.booking_id));
    IncomingRequest cr;
    cr.kind = "claim_ticket";
    cr.ev.queue_ticket = "T1";
    rig.engine->dispatch(cr);
    rig.clock->advance(kGrantTtlMs + 1000);  // 越过原授权期
    rig.engine->tick();
    expect(count_type(rig.events, EventType::GRANT_EXPIRE) == 0 &&
               std::string(rig.engine->state_of("G1")) == "PENDING_ENTRY",
           "W4 认领撤销授权定时器（无 GRANT_EXPIRE）");
  }

  // W5 授权超时：GRANT_EXPIRE 发布、票据失效、认领被拒
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r1"));
    rig.dispatch("reserve", reserve_ev(2, 1, 1, 120, 240, "r2"), false, true);
    rig.dispatch("cancel", booking_ev(a.booking_id));
    rig.clock->advance(kGrantTtlMs + 1000);
    rig.engine->tick();
    expect(count_type(rig.events, EventType::GRANT_EXPIRE) == 1,
           "W5 授权超时发布 GRANT_EXPIRE");
    IncomingRequest cr;
    cr.kind = "claim_ticket";
    cr.ev.queue_ticket = "T1";
    Outcome c = rig.engine->dispatch(cr);
    expect(!c.ok, "W5 超时后认领被拒");
  }

  // W6 超时顺延：队首过期后下一位获得授权
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r1"));
    rig.dispatch("reserve", reserve_ev(2, 1, 1, 120, 240, "r2"), false, true);
    rig.dispatch("reserve", reserve_ev(3, 1, 1, 120, 240, "r3"), false, true);
    rig.dispatch("cancel", booking_ev(a.booking_id));
    rig.clock->advance(kGrantTtlMs + 1000);
    rig.engine->tick();
    const Event* go2 = last_of(rig.events, EventType::GRANT_OFFER);
    expect(go2 && go2->queue_ticket == "T2" && go2->booking_id == "G2",
           "W6 顺延：T2 获得授权 G2");
    IncomingRequest cr;
    cr.kind = "claim_ticket";
    cr.ev.queue_ticket = "T2";
    expect(rig.engine->dispatch(cr).booking_id == "G2", "W6 T2 可认领");
  }

  // W7 队空才真正释放：过期且队列空 → 位图清零
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r1"));
    rig.dispatch("reserve", reserve_ev(2, 1, 1, 120, 240, "r2"), false, true);
    rig.dispatch("cancel", booking_ev(a.booking_id));
    expect(rig.engine->bits(1, 1) != 0, "W7 授权期位图保持置位");
    rig.clock->advance(kGrantTtlMs + 1000);
    rig.engine->tick();
    expect(rig.engine->bits(1, 1) == 0, "W7 队空过期后位图真正释放");
  }

  // W9 不同 (day,seat) 的队列不被误授
  {
    Rig rig;
    Outcome a1 = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r1"));
    Outcome a2 = rig.dispatch("reserve", reserve_ev(2, 1, 2, 120, 240, "r2"));
    rig.dispatch("reserve", reserve_ev(3, 1, 2, 120, 240, "r3"), false, true);
    rig.dispatch("cancel", booking_ev(a1.booking_id));  // day1 释放
    const Event* go = last_of(rig.events, EventType::GRANT_OFFER);
    expect(go == nullptr, "W9 day1 释放不误授 day2 队列");
    expect(rig.engine->bits(1, 1) == 0, "W9 day1 正常释放");
    rig.dispatch("cancel", booking_ev(a2.booking_id));  // day2 释放
    expect(last_of(rig.events, EventType::GRANT_OFFER) != nullptr,
           "W9 day2 释放正确授权");
  }

  // W10 授权期恰 60s：59s 时仍 GRANTED，61s 过期
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r1"));
    rig.dispatch("reserve", reserve_ev(2, 1, 1, 120, 240, "r2"), false, true);
    rig.dispatch("cancel", booking_ev(a.booking_id));
    rig.clock->advance(kGrantTtlMs - 1000);
    rig.engine->tick();
    expect(std::string(rig.engine->state_of("G1")) == "GRANTED",
           "W10 59s 仍 GRANTED");
    rig.clock->advance(2000);
    rig.engine->tick();
    expect(std::string(rig.engine->state_of("G1")) == "CANCELLED",
           "W10 61s 过期（票据放弃）");
  }

  // W11 GRANTED 占用活跃额度（E_QUOTA）
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r1"));
    rig.dispatch("reserve", reserve_ev(2, 1, 1, 120, 240, "r2"), false, true);
    rig.dispatch("cancel", booking_ev(a.booking_id));
    Outcome x = rig.dispatch("reserve", reserve_ev(2, 5, 2, 120, 240, "r3"));
    expect(!x.ok && x.detail == "E_QUOTA", "W11 GRANTED 计入活跃额度");
  }

  // W12 同用户重复入队返回原票据
  {
    Rig rig;
    rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r1"));
    Outcome q1 = rig.dispatch("reserve", reserve_ev(2, 1, 1, 120, 240, "r2"),
                              false, true);
    Outcome q2 = rig.dispatch("reserve", reserve_ev(2, 1, 1, 60, 240, "r3"),
                              false, true);
    expect(q1.queue_ticket == "T1" && q2.queue_ticket == "T1" &&
               q2.queue_pos == 1,
           "W12 重复入队去重返原票据");
  }

  // W13 CHECKOUT/NO_SHOW 释放同样进入候补授权
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r1"));
    rig.dispatch("reserve", reserve_ev(2, 1, 1, 120, 240, "r2"), false, true);
    rig.clock->advance(at(1, 120) - at(0, 90));
    rig.dispatch("checkin", booking_ev(a.booking_id));
    rig.dispatch("checkout", booking_ev(a.booking_id));
    expect(last_of(rig.events, EventType::GRANT_OFFER) != nullptr,
           "W13 CHECKOUT 释放进入候补授权");
  }
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r1"));
    rig.dispatch("reserve", reserve_ev(2, 1, 1, 120, 240, "r2"), false, true);
    rig.clock->advance(at(1, 150) - at(0, 90) + 60000);
    rig.engine->tick();  // NO_SHOW
    expect(last_of(rig.events, EventType::GRANT_OFFER) != nullptr,
           "W13 NO_SHOW 释放进入候补授权");
  }

  // W14 重启：GRANTED 单经快照保留、队列内存态清空；过期后真正释放
  {
    FileRig rig("/tmp/seatmesh_test/w14", at(1, 50));
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r1"));
    rig.dispatch("reserve", reserve_ev(2, 1, 1, 120, 240, "r2"), false, true);
    rig.dispatch("cancel", booking_ev(a.booking_id));
    rig.restart(at(1, 55));  // 快照 + 重启（队列内存态不进快照）
    expect(std::string(rig.engine->state_of("G1")) == "GRANTED",
           "W14 重启后 GRANTED 单保留（快照）");
    rig.restart(at(1, 50) + kGrantTtlMs + 1000);  // 跳到授权期后
    rig.engine->tick();
    expect(rig.engine->bits(1, 1) == 0, "W14 队列清空（已知限制）→ 过期真释放");
  }

  // ================= BREAKAWAY 中途离开（阶段二，≥6 条） =================

  // B1 IN_LIBRARY → BREAKAWAY；B7 非已入馆状态拒绝
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 360, "r1"));
    Outcome early = rig.dispatch("checkin", booking_ev(a.booking_id),
                                 true);
    expect(!early.ok && early.detail == "E_WINDOW",
           "B7 未入馆中途离开 E_WINDOW");
    rig.clock->advance(at(1, 120) - at(0, 90));
    rig.dispatch("checkin", booking_ev(a.booking_id));
    Outcome lv = rig.dispatch("checkin", booking_ev(a.booking_id),
                              true);
    expect(lv.ok && std::string(rig.engine->state_of(a.booking_id)) ==
                        "BREAKAWAY",
           "B1 leave=true → BREAKAWAY");
    Outcome again = rig.dispatch("checkin", booking_ev(a.booking_id),
                                 true);
    expect(!again.ok && again.detail == "E_WINDOW", "B8 重复中途离开 E_WINDOW");
  }

  // B2 峰值 11:00–13:00 离开 → 保留 60min
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 360, "r1"));
    rig.clock->advance(at(1, 120) - at(0, 90));  // 08:00 入馆
    rig.dispatch("checkin", booking_ev(a.booking_id));
    rig.clock->advance(at(1, 300) - at(1, 120));  // 11:00 离开（峰值）
    rig.dispatch("checkin", booking_ev(a.booking_id), true);
    rig.clock->advance(59 * 60000);
    rig.engine->tick();
    expect(std::string(rig.engine->state_of(a.booking_id)) == "BREAKAWAY",
           "B2 峰值保留 59min 仍 BREAKAWAY");
    rig.clock->advance(2 * 60000);
    rig.engine->tick();
    expect(std::string(rig.engine->state_of(a.booking_id)) == "LEFT",
           "B2 峰值保留 61min → LEFT");
  }

  // B3 第二峰值 17:00–19:00 离开 → 60min
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 720, 840, "r1"));
    rig.clock->advance(at(1, 720) - at(0, 90));  // 18:00 入馆（窗口内）
    rig.dispatch("checkin", booking_ev(a.booking_id));
    rig.clock->advance(at(1, 750) - at(1, 720));  // 18:30 离开
    rig.dispatch("checkin", booking_ev(a.booking_id), true);
    rig.clock->advance(59 * 60000);
    rig.engine->tick();
    expect(std::string(rig.engine->state_of(a.booking_id)) == "BREAKAWAY",
           "B3 第二峰值 59min 仍 BREAKAWAY");
    rig.clock->advance(2 * 60000);
    rig.engine->tick();
    expect(std::string(rig.engine->state_of(a.booking_id)) == "LEFT",
           "B3 第二峰值 61min → LEFT");
  }

  // B4 非峰值离开 → 30min
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r1"));
    rig.clock->advance(at(1, 120) - at(0, 90));
    rig.dispatch("checkin", booking_ev(a.booking_id));
    rig.clock->advance(at(1, 180) - at(1, 120));  // 09:00 离开（非峰值）
    rig.dispatch("checkin", booking_ev(a.booking_id), true);
    rig.clock->advance(29 * 60000);
    rig.engine->tick();
    expect(std::string(rig.engine->state_of(a.booking_id)) == "BREAKAWAY",
           "B4 非峰值 29min 仍 BREAKAWAY");
    rig.clock->advance(2 * 60000);
    rig.engine->tick();
    expect(std::string(rig.engine->state_of(a.booking_id)) == "LEFT",
           "B4 非峰值 31min → LEFT");
  }

  // B5 期内再入馆 → IN_LIBRARY；end 定时器重挂 → DONE(+1)
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r1"));
    rig.clock->advance(at(1, 120) - at(0, 90));
    rig.dispatch("checkin", booking_ev(a.booking_id));
    rig.clock->advance(at(1, 180) - at(1, 120));  // 09:00 离开（非峰值）
    rig.dispatch("checkin", booking_ev(a.booking_id), true);
    rig.clock->advance(20 * 60000);               // 09:20 回馆（保留期内）
    Outcome ret = rig.dispatch("checkin", booking_ev(a.booking_id));
    expect(ret.ok && std::string(rig.engine->state_of(a.booking_id)) ==
                         "IN_LIBRARY",
           "B5 期内再入馆回 IN_LIBRARY");
    rig.clock->advance(at(1, 240) - at(1, 200));  // 到 10:00 时段结束
    rig.engine->tick();
    expect(count_type(rig.events, EventType::DONE) == 1 &&
               last_of(rig.events, EventType::DONE)->detail == "credit=+1",
           "B5 返回后 end 定时器重挂 → DONE(+1)");
  }

  // B6 保留期超时 → LEFT(信用0) + 释放剩余片 + 候补授权
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 360, "r1"));
    rig.dispatch("reserve", reserve_ev(2, 1, 1, 120, 360, "r2"), false, true);
    rig.clock->advance(at(1, 120) - at(0, 90));
    rig.dispatch("checkin", booking_ev(a.booking_id));
    rig.clock->advance(at(1, 180) - at(1, 120));  // 09:00 离开（非峰值 30min）
    rig.dispatch("checkin", booking_ev(a.booking_id), true);
    rig.clock->advance(31 * 60000);               // 09:31 保留期已过
    rig.engine->tick();
    const Event* co = last_of(rig.events, EventType::CHECKOUT);
    expect(co && co->detail == "BREAKAWAY_EXPIRE" && co->released_mask != 0,
           "B6 超时出账 BREAKAWAY_EXPIRE 释放剩余片");
    expect(std::string(rig.engine->state_of(a.booking_id)) == "LEFT",
           "B6 超时转 LEFT");
    expect(last_of(rig.events, EventType::GRANT_OFFER) != nullptr,
           "B6 剩余片进入候补授权");
  }

  // ================= 阶段三增量（fuzz/region 之外的可单测部分） =================

  // X1 applied_seq 贯通：落日志事件带 seq；预检拒绝为 0
  {
    Rig rig;
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r"));
    expect(a.applied_seq == 1, "X1 applied_seq=首事件 seq");
    Outcome bad = rig.dispatch("reserve", reserve_ev(1, 1, 1, 7, 9, ""));
    expect(bad.applied_seq == 0, "X1 预检拒绝 applied_seq=0");
    Outcome b = rig.dispatch("reserve", reserve_ev(2, 1, 1, 120, 240, "r2"),
                             false, true);
    expect(b.detail == "E_QUEUED" && b.applied_seq == 0,
           "X1 入队不落日志 applied_seq=0");
  }

  // X2 幂等表 LRU：cap=2 时淘汰最旧；默认容量常量 = 100000
  {
    EngineConfig over;
    over.idempotent_cap = 2;
    Rig rig(at(0, 90), 20, over);
    rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "a"));
    rig.dispatch("reserve", reserve_ev(2, 2, 1, 120, 240, "b"));
    rig.dispatch("reserve", reserve_ev(3, 3, 1, 120, 240, "c"));  // 淘汰最旧 a
    Outcome rb = rig.dispatch("reserve", reserve_ev(5, 5, 1, 120, 240, "b"));
    expect(rb.detail == "E_IDEMPOTENT_REPLAY", "X2 未淘汰 rid 仍命中");
    Outcome ra = rig.dispatch("reserve", reserve_ev(4, 4, 1, 120, 240, "a"));
    expect(ra.ok && ra.detail.empty(),
           "X2 超限淘汰最旧：旧 rid 按新请求处理");
    expect(EngineConfig{}.idempotent_cap == 100000,
           "X2 默认幂等表上限 100000");
  }

  // X4 内存有界：终态且过日的 booking 移出内存
  {
    Rig rig(at(0, 1));  // 06:01
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 0, 0, 30, "r"));
    rig.clock->advance(at(0, 5) - at(0, 1));  // 06:05 入馆
    rig.dispatch("checkin", booking_ev(a.booking_id));
    rig.dispatch("checkout", booking_ev(a.booking_id));  // LEFT
    expect(rig.engine->state_of(a.booking_id) != nullptr,
           "X4 终态当日仍在内存");
    rig.clock->advance(86400000LL);  // 次日
    rig.dispatch("reserve", reserve_ev(2, 2, 2, 120, 240, "r2"));  // 触发清扫
    expect(rig.engine->state_of(a.booking_id) == nullptr &&
               rig.engine->bookings_in_memory() == 1,
           "X4 过日终态单移出内存");
  }

  // X5 I1 不变量：常规流量（含候补/BREAKAWAY）下恒成立
  {
    Rig rig;
    rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r1"));
    rig.dispatch("reserve", reserve_ev(2, 1, 1, 120, 240, "r2"), false, true);
    rig.dispatch("reserve", reserve_ev(3, 1, 1, 180, 300, "r3"), false, true);
    rig.clock->advance(at(1, 120) - at(0, 90));
    rig.dispatch("checkin", booking_ev("B1"));
    rig.dispatch("checkin", booking_ev("B1"), true);
    rig.clock->advance(61 * 60000);
    rig.engine->tick();
    std::string why;
    expect(rig.engine->check_invariant_i1(&why), "X5 I1 恒成立: " + why);
  }

  // X6 JSON 帧 v0：转义完整（含引号 detail）+ 解析往返
  {
    Outcome o;
    o.ok = true;
    o.booking_id = "B1";
    o.detail = "quote\"back\\slash\nnewline";
    const std::string j = outcome_to_json(o);
    expect(json_get_string(j, "detail") == "quote\"back\\slash\nnewline",
           "X6 outcome JSON 引号/反斜杠/换行转义还原");
    expect(json_escape("a\"b\\c\nd\te") == "a\\\"b\\\\c\\nd\\te",
           "X6 json_escape 完整性");
    IncomingRequest req;
    expect(parse_frame_v0(
               "{\"kind\":\"reserve\",\"user_id\":1001,\"seat_id\":3,"
               "\"day_off\":1,\"start_min\":120,\"end_min\":240,"
               "\"request_id\":\"r\\\"1\",\"trace_id\":\"t001\","
               "\"enqueue_if_conflict\":true,\"leave\":false}",
               req),
           "X6 parse_frame_v0 解析成功");
    expect(req.kind == "reserve" && req.ev.user_id == 1001 &&
               req.ev.seat_id == 3 && req.ev.day_off == 1 &&
               req.ev.start_min == 120 && req.ev.end_min == 240 &&
               req.ev.request_id == "r\"1" && req.enqueue_if_conflict &&
               !req.leave,
           "X6 JSON 帧 v0 字段对齐（含转义 rid 与布尔）");
    IncomingRequest bad;
    expect(!parse_frame_v0("{\"nokie\":\"x\"}", bad), "X6 缺 kind 解析失败");
  }

  // X7 region 参数：事件携带 region_id 且编解码往返
  {
    EngineConfig over;
    over.region_id = 7;
    Rig rig(at(0, 90), 20, over);
    Outcome a = rig.dispatch("reserve", reserve_ev(1, 1, 1, 120, 240, "r"));
    const Event* rs = last_of(rig.events, EventType::RESERVE);
    expect(rs && rs->region_id == 7, "X7 事件携带 region_id=7");
    Event decoded;
    expect(decode_event(encode_event(*rs).data(),
                        encode_event(*rs).size(), decoded) &&
               decoded.region_id == 7,
           "X7 region_id 编解码往返");
  }

  std::cout << "passed=" << passes << " failed=" << failures << "\n";
  std::cout << (failures ? "RESULT: FAIL\n" : "RESULT: ALL PASS\n");
  return failures ? 1 : 0;
}
