// Fuzz（T1 阶段三）：种子参数化随机非法入参打 dispatch。
//   用法：test_fuzz [seed] [iterations]   （CI 里跑固定种子；可复现）
// 断言：
//   1. 不崩（任何 std::exception 都算失败）；
//   2. I1 恒成立（任意 (seat,day) 置位片对应至多一个有效 booking）；
//   3. 错误码合法（白名单）/ ok 时 detail 形态合法；
//   4. 周期性快照 serialize → restore → serialize 往返全等，恢复后 I1 仍成立。
#include "engine/engine.hpp"
#include "engine/timing_wheel.hpp"
#include "storage/in_memory_log.hpp"
#include "storage/projections.hpp"
#include "adapters/publisher.hpp"

#include <random>
#include <string>
#include <vector>
#include <iostream>

using namespace seatmesh;

namespace {

constexpr int64_t kDay0 = 86400000LL * 1000;  // day0 的 06:00 锚点

int failures = 0;
void fail(const std::string& what) {
  std::cout << "FAIL " << what << "\n";
  ++failures;
}

bool legal_reject_detail(const std::string& d) {
  return d == "E_PARAM" || d == "E_CONFLICT" || d == "E_WINDOW" ||
         d == "E_NOT_FOUND" || d == "E_QUOTA" || d == "E_IDEMPOTENT_REPLAY" ||
         d == "E_QUEUED" || d == "E_SHED" || d == "NOT_IMPLEMENTED";
}

struct FuzzRig {
  EngineConfig cfg;
  std::shared_ptr<FakeClock> clock;
  InMemoryEventLog* log = nullptr;
  std::unique_ptr<Engine> engine;
  std::unique_ptr<Engine> shadow;   // 快照恢复对比用

  explicit FuzzRig(int seats) {
    cfg.n_seats = seats;
    cfg.day0_start_ms = kDay0;
    clock = std::make_shared<FakeClock>(kDay0 + 60 * 60000LL);
    auto l = std::make_unique<InMemoryEventLog>();
    log = l.get();
    engine = std::make_unique<Engine>(cfg, clock, std::move(l),
                                      std::make_unique<NullProjection>(),
                                      std::make_unique<InMemoryPublisher>(),
                                      std::make_unique<TimerHeap>());
    engine->recover();
    shadow = std::make_unique<Engine>(cfg, clock, std::make_unique<InMemoryEventLog>(),
                                      std::make_unique<NullProjection>(),
                                      std::make_unique<InMemoryPublisher>(),
                                      std::make_unique<TimerHeap>());
    shadow->recover();
  }
};

}  // namespace

int main(int argc, char** argv) {
  const uint64_t seed =
      argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 20261104ULL;
  const int iterations =
      argc > 2 ? std::atoi(argv[2]) : 2000;
  std::mt19937_64 rng(seed);

  const int kSeats = 8;
  FuzzRig rig(kSeats);
  std::vector<std::string> known_bookings;   // 真 id 池（混入垃圾 id 一并打）
  std::vector<std::string> known_tickets;
  std::vector<std::string> used_rids;
  uint64_t rid_counter = 0;

  const int kinds_n = 8;
  const int times[] = {-30, -15, 0, 7, 15, 16, 30, 120, 240, 480, 945, 960, 1000};

  for (int it = 0; it < iterations; ++it) {
    const int roll = static_cast<int>(rng() % 100);
    if (roll < 25) {                       // tick：随机推进时钟（含 0）
      static const int64_t steps[] = {0, 60000, 300000, 900000,
                                      3600000, 86400000LL};
      rig.clock->advance(steps[rng() % 5]);
      try {
        rig.engine->tick();
      } catch (const std::exception& e) {
        fail("tick threw at it=" + std::to_string(it) + ": " + e.what());
        break;
      }
    } else {                               // 请求：随机 kind + 非法入参
      IncomingRequest req;
      Event& ev = req.ev;
      switch (rng() % kinds_n) {
        case 0: case 1: req.kind = "reserve"; break;
        case 2: req.kind = "cancel"; break;
        case 3: req.kind = "checkin"; break;
        case 4: req.kind = "checkout"; break;
        case 5: req.kind = "claim_ticket"; break;
        case 6: req.kind = "quit"; break;              // 语义外但合法帧
        default: req.kind = "k" + std::to_string(rng() % 5); break;  // 未知 kind
      }
      ev.user_id = static_cast<int64_t>(rng() % 8) - 1;   // 含 -1
      ev.seat_id = static_cast<int32_t>(rng() % (kSeats + 5)) - 2;
      ev.day_off = static_cast<int32_t>(rng() % 18) - 2;
      ev.start_min = times[rng() % (sizeof(times) / sizeof(int))];
      ev.end_min = times[rng() % (sizeof(times) / sizeof(int))];
      req.leave = (rng() % 4) == 0;
      req.enqueue_if_conflict = (rng() % 3) == 0;
      const int rid_roll = static_cast<int>(rng() % 10);
      if (rid_roll < 4) {
        ev.request_id = "r" + std::to_string(++rid_counter);
        used_rids.push_back(ev.request_id);
      } else if (rid_roll < 7 && !used_rids.empty()) {
        ev.request_id = used_rids[rng() % used_rids.size()];  // 乱序重复请求
      } else if (rid_roll == 7) {
        ev.request_id = std::string(300, 'x');                // 超长字符串
      }  // 其余留空
      if (rng() % 20 == 0) ev.trace_id = std::string(200, 't');
      if (!known_bookings.empty() && rng() % 2)
        ev.booking_id = known_bookings[rng() % known_bookings.size()];
      else if (rng() % 4 == 0)
        ev.booking_id = (rng() % 2) ? "B999999" : std::string(500, 'z');
      if (!known_tickets.empty() && rng() % 2)
        ev.queue_ticket = known_tickets[rng() % known_tickets.size()];
      else if (rng() % 6 == 0)
        ev.queue_ticket = "T999999";

      try {
        const Outcome o = rig.engine->dispatch(req);
        if (o.ok) {
          if (!(o.booking_id.size() > 0 &&
                (o.detail.empty() || o.detail.rfind("credit=", 0) == 0 ||
                 o.detail == "grant_expired" ||
                 o.detail == "E_IDEMPOTENT_REPLAY")))
            fail("it=" + std::to_string(it) + " ok 但形态非法 detail=" + o.detail);
          if (!o.booking_id.empty() && o.booking_id[0] == 'B' &&
              (known_bookings.empty() ||
               known_bookings.back() != o.booking_id))
            known_bookings.push_back(o.booking_id);
        } else {
          if (o.shed && o.detail != "E_SHED")
            fail("it=" + std::to_string(it) + " shed 标记但 detail=" + o.detail);
          if (!o.shed && !legal_reject_detail(o.detail))
            fail("it=" + std::to_string(it) + " 非法错误码: " + o.detail);
          if (o.booking_id[0] == 'G' && !o.ok)
            ;  // 授权单拒绝同样合法
        }
        if (o.ok && o.booking_id[0] == 'G') {
          bool seen = false;
          for (const auto& t : known_tickets)
            if (t == ev.queue_ticket) seen = true;
          if (!seen) known_tickets.push_back(ev.queue_ticket);
        }
      } catch (const std::exception& e) {
        fail("dispatch threw at it=" + std::to_string(it) +
             " kind=" + req.kind + ": " + e.what());
        break;
      }
    }

    std::string why;
    if (!rig.engine->check_invariant_i1(&why))
      fail("it=" + std::to_string(it) + " I1 违例: " + why);
    if (failures) break;
    if (it % 250 == 249) {   // 快照往返：fuzz 状态下 serialize/restore 全等
      try {
        const std::string blob = rig.engine->serialize_state();
        rig.shadow->restore_state(blob);
        if (rig.shadow->serialize_state() != blob)
          fail("it=" + std::to_string(it) + " 快照往返不等");
        if (!rig.shadow->check_invariant_i1(&why))
          fail("it=" + std::to_string(it) + " 恢复态 I1 违例: " + why);
      } catch (const std::exception& e) {
        fail("snapshot threw at it=" + std::to_string(it) + ": " + e.what());
      }
    }
  }

  std::cout << "FUZZ seed=" << seed << " iterations=" << iterations
            << " failed=" << failures << "\n";
  std::cout << (failures ? "RESULT: FAIL\n" : "RESULT: ALL PASS\n");
  return failures ? 1 : 0;
}
