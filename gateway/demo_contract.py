"""契约演示：MockEngine 全语义走查 + 并发 0 超卖 + 副本视图 + 信用幂等。
运行：python3 gateway/demo_contract.py
（M4/M5/M6 在 C++ 引擎联调前，用 MockEngine 开发全部上层逻辑。）
"""
from __future__ import annotations

import os
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from contracts import ReplicaView, ReserveRequest  # noqa: E402
from mock_engine import MockEngine  # noqa: E402
from stores import InMemoryCreditStore  # noqa: E402


def main() -> None:
    # ---- 场景一：单线程语义走查 ----
    eng = MockEngine(n_seats=10)
    o1 = eng.reserve(ReserveRequest("r1", 1001, 3, 1, 120, 240, trace_id="t" * 16))
    assert o1.ok and o1.booking_id == "B1", o1

    o2 = eng.reserve(ReserveRequest("r2", 1002, 3, 1, 180, 300))
    assert not o2.ok and o2.conflict_mask == 0xF000 and o2.detail == "E_CONFLICT", o2

    o3 = eng.reserve(ReserveRequest("r3", 1002, 5, 1, 120, 240))
    assert o3.ok and o3.booking_id == "B2", o3

    o1b = eng.reserve(ReserveRequest("r1", 1001, 3, 1, 120, 240))
    assert o1b.detail == "E_IDEMPOTENT_REPLAY" and o1b.booking_id == "B1", o1b

    bad = eng.reserve(ReserveRequest("r4", 1001, 3, 1, 123, 240))  # 非 15 倍数
    assert not bad.ok and bad.detail == "E_PARAM", bad

    cancel = eng.cancel("r5", 1001, o1.booking_id)  # 明天开始 → 可取消
    assert cancel.ok, cancel

    # ---- 场景二：100 线程并发抢 1 座 → 恰好 1 人成功（0 超卖） ----
    eng2 = MockEngine(n_seats=10)
    results: list = []
    lock = threading.Lock()

    def worker(i: int) -> None:
        r = eng2.reserve(ReserveRequest(f"c{i}", 2000 + i, 0, 1, 120, 240))
        with lock:
            results.append(r)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(100)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    ok_count = sum(1 for r in results if r.ok)
    assert ok_count == 1, f"0 超卖被破坏：{ok_count} 人成功"
    assert all(r.conflict_mask != 0 for r in results if not r.ok)
    print(f"[0超卖] 100 并发抢 1 座 → 恰好 {ok_count} 人成功，"
          f"{len(results) - ok_count} 人收到精确冲突位图")

    # ---- 场景三：副本视图（SeatChanged 整字覆盖 + 水位） ----
    replica = ReplicaView()
    events = eng2.stream.poll()
    for ev in events:
        replica.on_event(ev)
    assert replica.bits(0, 1) == eng2.seat_bits(0, 1), "副本与引擎不一致"
    assert replica.last_seq() == eng2.stream.last_seq()
    print(f"[副本] 消费 {len(events)} 条事件后，副本位图与引擎一致 "
          f"(last_seq={replica.last_seq()})")

    # ---- 场景四：信用分幂等结算 ----
    credit = InMemoryCreditStore()
    seq_of_expire = events[0].seq
    assert credit.apply_delta(2000, -10, "BX", seq_of_expire) is True
    assert credit.apply_delta(2000, -10, "BX", seq_of_expire) is False  # 重复事件
    assert credit.balance(2000) == -10
    print("[信用] 同一事件重复消费不重复扣分，余额正确")

    # ---- 场景五：业务演练修订回归（过去时段 / 活跃唯一 / 早鸟保留不死锁） ----
    import time as _time
    now_ms = int(_time.time() * 1000)
    day0 = (now_ms // 86_400_000) * 86_400_000 + 6 * 3_600_000   # 当日 06:00
    eng0 = MockEngine(n_seats=10, clock=lambda: day0 + 3_600_000)  # 固定当日 07:00

    past = eng0.reserve(ReserveRequest("x1", 3001, 1, 0, 0, 45))     # 06:00-06:45 已过
    assert not past.ok and past.detail == "E_PARAM", past

    fut = eng0.reserve(ReserveRequest("x2", 3001, 2, 1, 120, 240))   # 明天 08:00-10:00
    assert fut.ok and fut.booking_id == "B1", fut
    dup = eng0.reserve(ReserveRequest("x3", 3001, 3, 1, 120, 240))   # 已有活跃预约
    assert not dup.ok and dup.detail == "E_QUOTA", dup

    aft = eng0.reserve(ReserveRequest("x4", 3002, 2, 0, 480, 600))   # 他人约今天 14:00-16:00
    assert aft.ok, aft
    b2 = eng0._bookings["B2"]
    # 下午场不适用早鸟保留：截止 = 开始+30min，且永不早于时段开始（死锁修复验证）
    assert b2.deadline_ms == b2.start_ms + 1_800_000 and b2.deadline_ms >= b2.start_ms, \
        "早鸟保留把截止压到时段开始之前（死锁）"

    m = eng0.reserve(ReserveRequest("x5", 3003, 4, 0, 90, 180))      # 今天 07:30-09:00
    assert m.ok, m
    b3 = eng0._bookings["B3"]
    assert b3.start_ms <= b3.deadline_ms <= b3.start_ms + 5_400_000  # 截止=08:30、夹在时段内
    print("[演练] 过去时段拒绝 / 活跃唯一 / 早鸟保留截止=08:30 且不越过时段 — 全部正确")

    print("RESULT: ALL PASS")


if __name__ == "__main__":
    main()
