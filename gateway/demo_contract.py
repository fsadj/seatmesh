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

    print("RESULT: ALL PASS")


if __name__ == "__main__":
    main()
