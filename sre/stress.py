"""并发压测：90% 查询 + 10% 并发抢同一座位，断言 0 超卖。

负载打在 gateway 侧 MockEngine（引擎契约的进程内实现，语义与 C++ 参考实现一致）。
"成功"以**响应**计数：拒绝（E_CONFLICT/E_QUOTA/E_PARAM）走预检不产生事件，
事件流里只有成功 RESERVE——所以成功数必须数响应，不能数事件。

断言（两条都必须成立）：
  1. 成功响应数 == 1        （同一座位同一时段，全并发只有一人拿到）
  2. double_assign == 0     （审计器影子位图核对事件流：无任何重叠施加）

末行输出 OVERSELL_CHECK: PASS/FAIL；FAIL 以非零码退出。

运行：python3 sre/stress.py [--quick] [--threads N] [--ops N]
"""
from __future__ import annotations

import argparse
import os
import random
import sys
import threading
import time

try:
    from gwpath import add_gateway
except ImportError:  # python3 -m sre.stress
    from sre.gwpath import add_gateway  # type: ignore

add_gateway()

from contracts import ReserveRequest  # noqa: E402
from mock_engine import MockEngine  # noqa: E402

try:
    from audit import Auditor
except ImportError:  # python3 -m sre.stress
    from sre.audit import Auditor  # type: ignore

# 抢座热点：同一座位、同一天、同一 8 片窗口（day_off=1 明天，避开"过去时段"拒绝）
HOT_SEAT = 3
HOT_DAY = 1
HOT_START = 600   # 16:00（距 06:00 的分钟）
HOT_END = 720     # 18:00，8 片


def run(threads: int, ops_per_thread: int, n_seats: int = 64,
        seed: int = 20260923) -> dict:
    """执行压测，返回结果字典（含断言结论）。"""
    eng = MockEngine(n_seats=n_seats)
    rng = random.Random(seed)
    lock = threading.Lock()
    stats = {
        "ops": 0, "queries": 0, "reserve_attempts": 0,
        "reserve_ok": 0, "rejects": {}, "errors": [],
    }
    n_reserve_total = max(threads * ops_per_thread // 10, threads)

    def worker(tid: int) -> None:
        local_q = local_r = local_o = 0
        rej: dict[str, int] = {}
        for j in range(ops_per_thread):
            n = tid * ops_per_thread + j
            # 全局配额：恰好 n_reserve_total 次抢座尝试，其余为查询（90/10）
            with lock:
                is_reserve = stats["reserve_attempts"] < n_reserve_total
                if is_reserve:
                    stats["reserve_attempts"] += 1
            if not is_reserve:
                seat = rng.randrange(n_seats)
                day = rng.randrange(3)
                eng.seat_bits(seat, day)
                local_q += 1
                continue
            out = eng.reserve(ReserveRequest(
                request_id=f"rid-{n}",           # 唯一幂等键 → 无重放干扰
                user_id=500000 + n,              # 唯一用户 → 触发 E_QUOTA 之外纯竞争
                seat_id=HOT_SEAT, day_off=HOT_DAY,
                start_min=HOT_START, end_min=HOT_END,
                trace_id=f"stress-{n:06d}"))
            local_r += 1
            if out.ok:
                local_o += 1
            else:
                rej[out.detail] = rej.get(out.detail, 0) + 1
            if out.ok and out.detail == "E_IDEMPOTENT_REPLAY":
                with lock:
                    stats["errors"].append(f"seq={n} 意外幂等重放")
        with lock:
            stats["ops"] += ops_per_thread
            stats["queries"] += local_q
            stats["reserve_ok"] += local_o
            for k, v in rej.items():
                stats["rejects"][k] = stats["rejects"].get(k, 0) + v

    t0 = time.perf_counter()
    ts = [threading.Thread(target=worker, args=(t,)) for t in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    elapsed = time.perf_counter() - t0

    # double_assign == 0：审计器影子位图核对全部事件流（I1 + SEAT_DRIFT）
    aud = Auditor(eng.stream)
    while True:
        if aud.poll_and_audit(timeout_s=0.05) == 0 and not eng.stream.poll(0.0):
            break

    # 直接状态核对：热点座位最终位图必须恰好 = 单个成功者的 8 片窗口
    n_slots = (HOT_END - HOT_START) // 15
    mask_expect = ((1 << n_slots) - 1) << (HOT_START // 15)
    hot_bits = eng.seat_bits(HOT_SEAT, HOT_DAY)
    stats.update({
        "threads": threads, "ops_per_thread": ops_per_thread,
        "elapsed_s": round(elapsed, 3),
        "ops_per_sec": round(stats["ops"] / elapsed, 1) if elapsed else 0.0,
        "double_assign": aud.violation_count,
        "hot_bits": hot_bits, "hot_bits_expect": mask_expect,
    })
    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description="SeatMesh 并发压测（0 超卖断言）")
    ap.add_argument("--quick", action="store_true", help="快速模式（CI 用）")
    ap.add_argument("--threads", type=int, default=0)
    ap.add_argument("--ops", type=int, default=0, help="每线程操作数")
    args = ap.parse_args()

    threads = args.threads or (8 if args.quick else 32)
    ops = args.ops or (50 if args.quick else 250)

    s = run(threads, ops)
    print(f"[stress] threads={s['threads']} ops/thread={s['ops_per_thread']} "
          f"total={s['ops']} ({s['ops_per_sec']} ops/s, {s['elapsed_s']}s)")
    print(f"[stress] 查询={s['queries']} ({s['queries'] / max(s['ops'], 1):.0%}) "
          f"抢座尝试={s['reserve_attempts']} 成功响应={s['reserve_ok']} "
          f"拒绝={s['rejects']}")
    print(f"[stress] 热点座位 seat={HOT_SEAT} day={HOT_DAY} "
          f"bits=0x{s['hot_bits']:x} expect=0x{s['hot_bits_expect']:x}")
    for k, d in (s["errors"][:3]):
        print(f"[stress] ERROR: {k}")

    ok_resp = s["reserve_ok"] == 1
    no_double = s["double_assign"] == 0
    bits_ok = s["hot_bits"] == s["hot_bits_expect"]
    print(f"[stress] 断言 成功响应==1: {'PASS' if ok_resp else 'FAIL'}; "
          f"double_assign==0: {'PASS' if no_double else 'FAIL'} "
          f"(violations={s['double_assign']}); "
          f"最终位图一致: {'PASS' if bits_ok else 'FAIL'}")
    verdict = ok_resp and no_double and bits_ok and not s["errors"]
    print(f"OVERSELL_CHECK: {'PASS' if verdict else 'FAIL'}")
    return 0 if verdict else 1


if __name__ == "__main__":
    sys.exit(main())
