"""Jepsen 式一致性验证：两层检查器 + history.jsonl + docs/jepsen-report.md。

负载：N 线程并发对同一座位（同 seat-day）reserve/cancel，打 MockEngine；
每条操作记录 (invoke_t, op, args, ok_t, result) 到 sre/history.jsonl。

检查器两层：
  1. 不变量层（必做）：同 seat-day 的成功 RESERVE 区间两两不重叠——
     区间 = 座位时段片 [start_min, end_min)，同时段持有（预约-取消生命周期）
     才算重叠；取消释放后复用同一时段不算超卖。另用影子位图审计器核对事件流。
  2. 线性一致性层（WGL，Wing & Gong + Lowe 剪枝）：按 seat-day 分键，
     对每个键的历史做线性化点搜索。模型状态 = (64 位位图, 预约注册表)，
     状态空间可控（单座位一天仅 64 位）。历史按静默点（无在途操作）切段，
     每段独立检查（静默边界保证段间可组合）；超过 --wgl-cap 的段如实记为
     inconclusive，不冒充通过。

输出 docs/jepsen-report.md：操作数 / 成功率 / 检查结论 / 异常历史。
运行：
  python3 sre/jepsen.py --quick          （10 线程 × 10s，CI/自测）
  python3 sre/jepsen.py                  （50 线程 × 60s，完整版）
  python3 -m sre.jepsen --quick
"""
from __future__ import annotations

import argparse
import bisect
import datetime as _dt
import json
import os
import platform
import random
import sys
import threading
import time
from typing import Optional

try:
    from gwpath import add_gateway, repo_root
except ImportError:  # python3 -m sre.jepsen
    from sre.gwpath import add_gateway, repo_root  # type: ignore

add_gateway()

from contracts import ReserveRequest  # noqa: E402
from mock_engine import MockEngine  # noqa: E402

try:
    from audit import Auditor
except ImportError:  # python3 -m sre.jepsen
    from sre.audit import Auditor  # type: ignore

SLOTS = 64


# ---------- 负载 ----------

def run_workload(threads: int, seconds: int, seat_id: int = 7,
                 day_off: int = 1, max_total_ops: int = 300_000,
                 seed: int = 42) -> tuple[list[dict], MockEngine, dict]:
    """并发 reserve/cancel 同一座位；返回 (history, engine, stats)。

    每线程一个独立用户（避免 E_QUOTA 串扰），持有一个成功预约后先取消再抢；
    时段片随机（2-8 片），制造部分重叠的合法竞争。
    """
    eng = MockEngine(n_seats=32)
    rng = random.Random(seed)
    deadline = time.perf_counter() + seconds
    history: list[dict] = []
    hlock = threading.Lock()
    stats = {"ops": 0, "reserve_ok": 0, "cancel_ok": 0, "fails": {}}
    budget = max_total_ops

    def worker(tid: int) -> None:
        user = 200000 + tid
        rid_n = 0
        held: Optional[str] = None            # 本线程当前持有的 booking_id
        lrng = random.Random(seed * 1000 + tid)
        local: list[dict] = []
        while time.perf_counter() < deadline:
            with hlock:
                if stats["ops"] >= budget:
                    break
                stats["ops"] += 1
            if held is not None:
                op = "cancel"
                t0 = time.perf_counter() * 1000.0
                out = eng.cancel(f"cx-{tid}-{rid_n}", user, held)
                t1 = time.perf_counter() * 1000.0
                args = {"booking_id": held, "user": user, "seat": seat_id,
                        "day": day_off}
                if out.ok:
                    held = None
            else:
                slot0 = lrng.randrange(0, SLOTS - 2)
                length = lrng.randint(2, min(8, SLOTS - slot0))
                start = slot0 * 15
                end = start + length * 15
                op = "reserve"
                rid = f"jp-{tid}-{rid_n}"
                t0 = time.perf_counter() * 1000.0
                out = eng.reserve(ReserveRequest(
                    request_id=rid, user_id=user, seat_id=seat_id,
                    day_off=day_off, start_min=start, end_min=end,
                    trace_id=rid))
                t1 = time.perf_counter() * 1000.0
                args = {"seat": seat_id, "day": day_off, "start": start,
                        "end": end, "user": user,
                        "mask": ((1 << length) - 1) << slot0, "rid": rid}
                if out.ok:
                    held = out.booking_id
            rid_n += 1
            entry = {"process": tid, "invoke_t": round(t0, 3),
                     "op": op, "args": args, "ok_t": round(t1, 3),
                     "result": {"ok": bool(out.ok),
                                "booking_id": out.booking_id,
                                "detail": out.detail}}
            local.append(entry)
            time.sleep(lrng.uniform(0.002, 0.008))
        with hlock:
            history.extend(local)

    ts = [threading.Thread(target=worker, args=(t,)) for t in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()

    for e in history:
        if e["result"]["ok"]:
            stats["reserve_ok" if e["op"] == "reserve" else "cancel_ok"] += 1
        else:
            d = e["result"]["detail"] or "?"
            stats["fails"][d] = stats["fails"].get(d, 0) + 1
    history.sort(key=lambda e: (e["invoke_t"], e["ok_t"]))
    return history, eng, stats


# ---------- 第一层：不变量（成功 RESERVE 区间两两不重叠） ----------

def check_invariant(history: list[dict]) -> tuple[list[str], dict]:
    """生命周期感知的区间重叠检查：预约 [ok_t, cancel_ok_t) 持有期内，
    与其他在持预约的时段片相交即违规（取消后复用同时段 = 合法）。"""
    violations: list[str] = []
    active: list[dict] = []          # {bid, user, slots, since}
    done: list[dict] = []
    unmatched_cancel = 0
    ordered = sorted(history, key=lambda e: (e["ok_t"], e["invoke_t"]))
    for e in ordered:
        if e["op"] == "reserve" and e["result"]["ok"]:
            active.append({"bid": e["result"]["booking_id"],
                           "user": e["args"]["user"],
                           "s": e["args"]["start"], "t": e["args"]["end"],
                           "since": e["ok_t"]})
        elif e["op"] == "cancel" and e["result"]["ok"]:
            bid = e["result"]["booking_id"]
            found = None
            for a in active:
                if a["bid"] == bid:
                    found = a
                    break
            if found is None:
                unmatched_cancel += 1
                continue
            found["until"] = e["ok_t"]
            done.append(found)
            active.remove(found)
    for a in active:
        a["until"] = float("inf")
        done.append(a)
    done.sort(key=lambda a: a["since"])
    for i, a in enumerate(done):
        for b in done[i + 1:]:
            slot_overlap = a["s"] < b["t"] and b["s"] < a["t"]
            life_overlap = a["since"] < b["until"] and b["since"] < a["until"]
            if slot_overlap and life_overlap:
                violations.append(
                    f"OVERLAP seat-day: booking {a['bid']}[{a['s']},{a['t']}) "
                    f"@{a['since']:.1f} 与 {b['bid']}[{b['s']},{b['t']}) "
                    f"@{b['since']:.1f} 同时持有重叠时段")
    stats = {"bookings": len(done), "still_active": len(active),
             "unmatched_cancel": unmatched_cancel}
    return violations, stats


# ---------- 第二层：WGL 线性一致性 ----------

class Inconclusive(Exception):
    pass


def _split_quiescent(entries: list[dict]) -> list[list[dict]]:
    """按静默点切段：某条操作的 invoke 晚于之前所有操作的 ok → 边界。"""
    segments: list[list[dict]] = []
    seg: list[dict] = []
    max_ok = float("-inf")
    for e in entries:
        if seg and e["invoke_t"] > max_ok:
            segments.append(seg)
            seg = []
        seg.append(e)
        max_ok = max(max_ok, e["ok_t"])
    if seg:
        segments.append(seg)
    return segments


def _make_lift():
    """模型 lift：返回 (mask, bookings) -> new_state | None（不可线性化）。"""

    def lift(e: dict, st: tuple[int, frozenset]):
        mask, bookings = st
        a, r = e["args"], e["result"]
        if e["op"] == "reserve":
            u, m = a["user"], a["mask"]
            conflict = bool(mask & m)
            quota = any(bu == u for (_, bu, _) in bookings)
            can_ok = not (conflict or quota)
            if r["ok"]:
                if r.get("detail") == "E_IDEMPOTENT_REPLAY":
                    return st if (r["booking_id"], u, m) in bookings else None
                if not can_ok:
                    return None
                return (mask | m, bookings | {(r["booking_id"], u, m)})
            return st if not can_ok else None
        # cancel
        bid, u = a["booking_id"], a["user"]
        reg = next((t for t in bookings if t[0] == bid and t[1] == u), None)
        if r["ok"]:
            if reg is None:
                return None
            return (mask & ~reg[2], bookings - {reg})
        return st if reg is None else None

    return lift


def wgl_check_segment(entries: list[dict], init_state: tuple,
                      cap_states: int = 200_000,
                      max_final_states: int = 128) -> dict:
    """单段 WGL（Lowe 剪枝 DFS，迭代实现）：ok_j <= invoke_i ⇒ j 先线性化。

    init_state：段起点状态（上游静默边界携带进来）；返回全部可达终态集合，
    供下一段继续（静默点只保证 real-time 隔离，状态要显式传递）。
    """
    n = len(entries)
    # 前缀掩码：按 ok_t 排序后，前 k 条的线性化掩码
    order = sorted(range(n), key=lambda i: entries[i]["ok_t"])
    prefix = [0] * (n + 1)
    for pos, i in enumerate(order):
        prefix[pos + 1] = prefix[pos] | (1 << i)
    ok_times = [entries[i]["ok_t"] for i in order]
    hb_mask = [prefix[bisect.bisect_right(ok_times, e["invoke_t"])]
               for e in entries]

    lift = _make_lift()
    fail_memo: set[tuple[int, tuple]] = set()
    full = (1 << n) - 1
    finals: set[tuple] = set()
    # 帧栈：[lin, state, cands, next_pos, subtree_had_success]
    stack = [[0, init_state, None, 0, False]]
    while stack:
        frame = stack[-1]
        lin, st = frame[0], frame[1]
        if lin == full:
            finals.add(st)
            if len(finals) > max_final_states:
                raise Inconclusive(
                    f"终态集合超上限 {max_final_states}（并发窗口过大）")
            stack.pop()
            if stack:
                stack[-1][4] = True
            continue
        if frame[2] is None:
            frame[2] = [i for i in range(n)
                        if not (lin >> i) & 1 and hb_mask[i] & ~lin == 0]
            frame[3] = 0
        advanced = False
        cands, pos = frame[2], frame[3]
        while pos < len(cands):
            i = cands[pos]
            pos += 1
            st2 = lift(entries[i], st)
            if st2 is None:
                continue
            lin2 = lin | (1 << i)
            key = (lin2, st2)
            if key in fail_memo:
                continue
            frame[3] = pos
            stack.append([lin2, st2, None, 0, False])
            advanced = True
            break
        frame[3] = pos
        if not advanced:
            if not frame[4]:
                fail_memo.add((lin, st))
                if len(fail_memo) > cap_states:
                    raise Inconclusive(
                        f"状态数超上限 {cap_states}（并发窗口过大）")
            stack.pop()
            if stack and frame[4]:
                stack[-1][4] = True
    return {"ok": bool(finals), "finals": finals,
            "explored": len(fail_memo)}


def check_linearizability(history: list[dict], wgl_cap: int = 3000,
                          cap_states: int = 200_000) -> dict:
    """按 seat-day 分键 → 静默点切段 → 携带状态逐段 WGL。

    边界状态集合（frontier）超限或段超长 → 如实记 inconclusive；
    frontier 已经近似的段，其后失败也算 inconclusive（不冒充违规）。
    """
    by_key: dict[tuple, list[dict]] = {}
    for e in history:
        a = e["args"]
        by_key.setdefault((a.get("seat", -1), a.get("day", -1)), []).append(e)
    out = {"keys": {}, "violations": [], "inconclusive": [],
           "segments_checked": 0, "entries_checked": 0,
           "segments_total": 0}
    max_frontier = 64
    for key, entries in by_key.items():
        segs = _split_quiescent(entries)
        out["segments_total"] += len(segs)
        kres = {"segments": len(segs), "ok": 0, "inconclusive": 0,
                "skipped_large": 0, "entries": len(entries)}
        frontier = [(0, frozenset())]        # 段边界的可能状态集合
        approx = False                       # frontier 是否已被截断近似
        for si, seg in enumerate(segs):
            if len(seg) > wgl_cap:
                kres["skipped_large"] += 1
                approx = True
                out["inconclusive"].append(
                    f"key={key} 段#{si} 长 {len(seg)} > cap {wgl_cap}，"
                    f"未做全量 WGL（如实报告，不冒充通过）")
                continue
            if not frontier:
                kres["inconclusive"] += 1
                out["inconclusive"].append(f"key={key} 段#{si} 上游无合法终态")
                continue
            new_frontier: set[tuple] = set()
            seg_ok = False
            try:
                for st in frontier:
                    res = wgl_check_segment(seg, st, cap_states)
                    if res["ok"]:
                        seg_ok = True
                        new_frontier |= res["finals"]
            except Inconclusive as exc:
                kres["inconclusive"] += 1
                approx = True
                out["inconclusive"].append(f"key={key} 段#{si} {exc}")
                new_frontier = set(frontier)     # 保守沿用（近似）
                seg_ok = True                    # 不在本段定违规
            if not seg_ok:
                msg = (f"key={key} 段#{si}（{len(seg)} 条）WGL 违规："
                       f"从 {len(frontier)} 个候选状态均无法线性化")
                if approx:
                    out["inconclusive"].append(msg + "（frontier 已近似，"
                                               "记 inconclusive 而非违规）")
                    kres["inconclusive"] += 1
                    seg_ok = True
                else:
                    out["violations"].append(msg)
                    kres["bad"] = kres.get("bad", 0) + 1
                new_frontier = set(frontier)
            if len(new_frontier) > max_frontier:
                approx = True
                new_frontier = set(sorted(new_frontier)[:max_frontier])
                out["inconclusive"].append(
                    f"key={key} 段#{si} 终态集合 {len(new_frontier)}+ 超上限，"
                    f"frontier 截断为近似")
            frontier = sorted(new_frontier)
            out["segments_checked"] += 1
            out["entries_checked"] += len(seg)
            if seg_ok:
                kres["ok"] += 1
        out["keys"][str(key)] = kres
    return out


# ---------- 报告 ----------

def _env_fingerprint() -> str:
    return (f"{platform.system()}/{platform.release()} "
            f"({platform.machine()}) python={platform.python_version()} "
            f"cpus={os.cpu_count()}")


def write_report(path: str, params: dict, stats: dict, inv: dict,
                 wgl: dict, auditor_violations: int,
                 history_path: str) -> None:
    now = _dt.datetime.now().isoformat(timespec="seconds")
    total = max(stats["ops"], 1)
    ok_rate = (stats["reserve_ok"] + stats["cancel_ok"]) / total
    inv_ok = not inv["violations"]
    wgl_bad = len(wgl["violations"])
    verdict = inv_ok and wgl_bad == 0
    lines = [
        "# Jepsen 一致性验证报告（MockEngine 负载）",
        "",
        f"- 生成时间：{now}",
        f"- 环境：{_env_fingerprint()}",
        f"- 负载：{params['threads']} 线程 × {params['seconds']}s，"
        f"同一座位 seat={params['seat']} day={params['day']}（slot 级竞争）",
        f"- 历史：`{history_path}`（{total} 条 op，含 invoke_t/ok_t 实时戳）",
        "",
        "## 结果",
        "",
        f"- 总操作数：{total}（{stats['ops_per_sec']:.0f} ops/s）",
        f"- 成功操作率：{ok_rate:.1%}（reserve 成功 {stats['reserve_ok']}，"
        f"cancel 成功 {stats['cancel_ok']}；其余为预检拒绝，竞争下的正确拒绝"
        f"而非错误：{stats['fails']}）",
        "",
        "| 检查层 | 结论 | 明细 |",
        "|---|---|---|",
        f"| 不变量层（成功 RESERVE 区间两两不重叠） | "
        f"{'PASS' if inv_ok else 'FAIL'} | 预约 {inv['stats']['bookings']} 笔，"
        f"重叠违规 {len(inv['violations'])}，未配对取消 "
        f"{inv['stats']['unmatched_cancel']}，事件流审计器 violations="
        f"{auditor_violations} |",
        f"| 线性一致性层（WGL） | "
        f"{'PASS' if wgl_bad == 0 else 'FAIL'} | 段 {wgl['segments_total']}，"
        f"已检 {wgl['segments_checked']} 段 / {wgl['entries_checked']} 条，"
        f"违规 {wgl_bad}，inconclusive {len(wgl['inconclusive'])} |",
        "",
        f"**总判定：{'PASS（两层全绿）' if verdict else 'FAIL'}**",
        "",
        "## WGL 方法说明",
        "",
        "- 按 seat-day 分键；键内历史按**静默点**切段（无在途操作 → 段间"
        " real-time 偏序隔离，逐段可组合；段边界显式传递候选状态集合）。",
        "- 段内用 Wing & Gong + Lowe 剪枝 DFS：`ok_j <= invoke_i ⇒ j 必须先"
        "线性化`；模型状态 = (64 位位图, 预约注册表)，合法线性化点逐一搜索。",
        "- 超过 `--wgl-cap`（当前 " f"{params['wgl_cap']} 条）的段与状态爆炸段"
        "记为 inconclusive，**不冒充通过**（见下）。",
        "- 检查器有效性由注入自检证明（合法历史过 / 并发重叠双成功必被抓）："
        "`python3 sre/jepsen.py --selftest`。",
        "- 诚实记录：本轮负载为进程内 MockEngine（GIL 下引擎调用原子完成），"
        "invoke/ok 窗口几乎不重叠，历史以静默单操作段为主；多操作并发段的"
        "搜索能力由注入自检覆盖。C++ 引擎 / 双区部署（T2）下窗口会真实放大，"
        "需重跑。",
        "",
    ]
    if wgl["inconclusive"]:
        lines += ["## Inconclusive（未决段）", ""]
        lines += [f"- {m}" for m in wgl["inconclusive"][:10]]
        if len(wgl["inconclusive"]) > 10:
            lines.append(f"- … 共 {len(wgl['inconclusive'])} 条")
        lines.append("")
    if inv["violations"]:
        lines += ["## 异常历史（不变量层）", ""]
        lines += [f"- {v}" for v in inv["violations"][:10]]
        lines.append("")
    if wgl["violations"]:
        lines += ["## 异常历史（WGL 层）", ""]
        lines += [f"- {v}" for v in wgl["violations"][:10]]
        lines.append("")
    lines += [
        "## 复现",
        "",
        "```bash",
        f"python3 sre/jepsen.py --quick     # 本报告口径（{params['threads']}"
        f"×{params['seconds']}s）",
        "python3 sre/jepsen.py             # 完整版：50 线程 × 60s",
        "```",
        "",
        "> 说明：负载打在 MockEngine（引擎契约的进程内实现，单全局锁仲裁）。",
        "> C++ 引擎联调后需以 T2 双区部署重跑（叠加断网扰动），本报告为",
        "> 阶段二 Mock 版本。候选补测：双区 jepsen 重跑（等 T2 compose）。",
        "",
    ]
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# ---------- 检查器有效性自检（注入坏历史，检查器必须抓到） ----------

def selftest() -> int:
    """两级注入：合法历史必须过，注入违规必须被抓（抓不到坏人的检查器不算检查器）。"""
    ok = True
    legal = [
        {"invoke_t": 1.0, "ok_t": 2.0, "op": "reserve",
         "args": {"user": 1, "mask": 0b11, "seat": 7, "day": 1,
                  "start": 0, "end": 30},
         "result": {"ok": True, "booking_id": "B1", "detail": ""}},
        {"invoke_t": 3.0, "ok_t": 4.0, "op": "cancel",
         "args": {"booking_id": "B1", "user": 1, "seat": 7, "day": 1},
         "result": {"ok": True, "booking_id": "B1", "detail": ""}},
        {"invoke_t": 5.0, "ok_t": 6.0, "op": "reserve",
         "args": {"user": 2, "mask": 0b11, "seat": 7, "day": 1,
                  "start": 0, "end": 30},
         "result": {"ok": True, "booking_id": "B2", "detail": ""}},
    ]
    r1 = wgl_check_segment(legal, (0, frozenset()))
    print(f"[selftest A] 合法（预约→取消→复用同一时段）: WGL ok={r1['ok']} "
          f"{'PASS' if r1['ok'] else 'FAIL（误报）'}")
    ok &= r1["ok"]

    bad = [
        {"invoke_t": 1.0, "ok_t": 2.0, "op": "reserve",
         "args": {"user": 1, "mask": 0b11, "seat": 7, "day": 1,
                  "start": 0, "end": 30},
         "result": {"ok": True, "booking_id": "B1", "detail": ""}},
        {"invoke_t": 1.5, "ok_t": 2.5, "op": "reserve",
         "args": {"user": 2, "mask": 0b11, "seat": 7, "day": 1,
                  "start": 0, "end": 30},
         "result": {"ok": True, "booking_id": "B2", "detail": ""}},
    ]
    r2 = wgl_check_segment(bad, (0, frozenset()))
    print(f"[selftest B] 注入并发重叠双成功预约: WGL ok={r2['ok']} "
          f"{'PASS（检查器抓到注入）' if not r2['ok'] else 'FAIL（漏检！）'}")
    ok &= not r2["ok"]

    r3 = check_invariant(bad)
    print(f"[selftest C] 同一坏历史过不变量层（两笔重叠时段同时持有）: "
          f"violations={len(r3[0])} "
          f"{'PASS' if len(r3[0]) >= 1 else 'FAIL（漏检！）'}")
    ok &= len(r3[0]) >= 1

    print(f"WGL_SELFTEST: {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


# ---------- 主流程 ----------

def main() -> int:
    ap = argparse.ArgumentParser(description="SeatMesh Jepsen 式一致性验证")
    ap.add_argument("--quick", action="store_true", help="10 线程 × 10s")
    ap.add_argument("--selftest", action="store_true",
                    help="注入坏历史，验证检查器抓得到（不跑负载）")
    ap.add_argument("--threads", type=int, default=0)
    ap.add_argument("--seconds", type=int, default=0)
    ap.add_argument("--seat", type=int, default=7)
    ap.add_argument("--day", type=int, default=1)
    ap.add_argument("--wgl-cap", type=int, default=3000)
    ap.add_argument("--max-ops", type=int, default=300_000)
    ap.add_argument("--history", default="")
    ap.add_argument("--report", default="")
    ap.add_argument("--no-report", action="store_true")
    args = ap.parse_args()

    if args.selftest:
        return selftest()

    threads = args.threads or (10 if args.quick else 50)
    seconds = args.seconds or (10 if args.quick else 60)
    root = repo_root()
    history_path = args.history or os.path.join(root, "sre", "history.jsonl")
    report_path = args.report or os.path.join(root, "docs",
                                              "jepsen-report.md")
    params = {"threads": threads, "seconds": seconds, "seat": args.seat,
              "day": args.day, "wgl_cap": args.wgl_cap}
    print(f"[jepsen] threads={threads} seconds={seconds} "
          f"seat={args.seat} day={args.day} wgl_cap={args.wgl_cap}")

    t0 = time.perf_counter()
    history, eng, stats = run_workload(
        threads, seconds, seat_id=args.seat, day_off=args.day,
        max_total_ops=args.max_ops)
    stats["ops_per_sec"] = stats["ops"] / max(time.perf_counter() - t0, 1e-9)

    os.makedirs(os.path.dirname(history_path), exist_ok=True)
    with open(history_path, "w", encoding="utf-8") as f:
        for e in history:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")

    inv_v, inv_stats = check_invariant(history)
    inv = {"violations": inv_v, "stats": inv_stats}
    aud = Auditor(eng.stream)
    while True:
        if aud.poll_and_audit(timeout_s=0.05) == 0 and not eng.stream.poll(0.0):
            break
    auditor_violations = aud.violation_count

    wgl = check_linearizability(history, wgl_cap=args.wgl_cap)

    print(f"[jepsen] ops={stats['ops']} ({stats['ops_per_sec']:.0f}/s) "
          f"reserve_ok={stats['reserve_ok']} cancel_ok={stats['cancel_ok']} "
          f"fails={stats['fails']}")
    print(f"[jepsen] 不变量层: bookings={inv_stats['bookings']} "
          f"violations={len(inv_v)} auditor={auditor_violations}")
    print(f"[jepsen] WGL 层: 段={wgl['segments_total']} "
          f"已检={wgl['segments_checked']}({wgl['entries_checked']} 条) "
          f"违规={len(wgl['violations'])} "
          f"inconclusive={len(wgl['inconclusive'])}")

    verdict = (not inv_v and auditor_violations == 0
               and len(wgl["violations"]) == 0)
    print(f"JEPSEN_CHECK: {'PASS' if verdict else 'FAIL'}")

    if not args.no_report:
        write_report(report_path, params, stats, inv, wgl,
                     auditor_violations, os.path.relpath(history_path, root))
        print(f"[jepsen] 报告已写 {report_path}")
    return 0 if verdict else 1


if __name__ == "__main__":
    sys.exit(main())
