"""审计器：订阅引擎事件流维护影子位图，断言不变量 I1。

I1：任何成功 RESERVE 事件的 mask 与"施加前"影子位图必须无重叠。
    （引擎契约：预检冲突拒绝不产生事件，事件流里的每个 RESERVE 都是成功预约。）
附带检查 SEAT_DRIFT：广播的 SEAT_CHANGED 位图必须与影子位图一致
    （影子由 RESERVE/CANCEL/NO_SHOW/CHECKOUT 精确维护，漂移 = 副本正确性破坏）。

violation > 0 → 非零退出。抓不到坏人的审计器不算审计器：
  --selftest 伪造"重叠双预约"事件流，验证审计器必抓（I1 >= 1 才算 PASS）。

运行：
  python3 sre/audit.py --selftest     自检（必须 PASS 才能用于验收）
  python3 sre/audit.py                对 MockEngine 真实事件流审计一轮（violation==0）
"""
from __future__ import annotations

import argparse
import os
import sys
import threading

try:
    from gwpath import add_gateway
except ImportError:  # python3 -m sre.audit
    from sre.gwpath import add_gateway  # type: ignore

add_gateway()

from contracts import Event, ReserveRequest  # noqa: E402
from mock_engine import MockEngine  # noqa: E402


class Auditor:
    """事件流影子审计器。poll_and_audit() 返回本轮新发现 violation 数。"""

    def __init__(self, stream) -> None:
        self._stream = stream
        self._shadow: dict[tuple[int, int], int] = {}   # (day_off, seat) -> uint64
        self._bookings: dict[str, tuple[int, int, int]] = {}  # bid -> (day, seat, mask)
        self.violations: list[tuple[str, str]] = []     # (kind, detail)
        self.gaps: list[str] = []                        # 注册表缺失（审计窗口外起点）
        self.seen: dict[str, int] = {}

    # ---- 事件处理 ----

    def poll_and_audit(self, timeout_s: float = 0.0) -> int:
        """消费一批事件并审计，返回本轮新增 violation 数。"""
        n0 = len(self.violations)
        for ev in self._stream.poll(timeout_s):
            self.on_event(ev)
        return len(self.violations) - n0

    def on_event(self, ev: Event) -> int:
        """审计单条事件，返回该事件引发的 violation 数。"""
        self.seen[ev.type] = self.seen.get(ev.type, 0) + 1
        before = len(self.violations)
        if ev.type == "RESERVE":
            key = (ev.day_off, ev.seat_id)
            cur = self._shadow.get(key, 0)
            if ev.mask & cur:  # I1：施加前位图必须与请求 mask 无重叠
                self.violations.append((
                    "I1_DOUBLE_ASSIGN",
                    f"seq={ev.seq} booking={ev.booking_id} seat={ev.seat_id} "
                    f"day={ev.day_off} mask=0x{ev.mask:x} shadow=0x{cur:x} "
                    f"overlap=0x{ev.mask & cur:x}"))
            self._shadow[key] = cur | ev.mask
            self._bookings[ev.booking_id] = (ev.day_off, ev.seat_id, ev.mask)
        elif ev.type == "SEAT_CHANGED":
            key = (ev.day_off, ev.seat_id)
            expect = self._shadow.get(key, 0)
            if ev.mask != expect:
                self.violations.append((
                    "SEAT_DRIFT",
                    f"seq={ev.seq} seat={ev.seat_id} day={ev.day_off} "
                    f"broadcast=0x{ev.mask:x} shadow=0x{expect:x}"))
        elif ev.type in ("CANCEL", "NO_SHOW"):
            self._release(ev.booking_id, full=True)
        elif ev.type == "CHECKOUT":
            self._release(ev.booking_id, mask=ev.released_mask)
        # DONE/GRANTED/BREAKAWAY/...：不改位图，计数即可
        return len(self.violations) - before

    # ---- 内部 ----

    def _release(self, booking_id: str, full: bool = False,
                 mask: int = 0) -> None:
        reg = self._bookings.get(booking_id)
        if reg is None:
            if full:
                self.gaps.append(f"booking={booking_id} 不在注册表（审计窗口外开始）")
            return
        day, seat, bmask = reg
        clear = bmask if full else mask
        # 超发检测：释放片必须 ⊆ 原预约 mask；越界释放 = 释放了别人的座位
        if clear & ~bmask:
            self.violations.append((
                "OVER_RELEASE",
                f"seq 处 booking={booking_id} 释放 0x{clear:x} ⊄ 预约 0x{bmask:x}"))
        key = (day, seat)
        self._shadow[key] = self._shadow.get(key, 0) & ~clear
        if full:
            del self._bookings[booking_id]

    # ---- 只读视图 ----

    def shadow_bits(self, day_off: int, seat_id: int) -> int:
        return self._shadow.get((day_off, seat_id), 0)

    @property
    def violation_count(self) -> int:
        return len(self.violations)


class _FakeStream:
    """--selftest 用：可控事件序列（伪造重叠双预约）。"""

    def __init__(self, events: list[Event]) -> None:
        self._events = list(events)

    def poll(self, timeout_s: float = 0.0) -> list[Event]:
        out, self._events = list(self._events), []
        return out

    def last_seq(self) -> int:
        return self._events[-1].seq if self._events else 0


def _ev(seq: int, type_: str, **kw) -> Event:
    return Event(seq=seq, ts_ms=1000 + seq, type=type_, **kw)


def selftest() -> int:
    """注入用例：重叠双预约必须被抓（violation>0），干净流必须零误报。"""
    ok = True

    # 用例 A：合法预约 → 无 violation
    a = Auditor(_FakeStream([
        _ev(1, "RESERVE", booking_id="B1", user_id=1, seat_id=1, day_off=1,
            mask=0b11),
        _ev(1, "SEAT_CHANGED", seat_id=1, day_off=1, mask=0b11),
        _ev(2, "RESERVE", booking_id="B2", user_id=2, seat_id=1, day_off=1,
            mask=0b1100),
        _ev(2, "SEAT_CHANGED", seat_id=1, day_off=1, mask=0b1111),
    ]))
    a.poll_and_audit()
    n_a = a.violation_count
    print(f"[selftest A] 合法连续预约（无重叠）: violations={n_a} "
          f"{'PASS' if n_a == 0 else 'FAIL（审计器误报）'}")
    ok &= n_a == 0

    # 用例 B：伪造重叠双预约 → 审计器必须抓到（I1 >= 1）
    b = Auditor(_FakeStream([
        _ev(1, "RESERVE", booking_id="B1", user_id=1, seat_id=2, day_off=1,
            mask=0b1111),
        _ev(1, "SEAT_CHANGED", seat_id=2, day_off=1, mask=0b1111),
        _ev(2, "RESERVE", booking_id="B2", user_id=2, seat_id=2, day_off=1,
            mask=0b11),  # 与 B1 的低 2 片重叠 = 超卖
        _ev(2, "SEAT_CHANGED", seat_id=2, day_off=1, mask=0b1111),
    ]))
    b.poll_and_audit()
    i1 = sum(1 for k, _ in b.violations if k == "I1_DOUBLE_ASSIGN")
    print(f"[selftest B] 伪造重叠双预约: I1 violations={i1} "
          f"{'PASS（审计器抓到注入）' if i1 >= 1 else 'FAIL（漏检！）'}")
    if i1 >= 1:
        print(f"             首条证据: {b.violations[0][1]}")
    ok &= i1 >= 1

    # 用例 C：取消释放后影子位图回落，广播一致 → 零 violation
    c = Auditor(_FakeStream([
        _ev(1, "RESERVE", booking_id="B1", user_id=1, seat_id=3, day_off=0,
            mask=0b1111),
        _ev(1, "SEAT_CHANGED", seat_id=3, day_off=0, mask=0b1111),
        _ev(2, "CANCEL", booking_id="B1", user_id=1),
        _ev(2, "SEAT_CHANGED", seat_id=3, day_off=0, mask=0),
    ]))
    c.poll_and_audit()
    n_c = c.violation_count
    print(f"[selftest C] 取消释放 + 广播一致: violations={n_c} "
          f"{'PASS' if n_c == 0 else 'FAIL'}")
    ok &= n_c == 0

    # 用例 D：广播位图漂移 → SEAT_DRIFT 必须被抓
    d = Auditor(_FakeStream([
        _ev(1, "RESERVE", booking_id="B1", user_id=1, seat_id=4, day_off=0,
            mask=0b11),
        _ev(1, "SEAT_CHANGED", seat_id=4, day_off=0, mask=0b111),  # 多报 1 片
    ]))
    d.poll_and_audit()
    drift = sum(1 for k, _ in d.violations if k == "SEAT_DRIFT")
    print(f"[selftest D] 广播漂移注入: SEAT_DRIFT={drift} "
          f"{'PASS' if drift >= 1 else 'FAIL'}")
    ok &= drift >= 1

    print(f"AUDIT_SELFTEST: {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


def run_live_audit(threads: int = 8, ops: int = 40) -> int:
    """对 MockEngine 真实并发事件流做一轮审计（预期 violations==0）。"""
    eng = MockEngine(n_seats=16)
    hot_user = [100000 + i for i in range(threads * ops)]

    def worker(tid: int) -> None:
        for j in range(ops):
            n = tid * ops + j
            eng.reserve(ReserveRequest(f"rid-{n}", hot_user[n], seat_id=3,
                                       day_off=1, start_min=600, end_min=720))
            eng.seat_bits(3, 1)

    ts = [threading.Thread(target=worker, args=(t,)) for t in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()

    aud = Auditor(eng.stream)
    drained = 0
    while True:
        got = aud.poll_and_audit(timeout_s=0.05)
        drained += got
        # poll 返回空且再等一轮仍为空 → 流已排干
        if got == 0 and not eng.stream.poll(0.0):
            break
    print(f"[live] 并发 {threads} 线程 × {ops} op，事件计数={aud.seen}，"
          f"gaps={len(aud.gaps)}")
    for k, d in aud.violations[:5]:
        print(f"  violation[{k}]: {d}")
    verdict = aud.violation_count == 0
    print(f"AUDIT_RESULT: {'PASS' if verdict else 'FAIL'} "
          f"(violations={aud.violation_count})")
    return 0 if verdict else 1


def main() -> int:
    ap = argparse.ArgumentParser(description="SeatMesh 影子位图审计器（I1）")
    ap.add_argument("--selftest", action="store_true",
                    help="注入重叠双预约事件，验证审计器抓得到")
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--ops", type=int, default=40)
    args = ap.parse_args()
    if args.selftest:
        return selftest()
    return run_live_audit(args.threads, args.ops)


if __name__ == "__main__":
    sys.exit(main())
