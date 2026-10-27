"""MockEngine：契约的进程内实现（C++ 引擎就绪前的全组开发替身）。

语义与 engine/src/engine.cpp 参考实现一致：
  预检冲突快速失败；仲裁即 apply（此处 = 持锁后的最终判定）；拒绝不产生日志事件；
  ID 确定性（B<seq>）；入馆刷卡窗 30min（当日 08:00 前申请保留至 08:30）；取消仅开始前；
  提前签退释放剩余片。
线程安全：1000 线程并发抢 1 座恰好 1 人成功（demo_contract.py 验证）。

TODO(M4)：C++ 引擎联调通过后，生产路径切换 ZmqEngineClient，本类仅留测试。
"""
from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Callable, Optional

from contracts import (ENTRY_GRACE_MS, DAY_START_MIN, ROLLING_DAYS, Event,
                       Outcome, ReserveRequest, mask_for)

DAY_MS = 86_400_000


def _now_ms() -> int:
    return int(time.time() * 1000)


def _infer_day0(now: int) -> int:
    """与 engine.cpp infer_day0 一致：UTC 当日 00:00 + 06:00（营业开始）。"""
    return (now // DAY_MS) * DAY_MS + DAY_START_MIN * 60_000


@dataclass
class _Booking:
    id: str
    user_id: int
    seat_id: int
    day_off: int
    mask: int
    state: str            # PENDING_ENTRY/IN_LIBRARY/BREAKAWAY/LEFT/NO_SHOW/CANCELLED/GRANTED
    start_ms: int
    deadline_ms: int
    seq: int


class MockEventStream:
    """contracts.EventStream 的内存实现。"""

    def __init__(self) -> None:
        self._events: deque[Event] = deque()
        self._cv = threading.Condition()
        self._last_seq = 0

    def feed(self, ev: Event) -> None:
        with self._cv:
            self._events.append(ev)
            self._last_seq = max(self._last_seq, ev.seq)
            self._cv.notify_all()

    def poll(self, timeout_s: float = 0.0) -> list[Event]:
        with self._cv:
            if not self._events and timeout_s > 0:
                self._cv.wait(timeout_s)
            out, self._events = list(self._events), deque()
            return out

    def last_seq(self) -> int:
        with self._cv:
            return self._last_seq


class MockEngine:
    """contracts.EngineClient 的进程内实现 + 内置事件流。"""

    def __init__(self, n_seats: int = 200,
                 clock: Optional[Callable[[], int]] = None):
        self._clock = clock or _now_ms
        self._n_seats = n_seats
        self._lock = threading.Lock()
        self._bits: dict[tuple[int, int], int] = {}   # (day_off, seat) → uint64
        self._bookings: dict[str, _Booking] = {}
        self._idem: dict[str, str] = {}               # request_id → booking_id
        self._seq = 0
        self._day0 = _infer_day0(self._clock())
        self.stream = MockEventStream()

    # ---- EngineClient ----

    def seat_bits(self, seat_id: int, day_off: int) -> int:
        with self._lock:
            return self._bits.get((day_off, seat_id), 0)

    def reserve(self, req: ReserveRequest) -> Outcome:
        mask = mask_for(req.start_min, req.end_min)
        if (mask is None or not 0 <= req.seat_id < self._n_seats
                or not 0 <= req.day_off < ROLLING_DAYS):
            return Outcome(False, detail="E_PARAM")
        with self._lock:
            if req.request_id and req.request_id in self._idem:
                bid = self._idem[req.request_id]
                return Outcome(bool(bid), bid, detail="E_IDEMPOTENT_REPLAY")
            cur = self._bits.get((req.day_off, req.seat_id), 0)
            conflict = cur & mask
            if conflict:  # 预检拒绝：不产生事件（与 C++ 预检一致）
                return Outcome(False, conflict_mask=conflict, detail="E_CONFLICT")
            self._seq += 1
            bid = f"B{self._seq}"
            self._bits[(req.day_off, req.seat_id)] = cur | mask
            now = self._clock()
            start_ms = (self._day0 + req.day_off * DAY_MS
                        + req.start_min * 60_000)
            # 入馆资格截止（对齐馆方规则）：当日 08:00 前申请保留至 08:30；
            # 其余 = max(申请时刻, 时段开始) + 30min
            if req.day_off == 0 and now < self._day0 + 2 * 3_600_000:
                deadline = self._day0 + (2 * 60 + 30) * 60_000
            else:
                deadline = max(now, start_ms) + ENTRY_GRACE_MS
            self._bookings[bid] = _Booking(
                id=bid, user_id=req.user_id, seat_id=req.seat_id,
                day_off=req.day_off, mask=mask, state="PENDING_ENTRY",
                start_ms=start_ms, deadline_ms=deadline,
                seq=self._seq)
            if req.request_id:
                self._idem[req.request_id] = bid
            self._publish("RESERVE", req.user_id, booking_id=bid,
                          seat_id=req.seat_id, day_off=req.day_off,
                          mask=mask, trace_id=req.trace_id)
            self._publish_seat_changed(req.day_off, req.seat_id)
            return Outcome(True, bid)

    def cancel(self, request_id: str, user_id: int, booking_id: str) -> Outcome:
        with self._lock:
            b = self._bookings.get(booking_id)
            if b is None:
                return Outcome(False, detail="E_NOT_FOUND")
            if b.state != "PENDING_ENTRY" or self._clock() >= b.start_ms:
                return Outcome(False, booking_id, detail="E_WINDOW")
            # TODO(M5, P2)：取消后 30min 冷却（CANCEL_COOLDOWN_MS，网关前置校验）
            self._seq += 1
            self._apply_bits(b.day_off, b.seat_id, b.mask, set=False)
            b.state = "CANCELLED"  # 信用 0（控制面结算）
            self._publish("CANCEL", b.user_id, booking_id=b.id)
            self._publish_seat_changed(b.day_off, b.seat_id)
            return Outcome(True, b.id)

    def checkin(self, request_id: str, user_id: int, booking_id: str) -> Outcome:
        with self._lock:
            b = self._bookings.get(booking_id)
            now = self._clock()
            if b is None:
                return Outcome(False, detail="E_NOT_FOUND")
            if b.state != "PENDING_ENTRY" or not b.start_ms <= now <= b.deadline_ms:
                return Outcome(False, booking_id, detail="E_WINDOW")
            self._seq += 1
            b.state = "IN_LIBRARY"
            self._publish("CHECKIN", b.user_id, booking_id=b.id)
            return Outcome(True, b.id)

    def checkout(self, request_id: str, user_id: int, booking_id: str) -> Outcome:
        with self._lock:
            b = self._bookings.get(booking_id)
            if b is None:
                return Outcome(False, detail="E_NOT_FOUND")
            if b.state != "IN_LIBRARY":
                # TODO(M1, P2)：BREAKAWAY（中途离开保留期）路径
                return Outcome(False, booking_id, detail="E_WINDOW")
            self._seq += 1
            consumed = self._consumed_slots(b, self._clock())
            released = b.mask & ~(((1 << consumed) - 1) if consumed < 64
                                  else (1 << 64) - 1)
            self._apply_bits(b.day_off, b.seat_id, released, set=False)
            b.state = "LEFT"  # released==0 → 按时完成 +1；提前离馆 0
            self._publish("CHECKOUT", b.user_id, booking_id=b.id,
                          released_mask=released)
            self._publish_seat_changed(b.day_off, b.seat_id)
            return Outcome(True, b.id)

    # ---- 超时（测试/演示用；生产由 C++ 时间轮驱动） ----

    def expire_due(self) -> int:
        """将所有超时未入馆的 PENDING_ENTRY 单置为 NO_SHOW，返回处理条数。"""
        n = 0
        with self._lock:
            now = self._clock()
            for b in list(self._bookings.values()):
                if b.state == "PENDING_ENTRY" and now > b.deadline_ms:
                    self._seq += 1
                    self._apply_bits(b.day_off, b.seat_id, b.mask, set=False)
                    b.state = "NO_SHOW"  # 超时未入馆：信用 -10 + 当日禁约（网关硬规则）
                    self._publish("EXPIRE", b.user_id, booking_id=b.id)
                    self._publish_seat_changed(b.day_off, b.seat_id)
                    n += 1
        return n

    # ---- 内部 ----

    def _apply_bits(self, day_off: int, seat_id: int, mask: int,
                    set: bool) -> None:  # noqa: A002（与位语义对齐）
        key = (day_off, seat_id)
        cur = self._bits.get(key, 0)
        self._bits[key] = (cur | mask) if set else (cur & ~mask)

    def _publish(self, type_: str, user_id: int, **kw) -> None:
        self.stream.feed(Event(seq=self._seq, ts_ms=self._clock(),
                               type=type_, user_id=user_id, **kw))

    def _publish_seat_changed(self, day_off: int, seat_id: int) -> None:
        """proto SeatChanged：变更后的完整位图（副本整字覆盖，docs/03 §4）。"""
        self.stream.feed(Event(seq=self._seq, ts_ms=self._clock(),
                               type="SEAT_CHANGED", seat_id=seat_id,
                               day_off=day_off,
                               mask=self._bits.get((day_off, seat_id), 0)))

    def _consumed_slots(self, b: _Booking, now_ms: int) -> int:
        """签退时点已消耗的时间片数（含当前片的剩余，立即释放给候补）。"""
        day_anchor = self._day0 + b.day_off * DAY_MS  # 该营业日的 07:00
        m = (now_ms - day_anchor) // 60_000           # 距 07:00 的分钟
        if m <= 0:
            return 0
        total = 64 * 15
        return total if m >= total else (int(m) + 14) // 15
