"""SeatMesh 网关侧全局契约（Python）。

与 engine/include/engine/contracts.hpp 一一对应；两端常量、字段、语义绑定。
修改 = 修改契约：M9 汇总、M1+M4 签字、契约测试回归、epoch+1。

依赖反转规则：网关/控制面/大屏只依赖本文件的 Protocol；
具体实现（ZMQ 版 / Mock 版 / SQLite 版 / 内存版）在各自模块中，装配点在应用入口。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Optional, Protocol, runtime_checkable

# ---------- 常量（与 protos/scheduler.proto 绑定，代码不得另立） ----------
SLOTS_PER_DAY = 64
SLOT_MINUTES = 15
DAY_START_MIN = 7 * 60            # 07:00
ROLLING_DAYS = 14
MAX_SLOTS_PER_REQ = 16
CHECKIN_GRACE_MS = 15 * 60 * 1000  # 签到窗口 = [开始, 开始+15min)
GRANT_TTL_MS = 60 * 1000
SAGA_TIMEOUT_MS = 30 * 1000


def mask_for(start_min: int, end_min: int) -> Optional[int]:
    """[start_min, end_min)（距 07:00 的分钟）→ 位掩码；非法返回 None。"""
    total = SLOTS_PER_DAY * SLOT_MINUTES
    if start_min < 0 or end_min > total or start_min >= end_min:
        return None
    if start_min % SLOT_MINUTES or (end_min - start_min) % SLOT_MINUTES:
        return None
    s, n = start_min // SLOT_MINUTES, (end_min - start_min) // SLOT_MINUTES
    if n > MAX_SLOTS_PER_REQ:
        return None
    return ((1 << n) - 1) << s


def mask_conflict(bits: int, mask: int) -> int:
    """与请求重叠的已占片（前端可精确渲染）。"""
    return bits & mask


# ---------- 数据载体 ----------

@dataclass(frozen=True)
class ReserveRequest:
    request_id: str          # UUIDv4，幂等键
    user_id: int
    seat_id: int
    day_off: int
    start_min: int           # 距 07:00 分钟数，15 的倍数
    end_min: int             # 不含端点
    enqueue_if_conflict: bool = False
    trace_id: str = ""


@dataclass(frozen=True)
class Outcome:
    ok: bool
    booking_id: str = ""
    conflict_mask: int = 0   # 冲突时：与请求重叠的已占片
    detail: str = ""         # E_CONFLICT/E_WINDOW/E_NOT_FOUND/...


@dataclass(frozen=True)
class Event:
    seq: int                 # 全局单调递增；断线按此续传
    ts_ms: int
    type: str                # RESERVE/CANCEL/CHECKIN/CHECKOUT/EXPIRE/...
    booking_id: str = ""
    user_id: int = 0
    seat_id: int = -1
    day_off: int = -1
    mask: int = 0
    released_mask: int = 0
    trace_id: str = ""
    detail: str = ""


# ---------- 契约接口（网关只依赖这些 Protocol） ----------

@runtime_checkable
class EngineClient(Protocol):
    """引擎客户端：写路径 DEALER→ROUTER，读路径走本地副本（见 ReplicaView）。"""

    def reserve(self, req: ReserveRequest) -> Outcome: ...
    def cancel(self, request_id: str, user_id: int, booking_id: str) -> Outcome: ...
    def checkin(self, request_id: str, user_id: int, booking_id: str) -> Outcome: ...
    def checkout(self, request_id: str, user_id: int, booking_id: str) -> Outcome: ...
    def seat_bits(self, seat_id: int, day_off: int) -> int: ...


@runtime_checkable
class EventStream(Protocol):
    """事件流订阅（引擎 PUB → 网关副本/大屏/AI/审计器）。"""

    def poll(self, timeout_s: float = 0.0) -> list[Event]: ...
    def last_seq(self) -> int: ...


@runtime_checkable
class CreditStore(Protocol):
    """信用分存储：以 event_seq 幂等结算（可全量重放）。"""

    def apply_delta(self, user_id: int, delta: int,
                    booking_id: str, event_seq: int) -> bool: ...
    def balance(self, user_id: int) -> int: ...


# ---------- 读路径：网关本地副本（CQRS-lite，查询不打引擎） ----------

class ReplicaView:
    """从事件流重建的位图副本。SeatChanged 整字覆盖，无 delta 合并。

    读己之写：reserve 成功返回的 outcome 携带 applied_seq（C++ 侧）；
    本副本提供 wait_until(min_seq, timeout) 供查询接口保证读己之写。
    """

    def __init__(self) -> None:
        self._bits: dict[tuple[int, int], int] = {}
        self._last_seq = 0
        self._cv = threading.Condition()

    def on_event(self, ev: Event) -> None:
        with self._cv:
            if ev.type == "SEAT_CHANGED":
                self._bits[(ev.seat_id, ev.day_off)] = ev.mask
            if ev.seq > self._last_seq:
                self._last_seq = ev.seq
            self._cv.notify_all()

    def bits(self, seat_id: int, day_off: int) -> int:
        with self._cv:
            return self._bits.get((seat_id, day_off), 0)

    def last_seq(self) -> int:
        with self._cv:
            return self._last_seq

    def wait_until(self, min_seq: int, timeout_s: float = 0.01) -> bool:
        """副本水位追到 min_seq 返回 True；超时返回 False（应答带 staleness）。"""
        with self._cv:
            return self._cv.wait_for(
                lambda: self._last_seq >= min_seq, timeout_s)
