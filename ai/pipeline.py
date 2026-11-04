"""数据管道：事件流 → SQLite events_raw → v_hourly 视图 → 造数器。

表结构：
  events_raw(ts_ms, seq, type, user_id, seat_id, day_off, mask, detail,
             booking_id, released_mask)
    mask 以字符串存储（64 位大整数十进制），语义与 contracts.Event 一致；
    day 为营业日绝对编号（UNIX 日 + 06:00 锚点，见 business_day()）。
  seat_slot(day, seat_id, slot)          -- 投影：流末态占用的 (营业日,座位,片)
  v_hourly(day, hour, occupied_slots, occupancy)
    hour = 片时序号 0..15（0 = 06:00）；occupancy = 该小时被占(座,片)实例数 / 64
    （64 = 单座日片数，按任务包口径作归一基准；多座系统该值可 > 1，
      趋势/比值用于训练，forecast 内部再按容量归一到 0..1）。

造数器 generate_synthetic：高峰小时 λ = 低谷 × 8 的泊松到达（random.expovariate
互达时间采样），行为分布 85% 预约 / 10% 开始前取消 / 5% 超时爽约；
同种子两次运行结果逐行一致。

运行：python3 ai/pipeline.py --selftest
"""
from __future__ import annotations

import argparse
import datetime as _dt
import os
import random
import sqlite3
import sys
import tempfile
from typing import Iterable, Optional

SLOTS_PER_DAY = 64
SLOT_MS = 15 * 60 * 1000
DAY_MS = 86_400_000
DAY_START_MS = 6 * 3_600_000            # 营业日锚点：当日 06:00
BUSINESS_HOURS = 16                      # 06:00–22:00 = 16 小时
BASE_DAY = (_dt.date(2026, 3, 2) - _dt.date(1970, 1, 1)).days  # 造数锚点（周一）
DEFAULT_SEED = 20260302

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events_raw(
  ts_ms      INTEGER NOT NULL,
  seq        INTEGER NOT NULL,
  type       TEXT    NOT NULL,
  user_id    INTEGER NOT NULL DEFAULT 0,
  seat_id    INTEGER NOT NULL DEFAULT -1,
  day_off    INTEGER NOT NULL DEFAULT -1,
  mask       TEXT    NOT NULL DEFAULT '0',
  detail     TEXT    NOT NULL DEFAULT '',
  booking_id  TEXT    NOT NULL DEFAULT '',
  released_mask TEXT  NOT NULL DEFAULT '0'
);
CREATE INDEX IF NOT EXISTS ix_events_seq ON events_raw(seq);
CREATE INDEX IF NOT EXISTS ix_events_type_user ON events_raw(type, user_id);
CREATE TABLE IF NOT EXISTS seat_slot(
  day     INTEGER NOT NULL,
  seat_id INTEGER NOT NULL,
  slot    INTEGER NOT NULL,
  PRIMARY KEY(day, seat_id, slot)
);
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
CREATE VIEW IF NOT EXISTS v_hourly AS
  SELECT day,
         slot / 4 AS hour,
         COUNT(*)     AS occupied_slots,
         COUNT(*) / 64.0 AS occupancy
  FROM seat_slot
  GROUP BY day, slot / 4;
"""


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.executescript(_SCHEMA)
    conn.commit()
    return conn


def business_day(ts_ms: int) -> int:
    """UNIX 时间 → 营业日绝对编号（[06:00, 次日 06:00) 属同一营业日）。"""
    return (ts_ms - DAY_START_MS) // DAY_MS


def day_to_date(day: int) -> _dt.date:
    return _dt.date(1970, 1, 1) + _dt.timedelta(days=day)


def weekday_of(day: int) -> int:
    return day_to_date(day).weekday()          # 0=周一


# ---------- 事件入库 ----------

def _insert_events(conn: sqlite3.Connection, events: Iterable) -> int:
    rows = [(ev.ts_ms, ev.seq, ev.type, ev.user_id, ev.seat_id, ev.day_off,
             str(ev.mask), ev.detail, ev.booking_id,
             str(getattr(ev, "released_mask", 0))) for ev in events]
    conn.executemany(
        "INSERT INTO events_raw(ts_ms, seq, type, user_id, seat_id, day_off,"
        " mask, detail, booking_id, released_mask)"
        " VALUES(?,?,?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    return len(rows)


def load_from_engine(stream, db, batch: int = 500,
                     drain_rounds: int = 3) -> int:
    """把引擎事件流（contracts.EventStream：poll/last_seq）批量入库。

    stream 持续 poll 直到连续 drain_rounds 轮为空；db 接受路径或连接。
    返回入库事件数。结束后重建 seat_slot 投影。
    """
    conn = db if isinstance(db, sqlite3.Connection) else connect(db)
    total = 0
    empty = 0
    while empty < drain_rounds:
        events = stream.poll(timeout_s=0.1)
        if not events:
            empty += 1
            continue
        empty = 0
        for i in range(0, len(events), batch):
            total += _insert_events(conn, events[i:i + batch])
    rebuild_projection(conn)
    return total


# ---------- 投影：流末态 → seat_slot + v_hourly ----------

def rebuild_projection(conn: sqlite3.Connection) -> None:
    """按 seq 重放 events_raw，重建 seat_slot（末态占用）与 meta。"""
    bits: dict[tuple[int, int], int] = {}          # (day, seat) -> uint64
    bookings: dict[str, tuple[int, int, int]] = {}  # bid -> (day, seat, mask)
    cur = conn.execute(
        "SELECT seq, type, seat_id, day_off, mask, booking_id, ts_ms,"
        " released_mask FROM events_raw ORDER BY seq")
    for seq, type_, seat_id, day_off, mask_s, bid, ts_ms, released_s in cur:
        mask = int(mask_s)
        if type_ == "RESERVE":
            day = business_day(ts_ms) + max(day_off, 0)
            key = (day, seat_id)
            bits[key] = bits.get(key, 0) | mask
            bookings[bid] = (day, seat_id, mask)
        elif type_ in ("CANCEL", "NO_SHOW"):
            reg = bookings.pop(bid, None)
            if reg is None:                     # 窗口外事件：无法定位，跳过
                continue
            day, seat, bmask = reg
            bits[(day, seat)] = bits.get((day, seat), 0) & ~bmask
        elif type_ == "CHECKOUT":
            reg = bookings.get(bid)
            if reg is None:
                continue
            day, seat, _ = reg
            bits[(day, seat)] = bits.get((day, seat), 0) & ~int(released_s)
    conn.execute("DELETE FROM seat_slot")
    rows = [(day, seat, slot)
            for (day, seat), m in bits.items()
            for slot in range(SLOTS_PER_DAY) if (m >> slot) & 1]
    conn.executemany("INSERT OR IGNORE INTO seat_slot VALUES(?,?,?)", rows)
    conn.commit()


def set_meta(conn: sqlite3.Connection, k: str, v: str) -> None:
    conn.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (k, v))
    conn.commit()


def get_meta(conn: sqlite3.Connection, k: str) -> Optional[str]:
    row = conn.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
    return row[0] if row else None


# ---------- 造数器 ----------

def _poisson_count(rng: random.Random, lam: float) -> int:
    """泊松到达计数：expovariate 互达时间累加，越过 1.0 停。"""
    if lam <= 0:
        return 0
    t = 0.0
    n = 0
    while True:
        t += rng.expovariate(lam)
        if t >= 1.0:
            return n
        n += 1


def generate_synthetic(days: int, db_path: str,
                       peak_hours: tuple[int, ...] = (19, 20, 21),
                       daily_users: int = 2000, seats: int = 512,
                       seed: int = DEFAULT_SEED,
                       keep_cancel_share: float = 0.10,
                       noshow_share: float = 0.05) -> dict:
    """造数：days 个营业日的泊松到达 + 85/10/5 行为分布，写入 db_path。

    高峰小时（wall-clock，默认 19/20/21 点）λ = 低谷 × 8；
    每次到达 = 一条 RESERVE（冲突即丢弃，同真实系统预检拒绝不产生事件），
    之后 10% 补一条开始前 CANCEL、5% 补一条超时 NO_SHOW（信用流水量）。
    同 seed 重跑逐行一致（rng 与时间锚点全部确定）。
    """
    if os.path.exists(db_path):
        os.remove(db_path)
    for suffix in ("-wal", "-shm"):
        if os.path.exists(db_path + suffix):
            os.remove(db_path + suffix)
    conn = connect(db_path)
    rng = random.Random(seed)

    peak_idx = sorted({h - 6 for h in peak_hours if 6 <= h < 22})
    n_peak = len(peak_idx)
    valley_rate = daily_users / (BUSINESS_HOURS + 7 * n_peak)
    peak_rate = valley_rate * 8.0

    stats = {"attempts": 0, "reserves": 0, "rejected_conflict": 0,
             "cancels": 0, "no_shows": 0}
    occupied: dict[tuple[int, int], int] = {}   # (day, seat) -> 当前占用
    bookings: dict[str, tuple[int, int, int]] = {}
    seq = 0
    buf: list[tuple] = []

    def emit(type_: str, ts_ms: int, user_id: int, seat_id: int, day_off: int,
             mask: int, booking_id: str, detail: str = "synthetic") -> None:
        nonlocal seq
        seq += 1
        buf.append((ts_ms, seq, type_, user_id, seat_id, day_off, str(mask),
                    detail, booking_id))
        if len(buf) >= 1000:
            _insert_events(conn, [_Row(*r) for r in buf])
            buf.clear()

    for d in range(BASE_DAY, BASE_DAY + days):
        day_start = d * DAY_MS + DAY_START_MS
        day = business_day(day_start)
        for h in range(BUSINESS_HOURS):
            lam = peak_rate if h in peak_idx else valley_rate
            for _ in range(_poisson_count(rng, lam)):
                stats["attempts"] += 1
                slot0 = h * 4 + rng.randrange(4)
                max_len = min(8, SLOTS_PER_DAY - slot0)
                if max_len < 2:
                    continue
                length = rng.randint(2, max_len)
                mask = ((1 << length) - 1) << slot0
                start_ts = day_start + slot0 * SLOT_MS
                arrive_ts = max(start_ts - rng.randint(30_000, 1_800_000),
                                day_start)
                user_id = rng.randrange(100000, 100000 + daily_users)
                seat_id = rng.randrange(seats)
                key = (day, seat_id)
                cur_bits = occupied.get(key, 0)
                if mask & cur_bits:              # 预检冲突：无事件（与引擎一致）
                    stats["rejected_conflict"] += 1
                    continue
                stats["reserves"] += 1
                bid = f"S{seq + 1}"
                emit("RESERVE", arrive_ts, user_id, seat_id, 0, mask, bid)
                occupied[key] = cur_bits | mask
                bookings[bid] = (day, seat_id, mask)
                roll = rng.random()
                if roll < keep_cancel_share:      # 10% 开始前取消
                    cts = arrive_ts + max((start_ts - arrive_ts) // 2, 1)
                    emit("CANCEL", cts, user_id, -1, -1, 0, bid)
                    occupied[key] = occupied.get(key, 0) & ~mask
                    bookings.pop(bid, None)
                    stats["cancels"] += 1
                elif roll < keep_cancel_share + noshow_share:   # 5% 超时爽约
                    emit("NO_SHOW", start_ts + 30 * 60 * 1000, user_id, -1,
                         -1, 0, bid)
                    occupied[key] = occupied.get(key, 0) & ~mask
                    bookings.pop(bid, None)
                    stats["no_shows"] += 1
    if buf:
        _insert_events(conn, [_Row(*r) for r in buf])
    set_meta(conn, "seats", str(seats))
    set_meta(conn, "seed", str(seed))
    set_meta(conn, "days", str(days))
    rebuild_projection(conn)
    stats["days"] = days
    stats["db"] = db_path
    conn.close()
    return stats


class _Row:
    """极简事件载体（避免循环依赖 gateway/contracts，字段名与 Event 一致）。"""

    __slots__ = ("ts_ms", "seq", "type", "user_id", "seat_id", "day_off",
                 "mask", "detail", "booking_id")

    def __init__(self, ts_ms, seq, type_, user_id, seat_id, day_off, mask,
                 detail, booking_id) -> None:
        self.ts_ms, self.seq, self.type = ts_ms, seq, type_
        self.user_id, self.seat_id, self.day_off = user_id, seat_id, day_off
        self.mask, self.detail, self.booking_id = int(mask), detail, booking_id


# ---------- 查询辅助 ----------

def hourly(conn: sqlite3.Connection) -> list[tuple[int, int, int, float]]:
    return list(conn.execute(
        "SELECT day, hour, occupied_slots, occupancy FROM v_hourly "
        "ORDER BY day, hour"))


def peak_valley_ratio(conn: sqlite3.Connection,
                      peak_hours: tuple[int, ...] = (19, 20, 21)) -> float:
    """高峰小时平均 occupancy / 低谷小时平均 occupancy（全天数平均）。"""
    peak_idx = {h - 6 for h in peak_hours}
    peak_sum = peak_n = valley_sum = valley_n = 0.0
    for _, hour, _, occ in hourly(conn):
        if hour in peak_idx:
            peak_sum += occ
            peak_n += 1
        else:
            valley_sum += occ
            valley_n += 1
    valley = valley_sum / valley_n if valley_n else 0.0
    if valley <= 0:
        return float("inf") if peak_sum > 0 else 0.0
    return (peak_sum / peak_n) / valley


# ---------- 自检 ----------

def selftest() -> int:
    ok = True
    tmp = tempfile.mkdtemp(prefix="sm_pipeline_")
    db1 = os.path.join(tmp, "run1.db")
    db2 = os.path.join(tmp, "run2.db")

    # 用例 A：同种子两次运行 → 逐行一致
    s1 = generate_synthetic(3, db1, daily_users=2000, seed=777)
    s2 = generate_synthetic(3, db2, daily_users=2000, seed=777)
    c1, c2 = connect(db1), connect(db2)
    dump1 = list(c1.execute("SELECT * FROM events_raw ORDER BY seq"))
    dump2 = list(c2.execute("SELECT * FROM events_raw ORDER BY seq"))
    same = dump1 == dump2 and dump1
    print(f"[selftest A] 同种子一致性: rows={len(dump1)} "
          f"{'PASS' if same else 'FAIL'}")
    ok &= bool(same)

    # 用例 B：高峰/低谷占用比 > 2
    ratio = peak_valley_ratio(c1)
    print(f"[selftest B] 高峰/低谷占用比 = {ratio:.2f} （要求 > 2） "
          f"{'PASS' if ratio > 2 else 'FAIL'}")
    ok &= ratio > 2

    # 用例 C：行为分布 85/10/5（容差 ±3pp）
    total_beh = s1["cancels"] + s1["no_shows"] + (
        s1["reserves"] - s1["cancels"] - s1["no_shows"])
    share_cancel = s1["cancels"] / total_beh
    share_no = s1["no_shows"] / total_beh
    b_ok = abs(share_cancel - 0.10) <= 0.03 and abs(share_no - 0.05) <= 0.03
    print(f"[selftest C] 行为分布: 预约={1 - share_cancel - share_no:.0%} "
          f"取消={share_cancel:.1%} 爽约={share_no:.1%} "
          f"(目标 85/10/5) {'PASS' if b_ok else 'FAIL'}")
    ok &= b_ok

    # 用例 D：造数统计与视图可读、mask 均为字符串存储
    stored = list(c1.execute("SELECT DISTINCT typeof(mask) FROM events_raw"))
    masks_text = stored == [("text",)]
    print(f"[selftest D] mask 字符串存储: {stored} "
          f"{'PASS' if masks_text else 'FAIL'}; "
          f"造数={s1}")
    ok &= masks_text

    c1.close()
    c2.close()
    print(f"PIPELINE_SELFTEST: {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description="SeatMesh 数据管道")
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--db", default="")
    ap.add_argument("--daily-users", type=int, default=2000)
    args = ap.parse_args()
    if args.selftest:
        return selftest()
    db = args.db or os.path.join(tempfile.gettempdir(), "seatmesh_synthetic.db")
    stats = generate_synthetic(args.days, db, daily_users=args.daily_users)
    conn = connect(db)
    ratio = peak_valley_ratio(conn)
    print(f"[pipeline] db={db} stats={stats} 高峰/低谷比={ratio:.2f}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
