"""预测与风控：分桶均值预测（hour × weekday）+ 规则风控，LightGBM 可选升级。

主模型 forecast(date) -> 64 点 [(slot, 0.0-1.0)]：slot 的占用概率，
内部把 v_hourly 的 occupancy（被占(座,片)数/64）按容量归一到 0..1：
    util(hour) = occupancy * 16 / seats      （seats 来自 meta 表）

LightGBM 路径用可选导入守卫：仅当 (库可导入 且 events_raw >= 500_000 事件)
才启用，接口与分桶完全一致；否则自动降级分桶均值（并在加载时说明原因）。

规则风控 risk(user)：历史爽约(NO_SHOW)>=2 → 高风险。

运行：
  python3 ai/forecast.py --selftest
  python3 ai/forecast.py --db /tmp/seatmesh_synthetic.db --date 2026-03-08
"""
from __future__ import annotations

import argparse
import datetime as _dt
import os
import sqlite3
import sys
import tempfile

try:
    from gwpath import add_ai_path
except ImportError:  # python3 -m ai.forecast
    from ai.gwpath import add_ai_path  # type: ignore

add_ai_path()

import pipeline  # noqa: E402

SLOTS = pipeline.SLOTS_PER_DAY
PEAK_IDX = (13, 14, 15)                    # 19/20/21 点（0 = 06:00）
LIGHTGBM_MIN_EVENTS = 500_000


# ---------- 分桶均值模型（保底，纯标准库） ----------

class BucketModel:
    """hour × weekday 历史均值分桶；缺桶回退 hour 均值，再回退全局均值。"""

    kind = "bucket(hour×weekday)"

    def __init__(self, rows, seats: int) -> None:
        # rows: [(day, hour, occupied_slots, occupancy)]（仅训练集）
        self.seats = max(seats, 1)
        by_wh: dict[tuple[int, int], list[float]] = {}
        by_h: dict[int, list[float]] = {}
        all_occ: list[float] = []
        for day, hour, _, occ in rows:
            by_wh.setdefault((pipeline.weekday_of(day), hour), []).append(occ)
            by_h.setdefault(hour, []).append(occ)
            all_occ.append(occ)
        self._by_wh = {k: sum(v) / len(v) for k, v in by_wh.items()}
        self._by_h = {k: sum(v) / len(v) for k, v in by_h.items()}
        self._global = sum(all_occ) / len(all_occ) if all_occ else 0.0

    def _util(self, weekday: int, hour: int) -> float:
        occ = self._by_wh.get((weekday, hour), self._by_h.get(
            hour, self._global))
        prob = occ * 16.0 / self.seats          # occupancy×64/(seats×4)
        return min(1.0, max(0.0, prob))

    def forecast(self, date: str) -> list[tuple[int, float]]:
        wd = _dt.date.fromisoformat(date).weekday()
        return [(s, round(self._util(wd, s // 4), 4)) for s in range(SLOTS)]


# ---------- LightGBM 升级路径（可选导入守卫） ----------

def _lightgbm_guard(n_events: int):
    """返回 (module, reason)。两者互斥：可用返回模块，不可用返回原因。"""
    if n_events < LIGHTGBM_MIN_EVENTS:
        return None, f"events_raw={n_events} < {LIGHTGBM_MIN_EVENTS}"
    try:
        import lightgbm as lgb  # noqa: PLC0415（守卫式可选导入）
        return lgb, "available"
    except ImportError as e:
        return None, f"lightgbm 未安装（{e.__class__.__name__}）"


class LgbmModel:
    """与 BucketModel 同接口：特征 hour/weekday/lag1d/lag7d/is_peak。

    仅在 _lightgbm_guard 通过时构建；训练数据不足的 lag 用全局均值填充。
    """

    kind = "lightgbm(regression)"

    def __init__(self, rows, seats: int, lgb) -> None:
        self.seats = max(seats, 1)
        by_dh = {(d, h): occ for d, h, _, occ in rows}
        glob = (sum(r[3] for r in rows) / len(rows)) if rows else 0.0
        feat, label = [], []
        for day, hour, _, occ in sorted(by_dh):
            wd = pipeline.weekday_of(day)
            lag1 = by_dh.get((day - 1, hour), glob)
            lag7 = by_dh.get((day - 7, hour), glob)
            feat.append([hour, wd, lag1, lag7, 1.0 if hour in PEAK_IDX else 0.0])
            label.append(occ)
        self._train_set = lgb.Dataset(feat, label=label)
        self._booster = lgb.train({"objective": "regression", "verbose": -1},
                                  self._train_set, num_boost_round=100)
        self._glob = glob
        self._feat_fn = self._make_feat_fn(by_dh, glob)

    @staticmethod
    def _make_feat_fn(by_dh, glob):
        def fn(day: int, hour: int) -> list[float]:
            wd = pipeline.weekday_of(day)
            return [hour, wd,
                    by_dh.get((day - 1, hour), glob),
                    by_dh.get((day - 7, hour), glob),
                    1.0 if hour in PEAK_IDX else 0.0]
        return fn

    def forecast(self, date: str) -> list[tuple[int, float]]:
        d0 = (_dt.date.fromisoformat(date) - _dt.date(1970, 1, 1)).days
        out = []
        for s in range(SLOTS):
            hour = s // 4
            occ = self._booster.predict([self._feat_fn(d0, hour)])[0]
            prob = min(1.0, max(0.0, occ * 16.0 / self.seats))
            out.append((s, round(prob, 4)))
        return out


# ---------- 规则风控 ----------

class NoShowRisk:
    """规则风控：历史爽约次数 >= 2 → 高风险。"""

    def __init__(self, counts: dict[int, int]) -> None:
        self._counts = counts

    @classmethod
    def load(cls, db) -> "NoShowRisk":
        conn = db if isinstance(db, sqlite3.Connection) else sqlite3.connect(db)
        rows = conn.execute(
            "SELECT user_id, COUNT(*) FROM events_raw "
            "WHERE type='NO_SHOW' GROUP BY user_id").fetchall()
        return cls(dict(rows))

    def risk(self, user_id: int) -> dict:
        n = self._counts.get(user_id, 0)
        if n >= 2:
            return {"risk": round(min(1.0, 0.5 + 0.25 * (n - 1)), 2),
                    "level": "high", "noshows": n}
        return {"risk": round(0.2 * n, 2), "level": "low", "noshows": n}


# ---------- 加载门面 + 评估 ----------

class Forecaster:
    def __init__(self, model, reason: str, db=None) -> None:
        self.model = model
        self.reason = reason      # 模型选择说明（降级原因 / 启用条件）
        self._db = db

    @classmethod
    def load(cls, db) -> "Forecaster":
        conn = db if isinstance(db, sqlite3.Connection) else pipeline.connect(db)
        n_events = conn.execute("SELECT COUNT(*) FROM events_raw").fetchone()[0]
        seats_s = pipeline.get_meta(conn, "seats")
        seats = int(seats_s) if seats_s else 512
        lgb, why = _lightgbm_guard(n_events)
        if lgb is not None:
            return cls(LgbmModel(_rows(conn), seats, lgb),
                       f"LightGBM 启用（events={n_events}）", db)
        return cls(BucketModel(_rows(conn), seats),
                   f"分桶均值降级：{why}", db)

    def forecast(self, date: str) -> list[tuple[int, float]]:
        return self.model.forecast(date)

    def risk(self, user_id: int) -> dict:
        return NoShowRisk.load(self._db).risk(user_id)


def _rows(conn: sqlite3.Connection) -> list[tuple[int, int, int, float]]:
    return list(conn.execute(
        "SELECT day, hour, occupied_slots, occupancy FROM v_hourly "
        "ORDER BY day, hour"))


def train_eval(db, holdout_days: int = 1) -> dict:
    """训练（留出最后 holdout_days 天）+ mini 评估：整体/高峰/平峰 MAE + 样例。"""
    conn = db if isinstance(db, sqlite3.Connection) else pipeline.connect(db)
    all_rows = _rows(conn)
    days = sorted({r[0] for r in all_rows})
    eval_days = set(days[-holdout_days:])
    train_rows = [r for r in all_rows if r[0] not in eval_days]

    n_events = conn.execute("SELECT COUNT(*) FROM events_raw").fetchone()[0]
    seats_s = pipeline.get_meta(conn, "seats")
    seats = int(seats_s) if seats_s else 512
    lgb, why = _lightgbm_guard(n_events)
    if lgb is not None:
        model = LgbmModel(train_rows, seats, lgb)
        chosen = "lightgbm"
    else:
        model = BucketModel(train_rows, seats)
        chosen = f"bucket（降级原因：{why}）"

    errs_all: list[float] = []
    errs_peak: list[float] = []
    samples: list[str] = []
    for day in sorted(eval_days):
        date = pipeline.day_to_date(day).isoformat()
        pred = {s: p for s, p in model.forecast(date)}
        for d, hour, _, occ in all_rows:
            if d != day:
                continue
            actual = min(1.0, occ * 16.0 / seats)
            guess = pred[hour * 4]
            errs_all.append(abs(guess - actual))
            if hour in PEAK_IDX:
                errs_peak.append(abs(guess - actual))
            elif len(samples) < 6:
                samples.append(f"{date} hour={hour + 6:02d}点 "
                               f"pred={guess:.3f} actual={actual:.3f}")
    mae = sum(errs_all) / len(errs_all) if errs_all else float("nan")
    mae_peak = sum(errs_peak) / len(errs_peak) if errs_peak else float("nan")
    return {"model": chosen, "mae": mae, "mae_peak": mae_peak,
            "mae_offpeak": (sum(errs_all) - sum(errs_peak)) /
                           max(len(errs_all) - len(errs_peak), 1),
            "eval_days": sorted(eval_days), "samples": samples,
            "seats": seats, "n_train_rows": len(train_rows)}


# ---------- 自检 ----------

def selftest() -> int:
    ok = True
    tmp = tempfile.mkdtemp(prefix="sm_forecast_")
    db_path = os.path.join(tmp, "fc.db")
    stats = pipeline.generate_synthetic(5, db_path, daily_users=800, seed=99)
    conn = pipeline.connect(db_path)

    ev = train_eval(conn, holdout_days=1)
    finite = ev["mae"] == ev["mae"]          # NaN 检查
    print(f"[selftest A] mini 评估: model={ev['model']}")
    print(f"            评估日={ev['eval_days']} 训练行={ev['n_train_rows']} "
          f"MAE={ev['mae']:.4f} 高峰MAE={ev['mae_peak']:.4f} "
          f"平峰MAE={ev['mae_offpeak']:.4f} {'PASS' if finite else 'FAIL'}")
    ok &= finite
    for s in ev["samples"][:3]:
        print(f"            命中样例: {s}")

    # 预测输出形状：64 点、概率 0..1、高峰小时概率应高于深夜
    model = Forecaster.load(conn)
    last_day = ev["eval_days"][0]
    date = pipeline.day_to_date(last_day).isoformat()
    fc = model.forecast(date)
    shape_ok = (len(fc) == SLOTS and
                all(isinstance(sl, int) and 0.0 <= p <= 1.0 for sl, p in fc))
    peak_p = max(p for sl, p in fc if sl // 4 in PEAK_IDX)
    night_p = max(p for sl, p in fc if sl // 4 in (0, 1))
    trend_ok = peak_p >= night_p
    print(f"[selftest B] forecast 形状 64 点且 0..1: "
          f"{'PASS' if shape_ok else 'FAIL'}; 高峰{peak_p:.3f} >= 夜间{night_p:.3f}: "
          f"{'PASS' if trend_ok else 'FAIL'}; 降级说明: {model.reason}")
    ok &= shape_ok and trend_ok

    # 规则风控：历史爽约 >= 2 → 高风险
    conn.execute("INSERT INTO events_raw VALUES(?,?,?,?,?,?,?,?,?,?)",
                 (0, 900001, "NO_SHOW", 424242, -1, -1, "0", "synthetic",
                  "SX1", "0"))
    conn.execute("INSERT INTO events_raw VALUES(?,?,?,?,?,?,?,?,?,?)",
                 (0, 900002, "NO_SHOW", 424242, -1, -1, "0", "synthetic",
                  "SX2", "0"))
    conn.commit()
    risk = NoShowRisk.load(conn)
    hi = risk.risk(424242)
    lo = risk.risk(424243)
    r_ok = hi["level"] == "high" and lo["level"] == "low"
    print(f"[selftest C] 风控: user424242(爽约2)={hi} "
          f"user424243(爽约0)={lo} {'PASS' if r_ok else 'FAIL'}")
    ok &= r_ok

    # LightGBM 守卫：本机未装库/数据量不足时必须明确降级（同接口）
    lgb, why = _lightgbm_guard(conn.execute(
        "SELECT COUNT(*) FROM events_raw").fetchone()[0])
    print(f"[selftest D] LightGBM 守卫: 可用={lgb is not None} 原因={why} "
          f"（>=50 万事件才启用，未启用走分桶同接口）")
    conn.close()
    print(f"FORECAST_SELFTEST: {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description="SeatMesh 预测/风控")
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--db", default="")
    ap.add_argument("--date", default="2026-03-08")
    args = ap.parse_args()
    if args.selftest:
        return selftest()
    db_path = args.db or os.path.join(tempfile.gettempdir(),
                                      "seatmesh_synthetic.db")
    conn = pipeline.connect(db_path)
    ev = train_eval(conn)
    print(f"[forecast] model={ev['model']} MAE={ev['mae']:.4f}")
    fc = Forecaster.load(conn).forecast(args.date)
    for slot, p in fc[::4]:                   # 每小时首片
        print(f"  {slot:02d} ({slot // 4 + 6:02d}:00) p={p:.3f}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
