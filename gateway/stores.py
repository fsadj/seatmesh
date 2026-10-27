"""contracts.CreditStore 的两个实现：内存（测试）与 SQLite（控制面）。
以 (user_id, event_seq) 幂等：同一事件重复消费不重复记账。
"""
from __future__ import annotations

import sqlite3
import threading


class InMemoryCreditStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ledger: dict[tuple[int, int], int] = {}   # (user, event_seq) → delta
        self._balance: dict[int, int] = {}

    def apply_delta(self, user_id: int, delta: int,
                    booking_id: str, event_seq: int) -> bool:
        with self._lock:
            key = (user_id, event_seq)
            if key in self._ledger:
                return False  # 幂等：重复事件
            self._ledger[key] = delta
            self._balance[user_id] = self._balance.get(user_id, 0) + delta
            return True

    def balance(self, user_id: int) -> int:
        with self._lock:
            return self._balance.get(user_id, 0)


class SqliteCreditStore:
    """表结构即对账凭据：ledger 可由事件流全量重建（docs/03 §5）。"""

    def __init__(self, path: str):
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._lock = threading.Lock()
        self._db.executescript(
            "PRAGMA journal_mode=WAL;"
            "CREATE TABLE IF NOT EXISTS credit_ledger("
            " user_id INTEGER, event_seq INTEGER, delta INTEGER,"
            " booking_id TEXT, PRIMARY KEY(user_id, event_seq));")

    def apply_delta(self, user_id: int, delta: int,
                    booking_id: str, event_seq: int) -> bool:
        with self._lock:
            cur = self._db.execute(
                "INSERT OR IGNORE INTO credit_ledger VALUES(?,?,?,?)",
                (user_id, event_seq, delta, booking_id))
            self._db.commit()
            return cur.rowcount > 0

    def balance(self, user_id: int) -> int:
        with self._lock:
            row = self._db.execute(
                "SELECT COALESCE(SUM(delta),0) FROM credit_ledger"
                " WHERE user_id=?", (user_id,)).fetchone()
            return int(row[0])
