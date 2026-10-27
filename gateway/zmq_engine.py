"""ZmqEngineClient：contracts.EngineClient 的 ZMQ 实现（正式拓扑）。

帧格式（W1 与引擎侧 ZmqRouterSource 对齐后冻结）：
  DEALER → ROUTER，JSON 编码 v0（protoc 就绪后切 protobuf，契约不变）。
  非 leader 节点回 {"redirect": "<endpoint>"} → 客户端缓存 leader（TTL 5s）。

依赖：pip install pyzmq（未安装时 import 本模块会给出明确报错）。
"""
from __future__ import annotations

import json
import uuid

try:
    import zmq
except ImportError as e:  # pragma: no cover
    raise ImportError("ZmqEngineClient 需要 pyzmq：pip install pyzmq") from e

from contracts import Outcome, ReserveRequest


class ZmqEngineClient:
    def __init__(self, endpoints: list[str], timeout_ms: int = 800):
        if not endpoints:
            raise ValueError("需要至少一个引擎节点 endpoint")
        self._eps = endpoints
        self._timeout_ms = timeout_ms
        self._ctx = zmq.Context()
        self._sock = self._ctx.socket(zmq.DEALER)
        self._sock.setsockopt(zmq.LINGER, 0)
        self._sock.setsockopt(zmq.RCVTIMEO, timeout_ms)
        for ep in endpoints:
            self._sock.connect(ep)
        self._leader_hint = endpoints[0]  # TODO(M4)：Redirect 缓存 + TTL

    def close(self) -> None:
        self._sock.close(0)
        self._ctx.term()

    # ---- 内部 ----

    def _call(self, kind: str, payload: dict) -> dict:
        payload["kind"] = kind
        payload["request_id"] = payload.get("request_id") or str(uuid.uuid4())
        self._sock.send_json(payload)
        try:
            return self._sock.recv_json()
        except zmq.Again as e:
            raise TimeoutError(f"引擎 {self._leader_hint} 超时（{self._timeout_ms}ms）") from e

    @staticmethod
    def _outcome(resp: dict) -> Outcome:
        return Outcome(ok=bool(resp.get("ok")),
                       booking_id=resp.get("booking_id", ""),
                       conflict_mask=int(resp.get("conflict_mask", 0)),
                       detail=resp.get("detail", ""))

    # ---- EngineClient ----

    def reserve(self, req: ReserveRequest) -> Outcome:
        return self._outcome(self._call("reserve", {
            "request_id": req.request_id, "user_id": req.user_id,
            "seat_id": req.seat_id, "day_off": req.day_off,
            "start_min": req.start_min, "end_min": req.end_min,
            "trace_id": req.trace_id}))

    def cancel(self, request_id: str, user_id: int, booking_id: str) -> Outcome:
        return self._outcome(self._call(
            "cancel", {"request_id": request_id, "user_id": user_id,
                       "booking_id": booking_id}))

    def checkin(self, request_id: str, user_id: int, booking_id: str) -> Outcome:
        return self._outcome(self._call(
            "checkin", {"request_id": request_id, "user_id": user_id,
                        "booking_id": booking_id}))

    def checkout(self, request_id: str, user_id: int, booking_id: str) -> Outcome:
        return self._outcome(self._call(
            "checkout", {"request_id": request_id, "user_id": user_id,
                         "booking_id": booking_id}))

    def seat_bits(self, seat_id: int, day_off: int) -> int:
        # 注意：正式读路径走 ReplicaView（docs/03 §4）；此接口仅供调试强读。
        resp = self._call("lease_read", {"seat_id": seat_id, "day_off": day_off})
        return int(resp.get("bits", 0))
