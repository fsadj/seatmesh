"""sys.path 处理：让 sre/ai 下的脚本能 import gateway 侧契约模块。

同时兼容两种运行方式：
  python3 sre/stress.py          （脚本目录入 path，`import gwpath`）
  python3 -m sre.jepsen          （仓库根入 path，`from sre.gwpath import ...`）
"""
from __future__ import annotations

import os
import sys


def repo_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def add_gateway() -> str:
    """把 gateway/ 加入 sys.path（contracts.py / mock_engine.py 所在）。"""
    gw = os.path.join(repo_root(), "gateway")
    if gw not in sys.path:
        sys.path.insert(0, gw)
    return gw
