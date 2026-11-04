"""sys.path 处理：让 ai/ 下的脚本能互认（pipeline/forecast）并按需访问 sre。

兼容两种运行方式：
  python3 ai/pipeline.py        （脚本目录入 path，`import gwpath`）
  python3 -m ai.forecast        （仓库根入 path，`from ai.gwpath import ...`）
"""
from __future__ import annotations

import os
import sys


def repo_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def add_ai_path() -> str:
    """把 ai/ 加入 sys.path（pipeline.py / forecast.py 互相导入用）。"""
    ai = os.path.join(repo_root(), "ai")
    if ai not in sys.path:
        sys.path.insert(0, ai)
    return ai


def add_sre_path() -> str:
    """把 sre/ 加入 sys.path（需要审计器/压测入口时）。"""
    sre = os.path.join(repo_root(), "sre")
    if sre not in sys.path:
        sys.path.insert(0, sre)
    return sre
