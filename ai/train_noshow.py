#!/usr/bin/env python3
"""爽约风控训练入口（薄包装）。

爽约风险（规则版：历史爽约 ≥2 次 → 高风险，LightGBM 升级路径）
在 ai/forecast.py 的 NoShowRisk / train_eval()。本文件只提供任务包
约定文件名的命令行入口：

    python3 ai/train_noshow.py <db_path> [top_n]
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from forecast import NoShowRisk, train_eval  # noqa: E402


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("用法: python3 ai/train_noshow.py <db_path> [top_n]")
        return 2
    db, top_n = argv[1], int(argv[2]) if len(argv) > 2 else 5
    report = train_eval(db)
    risk = NoShowRisk.load(db)
    # 抽样展示风险分（高风险排前）
    sample = sorted(risk._counts, key=lambda u: -risk._counts[u])[:top_n]
    print(json.dumps({
        "eval": {k: report[k] for k in report if k.startswith("noshow") or k == "mae"},
        "risk_top": [{"user_id": u, **risk.risk(u)} for u in sample],
    }, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
