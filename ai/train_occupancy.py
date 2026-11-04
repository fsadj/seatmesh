#!/usr/bin/env python3
"""占用率预测训练入口（薄包装）。

训练与评估逻辑在 ai/forecast.py 的 train_eval()（分桶均值基线 +
可选 LightGBM 路径，见 forecast._lightgbm_guard）。本文件只提供任务包
约定文件名的命令行入口：

    python3 ai/train_occupancy.py <db_path> [model_path]
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from forecast import train_eval  # noqa: E402


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("用法: python3 ai/train_occupancy.py <db_path> [model_path]")
        return 2
    db = argv[1]
    report = train_eval(db)   # 模型产物由 forecast 内部管理（分桶参数/可选 LGBM）
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
