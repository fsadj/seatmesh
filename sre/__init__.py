"""sre：质量工程包（压测 / 审计 / Jepsen 一致性 / 混沌 / 回归）。

入口：
  python3 sre/stress.py [--quick]      并发压测 + 0 超卖断言
  python3 sre/audit.py  [--selftest]   影子位图审计器（不变量 I1）
  python3 -m sre.jepsen [--quick]      两层一致性检查器（不变量 + WGL）
  python3 sre/regression.py            核心 AC 自动化回归
"""
