"""全量回归：编排当前可自动化的核心验收标准（AC）。

每项输出 {ac_id, status, pass, seconds, evidence_path}，汇总绿表到 stdout；
证据原文存 sre/evidence/。任何 FAIL → 非零退出；SKIPPED 不影响退出码
（依赖未交付的项如实标注，交付后本脚本自动纳入）。

当前 AC（阶段三全量版中的可自动化子集）：
  AC-01 0 超卖         sre/stress.py --quick（并发抢同一座位）
  AC-02 崩溃恢复       engine/scripts/crash_test.sh（T1 交付后自动通过，缺失 SKIP）
  AC-03 契约测试       make test-engine（C++ 契约 + 单测）
  AC-04 契约演示       make demo-python（MockEngine 全语义走查）

运行：python3 sre/regression.py
"""
from __future__ import annotations

import datetime as _dt
import os
import platform
import subprocess
import sys
import time

try:
    from gwpath import repo_root
except ImportError:  # python3 -m sre.regression
    from sre.gwpath import repo_root  # type: ignore

ROOT = repo_root()
EVIDENCE = os.path.join(ROOT, "sre", "evidence")


def _run(name: str, argv: list[str], ac_id: str, desc: str,
         shell: bool = False, cwd: str = ROOT) -> dict:
    t0 = time.perf_counter()
    os.makedirs(EVIDENCE, exist_ok=True)
    ev_path = os.path.join(EVIDENCE, f"{ac_id.replace('-', '_').lower()}.txt")
    proc = subprocess.run(argv, capture_output=True, text=True, shell=shell,
                          cwd=cwd, timeout=600)
    seconds = round(time.perf_counter() - t0, 2)
    output = (proc.stdout or "") + (proc.stderr or "")
    with open(ev_path, "w", encoding="utf-8") as f:
        f.write(f"# {ac_id} {desc}\n# cmd: {' '.join(argv)}\n"
                f"# exit={proc.returncode} seconds={seconds}\n"
                f"# at={_dt.datetime.now().isoformat(timespec='seconds')}\n\n"
                + output)
    return {"ac_id": ac_id, "desc": desc, "status":
            "PASS" if proc.returncode == 0 else "FAIL",
            "pass": proc.returncode == 0, "seconds": seconds,
            "evidence_path": os.path.relpath(ev_path, ROOT),
            "exit": proc.returncode, "tail": output.strip().splitlines()[-3:]}


def _skip(ac_id: str, desc: str, reason: str) -> dict:
    return {"ac_id": ac_id, "desc": desc, "status": "SKIPPED", "pass": False,
            "seconds": 0.0, "evidence_path": "-", "exit": None,
            "tail": [f"SKIP 原因: {reason}"]}


def main() -> int:
    started = _dt.datetime.now()
    print(f"[regression] 开始 {started.isoformat(timespec='seconds')} "
          f"root={ROOT}")

    results: list[dict] = []

    # AC-01 0 超卖（并发压测）
    results.append(_run("stress", [sys.executable, "sre/stress.py", "--quick"],
                        "AC-01", "0 超卖：并发抢同一座位，成功响应==1 且 "
                        "double_assign==0"))

    # AC-02 崩溃恢复（依赖 T1 的 crash_test.sh；缺失如实 SKIP）
    crash = os.path.join(ROOT, "engine", "scripts", "crash_test.sh")
    if os.path.exists(crash):
        results.append(_run("crash", ["bash", os.path.relpath(crash, ROOT)],
                            "AC-02", "崩溃恢复：回放→kill -9→重启→状态完整",
                            cwd=os.path.join(ROOT, "engine")))
    else:
        results.append(_skip("AC-02", "崩溃恢复：回放→kill -9→重启→状态完整",
                             "engine/scripts/crash_test.sh 未交付（T1 范围），"
                             "交付后本项自动纳入执行"))

    # AC-03 契约测试（C++ 契约 + 引擎单测）
    results.append(_run("contract", ["make", "test-engine"], "AC-03",
                        "契约测试：engine C++ 契约 + 单测全绿"))

    # AC-04 契约演示（MockEngine 全语义走查）
    results.append(_run("demo", ["make", "demo-python"], "AC-04",
                        "契约演示：MockEngine 全语义 + 并发 0 超卖 + 副本视图"))

    # ---- 汇总 ----
    fingerprint = (f"{platform.system()}/{platform.release()} "
                   f"({platform.machine()}) python={platform.python_version()}")
    total_s = round(sum(r["seconds"] for r in results), 2)
    n_fail = sum(1 for r in results if r["status"] == "FAIL")
    n_skip = sum(1 for r in results if r["status"] == "SKIPPED")
    n_pass = sum(1 for r in results if r["status"] == "PASS")

    print("\n== SeatMesh 回归绿表 ==")
    print(f"环境: {fingerprint}")
    print(f"开始: {started.isoformat(timespec='seconds')}  "
          f"总耗时: {total_s}s  PASS={n_pass} FAIL={n_fail} SKIPPED={n_skip}")
    print(f"{'AC':6} {'状态':8} {'耗时':>7}  说明 / 证据")
    for r in results:
        line = (f"{r['ac_id']:6} {r['status']:8} {r['seconds']:>6}s  "
                f"{r['desc']}")
        print(line)
        print(f"{'':6} {'':8} {'':>7}  证据: {r['evidence_path']}")
        for t in r["tail"]:
            print(f"{'':6} {'':8} {'':>7}  | {t}")
    verdict = "GREEN" if n_fail == 0 else "RED"
    print(f"REGRESSION: {verdict} "
          f"(pass={n_pass}, fail={n_fail}, skipped={n_skip})")
    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
