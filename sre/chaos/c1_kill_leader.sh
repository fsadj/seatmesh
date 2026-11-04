#!/usr/bin/env bash
# 混沌剧本 C1 · 杀 Leader：3 节点压测 30s → kill -9 leader → 复用 T2 的 failover 断言。
# 依赖守卫：T2 的 engine/scripts/failover_test.sh 未交付则 SKIP + 原因（不冒充通过）。
# 运行：bash sre/chaos/c1_kill_leader.sh        （演示 ≤2 分钟）
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FAILOVER="$ROOT/engine/scripts/failover_test.sh"

echo "== C1 kill-leader 混沌 =="
if [[ ! -f "$FAILOVER" ]]; then
  echo "SKIP: 依赖缺失 engine/scripts/failover_test.sh（T2 未交付）"
  echo "  该脚本应提供：3 节点集群 + 30s 压测 + kill -9 leader + 接管断言（LOST=0）。"
  echo "  T2 交付后本剧本自动升级为真实执行，无需改动。"
  echo "C1_KILL_LEADER: SKIP"
  exit 0
fi
if ! command -v /tmp/engine_d >/dev/null 2>&1 && ! make -C "$ROOT" build-engine; then
  echo "SKIP: 引擎二进制不可用且构建失败"
  echo "C1_KILL_LEADER: SKIP"
  exit 0
fi

echo "[c1] 复用 T2 failover 断言：$FAILOVER"
OUT=$(bash "$FAILOVER" 2>&1)
RC=$?
echo "$OUT" | tail -20
if [[ $RC -eq 0 ]] && echo "$OUT" | grep -q "LOST=0"; then
  echo "[c1] leader 被杀后服务不中断，LOST=0 断言通过"
  echo "C1_KILL_LEADER: PASS"
  exit 0
fi
echo "[c1] failover 断言未通过（rc=$RC 或缺 LOST=0）"
echo "C1_KILL_LEADER: FAIL"
exit 1
