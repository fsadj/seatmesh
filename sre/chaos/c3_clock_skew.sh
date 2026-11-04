#!/usr/bin/env bash
# 混沌剧本 C3 · 时钟偏移：对 follower 做 faketime +2s → 断言服务不中断。
# 红线：只对 follower 注入偏移；leader 偏移是另一个"刻意选主风暴"剧本，不在本剧本范围。
# 依赖守卫：faketime 未安装 → SKIP；T2 集群入口缺失 → SKIP（无 follower 可注入）。
# 运行：bash sre/chaos/c3_clock_skew.sh        （演示 ≤2 分钟）
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FAILOVER="$ROOT/engine/scripts/failover_test.sh"
COMPOSE="$ROOT/deploy/docker-compose.yml"

echo "== C3 follower 时钟偏移 (+2s) =="
if ! command -v faketime >/dev/null 2>&1; then
  echo "SKIP: 依赖缺失 faketime（libfaketime 未安装）。"
  echo "  安装后本剧本自动升级：macOS 'brew install libfaketime'，"
  echo "  Debian/Ubuntu 'apt install faketime'。"
  echo "C3_CLOCK_SKEW: SKIP"
  exit 0
fi
if [[ ! -f "$FAILOVER" && ! -f "$COMPOSE" ]]; then
  echo "SKIP: 依赖缺失 T2 集群（engine/scripts/failover_test.sh 与 deploy/docker-compose.yml 均不存在），无 follower 可注入。"
  echo "C3_CLOCK_SKEW: SKIP"
  exit 0
fi

# ---- 真实执行路径（T2 交付后启用）----
# 约定：T2 集群以 deploy/docker-compose.yml 起 3 节点（node-1 领导），
# 本剧本对 node-2（follower）进程包一层 faketime '+2s'：
#   faketime '+2s' /tmp/engine_d --storage=file --wal=/tmp/sm_c3/node-2/wal.log ...
# 断言：
#   1) 偏移期间持续 reserve 请求全部有应答（无 LOST / 超时计数为 0）；
#   2) 偏移期间不发生非必要选主（raft_elections_total 不因 follower 偏移增长，
#      Raft 只认日志水位，不认墙钟——这正是要演示的性质）；
#   3) 撤销偏移后 follower 追平（apply_lag_entries → 0）。
echo "[c3] faketime 可用，集群入口就绪 → 执行 follower +2s 注入"
echo "  TODO(T2): 需要集群 CLI（节点启停/指标查询）由 T2 的 failover_test.sh 提供；"
echo "  接口就绪后本段替换为真实调用并输出 C3_CLOCK_SKEW: PASS/FAIL。"
echo "C3_CLOCK_SKEW: SKIP"
exit 0
