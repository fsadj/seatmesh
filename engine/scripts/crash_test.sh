#!/usr/bin/env bash
# 崩溃恢复验收（T1 阶段一 DoD）：
#   回放 3 请求 → kill -9 → 重启 → 断言状态完整（幂等/冲突/额度三路证明）→ RECOVERED OK
# 依赖：c++（或 CXX 环境变量）、libsqlite3；不连接任何外部系统。
set -euo pipefail

ENGINE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CXX="${CXX:-c++}"
D="$(mktemp -d /tmp/seatmesh_crash.XXXXXX)"
BIN="$(mktemp /tmp/seatmesh_crash_bin.XXXXXX)"
trap 'rm -rf "$D" "$BIN"' EXIT

echo "== build =="
( cd "$ENGINE_DIR" && "$CXX" -std=c++17 -O2 -Iinclude -Isrc \
    src/main.cpp src/engine.cpp src/storage/file_event_log.cpp \
    src/storage/db_event_log.cpp src/storage/projections.cpp \
    -lsqlite3 -o "$BIN" )
chmod +x "$BIN"   # mktemp 落盘 0600，覆盖写后仍不可执行

RUN=("$BIN" --storage=file --wal="$D/wal.log" --snapshot="$D/snap" --seats=50)

echo "== phase 1: replay 3 requests (正常退出，退出时快照 + WAL 轮转) =="
cat > "$D/r1.txt" <<'EOF'
reserve user=11 seat=3 day=1 start=120 end=240 rid=a1 trace=t001
reserve user=12 seat=4 day=1 start=120 end=240 rid=a2 trace=t002
reserve user=13 seat=5 day=1 start=120 end=240 rid=a3 trace=t003
quit
EOF
"${RUN[@]}" --source=replay --replay="$D/r1.txt" > "$D/run1.out" 2>&1
cat "$D/run1.out"
n_ok=$(grep -c '^OK' "$D/run1.out" || true)
if [ "$n_ok" -ne 3 ]; then
  echo "FAIL: phase1 期望 3 条 OK，实际 $n_ok"; exit 1
fi
[ -f "$D/wal.log.1" ] && echo "wal.1 轮转存在"

echo "== phase 2: 3 requests via stdin, then kill -9 =="
cat > "$D/r2.txt" <<'EOF'
reserve user=14 seat=6 day=1 start=120 end=240 rid=a4 trace=t004
reserve user=15 seat=7 day=1 start=120 end=240 rid=a5 trace=t005
reserve user=16 seat=8 day=1 start=120 end=240 rid=a6 trace=t006
EOF
"${RUN[@]}" --source=stdin < "$D/r2.txt" > "$D/run2.out" 2>&1 &
PID=$!
sleep 1.2   # 引擎 20ms 轮询，3 行请求早已处理完毕（逐条 fsync 落 WAL）
kill -9 "$PID" 2>/dev/null || true
wait "$PID" 2>/dev/null || true
echo "killed -9 pid=$PID（stdout 缓冲随进程丢失属预期；WAL 已逐条 fsync）"

echo "== phase 3: restart + verify =="
# 三路证明状态完整：
#   a1 幂等重放（phase1 的 rid，跨快照/轮转仍拒）→ E_IDEMPOTENT_REPLAY
#   seat=3 重叠（phase1 预约占位）→ E_CONFLICT
#   user=14 再约（phase2 kill -9 前已成功）→ E_QUOTA
cat > "$D/r3.txt" <<'EOF'
reserve user=11 seat=9 day=1 start=120 end=240 rid=a1 trace=t101
reserve user=21 seat=3 day=1 start=120 end=240 rid=b1 trace=t102
reserve user=14 seat=9 day=1 start=120 end=240 rid=b2 trace=t103
quit
EOF
"${RUN[@]}" --source=replay --replay="$D/r3.txt" > "$D/run3.out" 2>&1
cat "$D/run3.out"

fail=0
grep -q "E_IDEMPOTENT_REPLAY" "$D/run3.out" || { echo "FAIL: 幂等表跨重启失效"; fail=1; }
grep -q "E_CONFLICT" "$D/run3.out" || { echo "FAIL: 位图跨重启失效"; fail=1; }
grep -q "E_QUOTA" "$D/run3.out" || { echo "FAIL: kill -9 前写入丢失"; fail=1; }
grep -q "replayed to index 6" "$D/run3.out" || { echo "FAIL: 恢复进度非 6"; fail=1; }
if [ "$fail" -ne 0 ]; then
  echo "RESULT: FAIL"; exit 1
fi
echo "RECOVERED OK"
