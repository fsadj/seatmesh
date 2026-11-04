#!/usr/bin/env bash
# 72h 长跑（T1 阶段三）：合成负载 + 每轮快照 + 定期 kill -9 自动重启 + 自动断言，无人值守。
#
# 用法：soak.sh [--smoke] [--hours N] [--kill-every N] [--reqs N]
#   --smoke       短跑冒烟（4 轮，~10s），验证脚本自身正确性（本会话只跑这个）；
#   --hours N     完整跑时长（默认 72；非 smoke 模式按墙钟到点退出）；
#   --kill-every N 每 N 轮注入一次 kill -9（默认 3，对应 72h 方案的每 6h）；
#   --reqs N      每轮合成请求数（默认 20）。
#
# 每轮：合成负载经 stdin 投喂 → 采样 RSS → 优雅退出（快照+WAL 轮转）或 kill -9。
# 断言：canary 幂等跨重启（E_IDEMPOTENT_REPLAY）、恢复进度单调不回退、
#        优雅轮 journal 精确对账（appends = OK 行 + E_CONFLICT 行）、violations=0。
# 采集：RSS 曲线 rss.csv、每轮 stdout、soak.log（供 72h 报告三曲线使用）。
set -euo pipefail

ENGINE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CXX="${CXX:-c++}"
SMOKE=0; HOURS=72; KILL_EVERY=3; REQS=20
while [ $# -gt 0 ]; do
  case "$1" in
    --smoke) SMOKE=1; shift;;
    --hours) HOURS="$2"; shift 2;;
    --kill-every) KILL_EVERY="$2"; shift 2;;
    --reqs) REQS="$2"; shift 2;;
    *) echo "unknown arg: $1"; exit 2;;
  esac
done

D="$(mktemp -d /tmp/seatmesh_soak.XXXXXX)"
BIN="$(mktemp /tmp/seatmesh_soak_bin.XXXXXX)"
LOG="$D/soak.log"
RSS_CSV="$D/rss.csv"
echo "round,rss_kb" > "$RSS_CSV"
trap 'rm -rf "$D" "$BIN"' EXIT

echo "== build =="
( cd "$ENGINE_DIR" && "$CXX" -std=c++17 -O2 -Iinclude -Isrc \
    src/main.cpp src/engine.cpp src/storage/file_event_log.cpp \
    src/storage/db_event_log.cpp src/storage/projections.cpp \
    -lsqlite3 -o "$BIN" )
chmod +x "$BIN"   # mktemp 落盘 0600，覆盖写后仍不可执行

RUN=("$BIN" --storage=file --wal="$D/wal.log" --snapshot="$D/snap" --seats=50)
# 引擎 20ms 轮询处理 stdin；留足处理时间
PAUSE="$(awk "BEGIN{printf \"%.1f\", 0.8 + $REQS * 0.04}")"

START=$(date +%s)
END_AT=$(( START + HOURS * 3600 ))
MAX_ROUNDS=999999
[ "$SMOKE" -eq 1 ] && MAX_ROUNDS=4

round=1
violations=0
last_idx=0
prev_round_appends=-1   # 上一优雅轮的 journal 追加数（-1 = 尚无对账基准）
prev_round_was_kill=0
kill_rounds=0

gen_load() {  # $1 = round → 合成负载文件（每轮带 canary；kill 轮无 quit）
  local f="$D/load_$1.txt"; local i u s d st
  echo "reserve user=990001 seat=1 day=1 start=120 end=240 rid=canary trace=c0000" >> "$f"
  for i in $(seq 1 "$REQS"); do
    u=$(( 1000 + ($1 - 1) * REQS + i ))
    s=$(( i % 10 + 1 ))
    d=$(( 1 + (i + $1) % 13 ))
    st=$(( 120 + (i % 8) * 15 ))
    echo "reserve user=$u seat=$s day=$d start=$st end=$(( st + 30 )) rid=r${1}_${i} trace=$(printf 't%04d' $i)" >> "$f"
  done
}

while [ "$round" -le "$MAX_ROUNDS" ]; do
  gen_load "$round"
  if [ "$round" -eq 1 ] || [ $(( round % KILL_EVERY )) -ne 0 ]; then
    echo "quit" >> "$D/load_$round.txt"      # 优雅轮：处理完即退出（快照+轮转）
    mode=graceful
  else
    mode=kill                                 # kill 轮：投喂后 SIGKILL
    kill_rounds=$(( kill_rounds + 1 ))
  fi

  "${RUN[@]}" --source=stdin < "$D/load_$round.txt" > "$D/round_$round.out" 2>&1 &
  PID=$!
  # 存活期采样 RSS（引擎处理完 quit 即退出，事后 ps 采样会扑空）
  ( while kill -0 "$PID" 2>/dev/null; do
      ps -o rss= -p "$PID" 2>/dev/null | tr -d ' ' | grep -v '^$' || true
      sleep 0.2
    done ) > "$D/rss_$round.txt" 2>/dev/null &
  SAMPLER=$!
  sleep "$PAUSE"

  if [ "$mode" = "kill" ]; then
    kill -9 "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
    echo "[round $round] kill -9 注入完成（WAL 逐条 fsync，重启恢复）" | tee -a "$LOG"
    round_appends=-1   # stdout 随进程丢失，追加数未知
  else
    RC=0; wait "$PID" || RC=$?
    if [ "$RC" -ne 0 ]; then
      echo "VIOLATION: round $round 优雅退出码 $RC（非零）" | tee -a "$LOG"
      violations=$(( violations + 1 ))
    fi
    grep -q "snapshot written: yes" "$D/round_$round.out" || {
      echo "VIOLATION: round $round 未写出快照" | tee -a "$LOG"
      violations=$(( violations + 1 ));
    }
    # journal 追加数：真 OK 行 + E_CONFLICT 行（幂等重放 OK 行与其余拒绝不落日志）
    J="$(grep '^OK' "$D/round_$round.out" | grep -v 'E_IDEMPOTENT_REPLAY' | grep -c '^OK' || true)"
    C="$(grep -c 'E_CONFLICT' "$D/round_$round.out" || true)"
    J=${J:-0}; C=${C:-0}
    round_appends=$(( J + C ))
  fi
  kill "$SAMPLER" 2>/dev/null || true
  wait "$SAMPLER" 2>/dev/null || true

  # canary 幂等跨重启：第 1 轮 OK，之后每轮必须 E_IDEMPOTENT_REPLAY
  if [ "$round" -ge 2 ]; then
    grep -q "E_IDEMPOTENT_REPLAY" "$D/round_$round.out" || {
      echo "VIOLATION: round $round canary 幂等跨重启失效" | tee -a "$LOG"
      violations=$(( violations + 1 ));
    }
  fi

  # 恢复进度（启动时回放到的 log index）
  IDX="$(grep -o 'replayed to index [0-9]*' "$D/round_$round.out" | awk '{print $4}' || true)"
  # 单调不回退
  if [ -n "${IDX:-}" ] && [ "$IDX" -lt "$last_idx" ]; then
    echo "VIOLATION: round $round 恢复进度回退 $last_idx → $IDX" | tee -a "$LOG"
    violations=$(( violations + 1 ))
  fi
  # journal 对账：上一轮为优雅轮时，本轮启动 index 应 = 上轮启动 index + 上轮 appends
  if [ "$prev_round_appends" -ge 0 ] && [ -n "${IDX:-}" ] && \
     [ "$IDX" -ne $(( last_idx + prev_round_appends )) ]; then
    echo "VIOLATION: round $round journal 对账不等 idx=$IDX 期望=$(( last_idx + prev_round_appends ))" | tee -a "$LOG"
    violations=$(( violations + 1 ))
  fi
  [ -n "${IDX:-}" ] && last_idx="$IDX"
  prev_round_appends="$round_appends"
  prev_round_was_kill=0
  [ "$mode" = "kill" ] && prev_round_was_kill=1

  RSS="$(sort -n "$D/rss_$round.txt" 2>/dev/null | tail -1 || true)"
  echo "$round,${RSS:-gone}" >> "$RSS_CSV"

  echo "[round $round] mode=$mode idx=${IDX:-?} rss=${RSS:-gone}KB" | tee -a "$LOG"
  round=$(( round + 1 ))

  if [ "$SMOKE" -ne 1 ] && [ "$(date +%s)" -ge "$END_AT" ]; then
    echo "到点退出（hours=$HOURS）" | tee -a "$LOG"; break
  fi
done

RSS_SAMPLES=$(( $(wc -l < "$RSS_CSV") - 1 ))
echo "------------------------------------------------------------"
echo "rounds=$(( round - 1 )) kill_rounds=$kill_rounds rss_samples=$RSS_SAMPLES violations=$violations"
echo "rss 曲线: $RSS_CSV（已随 trap 清理前打印）"
cat "$RSS_CSV"
if [ "$violations" -ne 0 ]; then
  echo "SOAK RESULT: FAIL (violations=$violations)"; exit 1
fi
if [ "$SMOKE" -eq 1 ]; then
  echo "SOAK SMOKE PASS rounds=$(( round - 1 )) violations=0"
else
  echo "SOAK PASS hours=$HOURS rounds=$(( round - 1 )) violations=0"
fi
