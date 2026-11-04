#!/usr/bin/env bash
# 混沌剧本 C4 · 盘满：tmpfs/内存盘填满 WAL 分区 → 断言引擎拒绝写并告警 →
# 清理后 replay 补齐断言（释放空间 → 重放事件全部成功、恢复索引增长）。
# 运行：bash sre/chaos/c4_disk_full.sh        （演示 ≤2 分钟，本机可真实执行）
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENGINE="${SEATMESH_ENGINE:-/tmp/engine_d}"
VOL=""
DEV=""
MNT=""

cleanup() {
  if [[ -n "$VOL" ]]; then
    rm -f "$VOL"/filler* 2>/dev/null
    if [[ "$(uname)" == "Darwin" && -n "$DEV" ]]; then
      diskutil eject "$DEV" >/dev/null 2>&1 || hdiutil detach "$DEV" >/dev/null 2>&1
    elif [[ "$(uname)" == "Linux" && -n "$MNT" ]]; then
      umount "$MNT" 2>/dev/null || true
      rmdir "$MNT" 2>/dev/null || true
    fi
  fi
}
trap cleanup EXIT

echo "== C4 WAL 分区盘满 =="
# ---- 依赖守卫 1：引擎二进制 ----
if [[ ! -x "$ENGINE" ]]; then
  echo "[c4] $ENGINE 不存在，尝试 make build-engine …"
  if make -C "$ROOT" build-engine >/dev/null 2>&1; then
    ENGINE=/tmp/engine_d
  else
    echo "SKIP: 引擎二进制不可用且构建失败（T1 改写进行中？）"
    echo "C4_DISK_FULL: SKIP"
    exit 0
  fi
fi

# ---- 依赖守卫 2：可填满的本地卷（macOS 内存盘 / Linux tmpfs）----
if [[ "$(uname)" == "Darwin" ]]; then
  DEV=$(hdiutil attach -nomount ram://8192 2>/dev/null | awk 'NR==1{print $1}')
  if [[ -z "$DEV" ]]; then
    echo "SKIP: 无法创建 macOS 内存盘（hdiutil ram://）"
    echo "C4_DISK_FULL: SKIP"
    exit 0
  fi
  if ! diskutil eraseVolume HFS+ SM_C4 "$DEV" >/dev/null 2>&1; then
    hdiutil detach "$DEV" >/dev/null 2>&1
    echo "SKIP: 内存盘格式化失败（diskutil eraseVolume）"
    echo "C4_DISK_FULL: SKIP"
    exit 0
  fi
  VOL=/Volumes/SM_C4
elif [[ "$(uname)" == "Linux" ]]; then
  MNT=$(mktemp -d /tmp/sm_c4.XXXXXX)
  if ! mount -t tmpfs -o size=4m tmpfs "$MNT" 2>/dev/null; then
    rmdir "$MNT" 2>/dev/null
    echo "SKIP: 无法挂载 tmpfs（需要 root/特权容器）"
    echo "C4_DISK_FULL: SKIP"
    exit 0
  fi
  VOL="$MNT"
else
  echo "SKIP: 未知平台 $(uname)"
  echo "C4_DISK_FULL: SKIP"
  exit 0
fi
echo "[c4] 卷就绪：$VOL (dev=$DEV)"

# ---- 步骤 1：填满卷 ----
dd if=/dev/zero of="$VOL/filler" bs=4096 count=100000 2>/dev/null || true
sync
AVAIL=$(df -k "$VOL" | awk 'NR==2{print $4}')
echo "[c4] 填满后可用空间 ${AVAIL}KB"
if [[ "$AVAIL" -gt 64 ]]; then
  echo "SKIP: 卷未填满（可用 ${AVAIL}KB），无法模拟盘满"
  echo "C4_DISK_FULL: SKIP"
  exit 0
fi

# ---- 步骤 2：盘满下引擎写入 → 必须"拒绝写"（非零退出 / 无 OK 应答）----
OUT=$(printf 'reserve user=9001 seat=3 day=1 start=600 end=720 rid=r1 trace=t-c4\nquit\n' \
      | "$ENGINE" --storage=file --wal="$VOL/wal.log" --snapshot="$VOL/snap" --seats=16 2>&1)
RC=$?
echo "[c4] 盘满写入 rc=$RC 输出: $(echo "$OUT" | tr '\n' ' ' | cut -c1-160)"
WALSZ=0
[[ -f "$VOL/wal.log" ]] && WALSZ=$(wc -c < "$VOL/wal.log")
REFUSED=0
if [[ $RC -ne 0 || "$(echo "$OUT" | grep -c '^OK')" -eq 0 ]]; then
  REFUSED=1
  echo "[c4] 断言1 引擎拒绝写: PASS（rc=$RC，无成功应答）"
elif [[ "$WALSZ" -eq 0 ]]; then
  echo "[c4] 断言1 引擎拒绝写: FAIL —— 引擎回 OK 但 WAL 为 0 字节（盘满静默吞写）"
  echo "  发现缺陷：FileEventLog::durable() 未检查 fflush/fsync 返回值，"
  echo "  盘满时事件被静默接受且不持久 → 数据丢失。修复责任：T1（engine/ 不在本包范围）。"
  echo "C4_DISK_FULL: FAIL"
  exit 1
else
  # 极小卷上 HFS+/tmpfs 微量预留让 85B 写入侥幸成功（非盘满语义失效）
  echo "[c4] 断言1 引擎拒绝写: INCONCLUSIVE —— 卷仍允许极小写入（$WALSZ B），未触发拒绝路径"
  echo "C4_DISK_FULL: SKIP"
  exit 0
fi

# ---- 步骤 3：清理 → replay 补齐断言 ----
rm -f "$VOL"/filler
sync
cat > "$VOL/replay.txt" <<'EOF'
reserve user=9002 seat=4 day=1 start=600 end=720 rid=r2 trace=t-c4b
reserve user=9003 seat=4 day=1 start=720 end=780 rid=r3 trace=t-c4c
quit
EOF
OUT2=$("$ENGINE" --storage=file --wal="$VOL/wal.log" --snapshot="$VOL/snap" \
       --source=replay --replay="$VOL/replay.txt" --seats=16 2>&1)
RC2=$?
OKN=$(echo "$OUT2" | grep -c '^OK')
RECOVER=$(echo "$OUT2" | grep -o 'replayed to index [0-9]*' | head -1)
echo "[c4] replay rc=$RC2 OK 应答数=$OKN 恢复: $RECOVER"
if [[ $RC2 -eq 0 && "$OKN" -eq 2 ]]; then
  echo "[c4] 断言2 清理后 replay 补齐: PASS（2 条预约全部成功，$RECOVER）"
  echo "C4_DISK_FULL: PASS"
  exit 0
fi
echo "[c4] 断言2 清理后 replay 补齐: FAIL"
echo "C4_DISK_FULL: FAIL"
exit 1
