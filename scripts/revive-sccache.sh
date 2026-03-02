#!/usr/bin/env bash
set -euo pipefail

TIMEOUT_SEC="${SCACHE_TIMEOUT_SEC:-10}"

collect_orphan_wrapper_pids() {
  ps -eo pid=,ppid=,args= 2>/dev/null \
    | awk '
      $2 == 1 && $3 == "sccache" {
        for (i = 4; i <= NF; i++) {
          if ($i ~ /(^|\/)rustc$/) {
            print $1
            break
          }
        }
      }
    '
}

if ! command -v sccache >/dev/null 2>&1; then
  echo "[sccache-guard] sccache not found; nothing to recover"
  exit 0
fi

# Resume paused sccache processes (STAT contains T).
paused_pids="$(ps -C sccache -o pid=,stat= 2>/dev/null | awk '$2 ~ /T/ {print $1}' | tr '\n' ' ')"
if [[ -n "${paused_pids}" ]]; then
  echo "[sccache-guard] resuming paused sccache pids: ${paused_pids}"
  kill -CONT ${paused_pids} 2>/dev/null || true
fi

# Clean orphaned sccache wrapper processes left after interrupted cargo runs.
orphan_wrapper_pids="$(collect_orphan_wrapper_pids | tr '\n' ' ')"
if [[ -n "${orphan_wrapper_pids}" ]]; then
  echo "[sccache-guard] terminating orphan sccache wrappers: ${orphan_wrapper_pids}"
  kill ${orphan_wrapper_pids} 2>/dev/null || true
  sleep 1

  remaining_orphans="$(collect_orphan_wrapper_pids | tr '\n' ' ')"
  if [[ -n "${remaining_orphans}" ]]; then
    echo "[sccache-guard] force-killing stubborn orphan wrappers: ${remaining_orphans}"
    kill -9 ${remaining_orphans} 2>/dev/null || true
  fi
fi

# If the daemon is unhealthy, restart once.
if ! timeout "${TIMEOUT_SEC}" sccache --show-stats >/dev/null 2>&1; then
  echo "[sccache-guard] sccache did not respond; restarting daemon"
  sccache --stop-server >/dev/null 2>&1 || true
  sccache --start-server >/dev/null 2>&1 || true
fi

# Final health check.
if ! timeout "${TIMEOUT_SEC}" sccache rustc -vV >/dev/null 2>&1; then
  echo "[sccache-guard] ERROR: sccache wrapper still unhealthy after recovery" >&2
  exit 124
fi

echo "[sccache-guard] healthy"
