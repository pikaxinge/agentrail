#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  codex-guarded-exec.sh --workdir <path> --timeout-sec <seconds> -- [codex exec args...]

Description:
  Runs `codex exec` and fails fast when no workspace mutation is detected
  within the configured timeout window.

Mutation is detected when either:
  - `git status --porcelain` is non-empty, or
  - `HEAD` changes from the initial commit.

Exit codes:
  124  no workspace mutation before timeout
  other propagated from `codex exec`
EOF
}

workdir=""
timeout_sec=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workdir)
      workdir="${2:-}"
      shift 2
      ;;
    --timeout-sec)
      timeout_sec="${2:-}"
      shift 2
      ;;
    --)
      shift
      break
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[guard] unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "${workdir}" ]]; then
  echo "[guard] missing required --workdir" >&2
  usage >&2
  exit 2
fi

if [[ -z "${timeout_sec}" ]]; then
  echo "[guard] missing required --timeout-sec" >&2
  usage >&2
  exit 2
fi

if ! [[ "${timeout_sec}" =~ ^[0-9]+$ ]] || [[ "${timeout_sec}" -eq 0 ]]; then
  echo "[guard] --timeout-sec must be a positive integer" >&2
  exit 2
fi

if ! command -v codex >/dev/null 2>&1; then
  echo "[guard] codex command not found in PATH" >&2
  exit 127
fi

if ! git -C "${workdir}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "[guard] workdir is not a git repository: ${workdir}" >&2
  exit 2
fi

baseline_head="$(git -C "${workdir}" rev-parse HEAD)"
started_at="$(date +%s)"
saw_mutation=0

(cd "${workdir}" && codex exec "$@") &
child_pid=$!

cleanup_child() {
  local pid="$1"
  if kill -0 "${pid}" 2>/dev/null; then
    kill "${pid}" 2>/dev/null || true
    sleep 1
    if kill -0 "${pid}" 2>/dev/null; then
      kill -9 "${pid}" 2>/dev/null || true
    fi
  fi
}

while kill -0 "${child_pid}" 2>/dev/null; do
  sleep 5
  if ! kill -0 "${child_pid}" 2>/dev/null; then
    break
  fi

  if [[ -n "$(git -C "${workdir}" status --porcelain)" ]]; then
    saw_mutation=1
    break
  fi

  current_head="$(git -C "${workdir}" rev-parse HEAD)"
  if [[ "${current_head}" != "${baseline_head}" ]]; then
    saw_mutation=1
    break
  fi

  now="$(date +%s)"
  if (( now - started_at >= timeout_sec )); then
    echo "[guard] no workspace mutation detected within ${timeout_sec}s; stopping codex exec" >&2
    cleanup_child "${child_pid}"
    wait "${child_pid}" 2>/dev/null || true
    exit 124
  fi
done

wait "${child_pid}"
exit_code=$?

if [[ "${saw_mutation}" -eq 0 ]]; then
  echo "[guard] codex exec exited without workspace mutation" >&2
fi

exit "${exit_code}"
