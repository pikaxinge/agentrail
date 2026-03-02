#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_SCRIPT="${ROOT_DIR}/scripts/revive-sccache.sh"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

mkdir -p "${tmp_dir}/bin"
sccache_log="${tmp_dir}/sccache.log"
ps_log="${tmp_dir}/ps.log"
touch "${sccache_log}" "${ps_log}"

cat > "${tmp_dir}/bin/ps" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
echo "$*" >> "${PS_LOG}"

if [[ "$*" == "-C sccache -o pid=,stat=" ]]; then
  # No paused sccache processes in this scenario.
  cat <<'OUT'
610 S
611 S
OUT
  exit 0
fi

if [[ "$*" == "-eo pid=,ppid=,args=" ]]; then
  # PID 610 is an orphan wrapper that should be cleaned up.
  # PID 611 is not orphaned and should be ignored.
  # PID 612 is orphaned but not a rustc wrapper and should be ignored.
  cat <<'OUT'
610 1 sccache /usr/bin/rustc --crate-name libc
611 42 sccache /usr/bin/rustc --crate-name serde_core
612 1 sccache --start-server
OUT
  exit 0
fi

echo "unexpected ps invocation: $*" >&2
exit 2
EOF

cat > "${tmp_dir}/bin/sccache" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
echo "$*" >> "${SCCACHE_LOG}"
case "${1:-}" in
  --show-stats|--stop-server|--start-server)
    exit 0
    ;;
  rustc)
    exit 0
    ;;
  *)
    exit 0
    ;;
esac
EOF

cat > "${tmp_dir}/bin/timeout" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ $# -lt 2 ]]; then
  echo "timeout mock expects at least 2 arguments" >&2
  exit 2
fi
shift
"$@"
EOF

chmod +x "${tmp_dir}/bin/ps" "${tmp_dir}/bin/sccache" "${tmp_dir}/bin/timeout"

# Sanity check the orphan-detection pattern used by the target script.
orphan_probe="$(PS_LOG="${ps_log}" "${tmp_dir}/bin/ps" -eo pid=,ppid=,args= | awk '
  $2 == 1 && $3 == "sccache" {
    for (i = 4; i <= NF; i++) {
      if ($i ~ /(^|\/)rustc$/) {
        print $1
        break
      }
    }
  }
' | tr '\n' ' ')"
if [[ "${orphan_probe}" != *"610"* ]]; then
  echo "test setup error: orphan probe did not detect pid 610" >&2
  exit 1
fi

script_output="$(
PATH="${tmp_dir}/bin:${PATH}" \
SCCACHE_LOG="${sccache_log}" \
PS_LOG="${ps_log}" \
SCACHE_TIMEOUT_SEC=1 \
bash "${TARGET_SCRIPT}" 2>&1
)"

if ! grep -Eq 'terminating orphan sccache wrappers: .*610' <<< "${script_output}"; then
  echo "expected orphan wrapper pid 610 cleanup log, but it was not emitted" >&2
  echo "--- script output ---" >&2
  echo "${script_output}" >&2
  echo "--- ps log ---" >&2
  cat "${ps_log}" >&2 || true
  echo "--- sccache log ---" >&2
  cat "${sccache_log}" >&2 || true
  exit 1
fi

if grep -Eq 'terminating orphan sccache wrappers: .*611' <<< "${script_output}"; then
  echo "non-orphan wrapper pid 611 should not be terminated" >&2
  exit 1
fi

if grep -Eq 'terminating orphan sccache wrappers: .*612' <<< "${script_output}"; then
  echo "non-wrapper orphan pid 612 should not be terminated" >&2
  exit 1
fi

echo "ok: orphan wrapper cleanup"
