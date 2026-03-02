#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_SCRIPT="${ROOT_DIR}/scripts/revive-sccache.sh"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

mkdir -p "${tmp_dir}/case1/bin" "${tmp_dir}/case2/bin"

cat > "${tmp_dir}/case1/bin/ps" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

if [[ "$*" == "-C sccache -o pid=,stat=" ]]; then
  # Simulate "no matching process" behavior from ps -C.
  exit 1
fi

if [[ "$*" == "-eo pid=,ppid=,args=" ]]; then
  # No orphan wrappers either.
  exit 0
fi

echo "unexpected ps invocation: $*" >&2
exit 2
EOF

cat > "${tmp_dir}/case2/bin/ps" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

if [[ "$*" == "-C sccache -o pid=,stat=" ]]; then
  # Simulate an actual probe failure, not "no process".
  exit 2
fi

if [[ "$*" == "-eo pid=,ppid=,args=" ]]; then
  exit 0
fi

echo "unexpected ps invocation: $*" >&2
exit 2
EOF

cat > "${tmp_dir}/case1/bin/sccache" <<'EOF'
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

cat > "${tmp_dir}/case2/bin/sccache" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
echo "$*" >> "${SCCACHE_LOG}"
exit 0
EOF

cat > "${tmp_dir}/case1/bin/timeout" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ $# -lt 2 ]]; then
  echo "timeout mock expects at least 2 arguments" >&2
  exit 2
fi
shift
"$@"
EOF

cat > "${tmp_dir}/case2/bin/timeout" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ $# -lt 2 ]]; then
  echo "timeout mock expects at least 2 arguments" >&2
  exit 2
fi
shift
"$@"
EOF

chmod +x \
  "${tmp_dir}/case1/bin/ps" "${tmp_dir}/case1/bin/sccache" "${tmp_dir}/case1/bin/timeout" \
  "${tmp_dir}/case2/bin/ps" "${tmp_dir}/case2/bin/sccache" "${tmp_dir}/case2/bin/timeout"

sccache_log="${tmp_dir}/case1/sccache.log"
touch "${sccache_log}"

script_output="$(
PATH="${tmp_dir}/case1/bin:${PATH}" \
SCCACHE_LOG="${sccache_log}" \
SCACHE_TIMEOUT_SEC=1 \
bash "${TARGET_SCRIPT}" 2>&1
)"

if ! grep -Fq "[sccache-guard] healthy" <<< "${script_output}"; then
  echo "expected healthy output when no sccache process exists" >&2
  echo "${script_output}" >&2
  exit 1
fi

if ! grep -Fxq -- "--show-stats" "${sccache_log}"; then
  echo "expected sccache --show-stats health probe" >&2
  cat "${sccache_log}" >&2 || true
  exit 1
fi

if ! grep -Fxq -- "rustc -vV" "${sccache_log}"; then
  echo "expected sccache rustc health probe" >&2
  cat "${sccache_log}" >&2 || true
  exit 1
fi

if grep -Fxq -- "--stop-server" "${sccache_log}" || grep -Fxq -- "--start-server" "${sccache_log}"; then
  echo "did not expect restart actions in healthy no-process path" >&2
  cat "${sccache_log}" >&2 || true
  exit 1
fi

case2_log="${tmp_dir}/case2/sccache.log"
touch "${case2_log}"
set +e
case2_output="$(
PATH="${tmp_dir}/case2/bin:${PATH}" \
SCCACHE_LOG="${case2_log}" \
SCACHE_TIMEOUT_SEC=1 \
bash "${TARGET_SCRIPT}" 2>&1
)"
case2_status=$?
set -e

if [[ "${case2_status}" -ne 125 ]]; then
  echo "expected ps probe hard failure to exit 125, got ${case2_status}" >&2
  echo "${case2_output}" >&2
  exit 1
fi

if ! grep -Fq "[sccache-guard] ERROR: ps probe failed with status 2" <<< "${case2_output}"; then
  echo "expected explicit ps probe error message for status 2" >&2
  echo "${case2_output}" >&2
  exit 1
fi

echo "ok: revive-sccache handles no-process ps probe"
