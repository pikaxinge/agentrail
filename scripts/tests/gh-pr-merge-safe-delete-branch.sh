#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_SCRIPT="${ROOT_DIR}/scripts/gh-pr-merge-safe.sh"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

mkdir -p "${tmp_dir}/case1/bin" "${tmp_dir}/case2/bin"

cat > "${tmp_dir}/case1/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

count_file="${GH_COUNT_FILE}"
count=0
if [[ -f "${count_file}" ]]; then
  count="$(cat "${count_file}")"
fi
count=$((count + 1))
printf '%s' "${count}" > "${count_file}"
echo "$*" >> "${GH_LOG}"

if [[ "${count}" -eq 1 ]]; then
  echo "! Pull request #82 was already merged" >&2
  echo "failed to run git: fatal: 'main' is already checked out at '/tmp/worktrees/main'" >&2
  exit 1
fi

if [[ "$*" == "pr merge 82 --merge" ]]; then
  exit 0
fi

echo "unexpected case1 invocation: $*" >&2
exit 2
EOF

cat > "${tmp_dir}/case2/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

echo "$*" >> "${GH_LOG}"
echo "GraphQL: merge mutation rejected" >&2
exit 1
EOF

chmod +x "${tmp_dir}/case1/bin/gh" "${tmp_dir}/case2/bin/gh"

case1_log="${tmp_dir}/case1/gh.log"
case1_count="${tmp_dir}/case1/count"
touch "${case1_log}" "${case1_count}"

PATH="${tmp_dir}/case1/bin:${PATH}" \
GH_LOG="${case1_log}" \
GH_COUNT_FILE="${case1_count}" \
bash "${TARGET_SCRIPT}" 82 --merge --delete-branch

if ! grep -Eq '^pr merge 82 --merge --delete-branch$' "${case1_log}"; then
  echo "expected primary merge invocation with --delete-branch not found" >&2
  cat "${case1_log}" >&2 || true
  exit 1
fi

if ! grep -Eq '^pr merge 82 --merge$' "${case1_log}"; then
  echo "expected fallback merge invocation without --delete-branch not found" >&2
  cat "${case1_log}" >&2 || true
  exit 1
fi

case2_log="${tmp_dir}/case2/gh.log"
touch "${case2_log}"
set +e
case2_output="$(
  PATH="${tmp_dir}/case2/bin:${PATH}" \
  GH_LOG="${case2_log}" \
  bash "${TARGET_SCRIPT}" 82 --merge --delete-branch 2>&1
)"
case2_status=$?
set -e

if [[ "${case2_status}" -ne 1 ]]; then
  echo "expected unrelated merge failure to preserve exit code 1, got ${case2_status}" >&2
  echo "${case2_output}" >&2
  exit 1
fi

case2_calls="$(wc -l < "${case2_log}" | tr -d ' ')"
if [[ "${case2_calls}" -ne 1 ]]; then
  echo "unexpected fallback retry for unrelated failure; calls=${case2_calls}" >&2
  cat "${case2_log}" >&2 || true
  exit 1
fi

if ! grep -Fq "GraphQL: merge mutation rejected" <<< "${case2_output}"; then
  echo "expected original failure output to be preserved" >&2
  echo "${case2_output}" >&2
  exit 1
fi

echo "ok: gh pr merge --delete-branch fallback behavior"
