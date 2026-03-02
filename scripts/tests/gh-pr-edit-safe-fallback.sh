#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TARGET_SCRIPT="${ROOT_DIR}/scripts/gh-pr-edit-safe.sh"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

mkdir -p "${tmp_dir}/case1/bin" "${tmp_dir}/case2/bin"

cat > "${tmp_dir}/case1/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

echo "$*" >> "${GH_LOG}"

if [[ "${1:-}" == "pr" && "${2:-}" == "edit" ]]; then
  echo "GraphQL: Projects (classic) is being deprecated in favor of the new Projects experience. (repository.pullRequest.projectCards)" >&2
  exit 1
fi

if [[ "${1:-}" == "api" ]]; then
  cat > "${GH_PAYLOAD}"
  exit 0
fi

echo "unexpected gh invocation: $*" >&2
exit 2
EOF

cat > "${tmp_dir}/case2/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

echo "$*" >> "${GH_LOG}"

if [[ "${1:-}" == "pr" && "${2:-}" == "edit" ]]; then
  echo "GraphQL: some unrelated failure" >&2
  exit 1
fi

if [[ "${1:-}" == "api" ]]; then
  echo "api should not be called for unrelated failures" >&2
  exit 2
fi

echo "unexpected gh invocation: $*" >&2
exit 2
EOF

chmod +x "${tmp_dir}/case1/bin/gh" "${tmp_dir}/case2/bin/gh"

repo_dir="${tmp_dir}/repo"
git init -q "${repo_dir}"
git -C "${repo_dir}" remote add origin "https://github.com/test-owner/test-repo.git"

gh_log="${tmp_dir}/case1/gh.log"
gh_payload="${tmp_dir}/case1/payload.json"
touch "${gh_log}" "${gh_payload}"

body_file="${tmp_dir}/pr-body.md"
cat > "${body_file}" <<'EOF'
Summary line

Detailed notes.
EOF

PATH="${tmp_dir}/case1/bin:${PATH}" \
GH_LOG="${gh_log}" \
GH_PAYLOAD="${gh_payload}" \
bash -c 'cd "$1" && bash "$2" 72 --title "safe title" --body-file "$3"' _ "${repo_dir}" "${TARGET_SCRIPT}" "${body_file}"

if ! grep -Eq '^pr edit 72 --title safe title --body-file .*pr-body\.md$' "${gh_log}"; then
  echo "expected gh pr edit invocation not found" >&2
  cat "${gh_log}" >&2 || true
  exit 1
fi

if ! grep -Eq '^api repos/test-owner/test-repo/pulls/72 -X PATCH --input -$' "${gh_log}"; then
  echo "expected gh api fallback invocation not found" >&2
  cat "${gh_log}" >&2 || true
  exit 1
fi

if ! jq -e '.title == "safe title" and .body == "Summary line\n\nDetailed notes."' "${gh_payload}" >/dev/null; then
  echo "fallback payload did not preserve expected title/body fields" >&2
  cat "${gh_payload}" >&2 || true
  exit 1
fi

case2_log="${tmp_dir}/case2/gh.log"
touch "${case2_log}"
set +e
case2_output="$(
  PATH="${tmp_dir}/case2/bin:${PATH}" \
  GH_LOG="${case2_log}" \
  bash -c 'cd "$1" && bash "$2" 72 --title "safe title"' _ "${repo_dir}" "${TARGET_SCRIPT}" 2>&1
)"
case2_status=$?
set -e

if [[ "${case2_status}" -ne 1 ]]; then
  echo "expected non-projectCards error to preserve exit code 1, got ${case2_status}" >&2
  exit 1
fi

if grep -Eq '^api ' "${case2_log}"; then
  echo "fallback API call should not run for unrelated errors" >&2
  cat "${case2_log}" >&2 || true
  exit 1
fi

if ! grep -Fq "GraphQL: some unrelated failure" <<< "${case2_output}"; then
  echo "original non-projectCards error message was not surfaced" >&2
  echo "${case2_output}" >&2
  exit 1
fi

cat > "${tmp_dir}/case2/bin/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

echo "$*" >> "${GH_LOG}"

if [[ "${1:-}" == "pr" && "${2:-}" == "edit" ]]; then
  echo "GraphQL: Projects (classic) is being deprecated in favor of the new Projects experience. (repository.pullRequest.projectCards)" >&2
  exit 1
fi

if [[ "${1:-}" == "api" ]]; then
  echo "api should not run when unsupported args are present" >&2
  exit 2
fi

echo "unexpected gh invocation: $*" >&2
exit 2
EOF
chmod +x "${tmp_dir}/case2/bin/gh"

: > "${case2_log}"
set +e
case3_output="$(
  PATH="${tmp_dir}/case2/bin:${PATH}" \
  GH_LOG="${case2_log}" \
  bash -c 'cd "$1" && bash "$2" 72 --title "safe title" --add-label bug' _ "${repo_dir}" "${TARGET_SCRIPT}" 2>&1
)"
case3_status=$?
set -e

if [[ "${case3_status}" -ne 1 ]]; then
  echo "expected unsupported-args fallback abort to keep status 1, got ${case3_status}" >&2
  echo "${case3_output}" >&2
  exit 1
fi

if grep -Eq '^api ' "${case2_log}"; then
  echo "fallback API call should not run when unsupported args are present" >&2
  cat "${case2_log}" >&2 || true
  exit 1
fi

if ! grep -Fq "fallback aborted due to unsupported args" <<< "${case3_output}"; then
  echo "expected unsupported-args diagnostic message was not emitted" >&2
  echo "${case3_output}" >&2
  exit 1
fi

echo "ok: gh pr edit fallback, error pass-through, and unsupported-args guard"
