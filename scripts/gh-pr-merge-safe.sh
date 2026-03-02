#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  gh-pr-merge-safe.sh <pr-number> [gh pr merge args...]

Description:
  Runs `gh pr merge` and guards against local multi-worktree cleanup failure
  triggered by `--delete-branch` after merge completion.

Default merge args when omitted:
  --merge --delete-branch
EOF
}

contains_delete_branch_flag() {
  local arg
  for arg in "$@"; do
    if [[ "${arg}" == "--delete-branch" ]]; then
      return 0
    fi
  done
  return 1
}

is_checked_out_branch_cleanup_error() {
  local output="$1"
  grep -Eq "fatal: '.+' is already checked out at '.+'" <<< "${output}"
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -lt 1 ]]; then
  usage >&2
  exit 2
fi

if ! command -v gh >/dev/null 2>&1; then
  echo "[gh-pr-merge-safe] gh command not found in PATH" >&2
  exit 127
fi

pr_number="$1"
shift

merge_args=("$@")
if [[ ${#merge_args[@]} -eq 0 ]]; then
  merge_args=(--merge --delete-branch)
fi

tmp_output="$(mktemp)"
trap 'rm -f "${tmp_output}"' EXIT

if gh pr merge "${pr_number}" "${merge_args[@]}" >"${tmp_output}" 2>&1; then
  cat "${tmp_output}"
  exit 0
else
  merge_status=$?
  merge_output="$(cat "${tmp_output}")"
fi

if ! contains_delete_branch_flag "${merge_args[@]}" \
  || ! is_checked_out_branch_cleanup_error "${merge_output}"; then
  printf '%s\n' "${merge_output}" >&2
  exit "${merge_status}"
fi

fallback_args=()
for arg in "${merge_args[@]}"; do
  if [[ "${arg}" != "--delete-branch" ]]; then
    fallback_args+=("${arg}")
  fi
done

printf '%s\n' "${merge_output}" >&2
echo "[gh-pr-merge-safe] detected local branch cleanup conflict; retrying without --delete-branch" >&2

gh pr merge "${pr_number}" "${fallback_args[@]}"
