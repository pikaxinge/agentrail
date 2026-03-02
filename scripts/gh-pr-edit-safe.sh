#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  gh-pr-edit-safe.sh <pr-number> [gh pr edit args...]

Description:
  Attempts `gh pr edit` first. If it fails with the known GraphQL
  projectCards deprecation error, falls back to REST PATCH via:
    gh api repos/<owner>/<repo>/pulls/<pr-number> -X PATCH --input -

Fallback supports:
  --title <text>
  --body <text>
  --body-file <path>
EOF
}

detect_owner_repo() {
  local remote_url owner repo
  remote_url="$(git config --get remote.origin.url || true)"

  if [[ "${remote_url}" =~ ^git@github\.com:([^/]+)/([^/]+)(\.git)?$ ]]; then
    owner="${BASH_REMATCH[1]}"
    repo="${BASH_REMATCH[2]}"
    repo="${repo%.git}"
    printf '%s/%s\n' "${owner}" "${repo}"
    return 0
  fi

  if [[ "${remote_url}" =~ ^ssh://git@github\.com/([^/]+)/([^/]+)(\.git)?$ ]]; then
    owner="${BASH_REMATCH[1]}"
    repo="${BASH_REMATCH[2]}"
    repo="${repo%.git}"
    printf '%s/%s\n' "${owner}" "${repo}"
    return 0
  fi

  if [[ "${remote_url}" =~ ^https?://github\.com/([^/]+)/([^/]+)(\.git)?$ ]]; then
    owner="${BASH_REMATCH[1]}"
    repo="${BASH_REMATCH[2]}"
    repo="${repo%.git}"
    printf '%s/%s\n' "${owner}" "${repo}"
    return 0
  fi

  return 1
}

is_projectcards_deprecation_error() {
  local message="$1"
  grep -Fq 'Projects (classic) is being deprecated' <<< "${message}" \
    && grep -Eq 'repository\.(pullRequest|issue)\.projectCards' <<< "${message}"
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
  echo "[gh-pr-edit-safe] gh command not found in PATH" >&2
  exit 127
fi

pr_number="$1"
shift
edit_args=("$@")

tmp_output="$(mktemp)"
trap 'rm -f "${tmp_output}"' EXIT

if gh pr edit "${pr_number}" "${edit_args[@]}" >"${tmp_output}" 2>&1; then
  cat "${tmp_output}"
  exit 0
else
  edit_status=$?
  edit_output="$(cat "${tmp_output}")"
fi

if ! is_projectcards_deprecation_error "${edit_output}"; then
  printf '%s\n' "${edit_output}" >&2
  exit "${edit_status}"
fi

title_set=0
body_set=0
title_value=""
body_value=""
body_file_path=""
unsupported_args=()

i=0
while [[ ${i} -lt ${#edit_args[@]} ]]; do
  arg="${edit_args[$i]}"
  case "${arg}" in
    --title)
      if [[ $((i + 1)) -ge ${#edit_args[@]} ]]; then
        echo "[gh-pr-edit-safe] missing value for --title" >&2
        exit 2
      fi
      ((i += 1))
      title_value="${edit_args[$i]}"
      title_set=1
      ;;
    --body)
      if [[ $((i + 1)) -ge ${#edit_args[@]} ]]; then
        echo "[gh-pr-edit-safe] missing value for --body" >&2
        exit 2
      fi
      ((i += 1))
      body_value="${edit_args[$i]}"
      body_set=1
      ;;
    --body-file)
      if [[ $((i + 1)) -ge ${#edit_args[@]} ]]; then
        echo "[gh-pr-edit-safe] missing value for --body-file" >&2
        exit 2
      fi
      ((i += 1))
      body_file_path="${edit_args[$i]}"
      if [[ ! -f "${body_file_path}" ]]; then
        echo "[gh-pr-edit-safe] body file does not exist: ${body_file_path}" >&2
        exit 2
      fi
      body_value="$(cat "${body_file_path}")"
      body_set=1
      ;;
    *)
      unsupported_args+=("${arg}")
      ;;
  esac
  ((i += 1))
done

if [[ "${title_set}" -eq 0 && "${body_set}" -eq 0 ]]; then
  echo "[gh-pr-edit-safe] fallback only supports --title/--body/--body-file; no supported fields were provided" >&2
  printf '%s\n' "${edit_output}" >&2
  exit "${edit_status}"
fi

if [[ ${#unsupported_args[@]} -gt 0 ]]; then
  echo "[gh-pr-edit-safe] fallback aborted due to unsupported args: ${unsupported_args[*]}" >&2
  printf '%s\n' "${edit_output}" >&2
  exit "${edit_status}"
fi

if ! owner_repo="$(detect_owner_repo)"; then
  echo "[gh-pr-edit-safe] could not determine owner/repo from remote.origin.url" >&2
  exit 2
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "[gh-pr-edit-safe] jq command not found in PATH (required for fallback payload)" >&2
  exit 127
fi

payload="$(
  jq -nc \
    --argjson title_set "${title_set}" \
    --argjson body_set "${body_set}" \
    --arg title "${title_value}" \
    --arg body "${body_value}" \
    '
      (if $title_set == 1 then {title: $title} else {} end)
      +
      (if $body_set == 1 then {body: $body} else {} end)
    '
)"

printf '%s' "${payload}" \
  | gh api "repos/${owner_repo}/pulls/${pr_number}" -X PATCH --input - >/dev/null

echo "[gh-pr-edit-safe] applied REST PATCH fallback for PR #${pr_number}"
