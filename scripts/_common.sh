#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
IMAGETRACKER_VENV="${REPOSITORY_ROOT}/.venv"
IMAGETRACKER_PYTHON="${IMAGETRACKER_VENV}/bin/python"
IMAGETRACKER_CLI="${IMAGETRACKER_VENV}/bin/imagetracker"

export IMAGETRACKER_AWS_REGION="${IMAGETRACKER_AWS_REGION:-us-east-2}"
export IMAGETRACKER_STACK_NAME="${IMAGETRACKER_STACK_NAME:-image-tracker-prod}"
export IMAGETRACKER_DB_SECRET_PARAMETER="${IMAGETRACKER_DB_SECRET_PARAMETER:-/imagetracker/prod/mysql}"

fail() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

step() {
    printf '\n%s\n' "$*"
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || fail "Required command is unavailable: $1"
}

require_environment() {
    [[ -x "${IMAGETRACKER_PYTHON}" ]] || fail "Run scripts/setup.sh first; .venv is missing."
    [[ -x "${IMAGETRACKER_CLI}" ]] || fail "Run scripts/setup.sh first; the imagetracker CLI is missing."
}

cd_repository() {
    cd -- "${REPOSITORY_ROOT}"
}
