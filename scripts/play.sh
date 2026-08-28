#!/usr/bin/env bash

set -Eeuo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

usage() {
    cat <<'EOF'
ImageTracker developer playground

Usage: ./scripts/play.sh <command> [arguments]

Commands:
  setup          Create/update the ignored Python environment
  cli [args...]  Run or forward ImageTracker CLI commands
  test           Run tests, contract validation, and pip checks
  api            Call the FastAPI health route in memory
  aws            Verify stack status and the protected HTTP boundary
  db             Run aggregate-only read-only MySQL checks
  package        Validate and package AWS infrastructure without deploying
  check          Run cli doctor, tests, local API, AWS, and DB checks
  help           Show this help

No `play.sh` command deploys AWS resources, applies database migrations, or
runs the legacy importer. The separate `migrate-db.sh` wrapper is intentionally
excluded from this playground.
EOF
}

command_name="${1:-help}"
if (($# > 0)); then
    shift
fi

case "${command_name}" in
    setup)
        exec "${SCRIPT_DIR}/setup.sh" "$@"
        ;;
    cli)
        exec "${SCRIPT_DIR}/cli.sh" "$@"
        ;;
    test)
        exec "${SCRIPT_DIR}/test.sh" "$@"
        ;;
    api)
        exec "${SCRIPT_DIR}/api-smoke.sh" "$@"
        ;;
    aws)
        exec "${SCRIPT_DIR}/aws-smoke.sh" "$@"
        ;;
    db)
        exec "${SCRIPT_DIR}/db-smoke.sh" "$@"
        ;;
    package)
        exec "${SCRIPT_DIR}/package-infra.sh" "$@"
        ;;
    check)
        "${SCRIPT_DIR}/cli.sh" doctor
        "${SCRIPT_DIR}/test.sh"
        "${SCRIPT_DIR}/api-smoke.sh"
        "${SCRIPT_DIR}/aws-smoke.sh"
        "${SCRIPT_DIR}/db-smoke.sh"
        ;;
    help|-h|--help)
        usage
        ;;
    *)
        usage >&2
        exit 2
        ;;
esac
