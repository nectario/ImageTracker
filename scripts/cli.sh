#!/usr/bin/env bash

set -Eeuo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

require_environment
cd_repository

if (($# > 0)); then
    exec "${IMAGETRACKER_CLI}" "$@"
fi

step "CLI help"
"${IMAGETRACKER_CLI}" --help

step "CLI version"
"${IMAGETRACKER_CLI}" version

step "Local diagnostics"
"${IMAGETRACKER_CLI}" doctor
