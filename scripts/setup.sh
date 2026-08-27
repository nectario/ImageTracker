#!/usr/bin/env bash

set -Eeuo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

require_command python
cd_repository

if [[ ! -x "${IMAGETRACKER_PYTHON}" ]]; then
    step "Creating the local Python environment"
    python -m venv "${IMAGETRACKER_VENV}"
fi

step "Installing ImageTracker and development dependencies"
"${IMAGETRACKER_PYTHON}" -m pip install --upgrade pip
"${IMAGETRACKER_PYTHON}" -m pip install -e ".[dev]"

step "Environment ready"
"${IMAGETRACKER_CLI}" doctor
