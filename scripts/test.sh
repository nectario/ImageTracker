#!/usr/bin/env bash

set -Eeuo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

require_environment
cd_repository

step "Running the complete test suite"
"${IMAGETRACKER_PYTHON}" -B -m pytest -q -p no:cacheprovider

step "Validating OpenAPI conventions"
"${IMAGETRACKER_PYTHON}" -B contracts/validate_openapi.py

step "Validating installed dependencies"
"${IMAGETRACKER_PYTHON}" -B -m pip check
