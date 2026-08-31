#!/usr/bin/env bash

set -Eeuo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

require_environment
cd_repository

step "Checking additive ImageTracker database migrations"
"${IMAGETRACKER_PYTHON}" -B infra/scripts/migrate_enrichment.py "$@"
