#!/usr/bin/env bash

set -Eeuo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

require_environment
cd_repository

exec "${IMAGETRACKER_PYTHON}" infra/scripts/mysql_one_file_import.py "$@"
