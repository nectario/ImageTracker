#!/usr/bin/env bash

set -Eeuo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

require_command npm
require_command python
cd -- "${REPOSITORY_ROOT}/infra"

if [[ ! -d node_modules ]]; then
    step "Installing pinned infrastructure tooling"
    npm ci
fi

step "Validating the infrastructure guardrails"
npm run validate

step "Building the ignored production Serverless package without deploying"
npm run package

step "Validating generated CloudFormation"
python scripts/validate_foundation.py \
    --stage prod \
    --packaged-template .build/.serverless/cloudformation-template-update-stack.json
