#!/usr/bin/env bash

set -Eeuo pipefail
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/_common.sh"

require_command aws
require_command curl

step "Checking the deployed CloudFormation stack"
stack_status="$(
    aws cloudformation describe-stacks \
        --region "${IMAGETRACKER_AWS_REGION}" \
        --stack-name "${IMAGETRACKER_STACK_NAME}" \
        --query 'Stacks[0].StackStatus' \
        --output text
)"

case "${stack_status}" in
    CREATE_COMPLETE|UPDATE_COMPLETE)
        ;;
    *)
        fail "Unexpected stack status: ${stack_status}"
        ;;
esac

api_url="$(
    aws cloudformation describe-stacks \
        --region "${IMAGETRACKER_AWS_REGION}" \
        --stack-name "${IMAGETRACKER_STACK_NAME}" \
        --query "Stacks[0].Outputs[?OutputKey=='ImageTrackerHttpApiUrl'].OutputValue | [0]" \
        --output text
)"
[[ -n "${api_url}" && "${api_url}" != "None" ]] || fail "ImageTracker API output is unavailable"

http_status="$(
    curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
        "${api_url}/v1/health"
)"
[[ "${http_status}" == "401" ]] || fail "Expected unauthenticated HTTP 401, received ${http_status}"

printf 'stack_status=%s\n' "${stack_status}"
printf 'unauthenticated_health_status=%s\n' "${http_status}"
