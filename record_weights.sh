#!/bin/bash

set -o errexit
set -o pipefail

LIMIT="${1:--0}" # default is no limit
RPC="${2:-[::1]:7076}"

echo "# Generated at $(date -u), limit $LIMIT"

curl "$RPC" -fsS -H 'Content-Type: application/json' -d '{"action":"representatives"}' | \
    jq -r '.representatives | to_entries[] | select(.value != "0") | .key + " " + .value' | \
    sort -r -n -k2,2 | \
    head -n "$LIMIT"
