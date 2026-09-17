#!/bin/bash

set -e

FILE="$1"
PROJECT="${2:-$FILE}"
# Optional third argument, normally --wait, so a compose file that defines a
# healthcheck can gate the tests on the broker actually being ready. Existing
# callers pass nothing and are unaffected.
WAIT="${3:-}"

docker compose -f .github/infrastructure/docker-compose-${FILE}.yml -p ${PROJECT} up -d ${WAIT}
