#!/bin/bash

set -e

usage() {
    printf 'Usage: %s FILE [PROJECT] [up|up-wait|logs|down] [PROFILE]\n' "$0" >&2
    exit 2
}

[[ $# -ge 1 && $# -le 4 ]] || usage
FILE="$1"
PROJECT="${2:-$FILE}"
ACTION="${3:-up}"

[[ "$FILE" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ && "$PROJECT" =~ ^[a-z0-9][a-z0-9_-]*$ ]] || usage
COMPOSE=(docker compose -f ".github/infrastructure/docker-compose-${FILE}.yml" -p "$PROJECT")
if [[ $# -eq 4 ]]; then
    [[ "$4" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || usage
    COMPOSE+=(--profile "$4")
    export COMPOSE_PROFILES=
fi

case "$ACTION" in
    up)
        "${COMPOSE[@]}" up -d
        ;;
    up-wait)
        ENV_LINES=$(
            "${COMPOSE[@]}" config --format json |
                jq -ers --arg profile "${4-}" '
                    if length == 1 and (.[0] | type == "object")
                    then .[0] else error("config") end
                    | if (.services | type == "object" and length > 0)
                        and ($profile == "" or any(.services[];
                            (.profiles? // []) | type == "array" and index($profile) != null))
                      then . else error("services") end
                    | if has("x-dapr-test-env") then .["x-dapr-test-env"] else {} end
                    | if type == "object" and all(to_entries[];
                        (.key | test("^[A-Za-z_][A-Za-z0-9_]*\\z"))
                        and (.value | type == "string" and (test("[\u0000\r\n]") | not)))
                      then to_entries | map("\(.key)=\(.value)") | join("\n")
                      else error("environment") end
                ' 2>/dev/null
            # Preserve Compose's exit code.
            STATUSES=("${PIPESTATUS[@]}")
            if (( STATUSES[0] != 0 )); then exit "${STATUSES[0]}"; fi
            exit "${STATUSES[1]}"
        ) || {
            STATUS=$?
            printf '%s\n' 'Unable to render or validate the Compose services, profile, or test environment (requires jq).' >&2
            exit "$STATUS"
        }
        if [[ -n "$ENV_LINES" ]]; then
            ENV_FILE="${GITHUB_ENV:-${DAPR_TEST_ENV_FILE:-}}"
            if [[ -z "$ENV_FILE" ]]; then
                printf '%s\n' 'Set GITHUB_ENV or DAPR_TEST_ENV_FILE for the Compose test environment.' >&2
                exit 1
            fi
            if [[ -s "$ENV_FILE" ]]; then
                printf '\n%s\n' "$ENV_LINES" >>"$ENV_FILE"
            else
                printf '%s\n' "$ENV_LINES" >>"$ENV_FILE"
            fi
        fi
        "${COMPOSE[@]}" up -d --wait --wait-timeout 120
        ;;
    logs)
        "${COMPOSE[@]}" ps --all
        "${COMPOSE[@]}" logs --no-color
        ;;
    down)
        # Preserve named volumes.
        "${COMPOSE[@]}" rm --stop --force --volumes
        "${COMPOSE[@]}" down
        ;;
    *)
        usage
        ;;
esac
