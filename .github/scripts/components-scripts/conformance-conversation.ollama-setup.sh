#!/bin/bash

set -euo pipefail

readonly COMPOSE_FILE=".github/infrastructure/docker-compose-ollama.yml"
readonly PROJECT="ollama"
readonly MODEL="${OLLAMA_MODEL:-qwen2.5:0.5b}"

docker compose -f "${COMPOSE_FILE}" -p "${PROJECT}" up -d --wait
docker compose -f "${COMPOSE_FILE}" -p "${PROJECT}" exec -T ollama ollama pull "${MODEL}"
docker compose -f "${COMPOSE_FILE}" -p "${PROJECT}" exec -T ollama ollama run "${MODEL}" "Reply with OK." > /dev/null

if [[ -n "${GITHUB_ENV:-}" ]]; then
  {
    echo "OLLAMA_ENABLED=1"
    echo "OLLAMA_MODEL=${MODEL}"
  } >> "${GITHUB_ENV}"
fi
