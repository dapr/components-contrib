#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../../.."
if [[ -z "${OPENSEARCH_FLOCI_PROJECT:-}" && -f tests/config/search/.env ]]; then
    set -a
    source tests/config/search/.env
    set +a
fi
export OPENSEARCH_FLOCI_PROJECT="${OPENSEARCH_FLOCI_PROJECT:-dapr-opensearch-floci}"
export OPENSEARCH_FLOCI_PORT="${OPENSEARCH_FLOCI_PORT:-4567}"
export OPENSEARCH_FLOCI_DOMAIN="${OPENSEARCH_FLOCI_DOMAIN:-dapr-search-$(printf '%s' "$OPENSEARCH_FLOCI_PROJECT" | cksum | cut -d ' ' -f 1)}"

# The managed backend is not a Compose service. Deleting the domain makes
# Floci stop/remove its backend before Compose removes the dedicated network.
export OPENSEARCH_FLOCI_CONTAINER
OPENSEARCH_FLOCI_CONTAINER="$(docker compose -f .github/infrastructure/docker-compose-opensearch-floci.yml \
    -p "$OPENSEARCH_FLOCI_PROJECT" ps -q floci)"
if [[ -n "$OPENSEARCH_FLOCI_CONTAINER" ]]; then
    python3 - <<'PY'
import json
import os
import subprocess
import urllib.error
import urllib.request

container = json.loads(subprocess.check_output(
    ["docker", "inspect", os.environ["OPENSEARCH_FLOCI_CONTAINER"]]))[0]
port = container["NetworkSettings"]["Ports"]["4566/tcp"][0]["HostPort"]
url = ("http://127.0.0.1:" + port +
       "/2021-01-01/opensearch/domain/" + os.environ["OPENSEARCH_FLOCI_DOMAIN"])
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
try:
    with opener.open(urllib.request.Request(url, method="DELETE"), timeout=60):
        pass
except urllib.error.HTTPError as exc:
    # Floci 2.1.0 encodes ResourceNotFoundException as HTTP 409.
    error = json.loads(exc.read())
    if exc.code != 404 and error.get("__type") != "ResourceNotFoundException":
        raise
PY
fi
docker compose -f .github/infrastructure/docker-compose-opensearch-floci.yml \
    -p "$OPENSEARCH_FLOCI_PROJECT" down

python3 - <<'PY'
import os
import pathlib

owned_keys = {
    "OPENSEARCH_FLOCI_PROJECT", "OPENSEARCH_FLOCI_PORT", "OPENSEARCH_FLOCI_DOMAIN",
    "OPENSEARCH_FLOCI_PROXY_BASE_PORT", "OPENSEARCH_FLOCI_PROXY_MAX_PORT",
    "OPENSEARCH_ENDPOINT",
}
for path in (pathlib.Path("tests/config/search/.env"), pathlib.Path("tests/config/vector/.env")):
    if not path.exists():
        continue
    lines = path.read_text().splitlines()
    values = dict(line.split("=", 1) for line in lines if "=" in line and not line.startswith("#"))
    if values.get("OPENSEARCH_FLOCI_PROJECT") != os.environ["OPENSEARCH_FLOCI_PROJECT"]:
        continue
    retained = [line for line in lines if line.split("=", 1)[0] not in owned_keys]
    if retained:
        path.write_text("\n".join(retained) + "\n")
    else:
        path.unlink()
PY
