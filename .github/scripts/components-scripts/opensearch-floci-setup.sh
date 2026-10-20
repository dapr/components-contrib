#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../../.."
export OPENSEARCH_FLOCI_PROJECT="${OPENSEARCH_FLOCI_PROJECT:-dapr-opensearch-floci}"
export OPENSEARCH_FLOCI_PORT="${OPENSEARCH_FLOCI_PORT:-4567}"
export OPENSEARCH_FLOCI_PROXY_BASE_PORT="${OPENSEARCH_FLOCI_PROXY_BASE_PORT:-9400}"
export OPENSEARCH_FLOCI_PROXY_MAX_PORT="${OPENSEARCH_FLOCI_PROXY_MAX_PORT:-9499}"
# Floci backend container names are global to the Docker daemon, not the
# compose project. Include a stable project hash in the domain name.
export OPENSEARCH_FLOCI_DOMAIN="${OPENSEARCH_FLOCI_DOMAIN:-dapr-search-$(printf '%s' "$OPENSEARCH_FLOCI_PROJECT" | cksum | cut -d ' ' -f 1)}"

if [[ -r /proc/sys/vm/max_map_count ]] && (( $(cat /proc/sys/vm/max_map_count) < 262144 )); then
    echo "OpenSearch requires vm.max_map_count >= 262144; ask the host administrator to configure it." >&2
    exit 1
fi
docker compose -f .github/infrastructure/docker-compose-opensearch-floci.yml \
    -p "$OPENSEARCH_FLOCI_PROJECT" up -d

python3 - <<'PY'
import json
import os
import pathlib
import subprocess
import time
import urllib.error
import urllib.request

project = os.environ["OPENSEARCH_FLOCI_PROJECT"]
domain = os.environ["OPENSEARCH_FLOCI_DOMAIN"]
base = "http://127.0.0.1:" + os.environ["OPENSEARCH_FLOCI_PORT"]
route = "/2021-01-01/opensearch/domain"
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def request(url, method="GET", data=None, timeout=10):
    body = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, method=method,
                                 headers={"Content-Type": "application/json"})
    with opener.open(req, timeout=timeout) as response:
        return json.load(response)


def wait_for(description, fn, seconds=240):
    deadline = time.monotonic() + seconds
    error = None
    while time.monotonic() < deadline:
        try:
            result = fn()
            if result:
                return result
        except (OSError, ValueError, urllib.error.URLError) as exc:
            error = exc
        time.sleep(2)
    raise RuntimeError(f"Timed out waiting for {description}: {error}")


wait_for("Floci /health", lambda: request(base + "/health"))
# Reject a domain owned by another project before Floci can try to reuse its
# globally named container.
existing = subprocess.check_output([
    "docker", "ps", "-aq", "--filter", "label=io.floci.service=opensearch",
    "--filter", f"label=io.floci.resource-id={domain}",
], text=True).split()
for ident in existing:
    backend = json.loads(subprocess.check_output(["docker", "inspect", ident]))[0]
    if project not in backend["NetworkSettings"]["Networks"]:
        raise RuntimeError(f"Domain {domain} belongs to another Docker project; choose a unique domain")
try:
    status = request(base + route + "/" + domain)["DomainStatus"]
except urllib.error.HTTPError as exc:
    # Floci 2.1.0 encodes ResourceNotFoundException as HTTP 409.
    error = json.loads(exc.read())
    if exc.code != 404 and error.get("__type") != "ResourceNotFoundException":
        raise
    status = request(base + route, "POST", {
        "DomainName": domain,
        "EngineVersion": "OpenSearch_2.19",
        "ClusterConfig": {"InstanceType": "m5.large.search", "InstanceCount": 1},
        "EBSOptions": {"EBSEnabled": True, "VolumeType": "gp2", "VolumeSize": 10},
    }, timeout=360)["DomainStatus"]

if not status.get("Endpoint"):
    raise RuntimeError("Floci returned no real OpenSearch endpoint; check Docker socket access and disable mock mode")

# Inspect only this domain on this project's network, never every container
# using an OpenSearch image (other projects may be running on the same host).
ids = subprocess.check_output([
    "docker", "ps", "-q", "--filter", f"network={project}",
    "--filter", "label=io.floci.service=opensearch",
    "--filter", f"label=io.floci.resource-id={domain}",
    "--filter", "label=io.floci.account=000000000000",
    "--filter", "label=io.floci.region=us-east-1",
], text=True).split()
if len(ids) != 1:
    raise RuntimeError(f"Expected one owned OpenSearch backend, got {len(ids)}")
container = json.loads(subprocess.check_output(["docker", "inspect", ids[0]]))[0]
port = container["NetworkSettings"]["Ports"]["9200/tcp"][0]["HostPort"]
endpoint = "http://127.0.0.1:" + port
wait_for("OpenSearch yellow/green health", lambda:
         request(endpoint + "/_cluster/health").get("status") in ("yellow", "green"))
plugins = request(endpoint + "/_cat/plugins?format=json")
if not any(plugin.get("component") == "opensearch-knn" for plugin in plugins):
    raise RuntimeError("The real OpenSearch k-NN plugin is required")

# A live data-plane probe prevents metadata-only/mock deployments passing setup.
index = "dapr-floci-probe-" + str(os.getpid())
try:
    request(endpoint + "/" + index, "PUT", {
        "settings": {"index": {"knn": True, "number_of_shards": 1, "number_of_replicas": 0}},
        "mappings": {"properties": {
            "title": {"type": "text"},
            "embedding": {"type": "knn_vector", "dimension": 2,
                          "method": {"name": "hnsw", "engine": "lucene", "space_type": "l2"}},
        }},
    })
    request(endpoint + "/" + index + "/_doc/probe?refresh=true", "PUT",
            {"title": "dapr opensearch probe", "embedding": [1.0, 0.0]})
    for query in ({"match": {"title": "opensearch"}},
                  {"knn": {"embedding": {"vector": [1.0, 0.0], "k": 1}}}):
        hits = request(endpoint + "/" + index + "/_search", "POST", {"query": query})
        if not any(hit["_id"] == "probe" for hit in hits["hits"]["hits"]):
            raise RuntimeError(f"OpenSearch data-plane probe failed: {hits}")
finally:
    request(endpoint + "/" + index, "DELETE")

values = {key: os.environ[key] for key in (
    "OPENSEARCH_FLOCI_PROJECT", "OPENSEARCH_FLOCI_PORT", "OPENSEARCH_FLOCI_DOMAIN",
    "OPENSEARCH_FLOCI_PROXY_BASE_PORT", "OPENSEARCH_FLOCI_PROXY_MAX_PORT",
)}
values["OPENSEARCH_ENDPOINT"] = endpoint
for path in (pathlib.Path("tests/config/search/.env"), pathlib.Path("tests/config/vector/.env")):
    existing = path.read_text().splitlines() if path.exists() else []
    existing = [line for line in existing if line.split("=", 1)[0] not in values]
    path.write_text("\n".join(existing + [f"{key}={value}" for key, value in values.items()]) + "\n")
if os.environ.get("GITHUB_ENV"):
    with open(os.environ["GITHUB_ENV"], "a") as output:
        output.write("".join(f"{key}={value}\n" for key, value in values.items()))
print(f"Real OpenSearch ready: {endpoint}; k-NN plugin, search and vector probes passed.")
print("Local environment: set -a; source tests/config/search/.env; set +a")
PY
