# Git Configuration Store

A Dapr configuration store backed by a git repository. The store clones the
configured repo on `Init`, polls the upstream for new commits at a configurable
interval, and notifies subscribers when keys change.

> **Status:** alpha. Requires a paired `dapr/dapr` PR registering
> `configuration.git` (e.g. `cmd/daprd/components/configuration_git.go`)
> before it is usable from a sidecar.

## When to use it

The component is intended for **operator-driven configuration repos**
(prompts, instructions, agent definitions, feature flags) where:

- The data is small (≤ 1 MiB per file by default).
- Changes are infrequent (commits, not high-frequency writes).
- A polling delay of a few minutes is acceptable.

It is **not** suitable for source-code monorepos or hot-path data planes.

## Quick start

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: configstore
spec:
  type: configuration.git
  version: v1
  metadata:
    - name: remoteUrl
      value: "https://github.com/example/agent-config.git"
    - name: branch
      value: "main"
    - name: mappingMode
      value: "file"
    - name: pollInterval
      value: "5m"
```

## Mapping modes

The `mappingMode` metadata field selects how files in the repository become
configuration items. Matching is case-insensitive. **Non-matching files in
the configured scope are a hard error** — if the directory contains a mix of
file types, either narrow `path` to the homogeneous subset or use
`mappingMode: file`.

### `file` (default)

Each file becomes one config item. The relative POSIX path is the key,
the file contents the value.

```
repo/
  agents/weather/agent_role.txt   → key "agents/weather/agent_role.txt"
  agents/weather/agent_goal.txt   → key "agents/weather/agent_goal.txt"
```

Recommended when the consumer (e.g. dapr-agents) expects scalar config keys.

### `agentYaml`

Each `*.yaml`, `*.yml`, or `*.json` file is parsed as a flat top-level map.
Each top-level field becomes a key prefixed by the filename stem with
directory separators replaced by `_`. **Non-YAML/JSON files in scope cause
Init to fail.**

```yaml
# repo/agents/weather.yaml
agent_role: Weather expert
agent_goal: Help users plan trips
agent_instructions:
  - be concise
  - cite sources
```

produces keys:

```
agents_weather/agent_role            = "Weather expert"
agents_weather/agent_goal            = "Help users plan trips"
agents_weather/agent_instructions    = (YAML-serialised list — round-trip via yaml.Unmarshal)
```

### `prompty`

Each `*.prompty` file's YAML frontmatter and body are split.
Frontmatter fields produce `<stem>/<field>` keys; the body is emitted as
`<stem>/agent_system_prompt`. See the [Prompty spec](https://github.com/microsoft/prompty)
for the file format. **Non-`.prompty` files in scope cause Init to fail.**

```
---
name: Weather Agent
agent_role: Weather expert
agent_goal: Help users plan trips
---
You are a friendly weather assistant.
```

produces:

```
weather/name                  = "Weather Agent"
weather/agent_role            = "Weather expert"
weather/agent_goal            = "Help users plan trips"
weather/agent_system_prompt   = "You are a friendly weather assistant."
```

## Authentication

There is **no explicit `authMode` selector** — the active profile is inferred
from which fields are set:

1. `appId` set → **GitHub App** profile.
2. URL begins with `git@` or `ssh://` → **SSH** profile.
3. `token` set → **PAT** profile.
4. Otherwise → no auth (public HTTPS or local `file://`).

Sensitive fields should be sourced from a configured secret store via
`secretKeyRef`.

### PAT (HTTPS basic auth)

Works with both GitHub classic PATs (`ghp_…`) and fine-grained PATs
(`github_pat_…`).

```yaml
spec:
  type: configuration.git
  version: v1
  metadata:
    - name: remoteUrl
      value: "https://github.com/example/private-config.git"
    - name: token
      secretKeyRef:
        name: github-pat
        key: token
auth:
  secretStore: <SECRET_STORE_NAME>
```

### SSH

```yaml
spec:
  type: configuration.git
  version: v1
  metadata:
    - name: remoteUrl
      value: "git@github.com:example/private-config.git"
    - name: privateKey
      secretKeyRef:
        name: git-ssh-deploy-key
        key: privateKey
    - name: knownHosts
      secretKeyRef:
        name: git-ssh-known-hosts
        key: knownHosts
auth:
  secretStore: <SECRET_STORE_NAME>
```

`insecureIgnoreHostKey: true` disables host-key verification — only use for
local development. A loud warning is logged at startup when this is enabled.

### GitHub App

```yaml
spec:
  type: configuration.git
  version: v1
  metadata:
    - name: remoteUrl
      value: "https://github.com/example/private-config.git"
    - name: appId
      value: "123456"
    - name: installationId
      value: "78901234"
    - name: privateKey
      secretKeyRef:
        name: github-app-key
        key: privateKey
auth:
  secretStore: <SECRET_STORE_NAME>
```

Accepts both PKCS#1 and PKCS#8 PEM-encoded RSA keys. The component mints an
RS256 JWT, exchanges it for a 1-hour installation token, and refreshes the
token `refreshSkew` (default 5m) before expiry.

## Behavioural notes

- **`Get` is cache-only.** Returns the most-recently polled snapshot and may
  be up to `pollInterval` old. Does not contact the remote.
- **`Subscribe` replays current state by default.** When `emitInitialState`
  is true (the default), the handler receives the current snapshot
  synchronously before `Subscribe` returns.
- **Returned items are deep copies** — callers own them and may mutate
  freely without affecting the store.
- **Deletions are signalled** via `Item{Value: "", Metadata: {"deleted": "true"}}`,
  same shape as the kubernetes ConfigMap configuration store.
- **Read-only**: the component never writes to the upstream. Configuration
  changes must be made by committing through your normal git workflow (PR
  review, branch protection, etc.).
- **Rate-limit handling**: on HTTP 429 from the GitHub API (used by the
  GitHub App installation-token exchange), or a transport-level rate-limit
  error from go-git, the poll loop pauses for `rateLimitRetryAfter`
  (default 5m) — or the server-supplied `Retry-After` when present —
  before the next tick.
- **`.git/` is always excluded** from the worktree walk regardless of
  `includeHidden`, so credentials in `.git/config` cannot leak. A `path`
  containing the `.git` segment is rejected at Init.
- **Per-file size cap.** Files larger than `maxFileSize` (default 1 MiB)
  are skipped with a warning.
- **LRU snapshot cache.** Per-subscriber diffs reuse cached snapshots keyed
  by commit SHA (default size 4). On a miss the diff over-emits a single
  batch — idempotent on the receiver.

## Limitations

- **Single GitHub App installation per component.** Multi-tenant routing
  (different repos via different installations on the same component) is
  not supported.

## Daprd registration

This component requires a paired `dapr/dapr` PR registering
`configuration.git` so that daprd can load it via component spec. See
`cmd/daprd/components/configuration_redis.go` for the registration shape.
Until that PR lands, the component can be exercised via the certification
test in `tests/certification/configuration/git/`.
