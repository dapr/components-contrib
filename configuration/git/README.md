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
- A polling delay of 5–60 seconds is acceptable.

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
    - name: url
      value: "https://github.com/example/agent-config.git"
    - name: branch
      value: "main"
    - name: pollInterval
      value: "30s"
```

## Mapping modes

The `mappingMode` metadata field selects how files in the repository become
configuration items. Matching is case-insensitive.

### `file` (default)

Each file becomes one config item. The relative POSIX path is the key,
the file contents the value.

```
repo/
  agents/weather/agent_role.txt   → key "agents/weather/agent_role.txt"
  agents/weather/agent_goal.txt   → key "agents/weather/agent_goal.txt"
```

This is the recommended mode when the consumer (e.g. dapr-agents) already
expects scalar config keys.

### `agentYaml`

Each `*.yaml`, `*.yml`, or `*.json` file is parsed as a flat top-level map.
Each top-level field becomes a key prefixed by the filename stem with
directory separators replaced by `_`. Non-YAML/JSON files are silently skipped.

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
`<stem>/agent_system_prompt`. Non-`.prompty` files are skipped.

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

The `authMode` metadata field selects an auth profile. When empty, the
component auto-detects: SSH-scheme URLs (`git@` or `ssh://`) → `ssh`,
`githubAppId` set → `githubApp`, `token` set → `pat`, otherwise `none`.

| Mode | Required fields |
|------|-----------------|
| `none` | (public HTTPS or `file://`) |
| `pat` | `token` (sensitive); optional `username` (defaults to `x-access-token`) |
| `ssh` | `sshPrivateKey` or `sshPrivateKeyPath` (sensitive); plus `sshKnownHosts`/`sshKnownHostsPath` unless `sshInsecureIgnoreHostKey: true` |
| `githubApp` | `githubAppId`, `githubAppInstallationId`, `githubAppPrivateKey` or `githubAppPrivateKeyPath` (sensitive) |

`http://` URLs are rejected for any authenticated mode to prevent cleartext
credential transmission. Embedding credentials directly in the URL
(e.g. `https://user:tok@host/`) is also rejected — use the appropriate
profile with a Dapr secret reference.

## Behavioural notes

- **`Get` is cache-only.** It returns the most-recently polled snapshot
  and may be up to `pollInterval` old. It does not contact the remote.
- **`Subscribe` replays current state by default.** When `emitInitialState`
  is true (the default), the handler receives the current snapshot
  synchronously before `Subscribe` returns. Set `emitInitialState: false`
  if the caller has just performed a `Get` and would receive a duplicate.
- **Deletions are signalled** via `Item{Value: "", Metadata: {"deleted": "true"}}` —
  the same shape as the kubernetes ConfigMap configuration store.
- **Commits are never written.** The component is read-only; nothing it
  does mutates the upstream repository.
- **Rate-limit budget.** With `pollInterval: 30s` and 1 replica, the
  component issues ~120 ref-fetches per hour — well under GitHub's 5 000/h
  PAT limit. Multi-replica deployments multiply that rate
  (`effective_rate = 3600/pollInterval × replicas`); if you push down to
  `pollInterval: 1s` (the hard floor for remote URLs), 10 replicas burn
  36 000 req/h, exhausting a PAT. Use a GitHub App (15 000/h) and tune
  carefully.
- **`.git/` is always excluded** from the worktree walk regardless of
  `includeHidden`, so credentials in `.git/config` cannot leak into the
  configuration items.
- **Per-file size cap.** Files larger than `maxFileSize` (default 1 MiB)
  are skipped with a warning. Protects the sidecar from OOM.
- **LRU snapshot cache.** Diff bases for per-subscriber updates live in a
  small LRU keyed by HEAD SHA (default size 4). On a miss the diff over-
  emits a single batch (every key as added/changed) — idempotent on the
  receiver. Tune via `snapshotCacheSize` for very high subscriber counts.

## Limitations

- **Shallow clone instability.** `go-git`'s shallow incremental fetch has
  open issues; the default `depth: 0` (full clone) is the safe choice.
  For repos with very deep history, set `depth: 1` and accept that
  `git log` from inside the worktree won't show ancestors.
- **No webhook support.** Polling only. A webhook listener would need an
  inbound network endpoint, secret verification, and retry logic — out of
  scope for v1.
- **Single-installation GitHub App.** One installation per component
  instance. Multi-tenant routing (different repos via different
  installations) is not supported.

## Daprd registration

This component requires a paired `dapr/dapr` PR registering
`configuration.git` so that daprd can load it via component spec. See
`cmd/daprd/components/configuration_redis.go` for the registration shape.
Until that PR lands, the component can be exercised via the certification
test in `tests/certification/configuration/git/`.
