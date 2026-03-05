# Controlled startup seek for Kafka pub/sub and binding

## Summary
This change adds an opt-in startup seek capability for Kafka consumers used by `pubsub.kafka` and `bindings.kafka`.

The feature allows a component to start consuming from:
- `earliest` retained offset
- `latest`
- a fixed `offset`
- an offset resolved from a Unix-millis `timestamp`

Default behavior is unchanged when the new metadata is not set.

## New metadata
| Name | Type | Default | Required | Description |
|---|---|---|---|---|
| `seekOnStart` | string | `never` | no | Startup seek mode: `never`, `earliest`, `latest`, `offset`, `timestamp` |
| `seekValue` | string | n/a | yes for `offset`/`timestamp` | Offset value or Unix milliseconds, depending on `seekOnStart` |
| `seekApplyWhen` | string | `ifNoCheckpoint` | no | Apply seek `always` or only `ifNoCheckpoint` (no committed offset) |
| `seekOnce` | bool | `true` | no | Apply startup seek at most once per component instance for each group/topic/partition when `seekApplyWhen=always`; no effect for `ifNoCheckpoint` |
| `seekPartition` | int | all claimed partitions | no | Restrict startup seek to one partition |

### Validation rules
- `seekOnStart=earliest|latest`: `seekValue` is ignored.
- `seekOnStart=earliest` is equivalent to Kafka `oldest`; `seekOnStart=latest` is equivalent to Kafka `newest` used by `initialOffset`.
- `seekOnStart=offset`: `seekValue` must be an integer offset `>=0`.
- `seekOnStart=timestamp`: `seekValue` must be Unix milliseconds.
- `seekApplyWhen=ifNoCheckpoint`: seek only when no committed offset exists for the partition.
- `seekApplyWhen=always`: seek on every startup; with `seekOnce=true`, apply once then skip subsequent sessions.
- `seekApplyWhen=ifNoCheckpoint`: seek applies whenever no committed offset exists; `seekOnce` does not suppress this re-application.

## Examples

### Example A — Start from earliest once when there’s no checkpoint
```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: kafka-pubsub
spec:
  type: pubsub.kafka
  version: v1
  metadata:
    - name: brokers
      value: "kafka:9092"
    - name: consumerID
      value: "{appID}"  # default anyway
    - name: seekOnStart
      value: "earliest"
    - name: seekApplyWhen
      value: "ifNoCheckpoint"
    - name: seekOnce
      value: "true"
```

### Example B — Force restart from a fixed offset (single partition)
```yaml
- name: seekOnStart
  value: "offset"
- name: seekValue
  value: "12345"
- name: seekPartition
  value: "0"
- name: seekApplyWhen
  value: "always"
- name: seekOnce
  value: "true"
```

### Example C — Start from timestamp (Unix millis) across all partitions
```yaml
- name: seekOnStart
  value: "timestamp"
- name: seekValue
  value: "1735689600000"  # 2025-01-01T00:00:00Z
- name: seekApplyWhen
  value: "ifNoCheckpoint"
```

## Compatibility and migration notes
- No metadata changes required: existing behavior remains unchanged.
- `pubsub.kafka`: `consumerID` still maps to Kafka consumer group (default app-id from runtime).
- `bindings.kafka`: `initialOffset` behavior remains unchanged when startup seek metadata is not used.
- If `seekOnStart != never`, startup seek takes precedence over first-start `initialOffset` behavior.
- `earliest` means earliest retained offset, not necessarily `0`.
- Keep `consumerGroup` configured in bindings when checkpointing behavior is required.

## Operational notes
- Startup seeks happen during consumer group session setup after partition claims.
- Normal offset commits (`MarkMessage`) are unchanged to preserve at-least-once behavior.
- If `seekPartition` is not assigned in the session, component logs a warning and continues.
- If `seekApplyWhen=always` and `seekOnce=false`, consumers intentionally re-read on every restart.

## Validation evidence (for PR)

### Unit tests (repo)
- `go test ./common/component/kafka` ✅
- Metadata wrappers compile path check:
  - `go test ./pubsub/kafka ./bindings/kafka` ✅

### End-to-end validation (non-PR harness)
- Runtime: custom `daprd` image built with this branch’s `components-contrib` changes
- Scenario matrix: 33 meaningful combinations of
  - `seekOnStart` (`earliest`, `latest`, `offset`, `timestamp`)
  - `seekApplyWhen` (`ifNoCheckpoint`, `always`)
  - `seekOnce` (`true`, `false`)
  - `seekPartition` (omitted/all vs `0`)
  - baseline `seekOnStart=never`
- Assertion result: `passed=33, failed=0` ✅