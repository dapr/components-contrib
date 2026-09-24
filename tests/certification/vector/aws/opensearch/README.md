<!--
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# AWS OpenSearch vector certification

These tests exercise the component directly against a **real OpenSearch backend
with the k-NN plugin**, not an in-memory emulator or embedded Dapr sidecar.
Every fixture uses a UUID collection and an independent cleanup context. Tests
never stop the backend and only delete their own collections.

```sh
export OPENSEARCH_ENDPOINT=http://127.0.0.1:9400
cd tests/certification
go test -tags certtests -count=1 -v -timeout 20m ./vector/aws/opensearch/...
```

The endpoint must be an OpenSearch data-plane endpoint, not Floci's AWS
control-plane port. The existing repository OpenSearch infrastructure can
provision a real backend through Floci. These local certification tests use
region `us-east-1`, static `test`/`test` credentials, and a `3m` HTTP timeout.
They skip when `OPENSEARCH_ENDPOINT` is unset. `components/opensearch.yaml` and
`config.yaml` are informational; these tests do not launch a sidecar.

## Coverage

- Independent numeric oracles for cosine, dot-product and actual Euclidean
  distance; negative scores, unequal norms, opposite/orthogonal vectors,
  inclusive thresholds, query-time overrides, ranking and ByID self exclusion.
- Mixed batch results, prefiltered top-k and canonical per-query errors.
- Dimensions 1, 1024 and 16000, zero/mismatched dimensions and explicit
  zero-vector behavior. No ANN-specific upper bound is imposed on exact scoring.
- 128-record, 1024-dimensional bulk round trips with binary payloads, Unicode
  IDs, long and date-shaped strings, structured metadata and persistent reads
  through a second component.
- Provider mapping-conflict partial failures, item attribution and repair;
  repeated deletes; collection-kind read/write/delete barriers.
- Cancellation, idempotent Close, and isolated native-source corruption/repair.

Cosine of a zero vector is mathematically undefined. Cosine collections reject
zero-vector writes, and cosine queries reject a zero query vector or matching
stored zero vectors. Dot product and Euclidean collections accept zeros.
Filtering all zero records out allows a valid cosine metric override.

## Limits

Floci does not enforce AWS IAM authorization for these requests, so wrong-key
tests would provide false assurance. This suite does **not** certify IAM policies,
credential rotation, role assumption, network failover, or AWS-managed service
availability. Signed transport and credential-chain behavior have separate unit
coverage. Exact prefiltered scoring is intentional; these tests certify metric
correctness, not ANN performance.
