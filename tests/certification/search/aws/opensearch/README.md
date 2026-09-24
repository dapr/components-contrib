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

# AWS OpenSearch Search certification

This suite calls the real `search.Search` component directly, without a Dapr
sidecar or an HTTP mock. The informational component/configuration YAML mirrors
the test settings. Every index has a UUID suffix and bounded independent cleanup;
tests never restart the provider or delete unrelated indexes.

With the repository's Floci/OpenSearch fixture already running:

```sh
export OPENSEARCH_ENDPOINT=http://127.0.0.1:9400
cd tests/certification
go test -tags certtests -count=1 -timeout=20m -v ./search/aws/opensearch/...
```

Without `OPENSEARCH_ENDPOINT`, tests skip. The fixture must provide the actual
OpenSearch data plane, not an emulator of individual search responses. Tests sign
requests using static test AWS credentials (`test`/`test`, `us-east-1`). These
credentials are only suitable for a local test fixture. HTTP request timeout is
three minutes; explicit completed-write waits allow four minutes.

## Certification coverage

- A 301-document, over-2-MiB batch and complete ascending/descending pagination
  across nonunique sort keys, including ID tie-breaks, exact totals and visibility.
- Byte-exact JSON (whitespace, large integers, number spelling, Unicode and
  base64 data), opaque metadata and IDs requiring escaping.
- Actual OpenSearch mapping conflicts, precise partial-bulk failure IDs,
  persistence of successful siblings, and repair/retry of rejected documents.
- All write modes returning completed acknowledgements, immediately visible
  deletes, repeated/missing deletes, and explicit rejection of continue-async.
- Persistence across component instances and Close, index isolation, vector and
  foreign ownership barriers, and prevention of bulk implicit index creation.
- Nested content filters/projections, array projections, string/numeric sorts,
  highlights, native Query DSL, and long/date-like keyword strings.
- Precision-preserving pagination above 2^53, malformed cursor structures,
  query-bound cursors, page-size boundaries, cancellation and post-Close errors.

## Not verified here

Floci's OpenSearch data plane has security disabled. This suite **does not verify
AWS IAM policy enforcement**, credential rejection, tenant authorization, or
production AWS networking. Signature generation, AWS credential configuration
and transport failures have separate unit tests. No wrong-credentials test is
used to claim authentication coverage against this fixture.
