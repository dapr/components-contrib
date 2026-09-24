# AWS OpenSearch search and vector components

`search.aws.opensearch` and `vector.aws.opensearch` use the OpenSearch Service
**domain data endpoint**, not the AWS domain-management API. Requests are signed
with AWS Signature Version 4 for the `es` service. OpenSearch Serverless (`aoss`)
is not supported.

Both components reuse `common/aws.NewConfig` and its credential provider chain:
static credentials, AWS SDK environment/shared configuration and workload
credentials, role assumption, and IAM Roles Anywhere. Credentials are retrieved
for each request through the shared provider/cache, rather than captured at
initialization.

## Configuration

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: catalog-search
spec:
  type: search.aws.opensearch
  version: v1
  metadata:
    - name: endpoint
      value: https://search-example.us-east-1.es.amazonaws.com
    - name: region
      value: us-east-1
    - name: timeout
      value: 30s
```

For vectors, use `type: vector.aws.opensearch`. AWS IAM credentials must permit
the corresponding `es:ESHttp*` operations on the domain. Listing indexes requires
permission to read `/_mapping`. In production, prefer workload credentials;
explicit `accessKey`, `secretKey`, and optional `sessionToken` should use Dapr
secret references. The shared AWS authentication metadata also supports
`assumeRoleArn`, `assumeRoleSessionName`, `trustAnchorArn`, and `trustProfileArn`.

| Metadata | Required | Default | Meaning |
| --- | --- | --- | --- |
| `endpoint` | Yes | | Domain data endpoint, including `https://` (`http://` for local tests) |
| `region` | Unless resolved by the AWS SDK | | AWS signing region; `awsRegion` is an alias |
| `timeout` | No | `30s` | Positive HTTP request timeout |

The components create and own their indexes. Search and vector indexes share
the domain's index namespace; use distinct names. Listing only returns indexes
with the matching Dapr mapping marker. Names start with a lowercase letter or
digit, followed by lowercase letters, digits, dots, hyphens or underscores,
up to 255 bytes. Wildcards, aliases spanning multiple indexes and index lists
are not accepted as request targets.

## Writes and errors

OpenSearch bulk operations do not offer Dapr's durable queued acknowledgement.
All successful writes and deletes return `INDEX_ACK_COMPLETED`, including
return-on-acceptance writes. `refresh=wait_for` makes completed writes searchable.
Bulk requests use HTTP PUT because AWS ignores URL parameters on signed POST
requests.
Wait-for-completion uses the requested wait timeout;
`INDEXING_WAIT_TIMEOUT_ACTION_CONTINUE_ASYNC` is rejected before submission.
A timeout does **not** roll back writes already received by OpenSearch.

Bulk item failures are returned with canonical status codes. Transport failures
and incomplete bulk responses report `INDEXING_OUTCOME_UNKNOWN`, rather than
claiming that no items were written. Missing document IDs are omitted from
ordered gets and may be deleted idempotently. Provider error reasons are not
echoed because they may contain document content.

## Filters

The portable filter DSL supports scalar equality, `$eq`, `$ne`, `$gt`, `$gte`,
`$lt`, `$lte`, `$in`, `$nin`, `$exists`, `$and`, `$or` and `$not`.
Search filters address document content; vector filters address record
metadata. Dotted paths address object fields. Strings use keyword subfields for
exact comparison, while numbers and booleans use their native mappings.
OpenSearch dynamically maps each field on first use, so subsequent values must
remain compatible with that field's type.

Null equality matches a field with no indexed value (null or missing), following
OpenSearch's `exists` semantics. Empty arrays likewise have no indexed value.
Regular-expression filters are not supported by the portable translator and
return `INVALID_ARGUMENT`; use a native search query when appropriate.

## Local provider

The Floci fixture provisions a **real OpenSearch domain with the k-NN plugin**.
Floci's mock mode cannot validate the lexical/vector data plane. It uses dummy
AWS credentials and digest-pinned Floci and OpenSearch images.

See the [search conformance instructions](../../../../tests/config/search/README.md)
and [vector conformance instructions](../../../../tests/config/vector/README.md)
for setup, conformance/certification execution and cleanup. Provider-specific
coverage is described in the
[search certification suite](../../../../tests/certification/search/aws/opensearch/README.md)
and [vector certification suite](../../../../tests/certification/vector/aws/opensearch/README.md).
The Floci management endpoint and the OpenSearch data endpoint are different
addresses. Floci disables data-plane security: local certification validates
the OpenSearch behavior but does not certify AWS IAM policy enforcement.
