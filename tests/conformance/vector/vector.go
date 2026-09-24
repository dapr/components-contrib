/*
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
*/

package vector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	"github.com/dapr/components-contrib/tests/conformance/utils"
	"github.com/dapr/components-contrib/vector"
	"github.com/dapr/kit/config"
)

// Capability opt-ins declared through the `operations` list of a component in
// tests/config/vector/tests.yml. Capabilities that are not declared must be
// reported as UNIMPLEMENTED (or INVALID_ARGUMENT where the contract says so)
// rather than silently ignored.
const (
	// OperationQueuedAck declares a native durable queued acknowledgement.
	OperationQueuedAck = "queued-ack"
	// OperationScoreThreshold declares score_threshold support.
	OperationScoreThreshold = "score-threshold"
	// OperationFilter declares portable filter support over record metadata.
	OperationFilter = "filter"
	// OperationQueryByID declares by_id query support.
	OperationQueryByID = "query-by-id"
	// OperationMetricCosine, OperationMetricDotProduct and
	// OperationMetricEuclidean declare the distance metrics a component
	// accepts, on CreateCollection and on Query. Metrics that are not declared
	// must be rejected with INVALID_ARGUMENT.
	OperationMetricCosine     = "metric-cosine"
	OperationMetricDotProduct = "metric-dot-product"
	OperationMetricEuclidean  = "metric-euclidean"
)

const (
	defaultWaitTimeout = 20 * time.Second
	defaultCallTimeout = 60 * time.Second
	defaultDimensions  = 4
	// scoreEpsilon absorbs the float32 round-tripping every provider performs
	// on stored vectors when comparing scores against a threshold.
	scoreEpsilon = 1e-5
)

// Record metadata keys used by the conformance records. Filters address these
// keys directly; the component maps them to wherever it stores structured
// metadata.
const (
	fieldMake      = "make"
	fieldBodyStyle = "bodyStyle"
	fieldPrice     = "price"
	fieldInStock   = "inStock"
	fieldNotes     = "notes"
)

// TestConfig is the vector conformance configuration. Provider-specific
// collection settings such as index parameters travel through
// CreateCollectionMetadata, mirroring the component contract; dimensions and
// the metric are typed request fields.
type TestConfig struct {
	utils.CommonConfig

	// CreateCollectionMetadata is passed verbatim to CreateCollection.
	CreateCollectionMetadata map[string]string `mapstructure:"createCollectionMetadata"`
	// UpsertMetadata is passed verbatim to Upsert and Delete.
	UpsertMetadata map[string]string `mapstructure:"upsertMetadata"`
	// QueryMetadata is passed verbatim to Query, BatchQuery and Get.
	QueryMetadata map[string]string `mapstructure:"queryMetadata"`
	// Dimensions is passed as CreateCollectionRequest.Dimensions and drives
	// the generated test vectors.
	Dimensions int `mapstructure:"dimensions"`
	// WaitTimeout is used with INDEXING_MODE_WAIT_FOR_COMPLETION.
	WaitTimeout time.Duration `mapstructure:"waitTimeout"`
}

func NewTestConfig(componentName string, operations []string, configMap map[string]interface{}) (TestConfig, error) {
	tc := TestConfig{
		CommonConfig: utils.CommonConfig{
			ComponentType: "vector",
			ComponentName: componentName,
			Operations:    utils.NewStringSet(operations...),
		},
		Dimensions:  defaultDimensions,
		WaitTimeout: defaultWaitTimeout,
	}

	err := config.Decode(configMap, &tc)

	return tc, err
}

// writeOperation is a write of the Vector contract that accepts
// IndexingOptions and reports an IndexAck: Upsert and Delete share the same
// mode/wait semantics and are exercised through the same matrix.
type writeOperation struct {
	name string
	call func(ctx context.Context, collection string, options search.IndexingOptions) (search.IndexAck, []search.FailedItem, error)
}

//nolint:gocyclo,maintidx // A conformance suite is intentionally a long linear list of scenarios.
func ConformanceTests(t *testing.T, props map[string]string, v vector.Vector, cfg TestConfig) {
	ctx := t.Context()
	component := cfg.ComponentName

	t.Run("init", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		require.NoError(t, v.Init(c, vector.Metadata{Base: metadata.Base{Properties: props}}))
	})
	if t.Failed() {
		t.Fatal("init failed")
	}

	t.Run("init idempotent", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		require.NoError(t, v.Init(c, vector.Metadata{Base: metadata.Base{Properties: props}}))
	})

	createRequest := func(name string, metric vector.DistanceMetric) *vector.CreateCollectionRequest {
		return &vector.CreateCollectionRequest{
			Collection: name,
			Metadata:   cfg.CreateCollectionMetadata,
			Dimensions: uint32(cfg.Dimensions), //nolint:gosec // test dimensions are small
			Metric:     metric,
		}
	}

	newCollection := func(t *testing.T, suffix string) string {
		t.Helper()
		name := fmt.Sprintf("conf-%s-%s-%s", component, suffix, randSuffix())
		c, cancel := context.WithTimeout(t.Context(), defaultCallTimeout)
		defer cancel()
		require.NoError(t, v.CreateCollection(c, createRequest(name, vector.DistanceMetricUnspecified)))
		t.Cleanup(func() {
			cleanCtx, cleanCancel := context.WithTimeout(context.Background(), defaultCallTimeout)
			defer cleanCancel()
			_ = v.DeleteCollection(cleanCtx, &vector.DeleteCollectionRequest{Collection: name, Metadata: cfg.CreateCollectionMetadata})
		})
		return name
	}

	writeOperations := []writeOperation{
		{
			name: "Upsert",
			call: func(c context.Context, collection string, options search.IndexingOptions) (search.IndexAck, []search.FailedItem, error) {
				records := []vector.Record{{ID: "write-" + randSuffix(), Values: basisValues(cfg.Dimensions, 0), Metadata: map[string]any{fieldMake: "hyundai", fieldPrice: 1.0, fieldInStock: true}}}
				resp, err := v.Upsert(c, &vector.UpsertRequest{Collection: collection, Records: records, Metadata: cfg.UpsertMetadata, Options: options})
				if err != nil {
					return search.IndexAckUnspecified, nil, err
				}
				require.NotNil(t, resp)
				return resp.Ack, resp.FailedItems, nil
			},
		},
		{
			name: "Delete",
			call: func(c context.Context, collection string, options search.IndexingOptions) (search.IndexAck, []search.FailedItem, error) {
				// Deletes of IDs that never existed are not failures, so the
				// delete matrix does not need a seeded record.
				resp, err := v.Delete(c, &vector.DeleteRequest{Collection: collection, IDs: []string{"delete-" + randSuffix()}, Metadata: cfg.UpsertMetadata, Options: options})
				if err != nil {
					return search.IndexAckUnspecified, nil, err
				}
				require.NotNil(t, resp)
				return resp.Ack, nil, nil
			},
		},
	}

	collection := fmt.Sprintf("conf-%s-%s", component, randSuffix())
	collectionDeleted := false
	t.Cleanup(func() {
		if collectionDeleted {
			return
		}
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), defaultCallTimeout)
		defer cleanCancel()
		_ = v.DeleteCollection(cleanCtx, &vector.DeleteCollectionRequest{Collection: collection, Metadata: cfg.CreateCollectionMetadata})
	})

	t.Run("CreateCollection", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		// DISTANCE_METRIC_UNSPECIFIED selects the component's documented
		// default metric.
		require.NoError(t, v.CreateCollection(c, createRequest(collection, vector.DistanceMetricUnspecified)))
	})
	if t.Failed() {
		t.Fatal("CreateCollection failed")
	}

	t.Run("CreateCollection on an existing collection returns ALREADY_EXISTS", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		err := v.CreateCollection(c, createRequest(collection, vector.DistanceMetricUnspecified))
		requireStatusCode(t, err, codes.AlreadyExists)

		// Settings are not reconciled either: a different metric on the
		// existing name is still ALREADY_EXISTS, and the collection is listed
		// exactly once.
		for _, metric := range []vector.DistanceMetric{vector.DistanceMetricCosine, vector.DistanceMetricDotProduct, vector.DistanceMetricEuclidean} {
			if !cfg.metricDeclared(metric) {
				continue
			}
			err = v.CreateCollection(c, createRequest(collection, metric))
			requireStatusCode(t, err, codes.AlreadyExists)
		}

		resp, err := v.ListCollections(c, &vector.ListCollectionsRequest{})
		require.NoError(t, err)
		assert.Equal(t, 1, countString(resp.Collections, collection))
	})

	t.Run("CreateCollection metric matrix", func(t *testing.T) {
		for _, metric := range []vector.DistanceMetric{
			vector.DistanceMetricCosine,
			vector.DistanceMetricDotProduct,
			vector.DistanceMetricEuclidean,
		} {
			t.Run(metricName(metric), func(t *testing.T) {
				name := fmt.Sprintf("conf-%s-metric-%s", component, randSuffix())
				c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
				defer cancel()
				t.Cleanup(func() {
					cleanCtx, cleanCancel := context.WithTimeout(context.Background(), defaultCallTimeout)
					defer cleanCancel()
					_ = v.DeleteCollection(cleanCtx, &vector.DeleteCollectionRequest{Collection: name, Metadata: cfg.CreateCollectionMetadata})
				})

				err := v.CreateCollection(c, createRequest(name, metric))
				if !cfg.metricDeclared(metric) {
					// A metric the component cannot provide is rejected, and
					// nothing is created.
					requireStatusCode(t, err, codes.InvalidArgument)
					_, getErr := v.GetCollection(c, &vector.GetCollectionRequest{Collection: name})
					requireStatusCode(t, getErr, codes.NotFound)
					return
				}
				require.NoError(t, err)

				got, err := v.GetCollection(c, &vector.GetCollectionRequest{Collection: name})
				require.NoError(t, err)
				assert.Equal(t, metric, got.Metric, "GetCollection reports the metric the collection was created with")
				assert.Equal(t, uint32(cfg.Dimensions), got.Dimensions) //nolint:gosec // test dimensions are small
			})
		}
	})

	t.Run("GetCollection", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		resp, err := v.GetCollection(c, &vector.GetCollectionRequest{Collection: collection})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, collection, resp.Collection)
		assert.Equal(t, uint32(cfg.Dimensions), resp.Dimensions, "GetCollection reports the configured dimensions") //nolint:gosec // test dimensions are small
		// The effective metric is always concrete, even when the collection
		// was created with DISTANCE_METRIC_UNSPECIFIED.
		assert.NotEqual(t, vector.DistanceMetricUnspecified, resp.Metric, "the effective metric is always concrete")
		assert.True(t, cfg.metricDeclared(resp.Metric), "the default metric %v is not declared as supported by %s", resp.Metric, component)
	})

	t.Run("GetCollection on a missing collection", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		_, err := v.GetCollection(c, &vector.GetCollectionRequest{Collection: "conf-" + component + "-missing-" + randSuffix()})
		requireStatusCode(t, err, codes.NotFound)
	})

	t.Run("DeleteCollection on a missing collection", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		err := v.DeleteCollection(c, &vector.DeleteCollectionRequest{Collection: "conf-" + component + "-missing-" + randSuffix()})
		requireStatusCode(t, err, codes.NotFound)
	})

	t.Run("ListCollections contains the collection", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		resp, err := v.ListCollections(c, &vector.ListCollectionsRequest{})
		require.NoError(t, err)
		require.Contains(t, resp.Collections, collection)
	})

	t.Run("keyed upsert validation", func(t *testing.T) {
		tests := []struct {
			name    string
			records []vector.Record
		}{
			{
				name:    "empty id",
				records: []vector.Record{{ID: "", Values: basisValues(cfg.Dimensions, 0)}},
			},
			{
				name: "empty id among valid ids",
				records: []vector.Record{
					{ID: "valid-1", Values: basisValues(cfg.Dimensions, 0)},
					{ID: "", Values: basisValues(cfg.Dimensions, 1)},
				},
			},
			{
				name: "duplicate ids",
				records: []vector.Record{
					{ID: "dupe", Values: basisValues(cfg.Dimensions, 0)},
					{ID: "dupe", Values: basisValues(cfg.Dimensions, 1)},
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
				defer cancel()
				resp, err := v.Upsert(c, &vector.UpsertRequest{Collection: collection, Records: tt.records, Metadata: cfg.UpsertMetadata})
				requireStatusCode(t, err, codes.InvalidArgument)
				if resp != nil {
					assert.Empty(t, resp.FailedItems, "a rejected keyed upsert must not report per-item failures")
				}
			})
		}
	})

	t.Run("indexing options validation", func(t *testing.T) {
		tests := []struct {
			name          string
			options       search.IndexingOptions
			shortDeadline bool
		}{
			{
				name:    "wait_timeout without wait-for-completion",
				options: search.IndexingOptions{Mode: search.IndexingModeReturnOnAcceptance, WaitTimeout: cfg.WaitTimeout},
			},
			{
				name:    "on_wait_timeout without wait-for-completion",
				options: search.IndexingOptions{Mode: search.IndexingModeUnspecified, OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest},
			},
			{
				name:    "wait-for-completion without wait_timeout",
				options: search.IndexingOptions{Mode: search.IndexingModeWaitForCompletion, OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest},
			},
			{
				name:    "wait-for-completion with a negative wait_timeout",
				options: search.IndexingOptions{Mode: search.IndexingModeWaitForCompletion, WaitTimeout: -time.Second, OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest},
			},
			{
				name:    "wait-for-completion without on_wait_timeout",
				options: search.IndexingOptions{Mode: search.IndexingModeWaitForCompletion, WaitTimeout: cfg.WaitTimeout},
			},
			{
				name:          "remaining deadline shorter than wait_timeout",
				options:       search.IndexingOptions{Mode: search.IndexingModeWaitForCompletion, WaitTimeout: cfg.WaitTimeout, OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest},
				shortDeadline: true,
			},
		}

		for _, op := range writeOperations {
			t.Run(op.name, func(t *testing.T) {
				for _, tt := range tests {
					t.Run(tt.name, func(t *testing.T) {
						timeout := defaultCallTimeout
						if tt.shortDeadline {
							timeout = cfg.WaitTimeout / 2
						}
						c, cancel := context.WithTimeout(ctx, timeout)
						defer cancel()
						_, _, err := op.call(c, collection, tt.options)
						requireStatusCode(t, err, codes.InvalidArgument)
					})
				}
			})
		}
	})

	t.Run("indexing modes", func(t *testing.T) {
		tests := []struct {
			name    string
			options search.IndexingOptions
		}{
			{name: "unspecified", options: search.IndexingOptions{Mode: search.IndexingModeUnspecified}},
			{name: "return on acceptance", options: search.IndexingOptions{Mode: search.IndexingModeReturnOnAcceptance}},
			{
				name:    "wait for completion, fail request on timeout",
				options: search.IndexingOptions{Mode: search.IndexingModeWaitForCompletion, WaitTimeout: cfg.WaitTimeout, OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest},
			},
			{
				name:    "wait for completion, continue async on timeout",
				options: search.IndexingOptions{Mode: search.IndexingModeWaitForCompletion, WaitTimeout: cfg.WaitTimeout, OnWaitTimeout: search.IndexingWaitTimeoutActionContinueAsync},
			},
		}

		for _, op := range writeOperations {
			t.Run(op.name, func(t *testing.T) {
				for _, tt := range tests {
					t.Run(tt.name, func(t *testing.T) {
						modeCollection := newCollection(t, "mode")
						c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
						defer cancel()
						ack, failed, err := op.call(c, modeCollection, tt.options)

						if tt.options.OnWaitTimeout == search.IndexingWaitTimeoutActionContinueAsync && !cfg.HasOperation(OperationQueuedAck) {
							requireStatusCode(t, err, codes.InvalidArgument)
							return
						}

						if tt.options.Mode == search.IndexingModeWaitForCompletion && tt.options.OnWaitTimeout == search.IndexingWaitTimeoutActionFailRequest && err != nil {
							requireStatusCode(t, err, codes.DeadlineExceeded)
							return
						}

						require.NoError(t, err)
						assert.NotEqual(t, search.IndexAckUnspecified, ack, "a successful write never returns INDEX_ACK_UNSPECIFIED")
						assert.Contains(t, []search.IndexAck{search.IndexAckQueued, search.IndexAckCompleted}, ack)
						if !cfg.HasOperation(OperationQueuedAck) {
							assert.Equal(t, search.IndexAckCompleted, ack, "a provider without a queued acknowledgement always completes the write")
						}
						if tt.options.Mode == search.IndexingModeWaitForCompletion && ack == search.IndexAckCompleted {
							assertFailedItems(t, failed, nil)
						}
					})
				}
			})
		}
	})

	t.Run("FailedItems semantics", func(t *testing.T) {
		t.Run("a successful batch reports no failed items", func(t *testing.T) {
			okCollection := newCollection(t, "ok")
			c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
			defer cancel()
			resp, err := v.Upsert(c, &vector.UpsertRequest{Collection: okCollection, Records: conformanceRecords(cfg.Dimensions), Metadata: cfg.UpsertMetadata, Options: cfg.waitOptions()})
			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Empty(t, resp.FailedItems)
			assert.Equal(t, search.IndexAckCompleted, resp.Ack)
		})

		t.Run("an item failure is attributed to its id", func(t *testing.T) {
			failCollection := newCollection(t, "fail")
			c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
			defer cancel()
			records := []vector.Record{
				{ID: "good-1", Values: basisValues(cfg.Dimensions, 0)},
				// A record whose dimensionality disagrees with the collection.
				{ID: "bad-dim", Values: basisValues(cfg.Dimensions+1, 0)},
			}
			resp, err := v.Upsert(c, &vector.UpsertRequest{Collection: failCollection, Records: records, Metadata: cfg.UpsertMetadata, Options: cfg.waitOptions()})
			if err != nil {
				requireStatusCode(t, err, codes.InvalidArgument)
				return
			}
			require.NotNil(t, resp)
			assertFailedItems(t, resp.FailedItems, []string{"bad-dim"})
		})

		t.Run("a request-wide failure is not duplicated per item", func(t *testing.T) {
			c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
			defer cancel()
			resp, err := v.Upsert(c, &vector.UpsertRequest{
				Collection: "conf-" + component + "-missing-" + randSuffix(),
				Records:    conformanceRecords(cfg.Dimensions),
				Metadata:   cfg.UpsertMetadata,
				Options:    cfg.waitOptions(),
			})
			if err == nil {
				t.Skip("provider implicitly creates a missing collection on write")
			}
			requireStatusError(t, err)
			if resp != nil {
				assert.Empty(t, resp.FailedItems, "request-wide failures belong in the RPC status, not in failed_items")
			}
		})
	})

	records := conformanceRecords(cfg.Dimensions)

	t.Run("Upsert seeds the shared collection", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		resp, err := v.Upsert(c, &vector.UpsertRequest{Collection: collection, Records: records, Metadata: cfg.UpsertMetadata, Options: cfg.waitOptions()})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Empty(t, resp.FailedItems)
		require.Equal(t, search.IndexAckCompleted, resp.Ack)
	})
	if t.Failed() {
		t.Fatal("seeding the shared collection failed")
	}

	t.Run("Upsert is an idempotent keyed upsert", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		_, err := v.Upsert(c, &vector.UpsertRequest{Collection: collection, Records: records, Metadata: cfg.UpsertMetadata, Options: cfg.waitOptions()})
		require.NoError(t, err)

		got, err := v.GetCollection(c, &vector.GetCollectionRequest{Collection: collection})
		require.NoError(t, err)
		if got.RecordCount == 0 {
			t.Skip("provider does not report a record count")
		}
		assert.Equal(t, uint64(len(records)), got.RecordCount, "re-sending the same keyed upsert must not duplicate records")
	})

	t.Run("Get", func(t *testing.T) {
		want := recordsByID(records)

		t.Run("round trips payload and structured metadata", func(t *testing.T) {
			c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
			defer cancel()
			resp, err := v.Get(c, &vector.GetRequest{Collection: collection, IDs: []string{"vec-1", "vec-3"}, IncludeValues: true, Metadata: cfg.QueryMetadata})
			require.NoError(t, err)
			require.Len(t, resp.Records, 2)
			for _, got := range resp.Records {
				expected, ok := want[got.ID]
				require.True(t, ok, "unexpected record id %q", got.ID)
				require.Len(t, got.Values, cfg.Dimensions)
				assert.Equal(t, expected.Payload, got.Payload, "payload bytes are opaque and returned unchanged")
				// Metadata is structured: strings, numbers and booleans keep
				// their JSON type when read back.
				assert.Equal(t, jsonNormalize(t, expected.Metadata), jsonNormalize(t, got.Metadata))
			}
		})

		t.Run("records are returned in request order", func(t *testing.T) {
			c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
			defer cancel()
			ids := []string{"vec-4", "vec-1", "vec-6", "vec-2"}
			resp, err := v.Get(c, &vector.GetRequest{Collection: collection, IDs: ids, Metadata: cfg.QueryMetadata})
			require.NoError(t, err)
			assert.Equal(t, ids, recordIDs(resp.Records))

			// A missing id in the middle is omitted without disturbing the
			// order of the remaining records.
			resp, err = v.Get(c, &vector.GetRequest{Collection: collection, IDs: []string{"vec-4", "missing-" + randSuffix(), "vec-1"}, Metadata: cfg.QueryMetadata})
			require.NoError(t, err)
			assert.Equal(t, []string{"vec-4", "vec-1"}, recordIDs(resp.Records))
		})

		t.Run("include_values false omits values", func(t *testing.T) {
			c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
			defer cancel()
			resp, err := v.Get(c, &vector.GetRequest{Collection: collection, IDs: []string{"vec-1"}, IncludeValues: false, Metadata: cfg.QueryMetadata})
			require.NoError(t, err)
			require.Len(t, resp.Records, 1)
			assert.Equal(t, "vec-1", resp.Records[0].ID)
			assert.Empty(t, resp.Records[0].Values)
		})

		t.Run("missing ids are omitted, not errors", func(t *testing.T) {
			c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
			defer cancel()
			resp, err := v.Get(c, &vector.GetRequest{Collection: collection, IDs: []string{"vec-1", "missing-" + randSuffix()}, IncludeValues: true, Metadata: cfg.QueryMetadata})
			require.NoError(t, err)
			require.Len(t, resp.Records, 1)
			assert.Equal(t, "vec-1", resp.Records[0].ID)
		})

		t.Run("all ids missing returns an empty list", func(t *testing.T) {
			c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
			defer cancel()
			resp, err := v.Get(c, &vector.GetRequest{Collection: collection, IDs: []string{"missing-a", "missing-b"}, Metadata: cfg.QueryMetadata})
			require.NoError(t, err)
			assert.Empty(t, resp.Records)
		})
	})

	t.Run("Query reports a concrete effective metric", func(t *testing.T) {
		// DISTANCE_METRIC_UNSPECIFIED uses the collection's configured metric,
		// and the response always names the metric the scores belong to.
		resp := requireQuery(t, v, cfg, collection, &vector.QueryRequest{
			Vector: &vector.Record{Values: basisValues(cfg.Dimensions, 0)},
			TopK:   3,
		})
		require.NotEmpty(t, resp.Matches)
		assert.NotEqual(t, vector.DistanceMetricUnspecified, resp.Metric, "the effective metric is always concrete")
		assert.True(t, cfg.metricDeclared(resp.Metric), "the effective metric %v is not declared as supported by %s", resp.Metric, component)

		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		got, err := v.GetCollection(c, &vector.GetCollectionRequest{Collection: collection})
		require.NoError(t, err)
		assert.Equal(t, got.Metric, resp.Metric, "an unspecified query metric resolves to the collection's metric")
	})

	t.Run("Query metric matrix", func(t *testing.T) {
		for _, metric := range []vector.DistanceMetric{
			vector.DistanceMetricCosine,
			vector.DistanceMetricDotProduct,
			vector.DistanceMetricEuclidean,
		} {
			t.Run(metricName(metric), func(t *testing.T) {
				c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
				defer cancel()
				resp, err := v.Query(c, &vector.QueryRequest{
					Collection: collection,
					Vector:     &vector.Record{Values: basisValues(cfg.Dimensions, 0)},
					TopK:       3,
					Metric:     metric,
					Metadata:   cfg.QueryMetadata,
				})
				if !cfg.metricDeclared(metric) {
					requireStatusCode(t, err, codes.InvalidArgument)
					return
				}
				require.NoError(t, err)
				require.NotEmpty(t, resp.Matches)
				assert.Equal(t, metric, resp.Metric, "the response reports the metric the scores were computed with")
				assertScoreOrder(t, metric, resp.Matches)
			})
		}
	})

	t.Run("Query by vector honours top_k and score order", func(t *testing.T) {
		resp := requireQuery(t, v, cfg, collection, &vector.QueryRequest{
			Vector:         &vector.Record{Values: basisValues(cfg.Dimensions, 0)},
			TopK:           3,
			IncludeValues:  true,
			IncludePayload: true,
		})
		require.Len(t, resp.Matches, 3)
		assertScoreOrder(t, resp.Metric, resp.Matches)
		assert.Equal(t, "vec-1", resp.Matches[0].Record.ID, "the nearest match to the seed vector is its own record")
		for _, match := range resp.Matches {
			assert.NotEmpty(t, match.Record.Values)
			assert.NotEmpty(t, match.Record.Payload)
		}
	})

	t.Run("Query include_payload false omits payloads", func(t *testing.T) {
		resp := requireQuery(t, v, cfg, collection, &vector.QueryRequest{
			Vector:         &vector.Record{Values: basisValues(cfg.Dimensions, 0)},
			TopK:           1,
			IncludePayload: false,
			IncludeValues:  false,
		})
		require.NotEmpty(t, resp.Matches)
		assert.Empty(t, resp.Matches[0].Record.Payload)
		assert.Empty(t, resp.Matches[0].Record.Values)
	})

	t.Run("Query requires exactly one of vector and by_id", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		_, err := v.Query(c, &vector.QueryRequest{Collection: collection, TopK: 3, Metadata: cfg.QueryMetadata})
		requireStatusCode(t, err, codes.InvalidArgument)

		_, err = v.Query(c, &vector.QueryRequest{Collection: collection, Vector: &vector.Record{Values: basisValues(cfg.Dimensions, 0)}, ByID: "vec-1", TopK: 3, Metadata: cfg.QueryMetadata})
		requireStatusCode(t, err, codes.InvalidArgument)
	})

	t.Run("Query by id", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		resp, err := v.Query(c, &vector.QueryRequest{Collection: collection, ByID: "vec-1", TopK: 3, Metadata: cfg.QueryMetadata})
		if !cfg.HasOperation(OperationQueryByID) {
			requireStatusCode(t, err, codes.Unimplemented, codes.InvalidArgument)
			return
		}
		require.NoError(t, err)
		require.NotEmpty(t, resp.Matches)
		assertScoreOrder(t, resp.Metric, resp.Matches)
	})

	t.Run("Query with filter", func(t *testing.T) {
		if !cfg.HasOperation(OperationFilter) {
			t.Skipf("component %s does not declare the %q operation", component, OperationFilter)
		}

		// Filters address record metadata with the JSON type of each value.
		// The query vector is similar to every seeded record so that the
		// filter alone determines the result set.
		tests := []struct {
			name    string
			filter  map[string]any
			wantIDs []string
		}{
			{name: "$eq on a string", filter: map[string]any{fieldMake: map[string]any{"$eq": "toyota"}}, wantIDs: []string{"vec-3", "vec-4"}},
			{name: "bare value is $eq shorthand", filter: map[string]any{fieldMake: "ford"}, wantIDs: []string{"vec-5", "vec-6"}},
			{name: "$ne on a string", filter: map[string]any{fieldMake: map[string]any{"$ne": "hyundai"}}, wantIDs: []string{"vec-3", "vec-4", "vec-5", "vec-6"}},
			{name: "$gt on a number", filter: map[string]any{fieldPrice: map[string]any{"$gt": 30.0}}, wantIDs: []string{"vec-4", "vec-5", "vec-6"}},
			{name: "$gte on a number", filter: map[string]any{fieldPrice: map[string]any{"$gte": 30.0}}, wantIDs: []string{"vec-3", "vec-4", "vec-5", "vec-6"}},
			{name: "$lt on a number", filter: map[string]any{fieldPrice: map[string]any{"$lt": 20.0}}, wantIDs: []string{"vec-1"}},
			{name: "$lte on a number", filter: map[string]any{fieldPrice: map[string]any{"$lte": 20.0}}, wantIDs: []string{"vec-1", "vec-2"}},
			{name: "numeric range on one field", filter: map[string]any{fieldPrice: map[string]any{"$gte": 20.0, "$lt": 50.0}}, wantIDs: []string{"vec-2", "vec-3", "vec-4"}},
			{name: "$in on strings", filter: map[string]any{fieldMake: map[string]any{"$in": []any{"hyundai", "ford"}}}, wantIDs: []string{"vec-1", "vec-2", "vec-5", "vec-6"}},
			{name: "$nin on strings", filter: map[string]any{fieldMake: map[string]any{"$nin": []any{"hyundai", "ford"}}}, wantIDs: []string{"vec-3", "vec-4"}},
			{name: "$in on numbers", filter: map[string]any{fieldPrice: map[string]any{"$in": []any{10.0, 60.0}}}, wantIDs: []string{"vec-1", "vec-6"}},
			{name: "$eq on a bool", filter: map[string]any{fieldInStock: map[string]any{"$eq": true}}, wantIDs: []string{"vec-1", "vec-3", "vec-5"}},
			{name: "bare bool is $eq shorthand", filter: map[string]any{fieldInStock: false}, wantIDs: []string{"vec-2", "vec-4", "vec-6"}},
			{name: "$exists", filter: map[string]any{fieldNotes: map[string]any{"$exists": true}}, wantIDs: []string{"vec-2", "vec-5"}},
			{name: "$exists false", filter: map[string]any{fieldNotes: map[string]any{"$exists": false}}, wantIDs: []string{"vec-1", "vec-3", "vec-4", "vec-6"}},
			{name: "$and", filter: map[string]any{"$and": []any{map[string]any{fieldMake: "toyota"}, map[string]any{fieldBodyStyle: "suv"}}}, wantIDs: []string{"vec-4"}},
			{name: "$or", filter: map[string]any{"$or": []any{map[string]any{fieldPrice: map[string]any{"$lt": 15.0}}, map[string]any{fieldPrice: map[string]any{"$gt": 55.0}}}}, wantIDs: []string{"vec-1", "vec-6"}},
			{name: "$not", filter: map[string]any{"$not": map[string]any{fieldBodyStyle: "sedan"}}, wantIDs: []string{"vec-4", "vec-6"}},
			{name: "implicit conjunction of fields", filter: map[string]any{fieldMake: "hyundai", fieldInStock: true}, wantIDs: []string{"vec-1"}},
			{name: "nested logical operators", filter: map[string]any{"$and": []any{
				map[string]any{"$or": []any{map[string]any{fieldMake: "hyundai"}, map[string]any{fieldMake: "toyota"}}},
				map[string]any{"$not": map[string]any{fieldInStock: true}},
			}}, wantIDs: []string{"vec-2", "vec-4"}},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				resp := requireQuery(t, v, cfg, collection, &vector.QueryRequest{
					Vector: &vector.Record{Values: uniformValues(cfg.Dimensions)},
					Filter: tt.filter,
					TopK:   uint32(len(records)), //nolint:gosec // small test set
				})
				assert.ElementsMatch(t, tt.wantIDs, matchIDs(resp.Matches))
				assertScoreOrder(t, resp.Metric, resp.Matches)
			})
		}

		t.Run("an unsupported operator is INVALID_ARGUMENT", func(t *testing.T) {
			c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
			defer cancel()
			_, err := v.Query(c, &vector.QueryRequest{
				Collection: collection,
				Vector:     &vector.Record{Values: uniformValues(cfg.Dimensions)},
				Filter:     map[string]any{fieldMake: map[string]any{"$regex": "hyun.*"}},
				TopK:       5,
				Metadata:   cfg.QueryMetadata,
			})
			requireStatusCode(t, err, codes.InvalidArgument)
		})
	})

	t.Run("score_threshold is inclusive", func(t *testing.T) {
		if !cfg.HasOperation(OperationScoreThreshold) {
			c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
			defer cancel()
			threshold := 0.0
			_, err := v.Query(c, &vector.QueryRequest{Collection: collection, Vector: &vector.Record{Values: basisValues(cfg.Dimensions, 0)}, TopK: 5, ScoreThreshold: &threshold, Metadata: cfg.QueryMetadata})
			requireStatusCode(t, err, codes.Unimplemented, codes.InvalidArgument)
			return
		}

		baseline := requireQuery(t, v, cfg, collection, &vector.QueryRequest{
			Vector: &vector.Record{Values: basisValues(cfg.Dimensions, 0)},
			TopK:   3,
		})
		require.Len(t, baseline.Matches, 3)
		boundary := baseline.Matches[len(baseline.Matches)-1]

		threshold := boundary.Score
		filtered := requireQuery(t, v, cfg, collection, &vector.QueryRequest{
			Vector:         &vector.Record{Values: basisValues(cfg.Dimensions, 0)},
			TopK:           10,
			ScoreThreshold: &threshold,
		})
		assert.Equal(t, baseline.Metric, filtered.Metric)

		var boundaryRetained bool
		for _, match := range filtered.Matches {
			if match.Record.ID == boundary.Record.ID {
				boundaryRetained = true
			}
			if filtered.Metric.HigherIsBetter() {
				assert.GreaterOrEqual(t, match.Score, threshold-scoreEpsilon, "a higher-is-better metric keeps scores >= score_threshold")
			} else {
				assert.LessOrEqual(t, match.Score, threshold+scoreEpsilon, "euclidean distance keeps scores <= score_threshold")
			}
		}
		assert.True(t, boundaryRetained, "score_threshold is inclusive, so the boundary match is retained")
	})

	t.Run("BatchQuery", func(t *testing.T) {
		t.Run("returns one result per query in request order", func(t *testing.T) {
			c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
			defer cancel()
			queries := []vector.QueryRequest{
				{Vector: &vector.Record{Values: basisValues(cfg.Dimensions, 0)}, TopK: 1, IncludeValues: true},
				{Vector: &vector.Record{Values: basisValues(cfg.Dimensions, 2)}, TopK: 1, IncludeValues: true},
				{Vector: &vector.Record{Values: basisValues(cfg.Dimensions, 0)}, TopK: 2, IncludeValues: true},
			}
			resp, err := v.BatchQuery(c, &vector.BatchQueryRequest{Collection: collection, Queries: queries, Metadata: cfg.QueryMetadata})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Len(t, resp.Results, len(queries))
			for i, result := range resp.Results {
				require.NoError(t, result.Error, "result %d must not carry an error", i)
				require.NotNil(t, result.Response, "result %d must carry a response", i)
				assert.NotEqual(t, vector.DistanceMetricUnspecified, result.Response.Metric, "result %d must report a concrete metric", i)
				assert.LessOrEqual(t, len(result.Response.Matches), int(queries[i].TopK))
				assertScoreOrder(t, result.Response.Metric, result.Response.Matches)
			}
			assert.Equal(t, "vec-1", resp.Results[0].Response.Matches[0].Record.ID)
			assert.Equal(t, "vec-3", resp.Results[1].Response.Matches[0].Record.ID)
		})

		t.Run("an invalid query yields a per-query error, not an RPC failure", func(t *testing.T) {
			c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
			defer cancel()
			queries := []vector.QueryRequest{
				{Vector: &vector.Record{Values: basisValues(cfg.Dimensions, 0)}, TopK: 1},
				// Neither vector nor by_id is set.
				{TopK: 1},
				{Vector: &vector.Record{Values: basisValues(cfg.Dimensions, 2)}, TopK: 1},
			}
			resp, err := v.BatchQuery(c, &vector.BatchQueryRequest{Collection: collection, Queries: queries, Metadata: cfg.QueryMetadata})
			require.NoError(t, err, "a failing query must not fail the RPC")
			require.NotNil(t, resp)
			require.Len(t, resp.Results, len(queries), "every query has a result, in request order")

			// Exactly one of Response or Error is set per result.
			invalid := resp.Results[1]
			require.Error(t, invalid.Error, "the invalid query carries an error")
			assert.Nil(t, invalid.Response, "an errored result carries no response")
			st, ok := status.FromError(invalid.Error)
			require.True(t, ok, "per-query errors must be gRPC status errors, got %T: %v", invalid.Error, invalid.Error)
			assert.Equal(t, codes.InvalidArgument, st.Code())

			for _, i := range []int{0, 2} {
				require.NoError(t, resp.Results[i].Error, "result %d must succeed", i)
				require.NotNil(t, resp.Results[i].Response, "result %d must carry a response", i)
				require.NotEmpty(t, resp.Results[i].Response.Matches)
			}
			assert.Equal(t, "vec-1", resp.Results[0].Response.Matches[0].Record.ID)
			assert.Equal(t, "vec-3", resp.Results[2].Response.Matches[0].Record.ID)
		})

		t.Run("a provider-rejected query yields a per-query error", func(t *testing.T) {
			c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
			defer cancel()
			queries := []vector.QueryRequest{
				// A query vector whose dimensionality disagrees with the
				// collection.
				{Vector: &vector.Record{Values: basisValues(cfg.Dimensions+1, 0)}, TopK: 1},
				{Vector: &vector.Record{Values: basisValues(cfg.Dimensions, 0)}, TopK: 1},
			}
			resp, err := v.BatchQuery(c, &vector.BatchQueryRequest{Collection: collection, Queries: queries, Metadata: cfg.QueryMetadata})
			require.NoError(t, err, "a failing query must not fail the RPC")
			require.NotNil(t, resp)
			require.Len(t, resp.Results, len(queries))

			require.Error(t, resp.Results[0].Error)
			assert.Nil(t, resp.Results[0].Response)
			st, ok := status.FromError(resp.Results[0].Error)
			require.True(t, ok, "per-query errors must be gRPC status errors, got %T: %v", resp.Results[0].Error, resp.Results[0].Error)
			assert.NotEqual(t, codes.OK, st.Code(), "a per-query error carries a canonical non-OK code")

			require.NoError(t, resp.Results[1].Error)
			require.NotNil(t, resp.Results[1].Response)
			require.NotEmpty(t, resp.Results[1].Response.Matches)
		})

		t.Run("a request-wide failure fails the RPC", func(t *testing.T) {
			c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
			defer cancel()
			_, err := v.BatchQuery(c, &vector.BatchQueryRequest{
				Collection: "conf-" + component + "-missing-" + randSuffix(),
				Queries:    []vector.QueryRequest{{Vector: &vector.Record{Values: basisValues(cfg.Dimensions, 0)}, TopK: 1}},
				Metadata:   cfg.QueryMetadata,
			})
			requireStatusCode(t, err, codes.NotFound)
		})

		t.Run("an empty batch", func(t *testing.T) {
			c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
			defer cancel()
			resp, err := v.BatchQuery(c, &vector.BatchQueryRequest{Collection: collection, Metadata: cfg.QueryMetadata})
			if err != nil {
				requireStatusCode(t, err, codes.InvalidArgument)
				return
			}
			require.NotNil(t, resp)
			assert.Empty(t, resp.Results)
		})
	})

	t.Run("concurrent upsert and query", func(t *testing.T) {
		raceCollection := newCollection(t, "race")
		var wg sync.WaitGroup
		errCh := make(chan error, 16)
		for i := range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
				defer cancel()
				_, err := v.Upsert(c, &vector.UpsertRequest{
					Collection: raceCollection,
					Records:    []vector.Record{{ID: fmt.Sprintf("race-%02d", i), Values: basisValues(cfg.Dimensions, i), Metadata: map[string]any{fieldMake: "race", fieldPrice: float64(i)}}},
					Metadata:   cfg.UpsertMetadata,
					Options:    cfg.waitOptions(),
				})
				if err != nil {
					errCh <- err
				}
			}()
			wg.Add(1)
			go func() {
				defer wg.Done()
				c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
				defer cancel()
				_, err := v.Query(c, &vector.QueryRequest{Collection: raceCollection, Vector: &vector.Record{Values: basisValues(cfg.Dimensions, 0)}, TopK: 2, Metadata: cfg.QueryMetadata})
				if err != nil {
					errCh <- err
				}
			}()
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			assert.NoError(t, err)
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()

		_, err := v.Query(canceled, &vector.QueryRequest{Collection: collection, Vector: &vector.Record{Values: basisValues(cfg.Dimensions, 0)}, TopK: 1})
		assertContextCanceled(t, err)

		_, err = v.Upsert(canceled, &vector.UpsertRequest{Collection: collection, Records: []vector.Record{{ID: "cancel-upsert", Values: basisValues(cfg.Dimensions, 0)}}})
		assertContextCanceled(t, err)

		_, err = v.Delete(canceled, &vector.DeleteRequest{Collection: collection, IDs: []string{"cancel-upsert"}})
		assertContextCanceled(t, err)
	})

	t.Run("Delete", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		resp, err := v.Delete(c, &vector.DeleteRequest{Collection: collection, IDs: []string{"vec-1", "vec-2"}, Metadata: cfg.UpsertMetadata, Options: cfg.waitOptions()})
		require.NoError(t, err)
		require.NotNil(t, resp)
		// A delete that waited for completion has been applied.
		assert.Equal(t, search.IndexAckCompleted, resp.Ack)

		got, err := v.Get(c, &vector.GetRequest{Collection: collection, IDs: []string{"vec-1", "vec-2"}, IncludeValues: true, Metadata: cfg.QueryMetadata})
		require.NoError(t, err)
		assert.Empty(t, got.Records)

		// The remaining records are untouched.
		got, err = v.Get(c, &vector.GetRequest{Collection: collection, IDs: []string{"vec-3", "vec-4", "vec-5", "vec-6"}, Metadata: cfg.QueryMetadata})
		require.NoError(t, err)
		assert.Len(t, got.Records, 4)
	})

	t.Run("Delete with missing ids succeeds", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		// vec-1 was deleted above and never-existed was never upserted:
		// missing ids are not failures, so a delete is safe to retry.
		resp, err := v.Delete(c, &vector.DeleteRequest{Collection: collection, IDs: []string{"vec-1", "never-existed"}, Metadata: cfg.UpsertMetadata, Options: cfg.waitOptions()})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, search.IndexAckCompleted, resp.Ack)

		resp, err = v.Delete(c, &vector.DeleteRequest{Collection: collection, IDs: []string{"never-existed-either"}, Metadata: cfg.UpsertMetadata})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Contains(t, []search.IndexAck{search.IndexAckQueued, search.IndexAckCompleted}, resp.Ack, "a successful delete never returns INDEX_ACK_UNSPECIFIED")
	})

	t.Run("DeleteCollection", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		require.NoError(t, v.DeleteCollection(c, &vector.DeleteCollectionRequest{Collection: collection, Metadata: cfg.CreateCollectionMetadata}))
		collectionDeleted = true

		_, err := v.GetCollection(c, &vector.GetCollectionRequest{Collection: collection})
		requireStatusCode(t, err, codes.NotFound)
	})

	t.Run("close", func(t *testing.T) { require.NoError(t, v.Close()) })

	t.Run("post-close calls return errors", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("post-close calls must return errors, not panic: %v", r)
			}
		}()

		_, err := v.Upsert(ctx, &vector.UpsertRequest{Collection: collection, Records: []vector.Record{{ID: "post-close", Values: basisValues(cfg.Dimensions, 0)}}})
		require.Error(t, err)
		_, err = v.Query(ctx, &vector.QueryRequest{Collection: collection, Vector: &vector.Record{Values: basisValues(cfg.Dimensions, 0)}, TopK: 1})
		require.Error(t, err)
		_, err = v.Get(ctx, &vector.GetRequest{Collection: collection, IDs: []string{"post-close"}})
		require.Error(t, err)
		_, err = v.Delete(ctx, &vector.DeleteRequest{Collection: collection, IDs: []string{"post-close"}})
		require.Error(t, err)
	})
}

// waitOptions returns the options used whenever the suite needs a final
// provider result before reading back.
func (c TestConfig) waitOptions() search.IndexingOptions {
	return search.IndexingOptions{
		Mode:          search.IndexingModeWaitForCompletion,
		WaitTimeout:   c.WaitTimeout,
		OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest,
	}
}

// metricDeclared reports whether the component declares support for a metric.
func (c TestConfig) metricDeclared(metric vector.DistanceMetric) bool {
	switch metric {
	case vector.DistanceMetricCosine:
		return c.HasOperation(OperationMetricCosine)
	case vector.DistanceMetricDotProduct:
		return c.HasOperation(OperationMetricDotProduct)
	case vector.DistanceMetricEuclidean:
		return c.HasOperation(OperationMetricEuclidean)
	case vector.DistanceMetricUnspecified:
		return false
	default:
		return false
	}
}

func metricName(metric vector.DistanceMetric) string {
	switch metric {
	case vector.DistanceMetricCosine:
		return "cosine"
	case vector.DistanceMetricDotProduct:
		return "dot-product"
	case vector.DistanceMetricEuclidean:
		return "euclidean"
	case vector.DistanceMetricUnspecified:
		return "unspecified"
	default:
		return fmt.Sprintf("metric-%d", metric)
	}
}

// basisValues returns a deterministic dense vector of the requested
// dimensionality with its peak at index seed.
func basisValues(dimensions, seed int) []float32 {
	if dimensions <= 0 {
		dimensions = defaultDimensions
	}
	values := make([]float32, dimensions)
	values[seed%dimensions] = 1
	values[(seed+1)%dimensions] = 0.1
	return values
}

// uniformValues returns a dense vector with every component set to 1, which
// has a strictly positive similarity to every basis vector: a filtered query
// with it returns exactly the records the filter selects.
func uniformValues(dimensions int) []float32 {
	if dimensions <= 0 {
		dimensions = defaultDimensions
	}
	values := make([]float32, dimensions)
	for i := range values {
		values[i] = 1
	}
	return values
}

// conformanceRecords are the records seeded into the shared collection. The
// filter cases above name the exact subset each filter selects, so any change
// here must be mirrored there.
func conformanceRecords(dimensions int) []vector.Record {
	makes := []string{"hyundai", "hyundai", "toyota", "toyota", "ford", "ford"}
	bodyStyles := []string{"sedan", "sedan", "sedan", "suv", "sedan", "suv"}
	prices := []float64{10, 20, 30, 40, 50, 60}
	inStock := []bool{true, false, true, false, true, false}
	notes := map[int]string{1: "demo unit", 4: "fleet"}
	records := make([]vector.Record, len(makes))
	for i := range records {
		md := map[string]any{
			fieldMake:      makes[i],
			fieldBodyStyle: bodyStyles[i],
			fieldPrice:     prices[i],
			fieldInStock:   inStock[i],
		}
		if note, ok := notes[i]; ok {
			md[fieldNotes] = note
		}
		records[i] = vector.Record{
			ID:       fmt.Sprintf("vec-%d", i+1),
			Values:   basisValues(dimensions, i),
			Payload:  []byte(fmt.Sprintf(`{"rank":%d}`, i+1)),
			Metadata: md,
		}
	}
	return records
}

func recordsByID(records []vector.Record) map[string]vector.Record {
	byID := make(map[string]vector.Record, len(records))
	for _, record := range records {
		byID[record.ID] = record
	}
	return byID
}

func recordIDs(records []vector.Record) []string {
	ids := make([]string, 0, len(records))
	for _, record := range records {
		ids = append(ids, record.ID)
	}
	return ids
}

func matchIDs(matches []vector.Match) []string {
	ids := make([]string, 0, len(matches))
	for _, match := range matches {
		ids = append(ids, match.Record.ID)
	}
	return ids
}

// jsonNormalize round-trips a metadata map through JSON so that numeric
// values compare as float64 regardless of the Go type a provider decoded them
// into.
func jsonNormalize(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	decoded := map[string]any{}
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	return decoded
}

func requireQuery(t *testing.T, v vector.Vector, cfg TestConfig, collection string, req *vector.QueryRequest) *vector.QueryResponse {
	t.Helper()
	c, cancel := context.WithTimeout(t.Context(), defaultCallTimeout)
	defer cancel()
	req.Collection = collection
	if req.Metadata == nil {
		req.Metadata = cfg.QueryMetadata
	}
	resp, err := v.Query(c, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	return resp
}

// assertScoreOrder asserts the score direction of the effective metric:
// cosine and dot product are higher-is-better, euclidean is lower-is-better.
func assertScoreOrder(t *testing.T, metric vector.DistanceMetric, matches []vector.Match) {
	t.Helper()
	for i := 1; i < len(matches); i++ {
		if metric.HigherIsBetter() {
			assert.GreaterOrEqual(t, matches[i-1].Score, matches[i].Score, "%s scores must be non-increasing", metricName(metric))
			continue
		}
		assert.LessOrEqual(t, matches[i-1].Score, matches[i].Score, "%s scores must be non-decreasing", metricName(metric))
	}
}

// requireStatusCode asserts that a component returned a gRPC status error with
// one of the expected canonical codes.
func requireStatusCode(t *testing.T, err error, want ...codes.Code) {
	t.Helper()
	st := requireStatusError(t, err)
	assert.Contains(t, want, st.Code(), "unexpected status code %s: %v", st.Code(), err)
}

// requireStatusError asserts that err is a non-OK gRPC status error.
func requireStatusError(t *testing.T, err error) *status.Status {
	t.Helper()
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok, "component errors must be gRPC status errors, got %T: %v", err, err)
	require.NotEqual(t, codes.OK, st.Code())
	return st
}

// assertFailedItems asserts that exactly the expected ids failed and that every
// failure carries a non-OK canonical code.
func assertFailedItems(t *testing.T, items []search.FailedItem, wantIDs []string) {
	t.Helper()
	gotIDs := make([]string, 0, len(items))
	for _, item := range items {
		gotIDs = append(gotIDs, item.ID)
		require.NotNil(t, item.Error, "FailedItem %q must carry an error", item.ID)
		assert.NotEqual(t, codes.OK, item.Error.Code(), "FailedItem %q must not carry an OK status", item.ID)
	}
	assert.ElementsMatch(t, wantIDs, gotIDs)
}

func assertContextCanceled(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	if errors.Is(err, context.Canceled) {
		return
	}
	st, ok := status.FromError(err)
	require.True(t, ok, "expected a context.Canceled or a status error, got %T: %v", err, err)
	assert.Equal(t, codes.Canceled, st.Code())
}

func countString(values []string, want string) int {
	count := 0
	for _, value := range values {
		if value == want {
			count++
		}
	}
	return count
}

func randSuffix() string { return uuid.NewString()[:8] }
