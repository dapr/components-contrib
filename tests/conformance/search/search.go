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

package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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
	"github.com/dapr/kit/config"
)

// Capability opt-ins declared through the `operations` list of a component in
// tests/config/search/tests.yml. A capability that is not declared is asserted
// to be reported as UNIMPLEMENTED or skipped, so that a component never
// silently loses coverage.
const (
	// OperationQueuedAck declares a native durable queued acknowledgement.
	// Components without one must reject CONTINUE_ASYNC with INVALID_ARGUMENT
	// and must never return INDEX_ACK_QUEUED.
	OperationQueuedAck = "queued-ack"
	// OperationTotalHits declares that the provider supplies total_hits.
	OperationTotalHits = "total-hits"
	// OperationContinuationToken declares continuation-token pagination.
	OperationContinuationToken = "continuation-token"
	// OperationHighlights declares highlight_fields support.
	OperationHighlights = "highlights"
	// OperationNativeQuery declares provider-native query passthrough.
	OperationNativeQuery = "native-query"
	// OperationFilter declares portable filter support.
	OperationFilter = "filter"
	// OperationSort declares sort clause support.
	OperationSort = "sort"
	// OperationReturnFields declares return_fields projection support.
	OperationReturnFields = "return-fields"
	// OperationSearchFields declares search_fields restriction support.
	OperationSearchFields = "search-fields"
)

const (
	defaultWaitTimeout = 20 * time.Second
	defaultCallTimeout = 60 * time.Second
)

// Content fields used by the conformance documents. Filters address these
// content keys directly (no prefix); the provider must be configured through
// TestConfig.CreateIndexMetadata so that they are filterable and sortable.
const (
	fieldTitle    = "title"
	fieldBody     = "body"
	fieldCategory = "category"
	fieldPrice    = "price"
	fieldInStock  = "inStock"
	fieldNotes    = "notes"
)

// TestConfig is the search conformance configuration. Everything that is
// provider-specific travels through component metadata maps supplied by
// tests.yml so that the suite itself stays portable.
type TestConfig struct {
	utils.CommonConfig

	// CreateIndexMetadata is passed verbatim to CreateIndex.
	CreateIndexMetadata map[string]string `mapstructure:"createIndexMetadata"`
	// IndexMetadata is passed verbatim to IndexDocuments and DeleteDocuments.
	IndexMetadata map[string]string `mapstructure:"indexMetadata"`
	// SearchMetadata is passed verbatim to Search and GetDocuments.
	SearchMetadata map[string]string `mapstructure:"searchMetadata"`
	// WaitTimeout is used with INDEXING_MODE_WAIT_FOR_COMPLETION.
	WaitTimeout time.Duration `mapstructure:"waitTimeout"`
}

func NewTestConfig(componentName string, operations []string, configMap map[string]interface{}) (TestConfig, error) {
	tc := TestConfig{
		CommonConfig: utils.CommonConfig{
			ComponentType: "search",
			ComponentName: componentName,
			Operations:    utils.NewStringSet(operations...),
		},
		WaitTimeout: defaultWaitTimeout,
	}

	err := config.Decode(configMap, &tc)

	return tc, err
}

// writeOperation is a write of the Search contract that accepts
// IndexingOptions and reports an IndexAck: IndexDocuments and DeleteDocuments
// share the same mode/wait semantics and are exercised through the same
// matrix.
type writeOperation struct {
	name string
	// call performs the write against index with options and returns the ack.
	call func(ctx context.Context, index string, options search.IndexingOptions) (search.IndexAck, []search.FailedItem, error)
}

//nolint:gocyclo,maintidx // A conformance suite is intentionally a long linear list of scenarios.
func ConformanceTests(t *testing.T, props map[string]string, s search.Search, cfg TestConfig) {
	ctx := t.Context()
	component := cfg.ComponentName

	t.Run("init", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		require.NoError(t, s.Init(c, search.Metadata{Base: metadata.Base{Properties: props}}))
	})
	if t.Failed() {
		t.Fatal("init failed")
	}

	t.Run("init idempotent", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		require.NoError(t, s.Init(c, search.Metadata{Base: metadata.Base{Properties: props}}))
	})

	newIndex := func(t *testing.T, suffix string) string {
		t.Helper()
		name := fmt.Sprintf("conf-%s-%s-%s", component, suffix, randSuffix())
		c, cancel := context.WithTimeout(t.Context(), defaultCallTimeout)
		defer cancel()
		require.NoError(t, s.CreateIndex(c, &search.CreateIndexRequest{Index: name, Metadata: cfg.CreateIndexMetadata}))
		t.Cleanup(func() {
			cleanCtx, cleanCancel := context.WithTimeout(context.Background(), defaultCallTimeout)
			defer cleanCancel()
			_ = s.DeleteIndex(cleanCtx, &search.DeleteIndexRequest{Index: name, Metadata: cfg.CreateIndexMetadata})
		})
		return name
	}

	// The write operations that share IndexingOptions semantics.
	writeOperations := []writeOperation{
		{
			name: "IndexDocuments",
			call: func(c context.Context, index string, options search.IndexingOptions) (search.IndexAck, []search.FailedItem, error) {
				id := "write-" + randSuffix()
				docs := []search.Document{{ID: id, Content: contentBytes(t, map[string]any{fieldTitle: "Hyundai Sonata " + id, fieldBody: "write matrix listing", fieldCategory: "sedan", fieldPrice: 11.0, fieldInStock: true})}}
				resp, err := s.IndexDocuments(c, &search.IndexDocumentsRequest{Index: index, Documents: docs, Metadata: cfg.IndexMetadata, Options: options})
				if err != nil {
					return search.IndexAckUnspecified, nil, err
				}
				require.NotNil(t, resp)
				return resp.Ack, resp.FailedItems, nil
			},
		},
		{
			name: "DeleteDocuments",
			call: func(c context.Context, index string, options search.IndexingOptions) (search.IndexAck, []search.FailedItem, error) {
				// Deletes of IDs that never existed are not failures, so the
				// delete matrix does not need a seeded document.
				resp, err := s.DeleteDocuments(c, &search.DeleteDocumentsRequest{Index: index, IDs: []string{"delete-" + randSuffix()}, Metadata: cfg.IndexMetadata, Options: options})
				if err != nil {
					return search.IndexAckUnspecified, nil, err
				}
				require.NotNil(t, resp)
				return resp.Ack, nil, nil
			},
		},
	}

	indexName := fmt.Sprintf("conf-%s-%s", component, randSuffix())
	indexDeleted := false
	t.Cleanup(func() {
		if indexDeleted {
			return
		}
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), defaultCallTimeout)
		defer cleanCancel()
		_ = s.DeleteIndex(cleanCtx, &search.DeleteIndexRequest{Index: indexName, Metadata: cfg.CreateIndexMetadata})
	})

	t.Run("CreateIndex", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		require.NoError(t, s.CreateIndex(c, &search.CreateIndexRequest{Index: indexName, Metadata: cfg.CreateIndexMetadata}))
	})
	if t.Failed() {
		t.Fatal("CreateIndex failed")
	}

	t.Run("CreateIndex on an existing index returns ALREADY_EXISTS", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		err := s.CreateIndex(c, &search.CreateIndexRequest{Index: indexName, Metadata: cfg.CreateIndexMetadata})
		requireStatusCode(t, err, codes.AlreadyExists)

		// The existing index is neither reconciled nor duplicated.
		resp, err := s.ListIndexes(c, &search.ListIndexesRequest{})
		require.NoError(t, err)
		assert.Equal(t, 1, countString(resp.Indexes, indexName), "an index must be listed exactly once after a repeated CreateIndex")
	})

	t.Run("GetIndex", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		resp, err := s.GetIndex(c, &search.GetIndexRequest{Index: indexName})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, indexName, resp.Index)
		// DocumentCount is approximate and providers that cannot supply it
		// efficiently return 0, so only its presence is asserted here.
		assert.GreaterOrEqual(t, resp.DocumentCount, uint64(0))
	})

	t.Run("GetIndex on a missing index", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		_, err := s.GetIndex(c, &search.GetIndexRequest{Index: "conf-" + component + "-missing-" + randSuffix()})
		requireStatusCode(t, err, codes.NotFound)
	})

	t.Run("DeleteIndex on a missing index", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		err := s.DeleteIndex(c, &search.DeleteIndexRequest{Index: "conf-" + component + "-missing-" + randSuffix()})
		requireStatusCode(t, err, codes.NotFound)
	})

	t.Run("ListIndexes contains the index", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		resp, err := s.ListIndexes(c, &search.ListIndexesRequest{})
		require.NoError(t, err)
		require.Contains(t, resp.Indexes, indexName)
	})

	t.Run("keyed upsert validation", func(t *testing.T) {
		tests := []struct {
			name      string
			documents []search.Document
		}{
			{
				name:      "empty id",
				documents: []search.Document{{ID: "", Content: contentBytes(t, map[string]any{fieldTitle: "no id"})}},
			},
			{
				name: "empty id among valid ids",
				documents: []search.Document{
					{ID: "valid-1", Content: contentBytes(t, map[string]any{fieldTitle: "valid"})},
					{ID: "", Content: contentBytes(t, map[string]any{fieldTitle: "no id"})},
				},
			},
			{
				name: "duplicate ids",
				documents: []search.Document{
					{ID: "dupe", Content: contentBytes(t, map[string]any{fieldTitle: "first"})},
					{ID: "dupe", Content: contentBytes(t, map[string]any{fieldTitle: "second"})},
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
				defer cancel()
				resp, err := s.IndexDocuments(c, &search.IndexDocumentsRequest{Index: indexName, Documents: tt.documents, Metadata: cfg.IndexMetadata})
				// The contract rejects the request before the provider is
				// invoked; the failure is request-wide, never a FailedItem.
				requireStatusCode(t, err, codes.InvalidArgument)
				if resp != nil {
					assert.Empty(t, resp.FailedItems, "a rejected keyed upsert must not report per-item failures")
				}
			})
		}
	})

	t.Run("indexing options validation", func(t *testing.T) {
		tests := []struct {
			name    string
			options search.IndexingOptions
			// deadline shorter than WaitTimeout when set.
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
						_, _, err := op.call(c, indexName, tt.options)
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
						modeIndex := newIndex(t, "mode")
						c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
						defer cancel()
						ack, failed, err := op.call(c, modeIndex, tt.options)

						if tt.options.OnWaitTimeout == search.IndexingWaitTimeoutActionContinueAsync && !cfg.HasOperation(OperationQueuedAck) {
							// CONTINUE_ASYNC requires a native durable queued
							// acknowledgement; providers without one reject it
							// before invoking the provider.
							requireStatusCode(t, err, codes.InvalidArgument)
							return
						}

						if tt.options.Mode == search.IndexingModeWaitForCompletion && tt.options.OnWaitTimeout == search.IndexingWaitTimeoutActionFailRequest && err != nil {
							// A wait that expires with FAIL_REQUEST is reported
							// as DEADLINE_EXCEEDED rather than a partial success.
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
			okIndex := newIndex(t, "ok")
			c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
			defer cancel()
			resp, err := s.IndexDocuments(c, &search.IndexDocumentsRequest{Index: okIndex, Documents: conformanceDocuments(t), Metadata: cfg.IndexMetadata, Options: cfg.waitOptions()})
			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Empty(t, resp.FailedItems)
			assert.Equal(t, search.IndexAckCompleted, resp.Ack)
		})

		// Content that is not a JSON object is rejected by the runtime into
		// failed_items before the component is invoked (see
		// search.ValidateDocumentContent), so the component is only ever handed
		// valid JSON objects and that case is not exercised here.

		t.Run("a request-wide failure is not duplicated per item", func(t *testing.T) {
			c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
			defer cancel()
			resp, err := s.IndexDocuments(c, &search.IndexDocumentsRequest{
				Index:     "conf-" + component + "-missing-" + randSuffix(),
				Documents: conformanceDocuments(t),
				Metadata:  cfg.IndexMetadata,
				Options:   cfg.waitOptions(),
			})
			if err == nil {
				t.Skip("provider implicitly creates a missing index on write")
			}
			requireStatusError(t, err)
			if resp != nil {
				assert.Empty(t, resp.FailedItems, "request-wide failures belong in the RPC status, not in failed_items")
			}
		})
	})

	t.Run("IndexDocuments seeds the shared index", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		resp, err := s.IndexDocuments(c, &search.IndexDocumentsRequest{Index: indexName, Documents: conformanceDocuments(t), Metadata: cfg.IndexMetadata, Options: cfg.waitOptions()})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Empty(t, resp.FailedItems)
		require.Equal(t, search.IndexAckCompleted, resp.Ack)
	})
	if t.Failed() {
		t.Fatal("seeding the shared index failed")
	}

	t.Run("IndexDocuments is an idempotent keyed upsert", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		resp, err := s.IndexDocuments(c, &search.IndexDocumentsRequest{Index: indexName, Documents: conformanceDocuments(t), Metadata: cfg.IndexMetadata, Options: cfg.waitOptions()})
		require.NoError(t, err)
		require.Empty(t, resp.FailedItems)

		got, err := s.GetIndex(c, &search.GetIndexRequest{Index: indexName})
		require.NoError(t, err)
		if got.DocumentCount == 0 {
			t.Skip("provider does not report a document count")
		}
		assert.Equal(t, uint64(len(conformanceDocuments(t))), got.DocumentCount, "re-sending the same keyed upsert must not duplicate documents")
	})

	t.Run("GetDocuments", func(t *testing.T) {
		t.Run("round trips content and metadata", func(t *testing.T) {
			c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
			defer cancel()
			resp, err := s.GetDocuments(c, &search.GetDocumentsRequest{Index: indexName, IDs: []string{"car-1", "car-3"}, IncludeContent: true, Metadata: cfg.SearchMetadata})
			require.NoError(t, err)
			require.Len(t, resp.Documents, 2)

			want := documentsByID(conformanceDocuments(t))
			for _, got := range resp.Documents {
				expected, ok := want[got.ID]
				require.True(t, ok, "unexpected document id %q", got.ID)
				assert.JSONEq(t, string(expected.Content), string(got.Content))
				// Metadata is opaque: it is returned unchanged.
				assert.Equal(t, expected.Metadata, got.Metadata)
			}
		})

		t.Run("documents are returned in request order", func(t *testing.T) {
			c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
			defer cancel()
			ids := []string{"car-4", "car-1", "car-5", "car-2"}
			resp, err := s.GetDocuments(c, &search.GetDocumentsRequest{Index: indexName, IDs: ids, IncludeContent: false, Metadata: cfg.SearchMetadata})
			require.NoError(t, err)
			assert.Equal(t, ids, documentIDs(resp.Documents))

			// A missing id in the middle is omitted without disturbing the
			// order of the remaining documents.
			resp, err = s.GetDocuments(c, &search.GetDocumentsRequest{Index: indexName, IDs: []string{"car-4", "missing-" + randSuffix(), "car-1"}, IncludeContent: false, Metadata: cfg.SearchMetadata})
			require.NoError(t, err)
			assert.Equal(t, []string{"car-4", "car-1"}, documentIDs(resp.Documents))
		})

		t.Run("include_content false omits content", func(t *testing.T) {
			c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
			defer cancel()
			resp, err := s.GetDocuments(c, &search.GetDocumentsRequest{Index: indexName, IDs: []string{"car-1"}, IncludeContent: false, Metadata: cfg.SearchMetadata})
			require.NoError(t, err)
			require.Len(t, resp.Documents, 1)
			assert.Equal(t, "car-1", resp.Documents[0].ID)
			assert.Empty(t, resp.Documents[0].Content)
		})

		t.Run("missing ids are omitted, not errors", func(t *testing.T) {
			c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
			defer cancel()
			resp, err := s.GetDocuments(c, &search.GetDocumentsRequest{Index: indexName, IDs: []string{"car-1", "missing-" + randSuffix()}, IncludeContent: true, Metadata: cfg.SearchMetadata})
			require.NoError(t, err)
			require.Len(t, resp.Documents, 1)
			assert.Equal(t, "car-1", resp.Documents[0].ID)
		})

		t.Run("all ids missing returns an empty list", func(t *testing.T) {
			c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
			defer cancel()
			resp, err := s.GetDocuments(c, &search.GetDocumentsRequest{Index: indexName, IDs: []string{"missing-a", "missing-b"}, IncludeContent: true, Metadata: cfg.SearchMetadata})
			require.NoError(t, err)
			assert.Empty(t, resp.Documents)
		})
	})

	t.Run("Search by text", func(t *testing.T) {
		resp := requireSearch(t, s, cfg, indexName, &search.SearchRequest{Text: "hyundai", TopK: 5, IncludeContent: true})
		require.NotEmpty(t, resp.Hits)
		for _, hit := range resp.Hits {
			assert.NotEmpty(t, hit.Document.ID)
			assert.NotEmpty(t, hit.Document.Content)
		}
	})

	t.Run("Search scores are higher-is-better", func(t *testing.T) {
		resp := requireSearch(t, s, cfg, indexName, &search.SearchRequest{Text: "hyundai suv", TopK: 10, IncludeContent: true})
		require.NotEmpty(t, resp.Hits)
		for i := 1; i < len(resp.Hits); i++ {
			assert.GreaterOrEqual(t, resp.Hits[i-1].Score, resp.Hits[i].Score, "relevance-ordered hits must be non-increasing in score")
		}
	})

	t.Run("Search include_content false", func(t *testing.T) {
		resp := requireSearch(t, s, cfg, indexName, &search.SearchRequest{Text: "hyundai", TopK: 1, IncludeContent: false})
		require.NotEmpty(t, resp.Hits)
		assert.Empty(t, resp.Hits[0].Document.Content)
		assert.NotEmpty(t, resp.Hits[0].Document.ID)
	})

	t.Run("Search text and native are mutually exclusive", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		_, err := s.Search(c, &search.SearchRequest{Index: indexName, Text: "hyundai", Native: map[string]any{"q": "hyundai"}, TopK: 5, Metadata: cfg.SearchMetadata})
		requireStatusCode(t, err, codes.InvalidArgument)
	})

	t.Run("Search filter", func(t *testing.T) {
		if !cfg.HasOperation(OperationFilter) {
			t.Skipf("component %s does not declare the %q operation", component, OperationFilter)
		}

		// Filters address content fields directly with their JSON type:
		// numbers compare numerically, strings lexically and booleans by
		// value. Each case names the exact set of conformance documents it
		// must select.
		tests := []struct {
			name    string
			filter  map[string]any
			wantIDs []string
		}{
			{name: "$eq on a string", filter: map[string]any{fieldCategory: map[string]any{"$eq": "sedan"}}, wantIDs: []string{"car-1", "car-2"}},
			{name: "bare value is $eq shorthand", filter: map[string]any{fieldCategory: "suv"}, wantIDs: []string{"car-3", "car-5"}},
			{name: "$ne on a string", filter: map[string]any{fieldCategory: map[string]any{"$ne": "sedan"}}, wantIDs: []string{"car-3", "car-4", "car-5"}},
			{name: "$gt on a number", filter: map[string]any{fieldPrice: map[string]any{"$gt": 25.0}}, wantIDs: []string{"car-3", "car-4", "car-5"}},
			{name: "$gte on a number", filter: map[string]any{fieldPrice: map[string]any{"$gte": 25.0}}, wantIDs: []string{"car-2", "car-3", "car-4", "car-5"}},
			{name: "$lt on a number", filter: map[string]any{fieldPrice: map[string]any{"$lt": 25.0}}, wantIDs: []string{"car-1"}},
			{name: "$lte on a number", filter: map[string]any{fieldPrice: map[string]any{"$lte": 25.0}}, wantIDs: []string{"car-1", "car-2"}},
			{name: "numeric range on one field", filter: map[string]any{fieldPrice: map[string]any{"$gte": 20.0, "$lt": 40.0}}, wantIDs: []string{"car-2", "car-3"}},
			{name: "$in on strings", filter: map[string]any{fieldCategory: map[string]any{"$in": []any{"sedan", "truck"}}}, wantIDs: []string{"car-1", "car-2", "car-4"}},
			{name: "$nin on strings", filter: map[string]any{fieldCategory: map[string]any{"$nin": []any{"sedan", "truck"}}}, wantIDs: []string{"car-3", "car-5"}},
			{name: "$eq on a bool", filter: map[string]any{fieldInStock: map[string]any{"$eq": true}}, wantIDs: []string{"car-1", "car-3", "car-5"}},
			{name: "bare bool is $eq shorthand", filter: map[string]any{fieldInStock: false}, wantIDs: []string{"car-2", "car-4"}},
			{name: "$exists", filter: map[string]any{fieldNotes: map[string]any{"$exists": true}}, wantIDs: []string{"car-2", "car-4"}},
			{name: "$exists false", filter: map[string]any{fieldNotes: map[string]any{"$exists": false}}, wantIDs: []string{"car-1", "car-3", "car-5"}},
			{name: "$and", filter: map[string]any{"$and": []any{map[string]any{fieldCategory: "suv"}, map[string]any{fieldPrice: map[string]any{"$lt": 40.0}}}}, wantIDs: []string{"car-3"}},
			{name: "$or", filter: map[string]any{"$or": []any{map[string]any{fieldPrice: map[string]any{"$lt": 15.0}}, map[string]any{fieldPrice: map[string]any{"$gt": 45.0}}}}, wantIDs: []string{"car-1", "car-5"}},
			{name: "$not", filter: map[string]any{"$not": map[string]any{fieldCategory: "sedan"}}, wantIDs: []string{"car-3", "car-4", "car-5"}},
			{name: "implicit conjunction of fields", filter: map[string]any{fieldCategory: "sedan", fieldInStock: true}, wantIDs: []string{"car-1"}},
			{name: "nested logical operators", filter: map[string]any{"$and": []any{
				map[string]any{"$or": []any{map[string]any{fieldCategory: "sedan"}, map[string]any{fieldCategory: "suv"}}},
				map[string]any{"$not": map[string]any{fieldInStock: true}},
			}}, wantIDs: []string{"car-2"}},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				resp := requireSearch(t, s, cfg, indexName, &search.SearchRequest{Filter: tt.filter, TopK: 10, IncludeContent: true})
				assert.ElementsMatch(t, tt.wantIDs, hitIDs(resp.Hits))
			})
		}

		t.Run("filters combine with text", func(t *testing.T) {
			resp := requireSearch(t, s, cfg, indexName, &search.SearchRequest{Text: "hyundai", Filter: map[string]any{fieldCategory: "suv"}, TopK: 10, IncludeContent: true})
			assert.ElementsMatch(t, []string{"car-3", "car-5"}, hitIDs(resp.Hits))
		})

		t.Run("an unsupported operator is INVALID_ARGUMENT", func(t *testing.T) {
			c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
			defer cancel()
			_, err := s.Search(c, &search.SearchRequest{Index: indexName, Filter: map[string]any{fieldTitle: map[string]any{"$regex": "hyun.*"}}, TopK: 10, Metadata: cfg.SearchMetadata})
			requireStatusCode(t, err, codes.InvalidArgument)
		})
	})

	t.Run("Search sort", func(t *testing.T) {
		if !cfg.HasOperation(OperationSort) {
			t.Skipf("component %s does not declare the %q operation", component, OperationSort)
		}
		for _, order := range []search.SortOrder{search.SortOrderAsc, search.SortOrderDesc} {
			t.Run(sortOrderName(order), func(t *testing.T) {
				resp := requireSearch(t, s, cfg, indexName, &search.SearchRequest{
					TopK:           10,
					IncludeContent: true,
					Sort:           []search.SortClause{{Field: fieldPrice, Order: order}},
				})
				require.NotEmpty(t, resp.Hits)
				for i := 1; i < len(resp.Hits); i++ {
					prev := numeric(t, decodeContent(t, resp.Hits[i-1].Document.Content)[fieldPrice])
					cur := numeric(t, decodeContent(t, resp.Hits[i].Document.Content)[fieldPrice])
					if order == search.SortOrderDesc {
						assert.GreaterOrEqual(t, prev, cur)
					} else {
						assert.LessOrEqual(t, prev, cur)
					}
				}
			})
		}
	})

	t.Run("Search return_fields projection", func(t *testing.T) {
		if !cfg.HasOperation(OperationReturnFields) {
			t.Skipf("component %s does not declare the %q operation", component, OperationReturnFields)
		}
		resp := requireSearch(t, s, cfg, indexName, &search.SearchRequest{Text: "hyundai", TopK: 1, IncludeContent: true, ReturnFields: []string{fieldTitle}})
		require.NotEmpty(t, resp.Hits)
		content := decodeContent(t, resp.Hits[0].Document.Content)
		assert.Contains(t, content, fieldTitle)
		assert.NotContains(t, content, fieldPrice)
	})

	t.Run("Search search_fields restriction", func(t *testing.T) {
		if !cfg.HasOperation(OperationSearchFields) {
			t.Skipf("component %s does not declare the %q operation", component, OperationSearchFields)
		}
		// "second" only appears in the body of car-2.
		resp := requireSearch(t, s, cfg, indexName, &search.SearchRequest{Text: "second", SearchFields: []string{fieldTitle}, TopK: 5, IncludeContent: true})
		assert.Empty(t, resp.Hits)
	})

	t.Run("Search highlights", func(t *testing.T) {
		if !cfg.HasOperation(OperationHighlights) {
			t.Skipf("component %s does not declare the %q operation", component, OperationHighlights)
		}
		resp := requireSearch(t, s, cfg, indexName, &search.SearchRequest{Text: "hyundai", TopK: 1, IncludeContent: true, HighlightFields: []string{fieldTitle}})
		require.NotEmpty(t, resp.Hits)
		assert.NotEmpty(t, resp.Hits[0].Highlights)
	})

	t.Run("total_hits and its relation", func(t *testing.T) {
		resp := requireSearch(t, s, cfg, indexName, &search.SearchRequest{TopK: 2, IncludeContent: false})
		if resp.TotalHits == nil {
			assert.Equal(t, search.TotalHitsRelationUnspecified, resp.TotalHitsRelation, "total_hits_relation is UNSPECIFIED when total_hits is omitted")
			if cfg.HasOperation(OperationTotalHits) {
				t.Errorf("component %s declares %q but omitted total_hits", component, OperationTotalHits)
			}
			return
		}
		assert.Contains(t, []search.TotalHitsRelation{
			search.TotalHitsRelationExact,
			search.TotalHitsRelationLowerBound,
			search.TotalHitsRelationEstimate,
		}, resp.TotalHitsRelation, "a reported total_hits carries a concrete relation")
		assert.GreaterOrEqual(t, *resp.TotalHits, uint64(len(resp.Hits)))
		if resp.TotalHitsRelation == search.TotalHitsRelationExact {
			assert.Equal(t, uint64(len(conformanceDocuments(t))), *resp.TotalHits, "an exact total counts every indexed document")
		}
	})

	t.Run("pagination", func(t *testing.T) {
		if !cfg.HasOperation(OperationContinuationToken) {
			t.Skipf("component %s does not declare the %q operation", component, OperationContinuationToken)
		}

		pageIndex := newIndex(t, "page")
		const total = 12
		docs := make([]search.Document, total)
		wantIDs := make([]string, total)
		for i := range docs {
			id := fmt.Sprintf("page-%02d", i)
			wantIDs[i] = id
			// price is deliberately non-unique (three documents per value) so
			// that a sort on it needs the document id as a tie-breaker.
			docs[i] = search.Document{ID: id, Content: contentBytes(t, map[string]any{fieldTitle: fmt.Sprintf("car listing %02d", i), fieldBody: "pagination", fieldCategory: "page", fieldPrice: float64(i % 4), fieldInStock: i%2 == 0})}
		}
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		_, err := s.IndexDocuments(c, &search.IndexDocumentsRequest{Index: pageIndex, Documents: docs, Metadata: cfg.IndexMetadata, Options: cfg.waitOptions()})
		require.NoError(t, err)

		// The query shape is repeated unchanged on every page; only the token
		// moves.
		query := func(token string) *search.SearchRequest {
			return &search.SearchRequest{
				Index:             pageIndex,
				TopK:              5,
				IncludeContent:    true,
				ContinuationToken: token,
				Metadata:          cfg.SearchMetadata,
			}
		}

		// walk pages through every page of shape and returns the document ids
		// in the order they were visited, asserting that no document repeats.
		walk := func(t *testing.T, shape func(token string) *search.SearchRequest) (gotIDs []string, firstToken string) {
			t.Helper()
			seen := map[string]struct{}{}
			token := ""
			for page := 0; ; page++ {
				require.LessOrEqual(t, page, total, "pagination did not terminate")
				resp, pageErr := s.Search(c, shape(token))
				require.NoError(t, pageErr)
				if page == 0 {
					require.NotEmpty(t, resp.ContinuationToken, "a partial first page must return a continuation token")
					firstToken = resp.ContinuationToken
					if resp.TotalHits != nil && resp.TotalHitsRelation == search.TotalHitsRelationExact {
						assert.Equal(t, uint64(total), *resp.TotalHits)
					}
				}
				for _, hit := range resp.Hits {
					_, duplicate := seen[hit.Document.ID]
					assert.False(t, duplicate, "duplicate document %q across pages", hit.Document.ID)
					seen[hit.Document.ID] = struct{}{}
					gotIDs = append(gotIDs, hit.Document.ID)
				}
				// A non-empty token may still lead to a final empty page.
				if resp.ContinuationToken == "" {
					break
				}
				token = resp.ContinuationToken
			}
			return gotIDs, firstToken
		}

		gotIDs, firstToken := walk(t, query)
		sort.Strings(gotIDs)
		assert.Equal(t, wantIDs, gotIDs, "paging must visit every document exactly once")

		t.Run("a non-unique sort visits every document exactly once", func(t *testing.T) {
			if !cfg.HasOperation(OperationSort) {
				t.Skipf("component %s does not declare the %q operation", component, OperationSort)
			}
			// The sort key repeats across page boundaries, so only a stable
			// tie-breaker on the document id keeps pages disjoint and
			// complete.
			sorted := func(token string) *search.SearchRequest {
				req := query(token)
				req.Sort = []search.SortClause{{Field: fieldPrice, Order: search.SortOrderAsc}}
				return req
			}
			sortedIDs, _ := walk(t, sorted)
			visited := append([]string(nil), sortedIDs...)
			sort.Strings(visited)
			assert.Equal(t, wantIDs, visited, "a non-unique sort must still visit every document exactly once")

			// And the requested order is honoured across page boundaries.
			byID := documentsByID(docs)
			for i := 1; i < len(sortedIDs); i++ {
				prev := numeric(t, decodeContent(t, byID[sortedIDs[i-1]].Content)[fieldPrice])
				cur := numeric(t, decodeContent(t, byID[sortedIDs[i]].Content)[fieldPrice])
				assert.LessOrEqual(t, prev, cur, "sort order must hold across pages")
			}
		})

		t.Run("a malformed token is rejected", func(t *testing.T) {
			req := query("not-a-real-continuation-token")
			_, tokenErr := s.Search(c, req)
			requireStatusCode(t, tokenErr, codes.InvalidArgument)
		})

		t.Run("a token is bound to the query shape", func(t *testing.T) {
			// Same index, different page size: the token no longer matches the
			// query it was issued for.
			mismatched := query(firstToken)
			mismatched.TopK = 3
			_, tokenErr := s.Search(c, mismatched)
			requireStatusCode(t, tokenErr, codes.InvalidArgument)

			// Same page size, different projection.
			mismatched = query(firstToken)
			mismatched.IncludeContent = false
			_, tokenErr = s.Search(c, mismatched)
			requireStatusCode(t, tokenErr, codes.InvalidArgument)

			// Same page size, different filter.
			if cfg.HasOperation(OperationFilter) {
				mismatched = query(firstToken)
				mismatched.Filter = map[string]any{fieldCategory: map[string]any{"$eq": "page"}}
				_, tokenErr = s.Search(c, mismatched)
				requireStatusCode(t, tokenErr, codes.InvalidArgument)
			}

			// Same page size, different sort.
			if cfg.HasOperation(OperationSort) {
				mismatched = query(firstToken)
				mismatched.Sort = []search.SortClause{{Field: fieldPrice, Order: search.SortOrderDesc}}
				_, tokenErr = s.Search(c, mismatched)
				requireStatusCode(t, tokenErr, codes.InvalidArgument)
			}
		})
	})

	t.Run("native query passthrough", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		_, err := s.Search(c, &search.SearchRequest{Index: indexName, Native: map[string]any{"q": "hyundai"}, TopK: 5, Metadata: cfg.SearchMetadata})
		if !cfg.HasOperation(OperationNativeQuery) {
			requireStatusCode(t, err, codes.Unimplemented, codes.InvalidArgument)
			return
		}
		require.NoError(t, err)

		t.Run("a native query must not embed its own pagination", func(t *testing.T) {
			_, nativeErr := s.Search(c, &search.SearchRequest{Index: indexName, Native: map[string]any{"q": "hyundai", "offset": 10, "limit": 5}, TopK: 5, Metadata: cfg.SearchMetadata})
			requireStatusCode(t, nativeErr, codes.InvalidArgument)
		})
	})

	t.Run("concurrent indexing and search", func(t *testing.T) {
		concurrentIndex := newIndex(t, "conc")
		var wg sync.WaitGroup
		errCh := make(chan error, 16)
		for i := range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
				defer cancel()
				_, err := s.IndexDocuments(c, &search.IndexDocumentsRequest{
					Index:     concurrentIndex,
					Documents: []search.Document{{ID: fmt.Sprintf("conc-%02d", i), Content: contentBytes(t, map[string]any{fieldTitle: fmt.Sprintf("Hyundai concurrent %02d", i), fieldBody: "concurrent", fieldCategory: "race", fieldPrice: float64(i), fieldInStock: true})}},
					Metadata:  cfg.IndexMetadata,
					Options:   cfg.waitOptions(),
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
				_, err := s.Search(c, &search.SearchRequest{Index: concurrentIndex, Text: "concurrent", TopK: 5, IncludeContent: true, Metadata: cfg.SearchMetadata})
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

		_, err := s.Search(canceled, &search.SearchRequest{Index: indexName, Text: "hyundai", TopK: 1, Metadata: cfg.SearchMetadata})
		assertContextCanceled(t, err)

		_, err = s.IndexDocuments(canceled, &search.IndexDocumentsRequest{
			Index:     indexName,
			Documents: []search.Document{{ID: "cancelled", Content: contentBytes(t, map[string]any{fieldTitle: "cancelled"})}},
			Metadata:  cfg.IndexMetadata,
		})
		assertContextCanceled(t, err)

		_, err = s.GetDocuments(canceled, &search.GetDocumentsRequest{Index: indexName, IDs: []string{"car-1"}})
		assertContextCanceled(t, err)

		_, err = s.DeleteDocuments(canceled, &search.DeleteDocumentsRequest{Index: indexName, IDs: []string{"cancelled"}, Metadata: cfg.IndexMetadata})
		assertContextCanceled(t, err)
	})

	t.Run("DeleteDocuments", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		resp, err := s.DeleteDocuments(c, &search.DeleteDocumentsRequest{Index: indexName, IDs: []string{"car-1", "car-2"}, Metadata: cfg.IndexMetadata, Options: cfg.waitOptions()})
		require.NoError(t, err)
		require.NotNil(t, resp)
		// A delete that waited for completion has been applied.
		assert.Equal(t, search.IndexAckCompleted, resp.Ack)

		got, err := s.GetDocuments(c, &search.GetDocumentsRequest{Index: indexName, IDs: []string{"car-1", "car-2"}, IncludeContent: true, Metadata: cfg.SearchMetadata})
		require.NoError(t, err)
		assert.Empty(t, got.Documents)

		// The remaining documents are untouched.
		got, err = s.GetDocuments(c, &search.GetDocumentsRequest{Index: indexName, IDs: []string{"car-3", "car-4", "car-5"}, IncludeContent: false, Metadata: cfg.SearchMetadata})
		require.NoError(t, err)
		assert.Len(t, got.Documents, 3)
	})

	t.Run("DeleteDocuments with missing ids succeeds", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		// car-1 was deleted above and never-existed was never indexed:
		// missing ids are not failures, so a delete is safe to retry.
		resp, err := s.DeleteDocuments(c, &search.DeleteDocumentsRequest{Index: indexName, IDs: []string{"car-1", "never-existed"}, Metadata: cfg.IndexMetadata, Options: cfg.waitOptions()})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, search.IndexAckCompleted, resp.Ack)

		resp, err = s.DeleteDocuments(c, &search.DeleteDocumentsRequest{Index: indexName, IDs: []string{"never-existed-either"}, Metadata: cfg.IndexMetadata})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Contains(t, []search.IndexAck{search.IndexAckQueued, search.IndexAckCompleted}, resp.Ack, "a successful delete never returns INDEX_ACK_UNSPECIFIED")
	})

	t.Run("DeleteIndex", func(t *testing.T) {
		c, cancel := context.WithTimeout(ctx, defaultCallTimeout)
		defer cancel()
		require.NoError(t, s.DeleteIndex(c, &search.DeleteIndexRequest{Index: indexName, Metadata: cfg.CreateIndexMetadata}))
		indexDeleted = true

		_, err := s.GetIndex(c, &search.GetIndexRequest{Index: indexName})
		requireStatusCode(t, err, codes.NotFound)
	})

	t.Run("close", func(t *testing.T) { require.NoError(t, s.Close()) })

	t.Run("post-close calls return errors", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("post-close calls must return errors, not panic: %v", r)
			}
		}()

		_, err := s.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: indexName, Documents: []search.Document{{ID: "post-close", Content: contentBytes(t, map[string]any{fieldTitle: "post close"})}}})
		require.Error(t, err)
		_, err = s.Search(ctx, &search.SearchRequest{Index: indexName, Text: "post"})
		require.Error(t, err)
		_, err = s.GetDocuments(ctx, &search.GetDocumentsRequest{Index: indexName, IDs: []string{"post-close"}})
		require.Error(t, err)
		_, err = s.DeleteDocuments(ctx, &search.DeleteDocumentsRequest{Index: indexName, IDs: []string{"post-close"}})
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

// conformanceDocuments are the documents seeded into the shared index. The
// filter cases above name the exact subset each filter selects, so any change
// here must be mirrored there.
func conformanceDocuments(t *testing.T) []search.Document {
	t.Helper()
	return []search.Document{
		{ID: "car-1", Content: contentBytes(t, map[string]any{fieldTitle: "Hyundai Sonata", fieldBody: "first hyundai sedan listing", fieldCategory: "sedan", fieldPrice: 10.0, fieldInStock: true}), Metadata: map[string]string{"source": "conformance"}},
		{ID: "car-2", Content: contentBytes(t, map[string]any{fieldTitle: "Toyota Camry", fieldBody: "second sedan listing", fieldCategory: "sedan", fieldPrice: 25.0, fieldInStock: false, fieldNotes: "demo unit"}), Metadata: map[string]string{"source": "conformance"}},
		{ID: "car-3", Content: contentBytes(t, map[string]any{fieldTitle: "Hyundai Tucson", fieldBody: "hyundai suv reference", fieldCategory: "suv", fieldPrice: 30.0, fieldInStock: true}), Metadata: map[string]string{"source": "conformance"}},
		{ID: "car-4", Content: contentBytes(t, map[string]any{fieldTitle: "Ford Ranger", fieldBody: "truck listing", fieldCategory: "truck", fieldPrice: 40.0, fieldInStock: false, fieldNotes: "fleet"}), Metadata: map[string]string{"source": "conformance"}},
		{ID: "car-5", Content: contentBytes(t, map[string]any{fieldTitle: "Hyundai Palisade", fieldBody: "another hyundai suv note", fieldCategory: "suv", fieldPrice: 50.0, fieldInStock: true}), Metadata: map[string]string{"source": "conformance"}},
	}
}

func documentsByID(docs []search.Document) map[string]search.Document {
	byID := make(map[string]search.Document, len(docs))
	for _, doc := range docs {
		byID[doc.ID] = doc
	}
	return byID
}

func documentIDs(docs []search.Document) []string {
	ids := make([]string, 0, len(docs))
	for _, doc := range docs {
		ids = append(ids, doc.ID)
	}
	return ids
}

func hitIDs(hits []search.Hit) []string {
	ids := make([]string, 0, len(hits))
	for _, hit := range hits {
		ids = append(ids, hit.Document.ID)
	}
	return ids
}

func contentBytes(t *testing.T, content map[string]any) []byte {
	t.Helper()
	encoded, err := json.Marshal(content)
	require.NoError(t, err)
	return encoded
}

func decodeContent(t *testing.T, content []byte) map[string]any {
	t.Helper()
	decoded := map[string]any{}
	require.NoError(t, json.Unmarshal(content, &decoded))
	return decoded
}

func numeric(t *testing.T, value any) float64 {
	t.Helper()
	number, ok := value.(float64)
	require.True(t, ok, "expected a JSON number, got %T", value)
	return number
}

func sortOrderName(order search.SortOrder) string {
	switch order {
	case search.SortOrderAsc:
		return "asc"
	case search.SortOrderDesc:
		return "desc"
	case search.SortOrderUnspecified:
		return "unspecified"
	default:
		return fmt.Sprintf("order-%d", order)
	}
}

func requireSearch(t *testing.T, s search.Search, cfg TestConfig, indexName string, req *search.SearchRequest) *search.SearchResponse {
	t.Helper()
	c, cancel := context.WithTimeout(t.Context(), defaultCallTimeout)
	defer cancel()
	req.Index = indexName
	if req.Metadata == nil {
		req.Metadata = cfg.SearchMetadata
	}
	resp, err := s.Search(c, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	return resp
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
