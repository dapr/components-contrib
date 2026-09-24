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

package meilisearch_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonmeilisearch "github.com/dapr/components-contrib/common/component/meilisearch"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/search"
	searchmeilisearch "github.com/dapr/components-contrib/search/meilisearch"
	"github.com/dapr/kit/logger"
)

const certWaitTimeout = 2 * time.Minute

// certIndexMetadata carries the component-specific index settings. The
// component itself adds the document id to sortableAttributes so that the
// pagination tie-breaker is always available.
func certIndexMetadata() map[string]string {
	return map[string]string{
		"searchableAttributes": "title,body,category,tag",
		"filterableAttributes": "category,tag,optional,price,available",
		"sortableAttributes":   "title,price,category",
	}
}

func certWaitOptions() search.IndexingOptions {
	return search.IndexingOptions{
		Mode:          search.IndexingModeWaitForCompletion,
		WaitTimeout:   certWaitTimeout,
		OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest,
	}
}

//nolint:gocyclo,maintidx // A certification suite is intentionally a long linear list of scenarios.
func TestMeilisearchSearchCertification(t *testing.T) {
	host := os.Getenv("MEILISEARCH_HOST")
	if host == "" {
		t.Skip("MEILISEARCH_HOST is required for Meilisearch certification tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	component := searchmeilisearch.NewMeilisearch(logger.NewLogger("cert"))
	require.NoError(t, component.Init(ctx, search.Metadata{Base: metadata.Base{Properties: map[string]string{"host": host, "apiKey": os.Getenv("MEILISEARCH_API_KEY")}}}))

	index := "cert-search-" + uuid.NewString()[:8]
	t.Cleanup(func() { _ = component.DeleteIndex(context.Background(), &search.DeleteIndexRequest{Index: index}) })

	t.Run("auth_failure", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("auth failure path panicked: %v", r)
			}
		}()

		bad := searchmeilisearch.NewMeilisearch(logger.NewLogger("cert.bad"))
		badCtx, badCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer badCancel()
		err := bad.Init(badCtx, search.Metadata{Base: metadata.Base{Properties: map[string]string{"host": host, "apiKey": "wrong-key"}}})
		if err == nil {
			err = bad.CreateIndex(badCtx, &search.CreateIndexRequest{Index: "cert-search-auth-" + uuid.NewString()[:8], Metadata: certIndexMetadata()})
			// A component-level failure is reported as a gRPC status error.
			requireStatusCode(t, err, codes.Unauthenticated, codes.PermissionDenied, codes.Internal)
			return
		}
		require.Error(t, err)
	})

	t.Run("component_metadata_contract", func(t *testing.T) {
		metadataProvider, ok := component.(interface{ GetComponentMetadata() metadata.MetadataMap })
		require.True(t, ok)
		assertMeilisearchMetadataContract(t, metadataProvider.GetComponentMetadata())
	})

	t.Run("unicode_content", func(t *testing.T) {
		unicodeIndex := "cert-search-unicode-" + uuid.NewString()[:8]
		t.Cleanup(func() {
			_ = component.DeleteIndex(context.Background(), &search.DeleteIndexRequest{Index: unicodeIndex})
		})
		require.NoError(t, component.CreateIndex(ctx, &search.CreateIndexRequest{Index: unicodeIndex, Metadata: certIndexMetadata()}))

		docs := []search.Document{
			{ID: "unicode-doc-1", Content: certContent(t, map[string]any{"title": "오만과 편견 📚", "body": "Pride and Prejudice book listing 📚", "category": "fiction", "tag": "i18n", "price": float64(1), "available": true}), Metadata: map[string]string{"locale": "ko-KR"}},
			{ID: "unicode-doc-2", Content: certContent(t, map[string]any{"title": "Pride and Prejudice", "body": "ความภาคภูมิใจและความอยุติธรรม 📚", "category": "fiction", "tag": "i18n", "price": float64(2), "available": false}), Metadata: map[string]string{"locale": "th-TH"}},
		}
		resp, err := component.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: unicodeIndex, Documents: docs, Options: certWaitOptions()})
		require.NoError(t, err)
		require.Empty(t, resp.FailedItems)
		require.Equal(t, search.IndexAckCompleted, resp.Ack)

		// Documents round-trip exactly, including the opaque Dapr document
		// metadata, and come back in request order.
		got, err := component.GetDocuments(ctx, &search.GetDocumentsRequest{Index: unicodeIndex, IDs: []string{"unicode-doc-2", "unicode-doc-1"}, IncludeContent: true})
		require.NoError(t, err)
		require.Len(t, got.Documents, 2)
		require.Equal(t, "unicode-doc-2", got.Documents[0].ID)
		require.Equal(t, "unicode-doc-1", got.Documents[1].ID)
		byID := map[string]search.Document{}
		for _, doc := range got.Documents {
			byID[doc.ID] = doc
		}
		for _, want := range docs {
			require.Contains(t, byID, want.ID)
			require.JSONEq(t, string(want.Content), string(byID[want.ID].Content))
			require.Equal(t, want.Metadata, byID[want.ID].Metadata)
		}

		searchResp, err := component.Search(ctx, &search.SearchRequest{Index: unicodeIndex, Text: "Pride", TopK: 5, IncludeContent: true})
		require.NoError(t, err)
		require.NotEmpty(t, searchResp.Hits)
	})

	t.Run("keyed_upsert_validation", func(t *testing.T) {
		validationIndex := "cert-search-keys-" + uuid.NewString()[:8]
		t.Cleanup(func() {
			_ = component.DeleteIndex(context.Background(), &search.DeleteIndexRequest{Index: validationIndex})
		})
		require.NoError(t, component.CreateIndex(ctx, &search.CreateIndexRequest{Index: validationIndex, Metadata: certIndexMetadata()}))

		_, err := component.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: validationIndex, Documents: []search.Document{
			{ID: "", Content: certContent(t, map[string]any{"title": "no id"})},
		}, Options: certWaitOptions()})
		requireStatusCode(t, err, codes.InvalidArgument)

		_, err = component.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: validationIndex, Documents: []search.Document{
			{ID: "dupe", Content: certContent(t, map[string]any{"title": "first"})},
			{ID: "dupe", Content: certContent(t, map[string]any{"title": "second"})},
		}, Options: certWaitOptions()})
		requireStatusCode(t, err, codes.InvalidArgument)
	})

	t.Run("failed_items_are_identified_before_enqueue", func(t *testing.T) {
		failIndex := "cert-search-failed-" + uuid.NewString()[:8]
		t.Cleanup(func() { _ = component.DeleteIndex(context.Background(), &search.DeleteIndexRequest{Index: failIndex}) })
		require.NoError(t, component.CreateIndex(ctx, &search.CreateIndexRequest{Index: failIndex, Metadata: certIndexMetadata()}))

		// The runtime rejects non-object content before the component is
		// invoked; the Meilisearch component keeps the same defensive check
		// (search.ValidateDocumentContent semantics) and attributes the
		// failure to the document rather than failing the request.
		resp, err := component.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: failIndex, Documents: []search.Document{
			{ID: "good-1", Content: certContent(t, map[string]any{"title": "Pride and Prejudice", "body": "good", "category": "fiction", "price": float64(1), "available": true})},
			{ID: "bad-json", Content: []byte("not json at all")},
			{ID: "bad-scalar", Content: []byte(`"a json string is not an object"`)},
			{ID: "bad-array", Content: []byte(`[{"title":"an array is not an object"}]`)},
		}, Options: certWaitOptions()})
		require.NoError(t, err)
		require.Equal(t, search.IndexAckCompleted, resp.Ack)
		failedIDs := make([]string, 0, len(resp.FailedItems))
		for _, item := range resp.FailedItems {
			failedIDs = append(failedIDs, item.ID)
			require.NotNil(t, item.Error)
			require.Equal(t, codes.InvalidArgument, item.Error.Code())
		}
		require.ElementsMatch(t, []string{"bad-json", "bad-scalar", "bad-array"}, failedIDs)

		// A Meilisearch task that succeeds has no eventual failures, so the
		// valid document is queryable.
		got, err := component.GetDocuments(ctx, &search.GetDocumentsRequest{Index: failIndex, IDs: []string{"good-1", "bad-json"}, IncludeContent: true})
		require.NoError(t, err)
		require.Len(t, got.Documents, 1)
		require.Equal(t, "good-1", got.Documents[0].ID)
	})

	t.Run("indexing_modes", func(t *testing.T) {
		modeIndex := "cert-search-modes-" + uuid.NewString()[:8]
		t.Cleanup(func() { _ = component.DeleteIndex(context.Background(), &search.DeleteIndexRequest{Index: modeIndex}) })
		require.NoError(t, component.CreateIndex(ctx, &search.CreateIndexRequest{Index: modeIndex, Metadata: certIndexMetadata()}))

		doc := func(id string) []search.Document {
			return []search.Document{{ID: id, Content: certContent(t, map[string]any{"title": "Pride and Prejudice " + id, "body": "mode", "category": "fiction", "price": float64(1), "available": true})}}
		}

		// Meilisearch has a native durable queued acknowledgement.
		queued, err := component.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: modeIndex, Documents: doc("mode-acceptance"), Options: search.IndexingOptions{Mode: search.IndexingModeReturnOnAcceptance}})
		require.NoError(t, err)
		require.Equal(t, search.IndexAckQueued, queued.Ack)

		// INDEXING_MODE_UNSPECIFIED is identical to RETURN_ON_ACCEPTANCE.
		unspecified, err := component.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: modeIndex, Documents: doc("mode-unspecified"), Options: search.IndexingOptions{}})
		require.NoError(t, err)
		require.Equal(t, search.IndexAckQueued, unspecified.Ack)

		completed, err := component.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: modeIndex, Documents: doc("mode-completed"), Options: certWaitOptions()})
		require.NoError(t, err)
		require.Equal(t, search.IndexAckCompleted, completed.Ack)

		// CONTINUE_ASYNC is valid because a queued acknowledgement exists.
		async, err := component.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: modeIndex, Documents: doc("mode-async"), Options: search.IndexingOptions{
			Mode:          search.IndexingModeWaitForCompletion,
			WaitTimeout:   certWaitTimeout,
			OnWaitTimeout: search.IndexingWaitTimeoutActionContinueAsync,
		}})
		require.NoError(t, err)
		require.Contains(t, []search.IndexAck{search.IndexAckQueued, search.IndexAckCompleted}, async.Ack)

		// A wait shorter than the provider can complete in with FAIL_REQUEST
		// surfaces DEADLINE_EXCEEDED rather than a partial success.
		shortCtx, shortCancel := context.WithTimeout(ctx, 30*time.Second)
		defer shortCancel()
		_, err = component.IndexDocuments(shortCtx, &search.IndexDocumentsRequest{Index: modeIndex, Documents: doc("mode-short"), Options: search.IndexingOptions{
			Mode:          search.IndexingModeWaitForCompletion,
			WaitTimeout:   time.Nanosecond,
			OnWaitTimeout: search.IndexingWaitTimeoutActionFailRequest,
		}})
		if err != nil {
			requireStatusCode(t, err, codes.DeadlineExceeded)
		}

		// Wait options are only valid with WAIT_FOR_COMPLETION.
		_, err = component.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: modeIndex, Documents: doc("mode-invalid"), Options: search.IndexingOptions{
			Mode:        search.IndexingModeReturnOnAcceptance,
			WaitTimeout: time.Second,
		}})
		requireStatusCode(t, err, codes.InvalidArgument)
	})

	t.Run("delete_modes", func(t *testing.T) {
		deleteIndex := "cert-search-delete-" + uuid.NewString()[:8]
		t.Cleanup(func() {
			_ = component.DeleteIndex(context.Background(), &search.DeleteIndexRequest{Index: deleteIndex})
		})
		require.NoError(t, component.CreateIndex(ctx, &search.CreateIndexRequest{Index: deleteIndex, Metadata: certIndexMetadata()}))

		ids := []string{"del-acceptance", "del-unspecified", "del-completed", "del-async"}
		docs := make([]search.Document, 0, len(ids))
		for _, id := range ids {
			docs = append(docs, search.Document{ID: id, Content: certContent(t, map[string]any{"title": "Pride and Prejudice " + id, "body": "delete", "category": "fiction", "price": float64(1), "available": true})})
		}
		seeded, err := component.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: deleteIndex, Documents: docs, Options: certWaitOptions()})
		require.NoError(t, err)
		require.Empty(t, seeded.FailedItems)

		// Deletes are writes: they take the same IndexingOptions and report
		// the same acknowledgement boundaries as IndexDocuments.
		queued, err := component.DeleteDocuments(ctx, &search.DeleteDocumentsRequest{Index: deleteIndex, IDs: []string{"del-acceptance"}, Options: search.IndexingOptions{Mode: search.IndexingModeReturnOnAcceptance}})
		require.NoError(t, err)
		require.Equal(t, search.IndexAckQueued, queued.Ack)

		unspecified, err := component.DeleteDocuments(ctx, &search.DeleteDocumentsRequest{Index: deleteIndex, IDs: []string{"del-unspecified"}})
		require.NoError(t, err)
		require.Equal(t, search.IndexAckQueued, unspecified.Ack)

		completed, err := component.DeleteDocuments(ctx, &search.DeleteDocumentsRequest{Index: deleteIndex, IDs: []string{"del-completed"}, Options: certWaitOptions()})
		require.NoError(t, err)
		require.Equal(t, search.IndexAckCompleted, completed.Ack)

		async, err := component.DeleteDocuments(ctx, &search.DeleteDocumentsRequest{Index: deleteIndex, IDs: []string{"del-async"}, Options: search.IndexingOptions{
			Mode:          search.IndexingModeWaitForCompletion,
			WaitTimeout:   certWaitTimeout,
			OnWaitTimeout: search.IndexingWaitTimeoutActionContinueAsync,
		}})
		require.NoError(t, err)
		require.Contains(t, []search.IndexAck{search.IndexAckQueued, search.IndexAckCompleted}, async.Ack)

		// Wait options are validated for deletes too.
		_, err = component.DeleteDocuments(ctx, &search.DeleteDocumentsRequest{Index: deleteIndex, IDs: []string{"del-invalid"}, Options: search.IndexingOptions{
			Mode:        search.IndexingModeReturnOnAcceptance,
			WaitTimeout: time.Second,
		}})
		requireStatusCode(t, err, codes.InvalidArgument)

		// Missing ids are not failures; a settled delete of the whole set
		// confirms everything is gone.
		settled, err := component.DeleteDocuments(ctx, &search.DeleteDocumentsRequest{Index: deleteIndex, IDs: append(ids, "never-existed"), Options: certWaitOptions()})
		require.NoError(t, err)
		require.Equal(t, search.IndexAckCompleted, settled.Ack)
		got, err := component.GetDocuments(ctx, &search.GetDocumentsRequest{Index: deleteIndex, IDs: ids})
		require.NoError(t, err)
		require.Empty(t, got.Documents)
	})

	t.Run("large_payload_batch", func(t *testing.T) {
		largeIndex := "cert-search-large-" + uuid.NewString()[:8]
		t.Cleanup(func() { _ = component.DeleteIndex(context.Background(), &search.DeleteIndexRequest{Index: largeIndex}) })
		size := indexLargestSearchBatch(t, ctx, component, largeIndex, 5000)
		if size < 5000 {
			t.Logf("meilisearch accepted %d documents in the largest successful single-call batch below 5000", size)
		}
		got, err := component.GetIndex(ctx, &search.GetIndexRequest{Index: largeIndex})
		require.NoError(t, err)
		require.Equal(t, uint64(size), got.DocumentCount)
	})

	t.Run("soak_no_leak", func(t *testing.T) {
		soakIndex := "cert-search-soak-" + uuid.NewString()[:8]
		t.Cleanup(func() { _ = component.DeleteIndex(context.Background(), &search.DeleteIndexRequest{Index: soakIndex}) })
		require.NoError(t, component.CreateIndex(ctx, &search.CreateIndexRequest{Index: soakIndex, Metadata: certIndexMetadata()}))

		runtime.GC()
		baseline := runtime.NumGoroutine()
		for i := range 50 {
			docs := make([]search.Document, 5)
			ids := make([]string, 5)
			for j := range docs {
				id := fmt.Sprintf("soak-%03d-%03d", i, j)
				ids[j] = id
				docs[j] = search.Document{ID: id, Content: certContent(t, map[string]any{"title": "soak book listing", "body": "short soak body", "category": "fiction", "tag": fmt.Sprintf("tag-%d", j), "price": float64(i*5 + j), "available": j%2 == 0})}
			}
			idxResp, err := component.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: soakIndex, Documents: docs, Options: certWaitOptions()})
			require.NoError(t, err)
			require.Empty(t, idxResp.FailedItems)
			delResp, err := component.DeleteDocuments(ctx, &search.DeleteDocumentsRequest{Index: soakIndex, IDs: ids, Options: certWaitOptions()})
			require.NoError(t, err)
			require.Equal(t, search.IndexAckCompleted, delResp.Ack)
		}
		runtime.GC()
		time.Sleep(200 * time.Millisecond)
		after := runtime.NumGoroutine()
		delta := after - baseline
		t.Logf("goroutines before=%d after=%d delta=%d", baseline, after, delta)
		require.LessOrEqual(t, delta, 20)
	})

	t.Run("create index", func(t *testing.T) {
		require.NoError(t, component.CreateIndex(ctx, &search.CreateIndexRequest{Index: index, Metadata: certIndexMetadata()}))
	})

	t.Run("create existing index is ALREADY_EXISTS", func(t *testing.T) {
		err := component.CreateIndex(ctx, &search.CreateIndexRequest{Index: index, Metadata: certIndexMetadata()})
		requireStatusCode(t, err, codes.AlreadyExists)

		// Settings are not reconciled: different settings on the same name
		// are still ALREADY_EXISTS.
		err = component.CreateIndex(ctx, &search.CreateIndexRequest{Index: index, Metadata: map[string]string{"filterableAttributes": "tag"}})
		requireStatusCode(t, err, codes.AlreadyExists)
	})

	t.Run("bulk index batches", func(t *testing.T) {
		docs := searchCertificationDocuments(t)
		for i := 0; i < len(docs); i += 25 {
			end := min(i+25, len(docs))
			resp, err := component.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: index, Documents: docs[i:end], Options: certWaitOptions()})
			require.NoError(t, err)
			require.Empty(t, resp.FailedItems)
			require.Equal(t, search.IndexAckCompleted, resp.Ack)
		}
	})

	t.Run("get index", func(t *testing.T) {
		resp, err := component.GetIndex(ctx, &search.GetIndexRequest{Index: index})
		require.NoError(t, err)
		require.Equal(t, index, resp.Index)
		require.GreaterOrEqual(t, resp.DocumentCount, uint64(100))
	})

	t.Run("get index missing", func(t *testing.T) {
		_, err := component.GetIndex(ctx, &search.GetIndexRequest{Index: "cert-search-missing-" + uuid.NewString()[:8]})
		requireStatusCode(t, err, codes.NotFound)
	})

	t.Run("delete index missing", func(t *testing.T) {
		err := component.DeleteIndex(ctx, &search.DeleteIndexRequest{Index: "cert-search-missing-" + uuid.NewString()[:8]})
		requireStatusCode(t, err, codes.NotFound)
	})

	t.Run("list", func(t *testing.T) {
		resp, err := component.ListIndexes(ctx, &search.ListIndexesRequest{})
		require.NoError(t, err)
		require.Contains(t, resp.Indexes, index)
	})

	t.Run("get documents", func(t *testing.T) {
		resp, err := component.GetDocuments(ctx, &search.GetDocumentsRequest{Index: index, IDs: []string{"book-001", "missing-doc", "book-000"}, IncludeContent: true})
		require.NoError(t, err)
		require.Len(t, resp.Documents, 2, "missing ids are omitted, not errors")
		require.Equal(t, "book-001", resp.Documents[0].ID, "documents are returned in request order")
		require.Equal(t, "book-000", resp.Documents[1].ID, "documents are returned in request order")

		idsOnly, err := component.GetDocuments(ctx, &search.GetDocumentsRequest{Index: index, IDs: []string{"book-000"}, IncludeContent: false})
		require.NoError(t, err)
		require.Len(t, idsOnly.Documents, 1)
		require.Empty(t, idsOnly.Documents[0].Content)
	})

	t.Run("text search", func(t *testing.T) {
		resp, err := component.Search(ctx, &search.SearchRequest{Index: index, Text: "pride", TopK: 10, IncludeContent: true})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Hits)
		for i := 1; i < len(resp.Hits); i++ {
			require.GreaterOrEqual(t, resp.Hits[i-1].Score, resp.Hits[i].Score, "search scores are higher-is-better")
		}
	})

	t.Run("total hits", func(t *testing.T) {
		resp, err := component.Search(ctx, &search.SearchRequest{Index: index, TopK: 10, IncludeContent: false})
		require.NoError(t, err)
		require.NotNil(t, resp.TotalHits, "meilisearch reports a total")
		require.NotEqual(t, search.TotalHitsRelationUnspecified, resp.TotalHitsRelation)
		require.GreaterOrEqual(t, *resp.TotalHits, uint64(len(resp.Hits)))
	})

	t.Run("filter variants", func(t *testing.T) {
		// Every document has: category (nonfiction for even ids, fiction for odd),
		// tag ("tag-<i%5>"), price (i), available (i%3 == 0) and, for even
		// ids only, optional ("present"). The filters address these content
		// fields with their JSON types.
		tests := []struct {
			name     string
			filter   map[string]any
			check    func(t *testing.T, content map[string]any)
			wantCode codes.Code
		}{
			{name: "eq", filter: map[string]any{"category": map[string]any{"$eq": "nonfiction"}}, check: func(t *testing.T, c map[string]any) { assert.Equal(t, "nonfiction", c["category"]) }},
			{name: "bare eq shorthand", filter: map[string]any{"category": "fiction"}, check: func(t *testing.T, c map[string]any) { assert.Equal(t, "fiction", c["category"]) }},
			{name: "ne", filter: map[string]any{"category": map[string]any{"$ne": "nonfiction"}}, check: func(t *testing.T, c map[string]any) { assert.Equal(t, "fiction", c["category"]) }},
			{name: "gt", filter: map[string]any{"price": map[string]any{"$gt": 90.0}}, check: func(t *testing.T, c map[string]any) { assert.Greater(t, c["price"], 90.0) }},
			{name: "gte", filter: map[string]any{"price": map[string]any{"$gte": 99.0}}, check: func(t *testing.T, c map[string]any) { assert.GreaterOrEqual(t, c["price"], 99.0) }},
			{name: "lt", filter: map[string]any{"price": map[string]any{"$lt": 5.0}}, check: func(t *testing.T, c map[string]any) { assert.Less(t, c["price"], 5.0) }},
			{name: "lte", filter: map[string]any{"price": map[string]any{"$lte": 0.0}}, check: func(t *testing.T, c map[string]any) { assert.LessOrEqual(t, c["price"], 0.0) }},
			{name: "numeric range", filter: map[string]any{"price": map[string]any{"$gte": 10.0, "$lt": 20.0}}, check: func(t *testing.T, c map[string]any) {
				assert.GreaterOrEqual(t, c["price"], 10.0)
				assert.Less(t, c["price"], 20.0)
			}},
			{name: "in strings", filter: map[string]any{"tag": map[string]any{"$in": []any{"tag-1", "tag-2"}}}, check: func(t *testing.T, c map[string]any) { assert.Contains(t, []any{"tag-1", "tag-2"}, c["tag"]) }},
			{name: "nin strings", filter: map[string]any{"tag": map[string]any{"$nin": []any{"tag-0", "tag-1", "tag-2", "tag-3"}}}, check: func(t *testing.T, c map[string]any) { assert.Equal(t, "tag-4", c["tag"]) }},
			{name: "in numbers", filter: map[string]any{"price": map[string]any{"$in": []any{1.0, 2.0, 3.0}}}, check: func(t *testing.T, c map[string]any) { assert.Contains(t, []any{1.0, 2.0, 3.0}, c["price"]) }},
			{name: "bool eq", filter: map[string]any{"available": map[string]any{"$eq": true}}, check: func(t *testing.T, c map[string]any) { assert.Equal(t, true, c["available"]) }},
			{name: "bare bool shorthand", filter: map[string]any{"available": false}, check: func(t *testing.T, c map[string]any) { assert.Equal(t, false, c["available"]) }},
			{name: "exists", filter: map[string]any{"optional": map[string]any{"$exists": true}}, check: func(t *testing.T, c map[string]any) { assert.Contains(t, c, "optional") }},
			{name: "exists false", filter: map[string]any{"optional": map[string]any{"$exists": false}}, check: func(t *testing.T, c map[string]any) { assert.NotContains(t, c, "optional") }},
			{name: "and", filter: map[string]any{"$and": []any{map[string]any{"category": "nonfiction"}, map[string]any{"price": map[string]any{"$gt": 90.0}}}}, check: func(t *testing.T, c map[string]any) {
				assert.Equal(t, "nonfiction", c["category"])
				assert.Greater(t, c["price"], 90.0)
			}},
			{name: "or", filter: map[string]any{"$or": []any{map[string]any{"price": map[string]any{"$lt": 1.0}}, map[string]any{"price": map[string]any{"$gt": 98.0}}}}, check: func(t *testing.T, c map[string]any) {
				price := c["price"].(float64) //nolint:forcetypeassert // content is generated by this test
				assert.True(t, price < 1 || price > 98, "price %v outside both branches", price)
			}},
			{name: "not", filter: map[string]any{"$not": map[string]any{"category": "nonfiction"}}, check: func(t *testing.T, c map[string]any) { assert.Equal(t, "fiction", c["category"]) }},
			{name: "nested logical", filter: map[string]any{"$and": []any{
				map[string]any{"$or": []any{map[string]any{"tag": "tag-0"}, map[string]any{"tag": "tag-1"}}},
				map[string]any{"$not": map[string]any{"available": true}},
			}}, check: func(t *testing.T, c map[string]any) {
				assert.Contains(t, []any{"tag-0", "tag-1"}, c["tag"])
				assert.Equal(t, false, c["available"])
			}},
			{name: "regex unsupported", filter: map[string]any{"title": map[string]any{"$regex": "pride"}}, wantCode: codes.InvalidArgument},
			{name: "unknown operator", filter: map[string]any{"title": map[string]any{"$fuzzy": "pride"}}, wantCode: codes.InvalidArgument},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				resp, err := component.Search(ctx, &search.SearchRequest{Index: index, Filter: tt.filter, TopK: 100, IncludeContent: true})
				if tt.wantCode != codes.OK {
					requireStatusCode(t, err, tt.wantCode)
					return
				}
				require.NoError(t, err)
				require.NotEmpty(t, resp.Hits)
				for _, hit := range resp.Hits {
					tt.check(t, certDecode(t, hit))
				}
			})
		}
	})

	t.Run("sort asc desc", func(t *testing.T) {
		asc, err := component.Search(ctx, &search.SearchRequest{Index: index, TopK: 5, IncludeContent: true, Sort: []search.SortClause{{Field: "price", Order: search.SortOrderAsc}}})
		require.NoError(t, err)
		desc, err := component.Search(ctx, &search.SearchRequest{Index: index, TopK: 5, IncludeContent: true, Sort: []search.SortClause{{Field: "price", Order: search.SortOrderDesc}}})
		require.NoError(t, err)
		require.NotEmpty(t, asc.Hits)
		require.NotEmpty(t, desc.Hits)
		require.LessOrEqual(t, certPrice(t, asc.Hits[0]), certPrice(t, asc.Hits[len(asc.Hits)-1]))
		require.GreaterOrEqual(t, certPrice(t, desc.Hits[0]), certPrice(t, desc.Hits[len(desc.Hits)-1]))
	})

	t.Run("pagination via continuation token", func(t *testing.T) {
		page := func(token string) *search.SearchRequest {
			return &search.SearchRequest{
				Index:             index,
				TopK:              10,
				IncludeContent:    true,
				Sort:              []search.SortClause{{Field: "price", Order: search.SortOrderAsc}},
				ContinuationToken: token,
			}
		}

		first, err := component.Search(ctx, page(""))
		require.NoError(t, err)
		require.NotEmpty(t, first.ContinuationToken)
		second, err := component.Search(ctx, page(first.ContinuationToken))
		require.NoError(t, err)
		require.NotEmpty(t, second.Hits)
		require.NotEqual(t, first.Hits[0].Document.ID, second.Hits[0].Document.ID)

		t.Run("malformed token", func(t *testing.T) {
			_, tokenErr := component.Search(ctx, page("definitely-not-a-token"))
			requireStatusCode(t, tokenErr, codes.InvalidArgument)
		})

		t.Run("token bound to the query shape", func(t *testing.T) {
			mismatched := page(first.ContinuationToken)
			mismatched.TopK = 5
			_, tokenErr := component.Search(ctx, mismatched)
			requireStatusCode(t, tokenErr, codes.InvalidArgument)

			mismatched = page(first.ContinuationToken)
			mismatched.Sort = []search.SortClause{{Field: "price", Order: search.SortOrderDesc}}
			_, tokenErr = component.Search(ctx, mismatched)
			requireStatusCode(t, tokenErr, codes.InvalidArgument)

			mismatched = page(first.ContinuationToken)
			mismatched.Filter = map[string]any{"category": map[string]any{"$eq": "nonfiction"}}
			_, tokenErr = component.Search(ctx, mismatched)
			requireStatusCode(t, tokenErr, codes.InvalidArgument)
		})

		t.Run("non-unique sort visits every document exactly once", func(t *testing.T) {
			// category has only two values across 100 documents, so every
			// page boundary falls inside a run of equal sort keys. The
			// component's id tie-breaker (declared sortable at CreateIndex)
			// keeps the pages disjoint and complete.
			byCategory := func(token string) *search.SearchRequest {
				return &search.SearchRequest{
					Index:             index,
					TopK:              10,
					IncludeContent:    true,
					Sort:              []search.SortClause{{Field: "category", Order: search.SortOrderAsc}},
					ContinuationToken: token,
				}
			}
			seen := map[string]int{}
			token := ""
			var lastCategory string
			for pageNo := 0; ; pageNo++ {
				require.LessOrEqual(t, pageNo, 100, "pagination did not terminate")
				resp, pageErr := component.Search(ctx, byCategory(token))
				require.NoError(t, pageErr)
				for _, hit := range resp.Hits {
					seen[hit.Document.ID]++
					category := certDecode(t, hit)["category"].(string) //nolint:forcetypeassert // content is generated by this test
					require.GreaterOrEqual(t, category, lastCategory, "sort order must hold across pages")
					lastCategory = category
				}
				if resp.ContinuationToken == "" {
					break
				}
				token = resp.ContinuationToken
			}
			want := make([]string, 0, 100)
			for i := range 100 {
				want = append(want, fmt.Sprintf("book-%03d", i))
			}
			got := make([]string, 0, len(seen))
			for id, count := range seen {
				require.Equal(t, 1, count, "document %q was visited %d times", id, count)
				got = append(got, id)
			}
			sort.Strings(got)
			require.Equal(t, want, got, "paging must visit every document exactly once")
		})
	})

	t.Run("delete subset", func(t *testing.T) {
		ids := make([]string, 20)
		for i := range ids {
			ids[i] = fmt.Sprintf("book-%03d", i)
		}
		resp, err := component.DeleteDocuments(ctx, &search.DeleteDocumentsRequest{Index: index, IDs: ids, Options: certWaitOptions()})
		require.NoError(t, err)
		require.Equal(t, search.IndexAckCompleted, resp.Ack)

		got, err := component.GetDocuments(ctx, &search.GetDocumentsRequest{Index: index, IDs: ids, IncludeContent: false})
		require.NoError(t, err)
		require.Empty(t, got.Documents)
	})

	t.Run("delete is idempotent", func(t *testing.T) {
		resp, err := component.DeleteDocuments(ctx, &search.DeleteDocumentsRequest{Index: index, IDs: []string{"book-000", "never-existed"}, Options: certWaitOptions()})
		require.NoError(t, err)
		require.Equal(t, search.IndexAckCompleted, resp.Ack)
	})

	t.Run("search returns fewer after delete", func(t *testing.T) {
		resp, err := component.Search(ctx, &search.SearchRequest{Index: index, TopK: 200, IncludeContent: true})
		require.NoError(t, err)
		require.LessOrEqual(t, len(resp.Hits), 80)
	})

	t.Run("delete index", func(t *testing.T) {
		require.NoError(t, component.DeleteIndex(ctx, &search.DeleteIndexRequest{Index: index}))
		_, err := component.GetIndex(ctx, &search.GetIndexRequest{Index: index})
		requireStatusCode(t, err, codes.NotFound)
	})

	t.Run("close", func(t *testing.T) { require.NoError(t, component.Close()) })
}

func indexLargestSearchBatch(t *testing.T, ctx context.Context, component search.Search, index string, target int) int {
	t.Helper()

	for size := target; size >= 1; size /= 2 {
		_ = component.DeleteIndex(context.Background(), &search.DeleteIndexRequest{Index: index})
		require.NoError(t, component.CreateIndex(ctx, &search.CreateIndexRequest{Index: index, Metadata: certIndexMetadata()}))
		docs := makeSearchBatch(t, size)
		resp, err := component.IndexDocuments(ctx, &search.IndexDocumentsRequest{Index: index, Documents: docs, Options: certWaitOptions()})
		if err == nil && len(resp.FailedItems) == 0 {
			return size
		}
		t.Logf("single-call search batch size %d rejected: err=%v failedItems=%d", size, err, failedItemCount(resp))
	}
	t.Fatal("no successful search batch size found")
	return 0
}

func failedItemCount(resp *search.IndexDocumentsResponse) int {
	if resp == nil {
		return 0
	}
	return len(resp.FailedItems)
}

func makeSearchBatch(t *testing.T, size int) []search.Document {
	t.Helper()
	docs := make([]search.Document, size)
	for i := range docs {
		docs[i] = search.Document{ID: fmt.Sprintf("large-book-%05d", i), Content: certContent(t, map[string]any{"title": fmt.Sprintf("large book listing %05d", i), "body": "small provider agnostic batch book listing", "category": "fiction", "tag": fmt.Sprintf("tag-%d", i%10), "price": float64(i), "available": i%2 == 0})}
	}
	return docs
}

func certContent(t *testing.T, content map[string]any) []byte {
	t.Helper()
	encoded, err := json.Marshal(content)
	require.NoError(t, err)
	return encoded
}

func certDecode(t *testing.T, hit search.Hit) map[string]any {
	t.Helper()
	decoded := map[string]any{}
	require.NoError(t, json.Unmarshal(hit.Document.Content, &decoded))
	return decoded
}

func certPrice(t *testing.T, hit search.Hit) float64 {
	t.Helper()
	price, ok := certDecode(t, hit)["price"].(float64)
	require.True(t, ok, "expected a numeric price")
	return price
}

// requireStatusCode asserts that a component returned a gRPC status error with
// one of the expected canonical codes.
func requireStatusCode(t *testing.T, err error, want ...codes.Code) {
	t.Helper()
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok, "component errors must be gRPC status errors, got %T: %v", err, err)
	require.NotEqual(t, codes.OK, st.Code())
	assert.Contains(t, want, st.Code(), "unexpected status code %s: %v", st.Code(), err)
}

func assertMeilisearchMetadataContract(t *testing.T, got metadata.MetadataMap) {
	t.Helper()

	actual := make(map[string]struct{}, len(got))
	for key := range got {
		actual[strings.ToLower(key)] = struct{}{}
	}

	mdType := reflect.TypeOf(commonmeilisearch.MeilisearchMetadata{})
	var missing []string
	for i := range mdType.NumField() {
		field := mdType.Field(i)
		if !field.IsExported() || field.Tag.Get("mapstructure") == "-" || strings.EqualFold(field.Tag.Get("mdignore"), "true") {
			continue
		}
		key := field.Name
		if tag := strings.Split(field.Tag.Get("mapstructure"), ",")[0]; tag != "" {
			key = tag
		}
		if _, ok := actual[strings.ToLower(key)]; !ok {
			missing = append(missing, key)
		}
	}
	require.Empty(t, missing, "component metadata missing fields: %s", strings.Join(missing, ", "))
}

func searchCertificationDocuments(t *testing.T) []search.Document {
	t.Helper()
	docs := make([]search.Document, 100)
	titles := []string{
		"Pride and Prejudice",
		"오만과 편견",
		"ความภาคภูมิใจและความอยุติธรรม",
		"Frankenstein",
		"Moby-Dick",
	}
	for i := range docs {
		category := "fiction"
		if i%2 == 0 {
			category = "nonfiction"
		}
		content := map[string]any{
			"title":     fmt.Sprintf("%s %03d", titles[i%len(titles)], i),
			"body":      "certification book listing for portable search filters",
			"category":  category,
			"tag":       fmt.Sprintf("tag-%d", i%5),
			"price":     float64(i),
			"available": i%3 == 0,
		}
		if i%2 == 0 {
			content["optional"] = "present"
		}
		docs[i] = search.Document{ID: fmt.Sprintf("book-%03d", i), Content: certContent(t, content), Metadata: map[string]string{"batch": "certification"}}
	}
	return docs
}
