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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexAckEnumValuesStable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ack  IndexAck
		want int32
	}{
		{name: "unspecified", ack: IndexAckUnspecified, want: 0},
		{name: "queued", ack: IndexAckQueued, want: 1},
		{name: "durable", ack: IndexAckDurable, want: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, int32(tt.ack))
		})
	}
}

func TestIndexFieldTypeEnumValuesStable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		fieldType IndexFieldType
		want      int32
	}{
		{name: "unspecified", fieldType: IndexFieldTypeUnspecified, want: 0},
		{name: "text", fieldType: IndexFieldTypeText, want: 1},
		{name: "keyword", fieldType: IndexFieldTypeKeyword, want: 2},
		{name: "int", fieldType: IndexFieldTypeInt, want: 3},
		{name: "double", fieldType: IndexFieldTypeDouble, want: 4},
		{name: "bool", fieldType: IndexFieldTypeBool, want: 5},
		{name: "date time", fieldType: IndexFieldTypeDateTime, want: 6},
		{name: "geo point", fieldType: IndexFieldTypeGeoPoint, want: 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, int32(tt.fieldType))
		})
	}
}

func TestSortOrderEnumValuesStable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		order SortOrder
		want  int32
	}{
		{name: "unspecified", order: SortOrderUnspecified, want: 0},
		{name: "ascending", order: SortOrderAsc, want: 1},
		{name: "descending", order: SortOrderDesc, want: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, int32(tt.order))
		})
	}
}

func TestZeroValueResponsesAreUsable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		check func(t *testing.T)
	}{
		{
			name: "create index response",
			check: func(t *testing.T) {
				resp := CreateIndexResponse{}
				assert.Equal(t, CreateIndexResponse{}, resp)
			},
		},
		{
			name: "drop index request",
			check: func(t *testing.T) {
				req := DropIndexRequest{}
				assert.Empty(t, req.Index)
				assert.Empty(t, req.Metadata)
				assert.Empty(t, req.Metadata["missing"])
			},
		},
		{
			name: "describe index response",
			check: func(t *testing.T) {
				resp := DescribeIndexResponse{}
				assert.Empty(t, resp.Index)
				assert.Empty(t, resp.Fields)
				assert.Zero(t, resp.DocumentCount)
				assert.Empty(t, resp.Properties)
				assert.Empty(t, resp.Properties["missing"])
			},
		},
		{
			name: "list indexes response",
			check: func(t *testing.T) {
				resp := ListIndexesResponse{}
				assert.Empty(t, resp.Indexes)
			},
		},
		{
			name: "index documents response",
			check: func(t *testing.T) {
				resp := IndexDocumentsResponse{}
				assert.Empty(t, resp.Results)
				assert.Equal(t, IndexAckUnspecified, resp.Ack)
			},
		},
		{
			name: "delete documents response",
			check: func(t *testing.T) {
				resp := DeleteDocumentsResponse{}
				assert.Empty(t, resp.Results)
				assert.Equal(t, IndexAckUnspecified, resp.Ack)
			},
		},
		{
			name: "search response",
			check: func(t *testing.T) {
				resp := SearchResponse{}
				assert.Empty(t, resp.Hits)
				assert.Zero(t, resp.TotalHits)
				assert.Empty(t, resp.ContinuationToken)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.NotPanics(t, func() { tt.check(t) })
		})
	}
}

func TestZeroValueDocumentIsUsable(t *testing.T) {
	t.Parallel()

	doc := Document{}

	assert.Empty(t, doc.ID)
	assert.Empty(t, doc.Content)
	assert.Empty(t, doc.Metadata)
	require.NotPanics(t, func() {
		assert.Nil(t, doc.Content["missing"])
		assert.Empty(t, doc.Metadata["missing"])
	})
}

func TestZeroValueHitHighlightsNilSafe(t *testing.T) {
	t.Parallel()

	hit := Hit{}

	assert.Empty(t, hit.Highlights)
	require.NotPanics(t, func() {
		assert.Equal(t, FieldHighlight{}, hit.Highlights["missing"])
	})
}

func TestOperationResultRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   OperationResult
	}{
		{
			name: "success",
			in: OperationResult{
				ID:      "doc-1",
				Success: true,
			},
		},
		{
			name: "failure",
			in: OperationResult{
				ID:           "doc-2",
				Success:      false,
				ErrorCode:    "invalid_document",
				ErrorMessage: "document is invalid",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.in
			assert.Equal(t, tt.in.ID, got.ID)
			assert.Equal(t, tt.in.Success, got.Success)
			assert.Equal(t, tt.in.ErrorCode, got.ErrorCode)
			assert.Equal(t, tt.in.ErrorMessage, got.ErrorMessage)
		})
	}
}

func TestIndexFieldSchemaOptionBooleansIndependent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filterable bool
		sortable   bool
		searchable bool
	}{
		{name: "none"},
		{name: "filterable only", filterable: true},
		{name: "sortable only", sortable: true},
		{name: "searchable only", searchable: true},
		{name: "all", filterable: true, sortable: true, searchable: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			field := IndexFieldSchema{
				Name:       "field",
				Type:       IndexFieldTypeText,
				Filterable: tt.filterable,
				Sortable:   tt.sortable,
				Searchable: tt.searchable,
			}

			assert.Equal(t, tt.filterable, field.Filterable)
			assert.Equal(t, tt.sortable, field.Sortable)
			assert.Equal(t, tt.searchable, field.Searchable)
		})
	}
}

func TestMetadataPropertiesReachable(t *testing.T) {
	t.Parallel()

	m := Metadata{}

	assert.Empty(t, m.Properties)
	require.NotPanics(t, func() {
		assert.Empty(t, m.Properties["missing"])
	})

	m.Properties = map[string]string{"endpoint": "localhost"}
	assert.Equal(t, "localhost", m.Properties["endpoint"])
}
