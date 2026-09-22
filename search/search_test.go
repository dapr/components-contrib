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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
		{name: "completed", ack: IndexAckCompleted, want: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, int32(tt.ack))
		})
	}
}

func TestIndexingEnumValuesStable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		got  int32
		want int32
	}{
		{name: "mode unspecified", got: int32(IndexingModeUnspecified), want: 0},
		{name: "mode wait for completion", got: int32(IndexingModeWaitForCompletion), want: 1},
		{name: "mode return on acceptance", got: int32(IndexingModeReturnOnAcceptance), want: 2},
		{name: "timeout action unspecified", got: int32(IndexingWaitTimeoutActionUnspecified), want: 0},
		{name: "timeout action continue async", got: int32(IndexingWaitTimeoutActionContinueAsync), want: 1},
		{name: "timeout action fail request", got: int32(IndexingWaitTimeoutActionFailRequest), want: 2},
		{name: "sort unspecified", got: int32(SortOrderUnspecified), want: 0},
		{name: "sort ascending", got: int32(SortOrderAsc), want: 1},
		{name: "sort descending", got: int32(SortOrderDesc), want: 2},
		{name: "total hits unspecified", got: int32(TotalHitsRelationUnspecified), want: 0},
		{name: "total hits exact", got: int32(TotalHitsRelationExact), want: 1},
		{name: "total hits lower bound", got: int32(TotalHitsRelationLowerBound), want: 2},
		{name: "total hits estimate", got: int32(TotalHitsRelationEstimate), want: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.got)
		})
	}
}

func TestValidateIndexingOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		opts              IndexingOptions
		supportsQueuedAck bool
		ctxTimeout        time.Duration
		wantErr           bool
		wantContains      string
	}{
		{
			name: "unspecified mode without wait fields",
			opts: IndexingOptions{},
		},
		{
			name: "return on acceptance without wait fields",
			opts: IndexingOptions{Mode: IndexingModeReturnOnAcceptance},
		},
		{
			name:         "return on acceptance with wait timeout",
			opts:         IndexingOptions{Mode: IndexingModeReturnOnAcceptance, WaitTimeout: time.Second},
			wantErr:      true,
			wantContains: "only valid with INDEXING_MODE_WAIT_FOR_COMPLETION",
		},
		{
			name:         "unspecified mode with timeout action",
			opts:         IndexingOptions{OnWaitTimeout: IndexingWaitTimeoutActionFailRequest},
			wantErr:      true,
			wantContains: "only valid with INDEXING_MODE_WAIT_FOR_COMPLETION",
		},
		{
			name: "wait for completion with fail request",
			opts: IndexingOptions{Mode: IndexingModeWaitForCompletion, WaitTimeout: time.Second, OnWaitTimeout: IndexingWaitTimeoutActionFailRequest},
		},
		{
			name:              "wait for completion with continue async and queued ack",
			opts:              IndexingOptions{Mode: IndexingModeWaitForCompletion, WaitTimeout: time.Second, OnWaitTimeout: IndexingWaitTimeoutActionContinueAsync},
			supportsQueuedAck: true,
		},
		{
			name:         "continue async without queued ack",
			opts:         IndexingOptions{Mode: IndexingModeWaitForCompletion, WaitTimeout: time.Second, OnWaitTimeout: IndexingWaitTimeoutActionContinueAsync},
			wantErr:      true,
			wantContains: "requires a provider with a queued acknowledgement",
		},
		{
			name:         "wait for completion without timeout",
			opts:         IndexingOptions{Mode: IndexingModeWaitForCompletion, OnWaitTimeout: IndexingWaitTimeoutActionFailRequest},
			wantErr:      true,
			wantContains: "wait_timeout must be positive",
		},
		{
			name:         "wait for completion with negative timeout",
			opts:         IndexingOptions{Mode: IndexingModeWaitForCompletion, WaitTimeout: -time.Second, OnWaitTimeout: IndexingWaitTimeoutActionFailRequest},
			wantErr:      true,
			wantContains: "wait_timeout must be positive",
		},
		{
			name:         "wait for completion without timeout action",
			opts:         IndexingOptions{Mode: IndexingModeWaitForCompletion, WaitTimeout: time.Second},
			wantErr:      true,
			wantContains: "on_wait_timeout is required",
		},
		{
			name:         "unknown mode",
			opts:         IndexingOptions{Mode: IndexingMode(7)},
			wantErr:      true,
			wantContains: "unknown indexing mode",
		},
		{
			name:         "unknown timeout action",
			opts:         IndexingOptions{Mode: IndexingModeWaitForCompletion, WaitTimeout: time.Second, OnWaitTimeout: IndexingWaitTimeoutAction(9)},
			wantErr:      true,
			wantContains: "unknown on_wait_timeout action",
		},
		{
			name:         "deadline shorter than wait timeout",
			opts:         IndexingOptions{Mode: IndexingModeWaitForCompletion, WaitTimeout: time.Minute, OnWaitTimeout: IndexingWaitTimeoutActionFailRequest},
			ctxTimeout:   time.Second,
			wantErr:      true,
			wantContains: "remaining request deadline must be longer",
		},
		{
			name:       "deadline longer than wait timeout",
			opts:       IndexingOptions{Mode: IndexingModeWaitForCompletion, WaitTimeout: time.Second, OnWaitTimeout: IndexingWaitTimeoutActionFailRequest},
			ctxTimeout: time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			if tt.ctxTimeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.ctxTimeout)
				defer cancel()
			}

			err := ValidateIndexingOptions(ctx, tt.opts, tt.supportsQueuedAck)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Contains(t, status.Convert(err).Message(), tt.wantContains)
		})
	}
}

func TestValidateWriteIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		ids          []string
		wantErr      bool
		wantContains string
	}{
		{name: "no ids", ids: nil},
		{name: "unique ids", ids: []string{"a", "b", "c"}},
		{name: "empty first id", ids: []string{"", "b"}, wantErr: true, wantContains: "item 0 has an empty id"},
		{name: "empty later id", ids: []string{"a", ""}, wantErr: true, wantContains: "item 1 has an empty id"},
		{name: "duplicate id", ids: []string{"a", "b", "a"}, wantErr: true, wantContains: `duplicate id "a"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateWriteIDs(tt.ids)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Contains(t, status.Convert(err).Message(), tt.wantContains)
		})
	}
}

func TestValidateDocumentContent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content []byte
		wantErr bool
	}{
		{name: "object", content: []byte(`{"title":"a"}`)},
		{name: "empty object", content: []byte(`{}`)},
		{name: "nested object", content: []byte(`{"a":{"b":[1,2]}}`)},
		{name: "surrounding whitespace", content: []byte(" \n\t{\"title\":\"a\"}\n ")},
		{name: "nil content", content: nil, wantErr: true},
		{name: "empty content", content: []byte{}, wantErr: true},
		{name: "whitespace only", content: []byte("  \n"), wantErr: true},
		{name: "array", content: []byte(`[{"title":"a"}]`), wantErr: true},
		{name: "string", content: []byte(`"title"`), wantErr: true},
		{name: "number", content: []byte(`42`), wantErr: true},
		{name: "null", content: []byte(`null`), wantErr: true},
		{name: "truncated object", content: []byte(`{"title":`), wantErr: true},
		{name: "trailing garbage", content: []byte(`{} {}`), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateDocumentContent(tt.content)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			st := status.Convert(err)
			assert.Equal(t, codes.InvalidArgument, st.Code(), "the error is ready to use in a FailedItem")
			assert.Equal(t, "content must be a JSON object", st.Message())
		})
	}
}

func TestErrorReasonHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		err        error
		wantCode   codes.Code
		wantReason string
	}{
		{
			name:       "continuation expired",
			err:        ContinuationExpiredError("the cursor expired"),
			wantCode:   codes.FailedPrecondition,
			wantReason: ReasonContinuationExpired,
		},
		{
			name:       "indexing outcome unknown",
			err:        IndexingOutcomeUnknownError(codes.Unavailable, "the connection dropped"),
			wantCode:   codes.Unavailable,
			wantReason: ReasonIndexingOutcomeUnknown,
		},
		{
			name:       "indexing outcome unknown defaults an OK code",
			err:        IndexingOutcomeUnknownError(codes.OK, "no code"),
			wantCode:   codes.Unknown,
			wantReason: ReasonIndexingOutcomeUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			st := status.Convert(tt.err)
			assert.Equal(t, tt.wantCode, st.Code())

			var info *errdetails.ErrorInfo
			for _, detail := range st.Details() {
				if candidate, ok := detail.(*errdetails.ErrorInfo); ok {
					info = candidate
				}
			}
			require.NotNil(t, info)
			assert.Equal(t, tt.wantReason, info.GetReason())
			assert.Equal(t, ErrorDomain, info.GetDomain())
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
			name: "get index response",
			check: func(t *testing.T) {
				resp := GetIndexResponse{}
				assert.Empty(t, resp.Index)
				assert.Zero(t, resp.DocumentCount)
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
				assert.Empty(t, resp.FailedItems)
				assert.Equal(t, IndexAckUnspecified, resp.Ack)
			},
		},
		{
			name: "get documents response",
			check: func(t *testing.T) {
				resp := GetDocumentsResponse{}
				assert.Empty(t, resp.Documents)
			},
		},
		{
			name: "delete documents response",
			check: func(t *testing.T) {
				resp := DeleteDocumentsResponse{}
				assert.Equal(t, IndexAckUnspecified, resp.Ack)
			},
		},
		{
			name: "delete documents request carries indexing options",
			check: func(t *testing.T) {
				req := DeleteDocumentsRequest{}
				assert.Empty(t, req.IDs)
				assert.Equal(t, IndexingModeUnspecified, req.Options.Mode)
				assert.Zero(t, req.Options.WaitTimeout)
			},
		},
		{
			name: "search response",
			check: func(t *testing.T) {
				resp := SearchResponse{}
				assert.Empty(t, resp.Hits)
				assert.Nil(t, resp.TotalHits)
				assert.Equal(t, TotalHitsRelationUnspecified, resp.TotalHitsRelation)
				assert.Empty(t, resp.ContinuationToken)
			},
		},
		{
			name: "document",
			check: func(t *testing.T) {
				doc := Document{}
				assert.Empty(t, doc.ID)
				assert.Empty(t, doc.Content)
				assert.Empty(t, doc.Metadata["missing"])
			},
		},
		{
			name: "hit",
			check: func(t *testing.T) {
				hit := Hit{}
				assert.Zero(t, hit.Score)
				assert.Empty(t, hit.Highlights["missing"])
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

func TestFailedItemCarriesANonOKStatus(t *testing.T) {
	t.Parallel()

	item := FailedItem{ID: "doc-1", Error: status.New(codes.InvalidArgument, "document content must be a JSON object")}

	assert.Equal(t, "doc-1", item.ID)
	require.NotNil(t, item.Error)
	assert.Equal(t, codes.InvalidArgument, item.Error.Code())
	assert.NotEqual(t, codes.OK, item.Error.Code())
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
