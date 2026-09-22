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

package meilisearch

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dapr/components-contrib/search"
)

func TestDecodeContent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		content  []byte
		want     map[string]any
		wantCode codes.Code
	}{
		{name: "json object", content: []byte(`{"title":"a","year":2024}`), want: map[string]any{"title": "a", "year": float64(2024)}},
		{name: "empty object", content: []byte(`{}`), want: map[string]any{}},
		{name: "surrounding whitespace", content: []byte("\n {\"title\":\"a\"} \n"), want: map[string]any{"title": "a"}},
		{name: "empty content", content: nil, wantCode: codes.InvalidArgument},
		{name: "json array", content: []byte(`["a"]`), wantCode: codes.InvalidArgument},
		{name: "json scalar", content: []byte(`"a"`), wantCode: codes.InvalidArgument},
		{name: "invalid json", content: []byte(`{`), wantCode: codes.InvalidArgument},
		{name: "trailing garbage", content: []byte(`{} x`), wantCode: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := DecodeContent(tt.content)
			if tt.wantCode != codes.OK {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, status.Code(err))
				// The defensive check produces the same item failure as the
				// shared validator the runtime applies.
				assert.Equal(t, status.Convert(search.ValidateDocumentContent(tt.content)).Message(), status.Convert(err).Message())
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestContentRoundTrip(t *testing.T) {
	t.Parallel()

	decoded, err := DecodeContent([]byte(`{"title":"a","nested":{"b":1}}`))
	require.NoError(t, err)

	encoded, err := EncodeContent(decoded)
	require.NoError(t, err)
	assert.JSONEq(t, `{"title":"a","nested":{"b":1}}`, string(encoded))
}

func TestMetadataRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   map[string]string
		want map[string]string
	}{
		{name: "nil metadata is not stored", in: nil},
		{name: "empty metadata is not stored", in: map[string]string{}},
		{name: "values round-trip exactly", in: map[string]string{"tenant": "acme", "empty": "", "json": `{"a":1}`}, want: map[string]string{"tenant": "acme", "empty": "", "json": `{"a":1}`}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stored := EncodeMetadata(tt.in)
			if len(tt.in) == 0 {
				assert.Nil(t, stored)
				assert.Nil(t, DecodeMetadata(stored))
				return
			}
			assert.Equal(t, tt.want, DecodeMetadata(stored))
		})
	}
}

func TestRecordMetadataRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   map[string]any
	}{
		{name: "nil metadata is not stored", in: nil},
		{name: "empty metadata is not stored", in: map[string]any{}},
		{
			name: "structured values round-trip exactly",
			in:   map[string]any{"author": "jane-austen", "year": float64(2024), "tags": []any{"a", "b"}, "publisher": map[string]any{"city": "london"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stored := EncodeRecordMetadata(tt.in)
			if len(tt.in) == 0 {
				assert.Nil(t, stored)
				assert.Nil(t, DecodeRecordMetadata(stored))
				return
			}
			assert.Equal(t, tt.in, DecodeRecordMetadata(stored))
		})
	}
}

func TestDecodeRecordMetadataIgnoresNonObjects(t *testing.T) {
	t.Parallel()

	assert.Nil(t, DecodeRecordMetadata("nope"))
	assert.Nil(t, DecodeRecordMetadata([]any{"a"}))
}

func TestDecodeMetadataRendersNonStringValues(t *testing.T) {
	t.Parallel()

	got := DecodeMetadata(map[string]any{"count": float64(3), "flag": true, "missing": nil})

	assert.Equal(t, map[string]string{"count": "3", "flag": "true", "missing": ""}, got)
}

func TestPayloadRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   []byte
	}{
		{name: "nil payload is not stored", in: nil},
		{name: "json payload", in: []byte(`{"title":"a"}`)},
		{name: "binary payload", in: []byte{0x00, 0xff, 0x10}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stored := EncodePayload(tt.in)
			if len(tt.in) == 0 {
				assert.Nil(t, stored)
			}
			got, err := DecodePayload(stored)
			require.NoError(t, err)
			if len(tt.in) == 0 {
				assert.Empty(t, got)
				return
			}
			assert.Equal(t, tt.in, got)
		})
	}
}

func TestDecodePayloadRejectsCorruptedValues(t *testing.T) {
	t.Parallel()

	_, err := DecodePayload("not base64!!")

	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
}

func TestContinuationTokenIsBoundToTheQueryShape(t *testing.T) {
	t.Parallel()

	fingerprint := QueryFingerprint("search", "books", "jane-austen", map[string]any{"author": "jane-austen"}, int64(10))

	tests := []struct {
		name         string
		token        string
		fingerprint  string
		wantOffset   int64
		wantCode     codes.Code
		wantContains string
	}{
		{
			name:        "round-trips the offset",
			token:       EncodeContinuationToken(fingerprint, 20),
			fingerprint: fingerprint,
			wantOffset:  20,
		},
		{
			name:         "rejects a token of another query",
			token:       EncodeContinuationToken(QueryFingerprint("search", "books", "mary-shelley"), 20),
			fingerprint:  fingerprint,
			wantCode:     codes.InvalidArgument,
			wantContains: "does not match the request",
		},
		{
			name:         "rejects a token that is not base64",
			token:        "not a token",
			fingerprint:  fingerprint,
			wantCode:     codes.InvalidArgument,
			wantContains: "malformed",
		},
		{
			name:         "rejects base64 that is not a token",
			token:        "bm90LWpzb24",
			fingerprint:  fingerprint,
			wantCode:     codes.InvalidArgument,
			wantContains: "malformed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			offset, err := DecodeContinuationToken(tt.token, tt.fingerprint)
			if tt.wantCode != codes.OK {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, status.Code(err))
				assert.Contains(t, status.Convert(err).Message(), tt.wantContains)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantOffset, offset)
		})
	}
}

func TestQueryFingerprintIsStableAcrossMapOrder(t *testing.T) {
	t.Parallel()

	filter := map[string]any{"author": "jane-austen", "year": float64(2024), "tags": []any{"a", "b"}}
	other := map[string]any{"year": float64(2024), "tags": []any{"a", "b"}, "author": "jane-austen"}

	assert.Equal(t, QueryFingerprint("search", "books", filter), QueryFingerprint("search", "books", other))
	assert.NotEqual(t, QueryFingerprint("search", "books", filter), QueryFingerprint("search", "books", map[string]any{"author": "mary-shelley"}))
}

func TestNewFailedItem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		code     codes.Code
		wantCode codes.Code
	}{
		{name: "keeps a non-OK code", code: codes.InvalidArgument, wantCode: codes.InvalidArgument},
		{name: "never carries OK", code: codes.OK, wantCode: codes.Unknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			item := NewFailedItem("doc-1", tt.code, "content is %s", "invalid")

			assert.Equal(t, "doc-1", item.ID)
			require.NotNil(t, item.Error)
			assert.Equal(t, tt.wantCode, item.Error.Code())
			assert.Equal(t, "content is invalid", item.Error.Message())
		})
	}
}

func TestIsReservedField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		field string
		want  bool
	}{
		{name: "primary key", field: PrimaryKey, want: true},
		{name: "dapr metadata", field: MetadataField, want: true},
		{name: "dapr payload", field: PayloadField, want: true},
		{name: "meilisearch vectors", field: VectorsField, want: true},
		{name: "meilisearch ranking score", field: RankingScoreField, want: true},
		{name: "user attribute", field: "title", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, IsReservedField(tt.field))
		})
	}
}

func TestSplitList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		want  []string
	}{
		{name: "empty", value: "", want: nil},
		{name: "blank", value: "   ", want: nil},
		{name: "single", value: "author", want: []string{"author"}},
		{name: "trims and drops empties", value: " author , , year ", want: []string{"author", "year"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, SplitList(tt.value))
		})
	}
}
