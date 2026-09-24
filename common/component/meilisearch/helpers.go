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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dapr/components-contrib/search"
)

const (
	// PrimaryKey is the Meilisearch primary key attribute used by the Dapr
	// search and vector components. It holds the caller-supplied document or
	// record ID.
	PrimaryKey = "id"

	// MetadataField is the reserved attribute holding Dapr document/record
	// metadata as a JSON object, so metadata round-trips exactly instead of
	// being flattened into the document body.
	MetadataField = "daprMetadata"

	// PayloadField is the reserved attribute holding the base64-encoded
	// opaque payload bytes of a vector record.
	PayloadField = "daprPayload"

	// VectorsField is the Meilisearch attribute holding user-provided
	// embeddings, keyed by embedder name.
	VectorsField = "_vectors"

	// RankingScoreField is the relevance score Meilisearch returns when
	// showRankingScore is enabled.
	RankingScoreField = "_rankingScore"

	// FormattedField holds the highlighted/cropped representation of a hit.
	FormattedField = "_formatted"

	// WaitInterval is the polling interval used while waiting for the
	// Meilisearch tasks that back synchronous index/collection lifecycle
	// operations. Document and vector writes go through the TaskDispatcher,
	// which uses the task-change stream or exponential-backoff polling.
	WaitInterval = 50 * time.Millisecond
)

// ReservedFields are the attributes the components own inside a Meilisearch
// document. They never appear in a document's content or in record metadata.
func ReservedFields() []string {
	return []string{PrimaryKey, MetadataField, PayloadField, VectorsField}
}

// IsReservedField reports whether an attribute is owned by the component or is
// one of Meilisearch's own underscore-prefixed response attributes.
func IsReservedField(name string) bool {
	if strings.HasPrefix(name, "_") {
		return true
	}
	return name == PrimaryKey || name == MetadataField || name == PayloadField
}

// NewFailedItem builds an item-specific write failure. code must not be OK.
func NewFailedItem(id string, code codes.Code, format string, args ...any) search.FailedItem {
	if code == codes.OK {
		code = codes.Unknown
	}
	return search.FailedItem{ID: id, Error: status.New(code, fmt.Sprintf(format, args...))}
}

// DecodeContent unmarshals document content bytes into a JSON object. The
// runtime rejects content that is not a JSON object before it reaches the
// component; this defensive check produces the same INVALID_ARGUMENT failure
// (search.ValidateDocumentContent) so a FailedItem has one shape wherever it
// is identified.
func DecodeContent(content []byte) (map[string]any, error) {
	if err := search.ValidateDocumentContent(content); err != nil {
		return nil, err
	}
	var obj map[string]any
	if err := json.Unmarshal(content, &obj); err != nil {
		return nil, status.Error(codes.InvalidArgument, "content must be a JSON object")
	}
	return obj, nil
}

// EncodeContent marshals a document body back into JSON object bytes.
func EncodeContent(content map[string]any) ([]byte, error) {
	if content == nil {
		content = map[string]any{}
	}
	out, err := json.Marshal(content)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode document content: %v", err)
	}
	return out, nil
}

// EncodeMetadata converts opaque Dapr document metadata into the value stored
// under MetadataField. It returns nil when there is nothing to store.
func EncodeMetadata(meta map[string]string) any {
	if len(meta) == 0 {
		return nil
	}
	out := make(map[string]any, len(meta))
	for k, v := range meta {
		out[k] = v
	}
	return out
}

// DecodeMetadata converts the value stored under MetadataField back into
// opaque Dapr document metadata. Values that are not strings are rendered with
// their JSON representation so a hand-written document never fails a read.
func DecodeMetadata(value any) map[string]string {
	obj, ok := value.(map[string]any)
	if !ok || len(obj) == 0 {
		return nil
	}
	out := make(map[string]string, len(obj))
	for k, v := range obj {
		switch typed := v.(type) {
		case string:
			out[k] = typed
		case nil:
			out[k] = ""
		default:
			encoded, err := json.Marshal(typed)
			if err != nil {
				out[k] = fmt.Sprint(typed)
				continue
			}
			out[k] = string(encoded)
		}
	}
	return out
}

// EncodeRecordMetadata converts the structured, filterable metadata of a
// vector record into the value stored under MetadataField. The object is
// stored as-is so nested filter paths resolve as MetadataField.<path>. It
// returns nil when there is nothing to store.
func EncodeRecordMetadata(meta map[string]any) any {
	if len(meta) == 0 {
		return nil
	}
	return meta
}

// DecodeRecordMetadata converts the value stored under MetadataField back into
// the structured metadata of a vector record.
func DecodeRecordMetadata(value any) map[string]any {
	obj, ok := value.(map[string]any)
	if !ok || len(obj) == 0 {
		return nil
	}
	return obj
}

// EncodePayload base64-encodes opaque payload bytes for storage in a
// Meilisearch document attribute. It returns nil when there is no payload.
func EncodePayload(payload []byte) any {
	if len(payload) == 0 {
		return nil
	}
	return base64.StdEncoding.EncodeToString(payload)
}

// DecodePayload decodes the value stored under PayloadField.
func DecodePayload(value any) ([]byte, error) {
	encoded, ok := value.(string)
	if !ok || encoded == "" {
		return nil, nil
	}
	out, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decode stored payload: %v", err)
	}
	return out, nil
}

// NumberAsFloat converts a decoded JSON numeric value into float64.
func NumberAsFloat(value any) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case json.Number:
		f, _ := v.Float64()
		return f
	default:
		return 0
	}
}

// CloneMap copies a document map leaving room for the component's reserved
// attributes.
func CloneMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in)+len(ReservedFields()))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// SplitList splits a comma-separated component metadata value.
func SplitList(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

// continuationToken is the wire form of an opaque pagination token. The
// fingerprint binds the token to the query shape it was produced for.
type continuationToken struct {
	Version     int    `json:"v"`
	Fingerprint string `json:"q"`
	Offset      int64  `json:"o"`
}

const continuationTokenVersion = 1

// QueryFingerprint derives the value a continuation token is bound to. parts
// must contain every result-affecting element of the request: store, index,
// query, filter, sort, page size, projection, highlighting and
// component-declared result-affecting metadata. Transport metadata such as
// trace or request IDs must not be included.
func QueryFingerprint(parts ...any) string {
	hash := fnv.New64a()
	for _, part := range parts {
		encoded, err := json.Marshal(canonical(part))
		if err != nil {
			encoded = []byte(fmt.Sprint(part))
		}
		_, _ = hash.Write(encoded)
		_, _ = hash.Write([]byte{0})
	}
	return strconv.FormatUint(hash.Sum64(), 16)
}

// canonical normalizes a value so that map iteration order cannot change a
// fingerprint.
func canonical(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for k := range typed {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([][2]any, 0, len(keys))
		for _, k := range keys {
			out = append(out, [2]any{k, canonical(typed[k])})
		}
		return out
	case map[string]string:
		keys := make([]string, 0, len(typed))
		for k := range typed {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([][2]any, 0, len(keys))
		for _, k := range keys {
			out = append(out, [2]any{k, typed[k]})
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, canonical(item))
		}
		return out
	default:
		return value
	}
}

// EncodeContinuationToken encodes the offset of the next page together with
// the fingerprint of the query that produced it.
func EncodeContinuationToken(fingerprint string, offset int64) string {
	encoded, err := json.Marshal(continuationToken{Version: continuationTokenVersion, Fingerprint: fingerprint, Offset: offset})
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(encoded)
}

// DecodeContinuationToken decodes a continuation token and verifies that it
// was produced for the same query shape. A malformed or mismatched token is an
// INVALID_ARGUMENT error.
func DecodeContinuationToken(token, fingerprint string) (int64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, status.Error(codes.InvalidArgument, "continuation_token is malformed")
	}
	var decoded continuationToken
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return 0, status.Error(codes.InvalidArgument, "continuation_token is malformed")
	}
	if decoded.Version != continuationTokenVersion || decoded.Offset < 0 {
		return 0, status.Error(codes.InvalidArgument, "continuation_token is malformed")
	}
	if decoded.Fingerprint != fingerprint {
		return 0, status.Error(codes.InvalidArgument, "continuation_token does not match the request: pagination requires an unchanged query")
	}
	return decoded.Offset, nil
}
