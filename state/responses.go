/*
Copyright 2021 The Dapr Authors
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

package state

const (
	// GetRespMetaKeyTTLExpireTime is the key for the metadata value of the TTL
	// expire time. Value is a RFC3339 formatted string.
	GetRespMetaKeyTTLExpireTime string = "ttlExpireTime"
)

// GetResponse is the response object for getting state.
type GetResponse struct {
	Data        []byte            `json:"data"`
	ETag        *string           `json:"etag,omitempty"`
	Metadata    map[string]string `json:"metadata"`
	ContentType *string           `json:"contentType,omitempty"`
}

// BulkGetResponse is the response object for bulk get response.
type BulkGetResponse struct {
	Key         string            `json:"key"`
	Data        []byte            `json:"data"`
	ETag        *string           `json:"etag,omitempty"`
	Metadata    map[string]string `json:"metadata"`
	Error       string            `json:"error,omitempty"`
	ContentType *string           `json:"contentType,omitempty"`
}

// QueryResponse is the response object for querying state.
type QueryResponse struct {
	Results  []QueryItem       `json:"results"`
	Token    string            `json:"token,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// QueryItem is an object representing a single entry in query results.
type QueryItem struct {
	Key         string  `json:"key"`
	Data        []byte  `json:"data"`
	ETag        *string `json:"etag,omitempty"`
	Error       string  `json:"error,omitempty"`
	ContentType *string `json:"contentType,omitempty"`
}

// DeleteWithPrefixResponse is the object representing a delete with prefix state response containing the number of items removed.
type DeleteWithPrefixResponse struct {
	Count int64 `json:"count"` // count of items removed
}

// KeysLikeResponse is the response object for getting keys like a pattern.
type KeysLikeResponse struct {
	Keys []string `json:"keys"`

	// ContinuationToken is an optional token which can be used to continue the
	// search of keys. Usually only present if a `PageSize` was set on the
	// request.
	ContinuationToken *string
}
