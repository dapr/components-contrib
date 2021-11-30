// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package state

// GetResponse is the response object for getting state.
type GetResponse struct {
	Data        []byte            `json:"data"`
	ETag        *string           `json:"etag,omitempty"`
	Metadata    map[string]string `json:"metadata"`
	ContentType string            `json:"contentType,omitempty"`
}

// BulkGetResponse is the response object for bulk get response.
type BulkGetResponse struct {
	Key         string            `json:"key"`
	Data        []byte            `json:"data"`
	ETag        *string           `json:"etag,omitempty"`
	Metadata    map[string]string `json:"metadata"`
	Error       string            `json:"error,omitempty"`
	ContentType string            `json:"contentType,omitempty"`
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
	ContentType string  `json:"contentType,omitempty"`
}
