// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package state

// GetResponse is the request object for getting state
type GetResponse struct {
	Data     []byte            `json:"data"`
	ETag     string            `json:"etag,omitempty"`
	Metadata map[string]string `json:"metadata"`
}

type BulkGetResponse struct {
	Key      string            `json:"key"`
	Data     []byte            `json:"data"`
	ETag     string            `json:"etag,omitempty"`
	Metadata map[string]string `json:"metadata"`
	Error    string            `json:"error,omitempty"`
}
