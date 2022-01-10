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
