/*
Copyright 2023 The Dapr Authors
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

import (
	"errors"
	"strings"

	"github.com/dapr/components-contrib/state/query"
)

// GetRequest is the object describing a state "fetch" request.
type GetRequest struct {
	Key      string            `json:"key"`
	Metadata map[string]string `json:"metadata"`
	Options  GetStateOption    `json:"options,omitempty"`
}

// Key gets the Key on a GetRequest.
func (r GetRequest) GetKey() string {
	return r.Key
}

// Metadata gets the Metadata on a GetRequest.
func (r GetRequest) GetMetadata() map[string]string {
	return r.Metadata
}

// GetStateOption controls how a state store reacts to a get request.
type GetStateOption struct {
	Consistency string `json:"consistency"` // "eventual, strong"
}

// DeleteRequest is the object describing a delete state request.
type DeleteRequest struct {
	Key      string            `json:"key"`
	ETag     *string           `json:"etag,omitempty"`
	Metadata map[string]string `json:"metadata"`
	Options  DeleteStateOption `json:"options,omitempty"`
}

// Key gets the Key on a DeleteRequest.
func (r DeleteRequest) GetKey() string {
	return r.Key
}

// Metadata gets the Metadata on a DeleteRequest.
func (r DeleteRequest) GetMetadata() map[string]string {
	return r.Metadata
}

// HasETag returns true if the request has a non-empty ETag.
func (r DeleteRequest) HasETag() bool {
	return r.ETag != nil && *r.ETag != ""
}

// Operation returns the operation type for DeleteRequest, implementing TransactionalStateOperationRequest.
func (r DeleteRequest) Operation() OperationType {
	return OperationDelete
}

// DeleteWithPrefixRequest is the object describing a delete with prefix state request used for deleting actors.
type DeleteWithPrefixRequest struct {
	Prefix string `json:"prefix"`
}

func (r *DeleteWithPrefixRequest) Validate() error {
	if r.Prefix == "" || r.Prefix == "||" {
		return errors.New("a prefix is required for deleteWithPrefix request")
	}
	if !strings.HasSuffix(r.Prefix, "||") {
		r.Prefix += "||"
	}
	return nil
}

// DeleteStateOption controls how a state store reacts to a delete request.
type DeleteStateOption struct {
	Concurrency string `json:"concurrency,omitempty"` // "concurrency"
	Consistency string `json:"consistency"`           // "eventual, strong"
}

// SetRequest is the object describing an upsert request.
type SetRequest struct {
	Key         string            `json:"key"`
	Value       any               `json:"value"`
	ETag        *string           `json:"etag,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	Options     SetStateOption    `json:"options,omitempty"`
	ContentType *string           `json:"contentType,omitempty"`
}

// GetKey gets the Key on a SetRequest.
func (r SetRequest) GetKey() string {
	return r.Key
}

// GetMetadata gets the Key on a SetRequest.
func (r SetRequest) GetMetadata() map[string]string {
	return r.Metadata
}

// HasETag returns true if the request has a non-empty ETag.
func (r SetRequest) HasETag() bool {
	return r.ETag != nil && *r.ETag != ""
}

// Operation returns the operation type for SetRequest, implementing TransactionalStateOperationRequest.
func (r SetRequest) Operation() OperationType {
	return OperationUpsert
}

// SetStateOption controls how a state store reacts to a set request.
type SetStateOption struct {
	Concurrency string // first-write, last-write
	Consistency string // "eventual, strong"
}

// OperationType describes a CRUD operation performed against a state store.
type OperationType string

const (
	// OperationUpsert is an update or create transactional operation.
	OperationUpsert OperationType = "upsert"
	// OperationDelete is a delete transactional operation.
	OperationDelete OperationType = "delete"
)

// TransactionalStateRequest describes a transactional operation against a state store that comprises multiple types of operations
// The Request field is either a DeleteRequest or SetRequest.
type TransactionalStateRequest struct {
	Operations []TransactionalStateOperation
	Metadata   map[string]string
}

// StateRequest is an interface that allows gets of the Key and Metadata inside requests.
type StateRequest interface {
	GetKey() string
	GetMetadata() map[string]string
}

// TransactionalStateOperation is an interface for all requests that can be part of a transaction.
type TransactionalStateOperation interface {
	StateRequest

	Operation() OperationType
}

type QueryRequest struct {
	Query    query.Query       `json:"query"`
	Metadata map[string]string `json:"metadata,omitempty"`
}
