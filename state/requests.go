// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package state

import "github.com/dapr/components-contrib/state/query"

// GetRequest is the object describing a state fetch request.
type GetRequest struct {
	Key      string            `json:"key"`
	Metadata map[string]string `json:"metadata"`
	Options  GetStateOption    `json:"options,omitempty"`
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

// DeleteStateOption controls how a state store reacts to a delete request.
type DeleteStateOption struct {
	Concurrency string `json:"concurrency,omitempty"` // "concurrency"
	Consistency string `json:"consistency"`           // "eventual, strong"
}

// SetRequest is the object describing an upsert request.
type SetRequest struct {
	Key      string            `json:"key"`
	Value    interface{}       `json:"value"`
	ETag     *string           `json:"etag,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Options  SetStateOption    `json:"options,omitempty"`
}

// GetKey gets the Key on a SetRequest.
func (r SetRequest) GetKey() string {
	return r.Key
}

// GetMetadata gets the Key on a SetRequest.
func (r SetRequest) GetMetadata() map[string]string {
	return r.Metadata
}

// SetStateOption controls how a state store reacts to a set request.
type SetStateOption struct {
	Concurrency string `json:"concurrency,omitempty"` // first-write, last-write
	Consistency string `json:"consistency"`           // "eventual, strong"
}

// OperationType describes a CRUD operation performed against a state store.
type OperationType string

// Upsert is an update or create operation.
const Upsert OperationType = "upsert"

// Delete is a delete operation.
const Delete OperationType = "delete"

// TransactionalStateRequest describes a transactional operation against a state store that comprises multiple types of operations
// The Request field is either a DeleteRequest or SetRequest.
type TransactionalStateRequest struct {
	Operations []TransactionalStateOperation `json:"operations"`
	Metadata   map[string]string             `json:"metadata,omitempty"`
}

// TransactionalStateOperation describes operation type, key, and value for transactional operation.
type TransactionalStateOperation struct {
	Operation OperationType `json:"operation"`
	Request   interface{}   `json:"request"`
}

// KeyInt is an interface that allows gets of the Key and Metadata inside requests.
type KeyInt interface {
	GetKey() string
	GetMetadata() map[string]string
}

type QueryRequest struct {
	Query query.Query `json:"query"`
}
