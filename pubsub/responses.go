// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package pubsub

// AppResponseStatus represents a status of a PubSub response.
type AppResponseStatus string

const (
	// Success means the message is received and processed correctly.
	Success AppResponseStatus = "SUCCESS"
	// Retry means the message is received but could not be processed and must be retried.
	Retry AppResponseStatus = "RETRY"
	// Drop means the message is received but should not be processed.
	Drop AppResponseStatus = "DROP"
)

// AppResponse is the object describing the response from user code after a pubsub event.
type AppResponse struct {
	Status AppResponseStatus `json:"status"`
}
