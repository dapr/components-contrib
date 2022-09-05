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

package pubsub

// AppResponseStatus represents a status of a PubSub response.
type AppResponseStatus string

// BatchMessageResponseStatus represents a status of a single batch message processing.
type BatchMessageResponseStatus string

const (
	// Success means the message is received and processed correctly.
	Success AppResponseStatus = "SUCCESS"
	// Retry means the message is received but could not be processed and must be retried.
	Retry AppResponseStatus = "RETRY"
	// Drop means the message is received but should not be processed.
	Drop AppResponseStatus = "DROP"
	// BatchMessageSuccess means the message processing succeeded.
	BatchMessageSuccess BatchMessageResponseStatus = "SUCCESS"
	// BatchMessageFail means the message processing failed.
	BatchMessageFail BatchMessageResponseStatus = "FAIL"
)

// AppResponse is the object describing the response from user code after a pubsub event.
type AppResponse struct {
	Status AppResponseStatus `json:"status"`
}

// BatchMessageResponse maps a batch message ID to a status.
type BatchMessageResponse struct {
	ID     string                     `json:"id"`
	Status BatchMessageResponseStatus `json:"status"`
}

// BatchPublishResponse contains a list of responses for each message in the batch publish request.
type BatchPublishResponse struct {
	Statuses []BatchMessageResponse `json:"statuses"`
}
