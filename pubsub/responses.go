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

// BulkPublishStatus represents a status of a Bulk Publish response.
type BulkPublishStatus string

const (
	// Success means the message is received and processed correctly.
	Success AppResponseStatus = "SUCCESS"
	// Retry means the message is received but could not be processed and must be retried.
	Retry AppResponseStatus = "RETRY"
	// Drop means the message is received but should not be processed.
	Drop AppResponseStatus = "DROP"
	// PublishSucceeded represents that message was published successfully.
	PublishSucceeded BulkPublishStatus = "SUCCESS"
	// PublishFailed represents that message publishing failed.
	PublishFailed BulkPublishStatus = "FAILED"
)

// AppResponse is the object describing the response from user code after a pubsub event.
type AppResponse struct {
	Status AppResponseStatus `json:"status"`
}

// BulkPublishResponse is the whole bulk publish response sent to App
type BulkPublishResponse struct {
	Statuses []BulkPublishResponseEntry `json:"statuses"`
}

// BulkPublishResponseEntry Represents single publish response, as part of BulkPublishResponse
// to be sent to publishing App for the corresponding single message during bulk publish
type BulkPublishResponseEntry struct {
	EntryID string            `json:"entryID"`
	Status  BulkPublishStatus `json:"status"`
	Error   error             `json:"error"`
}

// NewBulkPublishResponse returns a BulkPublishResponse with each entry having same status and error.
// This method is a helper method to map a single error/success response on BulkPublish to multiple events.
func NewBulkPublishResponse(messages []BulkMessageEntry, status BulkPublishStatus, err error) BulkPublishResponse {
	response := BulkPublishResponse{}
	response.Statuses = make([]BulkPublishResponseEntry, len(messages))
	for i, msg := range messages {
		st := BulkPublishResponseEntry{}
		st.EntryID = msg.EntryID
		st.Status = status
		if err != nil {
			st.Error = err
		}
		response.Statuses[i] = st
	}
	return response
}
