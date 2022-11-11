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

// AppBulkResponseEntry Represents single response, as part of AppBulkResponse, to be
// sent by subscibed App for the corresponding single message during bulk subscribe
type AppBulkResponseEntry struct {
	EntryId string            `json:"entryId"` //nolint:stylecheck
	Status  AppResponseStatus `json:"status"`
}

// AppBulkResponse is the whole bulk subscribe response sent by App
type AppBulkResponse struct {
	AppResponses []AppBulkResponseEntry `json:"statuses"`
}

// BulkPublishResponseEntry Represents single publish response, as part of BulkPublishResponse
// to be sent to publishing App for the corresponding single message during bulk publish
type BulkPublishResponseEntry struct {
	EntryId string `json:"entryId"` //nolint:stylecheck
	Error   error  `json:"error"`
}

// BulkPublishResponse contains the list of failed entries in a bulk publish request.
type BulkPublishResponse struct {
	FailedEntries []BulkPublishResponseEntry `json:"failedEntries"`
}

// BulkSubscribeResponseEntry Represents single subscribe response item, as part of BulkSubscribeResponse
// to be sent to building block for the corresponding single message during bulk subscribe
type BulkSubscribeResponseEntry struct {
	EntryId string `json:"entryId"` //nolint:stylecheck
	Error   error  `json:"error"`
}

// BulkSubscribeResponse is the whole bulk subscribe response sent to building block
type BulkSubscribeResponse struct {
	Error    error                        `json:"error"`
	Statuses []BulkSubscribeResponseEntry `json:"statuses"`
}

// NewBulkPublishResponse returns a BulkPublishResponse with each entry having same error.
// This method is a helper method to map a single error response on BulkPublish to multiple events.
func NewBulkPublishResponse(messages []BulkMessageEntry, err error) BulkPublishResponse {
	response := BulkPublishResponse{}
	response.FailedEntries = make([]BulkPublishResponseEntry, 0, len(messages))
	for _, msg := range messages {
		en := BulkPublishResponseEntry{}
		en.EntryId = msg.EntryId
		if err != nil {
			en.Error = err
		}
		response.FailedEntries = append(response.FailedEntries, en)
	}
	return response
}
