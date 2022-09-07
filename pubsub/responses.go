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

type BulkPublishStatus string

const (
	// Success means the message is received and processed correctly.
	Success AppResponseStatus = "SUCCESS"
	// Retry means the message is received but could not be processed and must be retried.
	Retry AppResponseStatus = "RETRY"
	// Drop means the message is received but should not be processed.
	Drop AppResponseStatus = "DROP"
	// Represents that message was published successfully.
	PublishSuccess BulkPublishStatus = "SUCCESS"
	// Represents that message publishing Failed.
	PublishFailed BulkPublishStatus = "FAILED"
)

// AppResponse is the object describing the response from user code after a pubsub event.
type AppResponse struct {
	Status AppResponseStatus `json:"status"`
}

// AppResponseItem Represents single response, as part of AppBulkResponse, to be
// sent by subscibed App for the corresponding single message during bulk subscribe
type AppResponseItem struct {
	EventId string            `json:"eventId"`
	Status  AppResponseStatus `json:"status"`
}

// AppBulkResponse is the whole bulk subscribe response sent by App
type AppBulkResponse struct {
	AppResponses []AppResponseItem `json:"statuses"`
}

// BulkPublishResponseItem Represents single publish response, as part of BulkPublishResponse
// to be sent to publishing App for the corresponding single message during bulk publish
type BulkPublishResponseItem struct {
	EventId string            `json:"eventId"`
	Status  BulkPublishStatus `json:"status"`
}

// BulkPublishResponse is the whole bulk publish response sent to App
type BulkPublishResponse struct {
	Error    error                     `json:"error"`
	Statuses []BulkPublishResponseItem `json:"statuses"`
}

// BulkSubscribeResponseItem Represents single subscribe response item, as part of BulkSubscribeResponse
// to be sent to building block for the corresponding single message during bulk subscribe
type BulkSubscribeResponseItem struct {
	EventId string `json:"eventId"`
	Error   error  `json:"error"`
}

// BulkSubscribeResponse is the whole bulk subscribe response sent to building block
type BulkSubscribeResponse struct {
	Error    error                       `json:"error"`
	Statuses []BulkSubscribeResponseItem `json:"statuses"`
}
