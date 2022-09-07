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

// PublishRequest is the request to publish a message.
type PublishRequest struct {
	Data        []byte            `json:"data"`
	PubsubName  string            `json:"pubsubname"`
	Topic       string            `json:"topic"`
	Metadata    map[string]string `json:"metadata"`
	ContentType *string           `json:"contentType,omitempty"`
}

// BulkPublishRequest is the request to publish mutilple messages.
type BulkPublishRequest struct {
	Data        []NewBulkEventItem `json:"data"`
	PubsubName  string             `json:"pubsubname"`
	Topic       string             `json:"topic"`
	Metadata    map[string]string  `json:"metadata"`
	ContentType *string            `json:"contentType,omitempty"`
}

// SubscribeRequest is the request to subscribe to a topic.
type SubscribeRequest struct {
	Topic    string            `json:"topic"`
	Metadata map[string]string `json:"metadata"`
}

// NewMessage is an event arriving from a message bus instance.
type NewMessage struct {
	Data        []byte            `json:"data"`
	Topic       string            `json:"topic"`
	Metadata    map[string]string `json:"metadata"`
	ContentType *string           `json:"contentType,omitempty"`
}

// NewBulkMessage Represents bulk message arriving from a message bus instance
type NewBulkMessage struct {
	Messages []NewBulkEventItem `json:"messages"`
	Topic    string             `json:"topic"`
	Metadata map[string]string  `json:"metadata"`
}

// NewBulkEventItem represents Single message inside bulk request
type NewBulkEventItem struct {
	EventId     string            `json:eventId`
	Event       []byte            `json:"event"`
	ContentType *string           `json:"contentType,omitempty"`
	Metadata    map[string]string `json:"metadata"`
}

// BulkSubscribeConfig defines the Configurations that can be applied to control bulk subscribe
// behavior - beahvior may depend per building block
type BulkSubscribeConfig struct {
	MaxBulkCount            int `json:"maxBulkCount"`
	MaxBulkLatencyInSeconds int `json:"maxBulkLatencyInSeconds"` // todo - change to millis
	MaxBulkSizeInBytes      int `json:"maxBulkSizeInBytes"`
}
