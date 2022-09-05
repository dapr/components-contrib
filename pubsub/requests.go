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

// BatchMessage is a single message in a batch request.
type BatchMessage struct {
	ID          string            `json:"id"`
	Event       []byte            `json:"event"`
	ContentType string            `json:"contentType,omitempty"`
	Metadata    map[string]string `json:"metadata"`
}

// BatchPublishRequest is the request to publish a batch of messages.
type BatchPublishRequest struct {
	Metadata   map[string]string `json:"metadata"`
	Messages   []BatchMessage    `json:"messages"`
	PubSubName string            `json:"pubsubname"`
	Topic      string            `json:"topic"`
}

// NewBatchMessage is an event arriving from a message bus instance on a batch subscription.
type NewBatchMessage struct {
	Metadata map[string]string `json:"metadata"`
	Messages []BatchMessage    `json:"messages"`
	Topic    string            `json:"topic"`
}
