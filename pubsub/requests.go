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

import (
	"encoding/json"
	"fmt"
	"strings"
)

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
	Entries    []BulkMessageEntry `json:"entries"`
	PubsubName string             `json:"pubsubname"`
	Topic      string             `json:"topic"`
	Metadata   map[string]string  `json:"metadata"`
}

// SubscribeRequest is the request to subscribe to a topic.
type SubscribeRequest struct {
	Topic               string              `json:"topic"`
	Metadata            map[string]string   `json:"metadata"`
	BulkSubscribeConfig BulkSubscribeConfig `json:"bulkSubscribe,omitempty"`
}

// NewMessage is an event arriving from a message bus instance.
type NewMessage struct {
	Data        []byte            `json:"data"`
	Topic       string            `json:"topic"`
	Metadata    map[string]string `json:"metadata"`
	ContentType *string           `json:"contentType,omitempty"`
}

// String implements fmt.Stringer and it's useful for debugging.
func (m NewMessage) String() string {
	ct := "(nil)"
	if m.ContentType != nil {
		ct = *m.ContentType
	}
	md, _ := json.Marshal(m.Metadata)
	return fmt.Sprintf("[NewMessage] topic='%s' data='%s' content-type='%s' metadata=%s", m.Topic, string(m.Data), ct, md)
}

// BulkMessage represents bulk message arriving from a message bus instance.
type BulkMessage struct {
	Entries  []BulkMessageEntry `json:"entries"`
	Topic    string             `json:"topic"`
	Metadata map[string]string  `json:"metadata"`
}

// String implements fmt.Stringer and it's useful for debugging.
func (m BulkMessage) String() string {
	md, _ := json.Marshal(m.Metadata)
	b := strings.Builder{}
	b.WriteString(fmt.Sprintf("[BulkMessage] topic='%s' metadata=%s entries=%d", m.Topic, md, len(m.Entries)))
	for i, e := range m.Entries {
		b.WriteString(fmt.Sprintf("\n%d: ", i))
		b.WriteString(e.String())
	}
	return b.String()
}

// BulkMessageEntry represents a single message inside a bulk request.
type BulkMessageEntry struct {
	EntryId     string            `json:"entryId"` //nolint:stylecheck
	Event       []byte            `json:"event"`
	ContentType string            `json:"contentType,omitempty"`
	Metadata    map[string]string `json:"metadata"`
}

// String implements fmt.Stringer and it's useful for debugging.
func (m BulkMessageEntry) String() string {
	md, _ := json.Marshal(m.Metadata)
	return fmt.Sprintf("[BulkMessageEntry] entryId='%s' data='%s' content-type='%s' metadata=%s", m.EntryId, string(m.Event), m.ContentType, md)
}

// BulkSubscribeConfig represents the configuration for bulk subscribe.
// It depends on specific componets to support these.
type BulkSubscribeConfig struct {
	MaxMessagesCount   int `json:"maxMessagesCount,omitempty"`
	MaxAwaitDurationMs int `json:"maxAwaitDurationMs,omitempty"`
}
