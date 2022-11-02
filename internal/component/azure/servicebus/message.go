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

package servicebus

import (
	"fmt"
	"net/http"
	"time"

	azservicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/dapr/components-contrib/bindings"
	mdutils "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/ptr"
)

const (
	// MessageKeyMessageID defines the metadata key for the message id.
	MessageKeyMessageID = "MessageId" // read, write.
	// MessageKeyMessageIDAlias is an alias for "MessageId" for write only, for backwards-compatibility
	MessageKeyMessageIDAlias = "id"

	// MessageKeyCorrelationID defines the metadata key for the correlation id.
	MessageKeyCorrelationID = "CorrelationId" // read, write.
	// MessageKeyCorrelationIDAlias is an alias for "CorrelationId" for write only, for backwards-compatibility
	MessageKeyCorrelationIDAlias = "correlationID"

	// MessageKeySessionID defines the metadata key for the session id.
	MessageKeySessionID = "SessionId" // read, write.

	// MessageKeyLabel defines the metadata key for the label.
	MessageKeyLabel = "Label" // read, write.

	// MessageKeyReplyTo defines the metadata key for the reply to value.
	MessageKeyReplyTo = "ReplyTo" // read, write.

	// MessageKeyTo defines the metadata key for the to value.
	MessageKeyTo = "To" // read, write.

	// MessageKeyPartitionKey defines the metadata key for the partition key.
	MessageKeyPartitionKey = "PartitionKey" // read, write.

	// MessageKeyContentType defines the metadata key for the content type.
	MessageKeyContentType = "ContentType" // read, write.

	// MessageKeyDeliveryCount defines the metadata key for the delivery count.
	MessageKeyDeliveryCount = "DeliveryCount" // read.

	// MessageKeyLockedUntilUtc defines the metadata key for the locked until utc value.
	MessageKeyLockedUntilUtc = "LockedUntilUtc" // read.

	// MessageKeyLockToken defines the metadata key for the lock token.
	MessageKeyLockToken = "LockToken" // read.

	// MessageKeyEnqueuedTimeUtc defines the metadata key for the enqueued time utc value.
	MessageKeyEnqueuedTimeUtc = "EnqueuedTimeUtc" // read.

	// MessageKeySequenceNumber defines the metadata key for the sequence number.
	MessageKeySequenceNumber = "SequenceNumber" // read.

	// MessageKeyScheduledEnqueueTimeUtc defines the metadata key for the scheduled enqueue time utc value.
	MessageKeyScheduledEnqueueTimeUtc = "ScheduledEnqueueTimeUtc" // read, write.

	// MessageKeyReplyToSessionID defines the metadata key for the reply to session id.
	// Currently unused.
	MessageKeyReplyToSessionID = "ReplyToSessionId" // read, write.
)

// NewASBMessageFromPubsubRequest builds a new Azure Service Bus message from a PublishRequest.
func NewASBMessageFromPubsubRequest(req *pubsub.PublishRequest) (*azservicebus.Message, error) {
	asbMsg := &azservicebus.Message{
		Body: req.Data,
	}

	err := addMetadataToMessage(asbMsg, req.Metadata)
	return asbMsg, err
}

// NewASBMessageFromBulkMessageEntry builds a new Azure Service Bus message from a BulkMessageEntry.
func NewASBMessageFromBulkMessageEntry(entry pubsub.BulkMessageEntry) (*azservicebus.Message, error) {
	asbMsg := &azservicebus.Message{
		Body:        entry.Event,
		ContentType: &entry.ContentType,
	}

	err := addMetadataToMessage(asbMsg, entry.Metadata)
	return asbMsg, err
}

// NewASBMessageFromInvokeRequest builds a new Azure Service Bus message from a binding's Invoke request.
func NewASBMessageFromInvokeRequest(req *bindings.InvokeRequest) (*azservicebus.Message, error) {
	asbMsg := &azservicebus.Message{
		Body: req.Data,
	}

	err := addMetadataToMessage(asbMsg, req.Metadata)
	return asbMsg, err
}

// Adds metadata to the message.
// Reference for Azure Service Bus specific properties: https://docs.microsoft.com/en-us/rest/api/servicebus/message-headers-and-properties#message-headers
func addMetadataToMessage(asbMsg *azservicebus.Message, metadata map[string]string) error {
	asbMsg.ApplicationProperties = make(map[string]interface{}, len(metadata))

	for k, v := range metadata {
		// Note: do not just do &v because we're in a loop
		if v == "" {
			continue
		}

		switch k {
		// Common keys
		case mdutils.TTLMetadataKey:
			// Ignore v here and use TryGetTTL for the validation it performs
			ttl, ok, _ := mdutils.TryGetTTL(metadata)
			if ok {
				asbMsg.TimeToLive = &ttl
			}

		// Keys with aliases
		case MessageKeyMessageID, MessageKeyMessageIDAlias:
			if asbMsg.MessageID == nil {
				asbMsg.MessageID = ptr.Of(v)
			}

		case MessageKeyCorrelationID, MessageKeyCorrelationIDAlias:
			if asbMsg.CorrelationID == nil {
				asbMsg.CorrelationID = ptr.Of(v)
			}

		// String types
		case MessageKeySessionID:
			asbMsg.SessionID = ptr.Of(v)
		case MessageKeyLabel:
			asbMsg.Subject = ptr.Of(v)
		case MessageKeyReplyTo:
			asbMsg.ReplyTo = ptr.Of(v)
		case MessageKeyTo:
			asbMsg.To = ptr.Of(v)
		case MessageKeyPartitionKey:
			asbMsg.PartitionKey = ptr.Of(v)
		case MessageKeyContentType:
			asbMsg.ContentType = ptr.Of(v)

		// Time
		case MessageKeyScheduledEnqueueTimeUtc:
			timeVal, err := time.Parse(http.TimeFormat, v)
			if err == nil {
				asbMsg.ScheduledEnqueueTime = &timeVal
			}

		// Fallback: set as application property
		default:
			asbMsg.ApplicationProperties[k] = v
		}
	}

	if asbMsg.PartitionKey != nil && asbMsg.SessionID != nil && *asbMsg.PartitionKey != *asbMsg.SessionID {
		return fmt.Errorf("session id %s and partition key %s should be equal when both present", *asbMsg.SessionID, *asbMsg.PartitionKey)
	}

	return nil
}
