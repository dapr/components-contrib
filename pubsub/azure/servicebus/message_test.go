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
	"testing"
	"time"

	"github.com/Azure/azure-amqp-common-go/v3/uuid"
	azservicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/pubsub"
)

func TestNewASBMessageFromPubsubRequest(t *testing.T) {
	testMessageData := []byte("test message")
	testMessageID := "testMessageId"
	testCorrelationID := "testCorrelationId"
	testSessionID := "testSessionId"
	testLabel := "testLabel"
	testReplyTo := "testReplyTo"
	testTo := "testTo"
	testPartitionKey := testSessionID
	testPartitionKeyUnique := "testPartitionKey"
	testContentType := "testContentType"
	nowUtc := time.Now().UTC()
	testScheduledEnqueueTimeUtc := nowUtc.Format(http.TimeFormat)

	testCases := []struct {
		name                        string
		pubsubRequest               pubsub.PublishRequest
		expectedAzServiceBusMessage azservicebus.Message
		expectError                 bool
	}{
		{
			name: "Maps pubsub request to azure service bus message.",
			pubsubRequest: pubsub.PublishRequest{
				Data: testMessageData,
				Metadata: map[string]string{
					MessageIDMetadataKey:               testMessageID,
					CorrelationIDMetadataKey:           testCorrelationID,
					SessionIDMetadataKey:               testSessionID,
					LabelMetadataKey:                   testLabel,
					ReplyToMetadataKey:                 testReplyTo,
					ToMetadataKey:                      testTo,
					PartitionKeyMetadataKey:            testPartitionKey,
					ContentTypeMetadataKey:             testContentType,
					ScheduledEnqueueTimeUtcMetadataKey: testScheduledEnqueueTimeUtc,
				},
			},
			expectedAzServiceBusMessage: azservicebus.Message{
				Body:                 testMessageData,
				MessageID:            &testMessageID,
				CorrelationID:        &testCorrelationID,
				SessionID:            &testSessionID,
				Subject:              &testLabel,
				ReplyTo:              &testReplyTo,
				To:                   &testTo,
				PartitionKey:         &testPartitionKey,
				ScheduledEnqueueTime: &nowUtc,
				ContentType:          &testContentType,
			},
			expectError: false,
		},
		{
			name: "Errors when partition key and session id set but not equal.",
			pubsubRequest: pubsub.PublishRequest{
				Data: testMessageData,
				Metadata: map[string]string{
					MessageIDMetadataKey:     testMessageID,
					CorrelationIDMetadataKey: testCorrelationID,
					SessionIDMetadataKey:     testSessionID,
					LabelMetadataKey:         testLabel,
					ReplyToMetadataKey:       testReplyTo,
					ToMetadataKey:            testTo,
					PartitionKeyMetadataKey:  testPartitionKeyUnique,
					ContentTypeMetadataKey:   testContentType,
				},
			},
			expectedAzServiceBusMessage: azservicebus.Message{
				Body:          testMessageData,
				MessageID:     &testMessageID,
				CorrelationID: &testCorrelationID,
				SessionID:     &testSessionID,
				Subject:       &testLabel,
				ReplyTo:       &testReplyTo,
				To:            &testTo,
				PartitionKey:  &testPartitionKey,
				ContentType:   &testContentType,
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// act.
			msg, err := NewASBMessageFromPubsubRequest(&tc.pubsubRequest)

			// assert.
			if tc.expectError {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
				assert.Equal(t, tc.expectedAzServiceBusMessage.Body, msg.Body)
				assert.Equal(t, tc.expectedAzServiceBusMessage.MessageID, msg.MessageID)
				assert.Equal(t, tc.expectedAzServiceBusMessage.CorrelationID, msg.CorrelationID)
				assert.Equal(t, tc.expectedAzServiceBusMessage.SessionID, msg.SessionID)
				assert.Equal(t, tc.expectedAzServiceBusMessage.ContentType, msg.ContentType)
				assert.Equal(t, tc.expectedAzServiceBusMessage.ReplyTo, msg.ReplyTo)
				assert.Equal(t, tc.expectedAzServiceBusMessage.TimeToLive, msg.TimeToLive)
				assert.Equal(t, tc.expectedAzServiceBusMessage.To, msg.To)
				assert.Equal(t, tc.expectedAzServiceBusMessage.Subject, msg.Subject)
				assert.Equal(t, tc.expectedAzServiceBusMessage.PartitionKey, msg.PartitionKey)
				assert.Equal(t, tc.expectedAzServiceBusMessage.ScheduledEnqueueTime.Unix(), msg.ScheduledEnqueueTime.Unix())
			}
		})
	}
}

func TestNewPubsubMessageFromAzServiceBusMessage(t *testing.T) {
	testMessageData := []byte("test message")
	testContentType := "testContentType"
	testMessageID := "testMessageId"
	testCorrelationID := "testCorrelationId"
	testSessionID := "testSessionId"
	testLabel := "testLabel"
	testReplyTo := "testReplyTo"
	testTo := "testTo"
	testPartitionKey := testSessionID
	testLockToken, _ := uuid.NewV4()
	nowUtc := time.Now().UTC()
	testSequenceNumber := int64(10)
	testSequenceNumberValue := "10"
	testDeliveryCount := uint32(2)
	testDeliveryCountValue := "2"

	testCases := []struct {
		name                  string
		azServiceBusMessage   azservicebus.ReceivedMessage
		topic                 string
		expectedPubsubMessage pubsub.NewMessage
		expectError           bool
	}{
		{
			name: "Maps azure service bus message to pubsub message",
			azServiceBusMessage: azservicebus.ReceivedMessage{
				ContentType:          &testContentType,
				MessageID:            testMessageID,
				CorrelationID:        &testCorrelationID,
				SessionID:            &testSessionID,
				ReplyTo:              &testReplyTo,
				DeliveryCount:        testDeliveryCount,
				To:                   &testTo,
				LockToken:            testLockToken,
				Subject:              &testLabel,
				LockedUntil:          &nowUtc,
				SequenceNumber:       &testSequenceNumber,
				ScheduledEnqueueTime: &nowUtc,
				PartitionKey:         &testPartitionKey,
				EnqueuedTime:         &nowUtc,
			},
			topic: "testTopic",
			expectedPubsubMessage: pubsub.NewMessage{
				Data:  testMessageData,
				Topic: "testTopic",
				Metadata: map[string]string{
					fmt.Sprintf("metadata.%s", MessageIDMetadataKey):               testMessageID,
					fmt.Sprintf("metadata.%s", SessionIDMetadataKey):               testSessionID,
					fmt.Sprintf("metadata.%s", CorrelationIDMetadataKey):           testCorrelationID,
					fmt.Sprintf("metadata.%s", ContentTypeMetadataKey):             testContentType,
					fmt.Sprintf("metadata.%s", LabelMetadataKey):                   testLabel,
					fmt.Sprintf("metadata.%s", DeliveryCountMetadataKey):           testDeliveryCountValue,
					fmt.Sprintf("metadata.%s", ToMetadataKey):                      testTo,
					fmt.Sprintf("metadata.%s", ReplyToMetadataKey):                 testReplyTo,
					fmt.Sprintf("metadata.%s", LockTokenMetadataKey):               testLockToken.String(),
					fmt.Sprintf("metadata.%s", LockedUntilUtcMetadataKey):          nowUtc.Format(http.TimeFormat),
					fmt.Sprintf("metadata.%s", SequenceNumberMetadataKey):          testSequenceNumberValue,
					fmt.Sprintf("metadata.%s", ScheduledEnqueueTimeUtcMetadataKey): nowUtc.Format(http.TimeFormat),
					fmt.Sprintf("metadata.%s", PartitionKeyMetadataKey):            testPartitionKey,
					fmt.Sprintf("metadata.%s", EnqueuedTimeUtcMetadataKey):         nowUtc.Format(http.TimeFormat),
				},
			},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// act.
			pubsubMsg, err := NewPubsubMessageFromASBMessage(&tc.azServiceBusMessage, tc.topic)

			// assert.
			if tc.expectError {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
				assert.Equal(t, tc.expectedPubsubMessage.Data, pubsubMsg.Data)
				assert.Equal(t, tc.expectedPubsubMessage.Topic, pubsubMsg.Topic)
				for k := range tc.expectedPubsubMessage.Metadata {
					assert.Equal(t, tc.expectedPubsubMessage.Metadata[k], pubsubMsg.Metadata[k])
				}
			}
		})
	}
}
