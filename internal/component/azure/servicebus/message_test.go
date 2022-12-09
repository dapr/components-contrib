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
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	azservicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

var (
	testMessageID               = "testMessageId"
	testCorrelationID           = "testCorrelationId"
	testSessionID               = "testSessionId"
	testLabel                   = "testLabel"
	testReplyTo                 = "testReplyTo"
	testTo                      = "testTo"
	testPartitionKey            = testSessionID
	testPartitionKeyUnique      = "testPartitionKey"
	testContentType             = "testContentType"
	nowUtc                      = time.Now().UTC()
	testScheduledEnqueueTimeUtc = nowUtc.Format(http.TimeFormat)
	testLockTokenString         = "bG9ja3Rva2VuAAAAAAAAAA==" //nolint:gosec
	testLockTokenBytes          = [16]byte{108, 111, 99, 107, 116, 111, 107, 101, 110}
	testDeliveryCount           = uint32(1)
	testSampleTime              = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	testSampleTimeHTTPFormat    = "Thu, 01 Jan 1970 00:00:00 GMT"
	testSequenceNumber          = int64(1)
)

func TestAddMetadataToMessage(t *testing.T) {
	testCases := []struct {
		name                        string
		metadata                    map[string]string
		expectedAzServiceBusMessage azservicebus.Message
		expectError                 bool
	}{
		{
			name: "Maps pubsub request to azure service bus message.",
			metadata: map[string]string{
				MessageKeyMessageID:               testMessageID,
				MessageKeyCorrelationID:           testCorrelationID,
				MessageKeySessionID:               testSessionID,
				MessageKeyLabel:                   testLabel,
				MessageKeyReplyTo:                 testReplyTo,
				MessageKeyTo:                      testTo,
				MessageKeyPartitionKey:            testPartitionKey,
				MessageKeyContentType:             testContentType,
				MessageKeyScheduledEnqueueTimeUtc: testScheduledEnqueueTimeUtc,
			},
			expectedAzServiceBusMessage: azservicebus.Message{
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
			metadata: map[string]string{
				MessageKeyMessageID:     testMessageID,
				MessageKeyCorrelationID: testCorrelationID,
				MessageKeySessionID:     testSessionID,
				MessageKeyLabel:         testLabel,
				MessageKeyReplyTo:       testReplyTo,
				MessageKeyTo:            testTo,
				MessageKeyPartitionKey:  testPartitionKeyUnique,
				MessageKeyContentType:   testContentType,
			},
			expectedAzServiceBusMessage: azservicebus.Message{
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
			msg := &azservicebus.Message{}
			err := addMetadataToMessage(msg, tc.metadata)

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
