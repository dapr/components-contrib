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

	"github.com/stretchr/testify/assert"

	azservicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	impl "github.com/dapr/components-contrib/internal/component/azure/servicebus"
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

func TestAddMessageAttributesToMetadata(t *testing.T) {
	testCases := []struct {
		name             string
		ASBMessage       azservicebus.ReceivedMessage
		expectedMetadata map[string]string
	}{
		{
			name: "Metadata must contain all attributes with the correct prefix",
			ASBMessage: azservicebus.ReceivedMessage{
				MessageID:            testMessageID,
				SessionID:            &testSessionID,
				CorrelationID:        &testCorrelationID,
				Subject:              &testLabel,
				ReplyTo:              &testReplyTo,
				To:                   &testTo,
				ContentType:          &testContentType,
				LockToken:            testLockTokenBytes,
				DeliveryCount:        testDeliveryCount,
				EnqueuedTime:         &testSampleTime,
				SequenceNumber:       &testSequenceNumber,
				ScheduledEnqueueTime: &testSampleTime,
				PartitionKey:         &testPartitionKey,
				LockedUntil:          &testSampleTime,
			},
			expectedMetadata: map[string]string{
				"metadata." + impl.MessageKeyMessageID:               testMessageID,
				"metadata." + impl.MessageKeySessionID:               testSessionID,
				"metadata." + impl.MessageKeyCorrelationID:           testCorrelationID,
				"metadata." + impl.MessageKeyLabel:                   testLabel, // Subject
				"metadata." + impl.MessageKeyReplyTo:                 testReplyTo,
				"metadata." + impl.MessageKeyTo:                      testTo,
				"metadata." + impl.MessageKeyContentType:             testContentType,
				"metadata." + impl.MessageKeyLockToken:               testLockTokenString,
				"metadata." + impl.MessageKeyDeliveryCount:           "1",
				"metadata." + impl.MessageKeyEnqueuedTimeUtc:         testSampleTimeHTTPFormat,
				"metadata." + impl.MessageKeySequenceNumber:          "1",
				"metadata." + impl.MessageKeyScheduledEnqueueTimeUtc: testSampleTimeHTTPFormat,
				"metadata." + impl.MessageKeyPartitionKey:            testPartitionKey,
				"metadata." + impl.MessageKeyLockedUntilUtc:          testSampleTimeHTTPFormat,
			},
		},
	}

	metadataMap := map[string]map[string]string{
		"Nil":   nil,
		"Empty": {},
	}

	for _, tc := range testCases {
		for mType, mMap := range metadataMap {
			t.Run(fmt.Sprintf("%s, metadata is %s", tc.name, mType), func(t *testing.T) {
				actual := addMessageAttributesToMetadata(mMap, &tc.ASBMessage)
				assert.Equal(t, tc.expectedMetadata, actual)
			})
		}
	}
}
