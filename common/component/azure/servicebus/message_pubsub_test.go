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
	"net/url"
	"testing"

	azservicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/stretchr/testify/assert"
)

// testInvalidHeaderKey is a sample URN-style key emitted by Dataverse / Azure Digital
// Twins that breaks HTTP header validation when forwarded by Dapr.
// See microsoft/azure-container-apps#1690.
const testInvalidHeaderKey = "http://schemas.microsoft.com/xrm/2011/Claims/requestname"

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
				ApplicationProperties: map[string]interface{}{
					"hello":   "world",
					"numeric": 1,
				},
			},
			expectedMetadata: map[string]string{
				"metadata." + MessageKeyMessageID:               testMessageID,
				"metadata." + MessageKeySessionID:               testSessionID,
				"metadata." + MessageKeyCorrelationID:           testCorrelationID,
				"metadata." + MessageKeyLabel:                   testLabel, // Subject
				"metadata." + MessageKeyReplyTo:                 testReplyTo,
				"metadata." + MessageKeyTo:                      testTo,
				"metadata." + MessageKeyContentType:             testContentType,
				"metadata." + MessageKeyLockToken:               testLockTokenString,
				"metadata." + MessageKeyDeliveryCount:           "1",
				"metadata." + MessageKeyEnqueuedTimeUtc:         testSampleTimeHTTPFormat,
				"metadata." + MessageKeySequenceNumber:          "1",
				"metadata." + MessageKeyScheduledEnqueueTimeUtc: testSampleTimeHTTPFormat,
				"metadata." + MessageKeyPartitionKey:            testPartitionKey,
				"metadata." + MessageKeyLockedUntilUtc:          testSampleTimeHTTPFormat,
				"metadata.hello":                                "world",
				"metadata.numeric":                              "1",
			},
		},
		{
			name: "ApplicationProperties with reserved characters in key are URL-escaped, values pass through unchanged",
			ASBMessage: azservicebus.ReceivedMessage{
				DeliveryCount: testDeliveryCount,
				LockToken:     testLockTokenBytes,
				ApplicationProperties: map[string]interface{}{
					testInvalidHeaderKey: "value with: special/chars and +",
					"safe-key":           "safe-value",
					"numeric":            42,
					"nil-val":            nil,
				},
			},
			expectedMetadata: map[string]string{
				"metadata." + MessageKeyDeliveryCount:               "1",
				"metadata." + MessageKeyLockToken:                   testLockTokenString,
				"metadata." + url.QueryEscape(testInvalidHeaderKey): "value with: special/chars and +",
				"metadata.safe-key":                                 "safe-value",
				"metadata.numeric":                                  "42",
				"metadata.nil-val":                                  "",
			},
		},
	}

	for _, tc := range testCases {
		for _, mType := range []string{"Nil", "Empty"} {
			// Construct a fresh map per run; the function under test mutates its input,
			// so sharing a non-nil map across cases would cause cross-case contamination.
			var mMap map[string]string
			if mType == "Empty" {
				mMap = map[string]string{}
			}
			t.Run(fmt.Sprintf("%s, metadata is %s", tc.name, mType), func(t *testing.T) {
				actual := addMessageAttributesToMetadata(mMap, &tc.ASBMessage)
				assert.Equal(t, tc.expectedMetadata, actual)
			})
		}
	}
}
