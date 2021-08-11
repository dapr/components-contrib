package servicebus

import (
	"testing"

	azservicebus "github.com/Azure/azure-service-bus-go"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMessageFromRequest(t *testing.T) {
	testMessageData := []byte("test message")
	testMessageId := "testMessageId"
	testCorrelationId := "testCorrelationId"
	testSessionId := "testSessionId"
	testLabel := "testLabel"
	testReplyTo := "testReplyTo"
	testTo := "testTo"
	testPartitionKey := testSessionId
	testPartitionKeyUnique := "testPartitionKey"
	testContentType := "testContentType"

	testCases := []struct {
		name            string
		request         pubsub.PublishRequest
		expectedMessage azservicebus.Message
		expectError     bool
	}{
		{
			name: "Sets valid values",
			request: pubsub.PublishRequest{
				Data: testMessageData,
				Metadata: map[string]string{
					MessageIdMetadataKey:     testMessageId,
					CorrelationIdMetadataKey: testCorrelationId,
					SessionIdMetadataKey:     testSessionId,
					LabelMetadataKey:         testLabel,
					ReplyToMetadataKey:       testReplyTo,
					ToMetadataKey:            testTo,
					PartitionKeyMetadataKey:  testPartitionKey,
					ContentTypeMetadataKey:   testContentType,
				},
			},
			expectedMessage: azservicebus.Message{
				Data:          testMessageData,
				ID:            testMessageId,
				CorrelationID: testCorrelationId,
				SessionID:     &testSessionId,
				Label:         testLabel,
				ReplyTo:       testReplyTo,
				To:            testTo,
				SystemProperties: &azservicebus.SystemProperties{
					PartitionKey: &testPartitionKey,
				},
				ContentType: testContentType,
			},
			expectError: false,
		},
		{
			name: "Errors when partition key and session id set but not equal",
			request: pubsub.PublishRequest{
				Data: testMessageData,
				Metadata: map[string]string{
					MessageIdMetadataKey:     testMessageId,
					CorrelationIdMetadataKey: testCorrelationId,
					SessionIdMetadataKey:     testSessionId,
					LabelMetadataKey:         testLabel,
					ReplyToMetadataKey:       testReplyTo,
					ToMetadataKey:            testTo,
					PartitionKeyMetadataKey:  testPartitionKeyUnique,
					ContentTypeMetadataKey:   testContentType,
				},
			},
			expectedMessage: azservicebus.Message{
				Data:          testMessageData,
				ID:            testMessageId,
				CorrelationID: testCorrelationId,
				SessionID:     &testSessionId,
				Label:         testLabel,
				ReplyTo:       testReplyTo,
				To:            testTo,
				SystemProperties: &azservicebus.SystemProperties{
					PartitionKey: &testPartitionKey,
				},
				ContentType: testContentType,
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// arrange
			req := tc.request

			// act
			msg, err := NewMessageFromRequest(&req)

			// assert
			if tc.expectError {
				require.NotNil(t, err)
			} else {
				require.Nil(t, err)
				assert.Equal(t, tc.expectedMessage.Data, msg.Data)
				assert.Equal(t, tc.expectedMessage.ID, msg.ID)
				assert.Equal(t, tc.expectedMessage.CorrelationID, msg.CorrelationID)
				assert.Equal(t, tc.expectedMessage.SessionID, msg.SessionID)
				assert.Equal(t, tc.expectedMessage.ContentType, msg.ContentType)
				assert.Equal(t, tc.expectedMessage.ReplyTo, msg.ReplyTo)
				assert.Equal(t, tc.expectedMessage.TTL, msg.TTL)
				assert.Equal(t, tc.expectedMessage.To, msg.To)
				assert.Equal(t, tc.expectedMessage.Label, msg.Label)
				assert.NotNil(t, msg.SystemProperties)
				assert.Equal(t, tc.expectedMessage.SystemProperties.PartitionKey, msg.SystemProperties.PartitionKey)
			}
		})
	}
}
