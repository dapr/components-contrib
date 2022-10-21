package servicebus

import (
	"context"
	"fmt"
	"time"

	servicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

// GetPubSubHandlerFunc returns the handler function for pubsub messages.
func GetPubSubHandlerFunc(topic string, handler pubsub.Handler, log logger.Logger, timeout time.Duration) HandlerFunc {
	emptyResponseItems := []HandlerResponseItem{}
	// Only the first ASB message is used in the actual handler invocation.
	return func(ctx context.Context, asbMsgs []*servicebus.ReceivedMessage) ([]HandlerResponseItem, error) {
		if len(asbMsgs) != 1 {
			return nil, fmt.Errorf("expected 1 message, got %d", len(asbMsgs))
		}

		pubsubMsg, err := NewPubsubMessageFromASBMessage(asbMsgs[0], topic)
		if err != nil {
			return emptyResponseItems, fmt.Errorf("failed to get pubsub message from azure service bus message: %+v", err)
		}

		handleCtx, handleCancel := context.WithTimeout(ctx, timeout)
		defer handleCancel()
		log.Debugf("Calling app's handler for message %s on topic %s", asbMsgs[0].MessageID, topic)
		return emptyResponseItems, handler(handleCtx, pubsubMsg)
	}
}

// GetPubSubHandlerFunc returns the handler function for bulk pubsub messages.
func GetBulkPubSubHandlerFunc(topic string, handler pubsub.BulkHandler, log logger.Logger, timeout time.Duration) HandlerFunc {
	return func(ctx context.Context, asbMsgs []*servicebus.ReceivedMessage) ([]HandlerResponseItem, error) {
		pubsubMsgs := make([]pubsub.BulkMessageEntry, len(asbMsgs))
		for i, asbMsg := range asbMsgs {
			pubsubMsg, err := NewBulkMessageEntryFromASBMessage(asbMsg)
			if err != nil {
				return nil, fmt.Errorf("failed to get pubsub message from azure service bus message: %+v", err)
			}
			pubsubMsgs[i] = pubsubMsg
		}

		// Note, no metadata is currently supported here.
		// In the future, we could add propagate metadata to the handler if required.
		bulkMessage := &pubsub.BulkMessage{
			Entries:  pubsubMsgs,
			Metadata: map[string]string{},
			Topic:    topic,
		}

		handleCtx, handleCancel := context.WithTimeout(ctx, timeout)
		defer handleCancel()
		log.Debugf("Calling app's handler for %d messages on topic %s", len(asbMsgs), topic)
		resps, err := handler(handleCtx, bulkMessage)

		implResps := make([]HandlerResponseItem, len(resps))
		for i, resp := range resps {
			implResps[i] = HandlerResponseItem{
				EntryId: resp.EntryId,
				Error:   resp.Error,
			}
		}

		return implResps, err
	}
}
