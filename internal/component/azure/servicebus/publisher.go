/*
Copyright 2023 The Dapr Authors
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
	"context"
	"fmt"
	"time"

	servicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/cenkalti/backoff/v4"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/internal/utils"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

const (
	defaultMaxBulkSubCount        = 100
	defaultMaxBulkPubBytes uint64 = 128 << 10 // 128 KiB
)

// PublishPubSub is used by PubSub components to publish messages. It includes a retry logic that can also cause reconnections.
func (c *Client) PublishPubSub(
	ctx context.Context,
	req *pubsub.PublishRequest,
	ensureMethod func(context.Context, string) error,
	log logger.Logger,
) error {
	msg, err := NewASBMessageFromPubsubRequest(req)
	if err != nil {
		return err
	}

	bo := c.publishBackOff(ctx)

	msgID := "nil"
	if msg.MessageID != nil {
		msgID = *msg.MessageID
	}
	return retry.NotifyRecover(
		func() (err error) {
			// Ensure the queue or topic exists the first time it is referenced
			// This does nothing if DisableEntityManagement is true
			// Note that the parameter is called "Topic" but it could be the name of a queue
			err = ensureMethod(ctx, req.Topic)
			if err != nil {
				return err
			}

			// Get the sender
			var sender *servicebus.Sender
			sender, err = c.GetSender(ctx, req.Topic)
			if err != nil {
				return fmt.Errorf("failed to create a sender: %w", err)
			}

			// Try sending the message
			publishCtx, publisCancel := context.WithTimeout(ctx, time.Second*time.Duration(c.metadata.TimeoutInSec))
			err = sender.SendMessage(publishCtx, msg, nil)
			publisCancel()
			if err != nil {
				if IsNetworkError(err) {
					// Retry after reconnecting
					c.CloseSender(req.Topic, log)
					return err
				}

				if IsRetriableAMQPError(err) {
					// Retry (no need to reconnect)
					return err
				}

				// Do not retry on other errors
				return backoff.Permanent(err)
			}
			return nil
		},
		bo,
		func(err error, _ time.Duration) {
			log.Warnf("Could not publish service bus message (%s). Retrying...: %v", msgID, err)
		},
		func() {
			log.Infof("Successfully published service bus message (%s) after it previously failed", msgID)
		},
	)
}

// PublishPubSubBulk is used by PubSub components to publush bulk messages.
func (c *Client) PublishPubSubBulk(
	ctx context.Context,
	req *pubsub.BulkPublishRequest,
	ensureMethod func(context.Context, string) error,
	log logger.Logger,
) (pubsub.BulkPublishResponse, error) {
	// If the request is empty, sender.SendMessageBatch will panic later.
	// Return an empty response to avoid this.
	if len(req.Entries) == 0 {
		log.Warnf("Empty bulk publish request, skipping")
		return pubsub.BulkPublishResponse{}, nil
	}

	// Ensure the queue or topic exists the first time it is referenced
	// This does nothing if DisableEntityManagement is true
	// Note that the parameter is called "Topic" but it could be the name of a queue
	err := ensureMethod(ctx, req.Topic)
	if err != nil {
		return pubsub.NewBulkPublishResponse(req.Entries, err), err
	}

	// Get the sender
	sender, err := c.GetSender(ctx, req.Topic)
	if err != nil {
		return pubsub.NewBulkPublishResponse(req.Entries, err), err
	}

	// Create a new batch of messages with batch options.
	batchOpts := &servicebus.MessageBatchOptions{
		MaxBytes: utils.GetElemOrDefaultFromMap(req.Metadata, contribMetadata.MaxBulkPubBytesKey, defaultMaxBulkPubBytes),
	}

	batchMsg, err := sender.NewMessageBatch(ctx, batchOpts)
	if err != nil {
		return pubsub.NewBulkPublishResponse(req.Entries, err), err
	}

	// Add messages from the bulk publish request to the batch.
	err = UpdateASBBatchMessageWithBulkPublishRequest(batchMsg, req)
	if err != nil {
		return pubsub.NewBulkPublishResponse(req.Entries, err), err
	}

	// Azure Service Bus does not return individual status for each message in the request.
	err = sender.SendMessageBatch(ctx, batchMsg, nil)
	if err != nil {
		return pubsub.NewBulkPublishResponse(req.Entries, err), err
	}

	return pubsub.BulkPublishResponse{}, nil
}

// PublishBinding is used by binding components to publish messages. It includes a retry logic that can also cause reconnections.
// Note this doesn't invoke "EnsureQueue" or "EnsureTopic" because bindings don't do that on publishing.
func (c *Client) PublishBinding(ctx context.Context, req *bindings.InvokeRequest, queueOrTopic string, log logger.Logger) (*bindings.InvokeResponse, error) {
	msg, err := NewASBMessageFromInvokeRequest(req)
	if err != nil {
		return nil, fmt.Errorf("failed to create message: %w", err)
	}

	bo := c.publishBackOff(ctx)

	msgID := "nil"
	if msg.MessageID != nil {
		msgID = *msg.MessageID
	}

	err = retry.NotifyRecover(
		func() (err error) {
			// Get the sender
			var sender *servicebus.Sender
			sender, err = c.GetSender(ctx, queueOrTopic)
			if err != nil {
				return fmt.Errorf("failed to create a sender: %w", err)
			}

			// Try sending the message
			publishCtx, publisCancel := context.WithTimeout(ctx, time.Second*time.Duration(c.metadata.TimeoutInSec))
			err = sender.SendMessage(publishCtx, msg, nil)
			publisCancel()
			if err != nil {
				if IsNetworkError(err) {
					// Retry after reconnecting
					c.CloseSender(queueOrTopic, log)
					return err
				}

				if IsRetriableAMQPError(err) {
					// Retry (no need to reconnect)
					return err
				}

				// Do not retry on other errors
				return backoff.Permanent(err)
			}
			return nil
		},
		bo,
		func(err error, _ time.Duration) {
			log.Warnf("Could not publish service bus message (%s). Retrying...: %v", msgID, err)
		},
		func() {
			log.Infof("Successfully published service bus message (%s) after it previously failed", msgID)
		},
	)

	return nil, err
}

func (c *Client) publishBackOff(ctx context.Context) (bo backoff.BackOff) {
	ebo := backoff.NewExponentialBackOff()
	ebo.InitialInterval = time.Duration(c.metadata.PublishInitialRetryIntervalInMs) * time.Millisecond
	bo = backoff.WithMaxRetries(ebo, uint64(c.metadata.PublishMaxRetries))
	bo = backoff.WithContext(bo, ctx)
	return bo
}
