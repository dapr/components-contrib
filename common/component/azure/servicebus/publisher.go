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
	commonutils "github.com/dapr/components-contrib/common/utils"
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
func (c *Client) PublishPubSub(ctx context.Context, req *pubsub.PublishRequest, ensureFn ensureFn, log logger.Logger) error {
	msg, err := NewASBMessageFromPubsubRequest(req)
	if err != nil {
		return err
	}

	bo := c.publishBackOff(ctx)

	msgID := "nil"
	if msg.MessageID != nil {
		msgID = *msg.MessageID
	}
	err = retry.NotifyRecover(
		func() error {
			// Get the sender
			sender, rErr := c.GetSender(ctx, req.Topic, ensureFn)
			if rErr != nil {
				return fmt.Errorf("failed to create a sender: %w", rErr)
			}

			// Try sending the message
			publishCtx, publisCancel := context.WithTimeout(ctx, time.Second*time.Duration(c.metadata.TimeoutInSec))
			rErr = sender.SendMessage(publishCtx, msg, nil)
			publisCancel()
			if rErr != nil {
				if IsNetworkError(rErr) {
					// Retry after reconnecting
					c.CloseSender(req.Topic, log)
					return rErr
				}

				if IsRetriableAMQPError(rErr) {
					// Retry (no need to reconnect)
					return rErr
				}

				// Do not retry on other errors
				return backoff.Permanent(rErr)
			}
			return nil
		},
		bo,
		func(err error, _ time.Duration) {
			log.Warnf("Could not publish Service Bus message (%s). Retrying...: %v", msgID, err)
		},
		func() {
			log.Infof("Successfully published Service Bus message (%s) after it previously failed", msgID)
		},
	)
	if err != nil {
		log.Errorf("Too many failed attempts while publishing Service Bus message (%s): %v", msgID, err)
	}
	return err
}

// PublishPubSubBulk is used by PubSub components to publush bulk messages.
func (c *Client) PublishPubSubBulk(ctx context.Context, req *pubsub.BulkPublishRequest, ensureFn ensureFn, log logger.Logger) (pubsub.BulkPublishResponse, error) {
	// If the request is empty, sender.SendMessageBatch will panic later.
	// Return an empty response to avoid this.
	if len(req.Entries) == 0 {
		log.Warnf("Empty bulk publish request, skipping")
		return pubsub.BulkPublishResponse{}, nil
	}

	// Get the sender
	sender, err := c.GetSender(ctx, req.Topic, ensureFn)
	if err != nil {
		return pubsub.NewBulkPublishResponse(req.Entries, err), err
	}

	// Create a new batch of messages with batch options.
	batchOpts := &servicebus.MessageBatchOptions{
		MaxBytes: commonutils.GetElemOrDefaultFromMap(req.Metadata, contribMetadata.MaxBulkPubBytesKey, defaultMaxBulkPubBytes),
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
		func() error {
			// Get the sender
			sender, rErr := c.GetSender(ctx, queueOrTopic, nil)
			if rErr != nil {
				return fmt.Errorf("failed to create a sender: %w", rErr)
			}

			// Try sending the message
			publishCtx, publisCancel := context.WithTimeout(ctx, time.Second*time.Duration(c.metadata.TimeoutInSec))
			rErr = sender.SendMessage(publishCtx, msg, nil)
			publisCancel()
			if rErr != nil {
				if IsNetworkError(rErr) {
					// Retry after reconnecting
					c.CloseSender(queueOrTopic, log)
					return rErr
				}

				if IsRetriableAMQPError(rErr) {
					// Retry (no need to reconnect)
					return rErr
				}

				// Do not retry on other errors
				return backoff.Permanent(rErr)
			}
			return nil
		},
		bo,
		func(err error, _ time.Duration) {
			log.Warnf("Could not publish Service Bus message (%s). Retrying...: %v", msgID, err)
		},
		func() {
			log.Infof("Successfully published Service Bus message (%s) after it previously failed", msgID)
		},
	)
	if err != nil {
		log.Errorf("Too many failed attempts while publishing Service Bus message (%s): %v", msgID, err)
	}
	return nil, err
}

func (c *Client) publishBackOff(ctx context.Context) (bo backoff.BackOff) {
	ebo := backoff.NewExponentialBackOff()
	ebo.InitialInterval = time.Duration(c.metadata.PublishInitialRetryIntervalInMs) * time.Millisecond
	bo = backoff.WithMaxRetries(ebo, uint64(c.metadata.PublishMaxRetries)) //nolint:gosec
	bo = backoff.WithContext(bo, ctx)
	return bo
}
