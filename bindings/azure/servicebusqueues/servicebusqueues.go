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

package servicebusqueues

import (
	"context"
	"errors"
	"fmt"
	"time"

	servicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	backoff "github.com/cenkalti/backoff/v4"

	"github.com/dapr/components-contrib/bindings"
	impl "github.com/dapr/components-contrib/internal/component/azure/servicebus"
	"github.com/dapr/kit/logger"
)

const (
	correlationID = "correlationID"
	label         = "label"
	id            = "id"
)

// AzureServiceBusQueues is an input/output binding reading from and sending events to Azure Service Bus queues.
type AzureServiceBusQueues struct {
	metadata *impl.Metadata
	client   *impl.Client
	timeout  time.Duration
	logger   logger.Logger
}

// NewAzureServiceBusQueues returns a new AzureServiceBusQueues instance.
func NewAzureServiceBusQueues(logger logger.Logger) bindings.InputOutputBinding {
	return &AzureServiceBusQueues{
		logger: logger,
	}
}

// Init parses connection properties and creates a new Service Bus Queue client.
func (a *AzureServiceBusQueues) Init(metadata bindings.Metadata) (err error) {
	a.metadata, err = impl.ParseMetadata(metadata.Properties, a.logger, (impl.MetadataModeBinding | impl.MetadataModeQueues))
	if err != nil {
		return err
	}
	a.timeout = time.Duration(a.metadata.TimeoutInSec) * time.Second

	a.client, err = impl.NewClient(a.metadata, metadata.Properties)
	if err != nil {
		return err
	}

	// Will do nothing if DisableEntityManagement is false
	err = a.client.EnsureQueue(context.Background(), a.metadata.QueueName)
	if err != nil {
		return err
	}

	return nil
}

func (a *AzureServiceBusQueues) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (a *AzureServiceBusQueues) Invoke(invokeCtx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	sender, err := a.client.GetSender(invokeCtx, a.metadata.QueueName)
	if err != nil {
		return nil, fmt.Errorf("failed to create a sender for the Service Bus queue: %w", err)
	}

	msg, err := impl.NewASBMessageFromInvokeRequest(req)
	if err != nil {
		return nil, fmt.Errorf("failed to create message: %w", err)
	}

	// Send the message
	ctx, cancel := context.WithTimeout(invokeCtx, a.timeout)
	defer cancel()
	err = sender.SendMessage(ctx, msg, nil)
	if err != nil {
		if impl.IsNetworkError(err) {
			// Force reconnection on next call
			a.client.CloseSender(a.metadata.QueueName)
		}
		return nil, err
	}

	return nil, nil
}

func (a *AzureServiceBusQueues) Read(subscribeCtx context.Context, handler bindings.Handler) error {
	// Reconnection backoff policy
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = 0
	bo.InitialInterval = time.Duration(a.metadata.MinConnectionRecoveryInSec) * time.Second
	bo.MaxInterval = time.Duration(a.metadata.MaxConnectionRecoveryInSec) * time.Second

	go func() {
		// Reconnect loop.
		for {
			sub := impl.NewSubscription(
				subscribeCtx,
				a.metadata.MaxActiveMessages,
				a.metadata.TimeoutInSec,
				nil,
				a.metadata.MaxRetriableErrorsPerSec,
				a.metadata.MaxConcurrentHandlers,
				"queue "+a.metadata.QueueName,
				a.logger,
			)

			// Blocks until a successful connection (or until context is canceled)
			err := sub.Connect(func() (*servicebus.Receiver, error) {
				return a.client.GetClient().NewReceiverForQueue(a.metadata.QueueName, nil)
			})
			if err != nil {
				// Realistically, the only time we should get to this point is if the context was canceled, but let's log any other error we may get.
				if errors.Is(err, context.Canceled) {
					a.logger.Warnf("Error reading from Azure Service Bus Queue binding: %s", err.Error())
				}
				return
			}

			// ReceiveAndBlock will only return with an error that it cannot handle internally. The subscription connection is closed when this method returns.
			// If that occurs, we will log the error and attempt to re-establish the subscription connection until we exhaust the number of reconnect attempts.
			err = sub.ReceiveAndBlock(
				a.getHandlerFunc(handler),
				a.metadata.LockRenewalInSec,
				false, // Bulk is not supported here.
				func() {
					// Reset the backoff when the subscription is successful and we have received the first message
					bo.Reset()
				},
			)
			if err != nil && !errors.Is(err, context.Canceled) {
				a.logger.Error(err)
			}

			// Gracefully close the connection (in case it's not closed already)
			// Use a background context here (with timeout) because ctx may be closed already
			closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second*time.Duration(a.metadata.TimeoutInSec))
			sub.Close(closeCtx)
			closeCancel()

			// If context was canceled, do not attempt to reconnect
			if subscribeCtx.Err() != nil {
				a.logger.Debug("Context canceled; will not reconnect")
				return
			}

			wait := bo.NextBackOff()
			a.logger.Warnf("Subscription to queue %s lost connection, attempting to reconnect in %s...", a.metadata.QueueName, wait)
			time.Sleep(wait)
		}
	}()

	return nil
}

func (a *AzureServiceBusQueues) getHandlerFunc(handler bindings.Handler) impl.HandlerFunc {
	return func(ctx context.Context, asbMsgs []*servicebus.ReceivedMessage) ([]impl.HandlerResponseItem, error) {
		if len(asbMsgs) != 1 {
			return nil, fmt.Errorf("expected 1 message, got %d", len(asbMsgs))
		}

		msg := asbMsgs[0]
		metadata := make(map[string]string)
		metadata[id] = msg.MessageID
		if msg.CorrelationID != nil {
			metadata[correlationID] = *msg.CorrelationID
		}
		if msg.Subject != nil {
			metadata[label] = *msg.Subject
		}

		// Passthrough any custom metadata to the handler.
		for key, val := range msg.ApplicationProperties {
			if stringVal, ok := val.(string); ok {
				metadata[key] = stringVal
			}
		}

		_, err := handler(ctx, &bindings.ReadResponse{
			Data:     msg.Body,
			Metadata: metadata,
		})
		return []impl.HandlerResponseItem{}, err
	}
}

func (a *AzureServiceBusQueues) Close() (err error) {
	a.logger.Debug("Closing component")
	a.client.CloseSender(a.metadata.QueueName)
	return nil
}
