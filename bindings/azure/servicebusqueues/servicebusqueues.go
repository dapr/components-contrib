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
	"sync"
	"time"

	servicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	sbadmin "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	backoff "github.com/cenkalti/backoff/v4"

	"github.com/dapr/components-contrib/bindings"
	azauth "github.com/dapr/components-contrib/internal/authentication/azure"
	impl "github.com/dapr/components-contrib/internal/component/azure/servicebus"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const (
	correlationID = "correlationID"
	label         = "label"
	id            = "id"
)

// AzureServiceBusQueues is an input/output binding reading from and sending events to Azure Service Bus queues.
type AzureServiceBusQueues struct {
	metadata    *impl.Metadata
	client      *servicebus.Client
	adminClient *sbadmin.Client
	timeout     time.Duration
	sender      *servicebus.Sender
	senderLock  sync.RWMutex
	logger      logger.Logger
}

// NewAzureServiceBusQueues returns a new AzureServiceBusQueues instance.
func NewAzureServiceBusQueues(logger logger.Logger) bindings.InputOutputBinding {
	return &AzureServiceBusQueues{
		senderLock: sync.RWMutex{},
		logger:     logger,
	}
}

// Init parses connection properties and creates a new Service Bus Queue client.
func (a *AzureServiceBusQueues) Init(metadata bindings.Metadata) (err error) {
	a.metadata, err = impl.ParseMetadata(metadata.Properties, a.logger, impl.MetadataModeBinding)
	if err != nil {
		return err
	}
	a.timeout = time.Duration(a.metadata.TimeoutInSec) * time.Second

	userAgent := "dapr-" + logger.DaprVersion
	if a.metadata.ConnectionString != "" {
		a.client, err = servicebus.NewClientFromConnectionString(a.metadata.ConnectionString, &servicebus.ClientOptions{
			ApplicationID: userAgent,
		})
		if err != nil {
			return err
		}

		if !a.metadata.DisableEntityManagement {
			a.adminClient, err = sbadmin.NewClientFromConnectionString(a.metadata.ConnectionString, nil)
			if err != nil {
				return err
			}
		}
	} else {
		settings, innerErr := azauth.NewEnvironmentSettings(azauth.AzureServiceBusResourceName, metadata.Properties)
		if innerErr != nil {
			return innerErr
		}

		token, innerErr := settings.GetTokenCredential()
		if innerErr != nil {
			return innerErr
		}

		a.client, innerErr = servicebus.NewClient(a.metadata.NamespaceName, token, &servicebus.ClientOptions{
			ApplicationID: userAgent,
		})
		if innerErr != nil {
			return innerErr
		}

		if !a.metadata.DisableEntityManagement {
			a.adminClient, innerErr = sbadmin.NewClient(a.metadata.NamespaceName, token, nil)
			if innerErr != nil {
				return innerErr
			}
		}
	}

	if a.adminClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
		getQueueRes, err := a.adminClient.GetQueue(ctx, a.metadata.QueueName, nil)
		cancel()
		if err != nil {
			return err
		}
		if getQueueRes == nil {
			// Need to create the queue
			ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
			_, err = a.adminClient.CreateQueue(ctx, a.metadata.QueueName, &sbadmin.CreateQueueOptions{
				Properties: a.metadata.CreateQueueProperties(),
			})
			cancel()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *AzureServiceBusQueues) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (a *AzureServiceBusQueues) Invoke(invokeCtx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	sender, err := a.getSender()
	if err != nil {
		return nil, fmt.Errorf("failed to create a sender for the Service Bus queue: %w", err)
	}

	msg := &servicebus.Message{
		Body:                  req.Data,
		ApplicationProperties: make(map[string]interface{}),
	}
	if val, ok := req.Metadata[id]; ok && val != "" {
		msg.MessageID = &val
	}
	if val, ok := req.Metadata[correlationID]; ok && val != "" {
		msg.CorrelationID = &val
	}

	// Include incoming metadata in the message to be used when it is read.
	for k, v := range req.Metadata {
		// Don't include the values that are saved in MessageID or CorrelationID.
		if k == id || k == correlationID {
			continue
		}
		msg.ApplicationProperties[k] = v
	}

	ttl, ok, err := contribMetadata.TryGetTTL(req.Metadata)
	if err != nil {
		return nil, err
	}
	if ok {
		msg.TimeToLive = &ttl
	}

	// Send the message
	ctx, cancel := context.WithTimeout(invokeCtx, a.timeout)
	defer cancel()
	err = sender.SendMessage(ctx, msg, nil)
	if err != nil {
		if impl.IsNetworkError(err) {
			// Force reconnection on next call
			a.deleteSender()
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
				return a.client.NewReceiverForQueue(a.metadata.QueueName, nil)
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

// getSender returns the Sender object, creating a new connection if needed
func (a *AzureServiceBusQueues) getSender() (*servicebus.Sender, error) {
	// Check if the sender already exists
	a.senderLock.RLock()
	if a.sender != nil {
		a.senderLock.RUnlock()
		return a.sender, nil
	}
	a.senderLock.RUnlock()

	// Acquire a write lock then try checking a.sender again in case another goroutine modified that in the meanwhile
	a.senderLock.Lock()
	defer a.senderLock.Unlock()

	if a.sender != nil {
		return a.sender, nil
	}

	// Create a new sender
	sender, err := a.client.NewSender(a.metadata.QueueName, nil)
	if err != nil {
		return nil, err
	}
	a.sender = sender

	return sender, nil
}

// deleteSender deletes the sender, closing the connection
func (a *AzureServiceBusQueues) deleteSender() {
	a.senderLock.Lock()
	if a.sender != nil {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
		_ = a.sender.Close(closeCtx)
		closeCancel()
		a.sender = nil
	}
	a.senderLock.Unlock()
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
	a.senderLock.Lock()
	defer a.senderLock.Unlock()

	a.logger.Debug("Closing component")

	if a.sender != nil {
		ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
		err = a.sender.Close(ctx)
		cancel()
		a.sender = nil
		if err != nil {
			return err
		}
	}
	return nil
}
