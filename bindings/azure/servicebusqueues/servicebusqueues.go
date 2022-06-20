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
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	servicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	sbadmin "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"github.com/Azure/go-amqp"
	backoff "github.com/cenkalti/backoff/v4"

	azauth "github.com/dapr/components-contrib/authentication/azure"
	"github.com/dapr/components-contrib/bindings"
	impl "github.com/dapr/components-contrib/internal/component/azure/servicebus"
	contrib_metadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const (
	correlationID = "correlationID"
	label         = "label"
	id            = "id"
)

// AzureServiceBusQueues is an input/output binding reading from and sending events to Azure Service Bus queues.
type AzureServiceBusQueues struct {
	metadata    *serviceBusQueuesMetadata
	client      *servicebus.Client
	adminClient *sbadmin.Client
	timeout     time.Duration
	sender      *servicebus.Sender
	senderLock  sync.RWMutex
	logger      logger.Logger
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewAzureServiceBusQueues returns a new AzureServiceBusQueues instance.
func NewAzureServiceBusQueues(logger logger.Logger) *AzureServiceBusQueues {
	return &AzureServiceBusQueues{
		senderLock: sync.RWMutex{},
		logger:     logger,
	}
}

// Init parses connection properties and creates a new Service Bus Queue client.
func (a *AzureServiceBusQueues) Init(metadata bindings.Metadata) (err error) {
	a.metadata, err = a.parseMetadata(metadata)
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

		a.adminClient, err = sbadmin.NewClientFromConnectionString(a.metadata.ConnectionString, nil)
		if err != nil {
			return err
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

		a.adminClient, innerErr = sbadmin.NewClient(a.metadata.NamespaceName, token, nil)
		if innerErr != nil {
			return innerErr
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
	defer cancel()
	getQueueRes, err := a.adminClient.GetQueue(ctx, a.metadata.QueueName, nil)
	if err != nil {
		return err
	}
	if getQueueRes == nil {
		// Need to create the queue
		ttlDur := contrib_metadata.Duration{
			Duration: a.metadata.ttl,
		}
		ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
		defer cancel()
		_, err = a.adminClient.CreateQueue(ctx, a.metadata.QueueName, &sbadmin.CreateQueueOptions{
			Properties: &sbadmin.QueueProperties{
				DefaultMessageTimeToLive: to.Ptr(ttlDur.ToISOString()),
			},
		})
		if err != nil {
			return err
		}
	}

	a.ctx, a.cancel = context.WithCancel(context.Background())

	return nil
}

func (a *AzureServiceBusQueues) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (a *AzureServiceBusQueues) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var err error
	ctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()

	a.senderLock.RLock()
	sender := a.sender
	a.senderLock.RUnlock()

	if sender == nil {
		a.senderLock.Lock()
		sender, err = a.client.NewSender(a.metadata.QueueName, nil)
		if err != nil {
			a.senderLock.Unlock()
			return nil, err
		}
		a.sender = sender
		a.senderLock.Unlock()
	}

	msg := &servicebus.Message{
		Body: req.Data,
	}
	if val, ok := req.Metadata[id]; ok && val != "" {
		msg.MessageID = &val
	}
	if val, ok := req.Metadata[correlationID]; ok && val != "" {
		msg.CorrelationID = &val
	}
	ttl, ok, err := contrib_metadata.TryGetTTL(req.Metadata)
	if err != nil {
		return nil, err
	}
	if ok {
		msg.TimeToLive = &ttl
	}

	return nil, sender.SendMessage(ctx, msg, nil)
}

func (a *AzureServiceBusQueues) Read(handler bindings.Handler) error {
	subscribeCtx := context.Background()

	// Reconnection backoff policy
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = 0
	bo.InitialInterval = time.Duration(a.metadata.MinConnectionRecoveryInSec) * time.Second
	bo.MaxInterval = time.Duration(a.metadata.MaxConnectionRecoveryInSec) * time.Second

	// Reconnect loop.
	for {
		sub := impl.NewSubscription(
			subscribeCtx,
			a.metadata.MaxActiveMessages,
			a.metadata.TimeoutInSec,
			*a.metadata.MaxRetriableErrorsPerSec,
			&a.metadata.MaxConcurrentHandlers,
			"queue "+a.metadata.QueueName,
			a.logger,
		)

		// Blocks until a successful connection (or until context is canceled)
		err := sub.Connect(func() (*servicebus.Receiver, error) {
			return a.client.NewReceiverForQueue(a.metadata.QueueName, nil)
		})
		if err != nil {
			// Realistically, the only time we should get to this point is if the context was canceled, but let's log any other error we may get.
			if err != context.Canceled {
				a.logger.Warnf("Error reading from Azure Service Bus Queue binding: %s", err.Error())
			}
			break
		}

		// ReceiveAndBlock will only return with an error that it cannot handle internally. The subscription connection is closed when this method returns.
		// If that occurs, we will log the error and attempt to re-establish the subscription connection until we exhaust the number of reconnect attempts.
		err = sub.ReceiveAndBlock(
			a.getHandlerFunc(handler),
			a.metadata.LockRenewalInSec,
			func() {
				// Reset the backoff when the subscription is successful and we have received the first message
				bo.Reset()
			},
		)
		if err != nil {
			var detachError *amqp.DetachError
			var amqpError *amqp.Error
			if errors.Is(err, detachError) ||
				(errors.As(err, &amqpError) && amqpError.Condition == amqp.ErrorDetachForced) {
				a.logger.Debug(err)
			} else {
				a.logger.Error(err)
			}
		}

		// Gracefully close the connection (in case it's not closed already)
		// Use a background context here (with timeout) because ctx may be closed already
		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second*time.Duration(a.metadata.TimeoutInSec))
		sub.Close(closeCtx)
		closeCancel()

		// If context was canceled, do not attempt to reconnect
		if subscribeCtx.Err() != nil {
			a.logger.Debug("Context canceled; will not reconnect")
			break
		}

		wait := bo.NextBackOff()
		a.logger.Warnf("Subscription to queue %s lost connection, attempting to reconnect in %s...", a.metadata.QueueName, wait)
		time.Sleep(wait)
	}

	return nil
}

func (a *AzureServiceBusQueues) getHandlerFunc(handler bindings.Handler) impl.HandlerFunc {
	return func(ctx context.Context, msg *servicebus.ReceivedMessage) error {
		metadata := make(map[string]string)
		metadata[id] = msg.MessageID
		if msg.CorrelationID != nil {
			metadata[correlationID] = *msg.CorrelationID
		}
		if msg.Subject != nil {
			metadata[label] = *msg.Subject
		}

		_, err := handler(a.ctx, &bindings.ReadResponse{
			Data:     msg.Body,
			Metadata: metadata,
		})
		return err
	}
}

func (a *AzureServiceBusQueues) Close() (err error) {
	a.logger.Info("Shutdown called!")
	a.senderLock.Lock()
	defer func() {
		a.senderLock.Unlock()
		a.cancel()
	}()
	if a.sender != nil {
		ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
		err = a.sender.Close(ctx)
		a.sender = nil
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}
