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
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	servicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	sbadmin "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	backoff "github.com/cenkalti/backoff/v4"
	"go.uber.org/ratelimit"

	azauth "github.com/dapr/components-contrib/authentication/azure"
	"github.com/dapr/components-contrib/bindings"
	contrib_metadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

const (
	correlationID = "correlationID"
	label         = "label"
	id            = "id"

	// azureServiceBusDefaultMessageTimeToLive defines the default time to live for queues, which is 14 days. The same way Azure Portal does.
	azureServiceBusDefaultMessageTimeToLive = time.Hour * 24 * 14

	// Default timeout in seconds
	defaultTimeoutInSec = 60

	// Default minimum and maximum recovery time while trying to reconnect
	defaultMinConnectionRecoveryInSec = 2
	defaultMaxConnectionRecoveryInSec = 300

	// Default rate of retriable errors per second
	defaultMaxRetriableErrorsPerSec = 10
)

// AzureServiceBusQueues is an input/output binding reading from and sending events to Azure Service Bus queues.
type AzureServiceBusQueues struct {
	metadata          *serviceBusQueuesMetadata
	client            *servicebus.Client
	adminClient       *sbadmin.Client
	timeout           time.Duration
	sender            *servicebus.Sender
	senderLock        sync.RWMutex
	retriableErrLimit ratelimit.Limiter
	logger            logger.Logger
	ctx               context.Context
	cancel            context.CancelFunc
}

type serviceBusQueuesMetadata struct {
	ConnectionString           string `json:"connectionString"`
	NamespaceName              string `json:"namespaceName,omitempty"`
	QueueName                  string `json:"queueName"`
	TimeoutInSec               int    `json:"timeoutInSec"`
	MaxConnectionRecoveryInSec int    `json:"maxConnectionRecoveryInSec"`
	MinConnectionRecoveryInSec int    `json:"minConnectionRecoveryInSec"`
	MaxRetriableErrorsPerSec   *int   `json:"maxRetriableErrorsPerSec"`
	ttl                        time.Duration
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
	if *a.metadata.MaxRetriableErrorsPerSec > 0 {
		a.retriableErrLimit = ratelimit.New(*a.metadata.MaxRetriableErrorsPerSec)
	} else {
		a.retriableErrLimit = ratelimit.NewUnlimited()
	}

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

func (a *AzureServiceBusQueues) parseMetadata(metadata bindings.Metadata) (*serviceBusQueuesMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var m serviceBusQueuesMetadata
	err = json.Unmarshal(b, &m)
	if err != nil {
		return nil, err
	}

	if m.ConnectionString != "" && m.NamespaceName != "" {
		return nil, errors.New("connectionString and namespaceName are mutually exclusive")
	}

	ttl, ok, err := contrib_metadata.TryGetTTL(metadata.Properties)
	if err != nil {
		return nil, err
	}
	if !ok {
		// set the same default message time to live as suggested in Azure Portal to 14 days (otherwise it will be 10675199 days)
		ttl = azureServiceBusDefaultMessageTimeToLive
	}
	m.ttl = ttl

	// Queue names are case-insensitive and are forced to lowercase. This mimics the Azure portal's behavior.
	m.QueueName = strings.ToLower(m.QueueName)

	if m.TimeoutInSec < 1 {
		m.TimeoutInSec = defaultTimeoutInSec
	}

	if m.MinConnectionRecoveryInSec < 1 {
		m.MinConnectionRecoveryInSec = defaultMinConnectionRecoveryInSec
	}

	if m.MaxConnectionRecoveryInSec < 1 {
		m.MaxConnectionRecoveryInSec = defaultMaxConnectionRecoveryInSec
	}

	if m.MinConnectionRecoveryInSec > m.MaxConnectionRecoveryInSec {
		return nil, errors.New("maxConnectionRecoveryInSec must be greater than minConnectionRecoveryInSec")
	}

	if m.MaxRetriableErrorsPerSec == nil {
		m.MaxRetriableErrorsPerSec = to.Ptr(defaultMaxRetriableErrorsPerSec)
	}
	if *m.MaxRetriableErrorsPerSec < 0 {
		return nil, errors.New("maxRetriableErrorsPerSec must be non-negative")
	}

	return &m, nil
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
	// Reconnection backoff policy
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = 0
	bo.InitialInterval = time.Duration(a.metadata.MinConnectionRecoveryInSec) * time.Second
	bo.MaxInterval = time.Duration(a.metadata.MaxConnectionRecoveryInSec) * time.Second

	for {
		receiver, _ := a.attemptConnectionForever(a.ctx)
		if receiver == nil {
			a.logger.Errorf("Failed to connect to Azure Service Bus Queue.")
			continue
		}

		// Receive messages loop
		// This continues until the context is canceled
		for a.ctx.Err() == nil {
			// Blocks until the connection is closed or the context is canceled
			msgs, err := receiver.ReceiveMessages(a.ctx, 1, nil)
			if err != nil {
				if err != context.Canceled {
					a.logger.Warnf("Error reading from Azure Service Bus Queue binding: %s", err.Error())
				}
				// Exit from the receive loop to force a reconnection
				break
			}

			// If we got a message, reset the reconnection backoff
			bo.Reset()

			l := len(msgs)
			if l == 0 {
				// We got no message, which is unusual too
				a.logger.Warn("Received 0 messages from Service Bus")
				continue
			} else if l > 1 {
				// We are requesting one message only; this should never happen
				a.logger.Errorf("Expected one message from Service Bus, but received %d", l)
			}

			msg := msgs[0]

			body, err := msg.Body()
			if err != nil {
				a.logger.Warnf("Error reading message body: %s", err.Error())
				a.abandonMessage(receiver, msg)
				continue
			}

			metadata := make(map[string]string)
			metadata[id] = msg.MessageID
			if msg.CorrelationID != nil {
				metadata[correlationID] = *msg.CorrelationID
			}
			if msg.Subject != nil {
				metadata[label] = *msg.Subject
			}

			_, err = handler(a.ctx, &bindings.ReadResponse{
				Data:     body,
				Metadata: metadata,
			})
			if err != nil {
				a.abandonMessage(receiver, msg)
				continue
			}

			// Use a background context in case a.ctx has been canceled already
			ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
			err = receiver.CompleteMessage(ctx, msg, nil)
			cancel()
			if err != nil {
				a.logger.Warnf("Error completing message: %s", err.Error())
				continue
			}
		}

		// Disconnect (gracefully) before attempting to re-connect (unless we're shutting down)
		// Use a background context here because a.ctx may be canceled already at this stage
		ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
		if err := receiver.Close(ctx); err != nil {
			// Log only
			a.logger.Warnf("Error closing receiver of Azure Service Bus Queue binding: %s", err.Error())
		}
		cancel()

		// Reconnect until context is canceled
		if a.ctx.Err() != nil {
			a.logger.Debug("Context canceled; will not reconnect")
			break
		}

		wait := bo.NextBackOff()
		a.logger.Warnf("Subscription to queue %s lost connection, attempting to reconnect in %s...", a.metadata.QueueName, wait)
		time.Sleep(wait)
	}
	return nil
}

func (a *AzureServiceBusQueues) abandonMessage(receiver *servicebus.Receiver, msg *servicebus.ReceivedMessage) {
	// Use a background context in case a.ctx has been canceled already
	ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
	err := receiver.AbandonMessage(ctx, msg, nil)
	cancel()
	if err != nil {
		// Log only
		a.logger.Warnf("Error abandoning message: %s", err.Error())
	}

	// If we're here, it means we got a retriable error, so we need to consume a retriable error token before this (synchronous) method returns
	// If there have been too many retriable errors per second, this method slows the consuming down
	a.logger.Debugf("Taking a retriable error token")
	before := time.Now()
	_ = a.retriableErrLimit.Take()
	a.logger.Debugf("Resumed after pausing for %v", time.Now().Sub(before))
}

// Attempts to connect to a Service Bus queue and blocks until it succeeds; it can retry forever (until the context is canceled)
func (a *AzureServiceBusQueues) attemptConnectionForever(ctx context.Context) (receiver *servicebus.Receiver, err error) {
	// Connections need to retry forever with a maximum backoff of 5 minutes and exponential scaling.
	config := retry.DefaultConfig()
	config.Policy = retry.PolicyExponential
	config.MaxInterval = 5 * time.Minute
	config.MaxElapsedTime = 0
	backoff := config.NewBackOffWithContext(ctx)

	err = retry.NotifyRecover(
		func() error {
			clientAttempt, err := a.client.NewReceiverForQueue(a.metadata.QueueName, nil)
			if err != nil {
				return err
			}
			receiver = clientAttempt
			return nil
		},
		backoff,
		func(err error, d time.Duration) {
			a.logger.Debugf("Failed to connect to Azure Service Bus Queue Binding with error: %s", err.Error())
		},
		func() {
			a.logger.Debug("Successfully reconnected to Azure Service Bus.")
			backoff.Reset()
		},
	)
	return receiver, err
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
