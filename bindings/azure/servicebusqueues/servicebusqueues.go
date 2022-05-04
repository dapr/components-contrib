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
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	servicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	sbadmin "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"github.com/cenkalti/backoff/v4"

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

	// AzureServiceBusDefaultMessageTimeToLive defines the default time to live for queues, which is 14 days. The same way Azure Portal does.
	AzureServiceBusDefaultMessageTimeToLive = time.Hour * 24 * 14
)

// AzureServiceBusQueues is an input/output binding reading from and sending events to Azure Service Bus queues.
type AzureServiceBusQueues struct {
	metadata       *serviceBusQueuesMetadata
	client         *servicebus.Client
	adminClient    *sbadmin.Client
	shutdownSignal int32
	logger         logger.Logger
	ctx            context.Context
	cancel         context.CancelFunc
}

type serviceBusQueuesMetadata struct {
	ConnectionString string `json:"connectionString"`
	NamespaceName    string `json:"namespaceName,omitempty"`
	QueueName        string `json:"queueName"`
	ttl              time.Duration
}

// NewAzureServiceBusQueues returns a new AzureServiceBusQueues instance.
func NewAzureServiceBusQueues(logger logger.Logger) *AzureServiceBusQueues {
	return &AzureServiceBusQueues{logger: logger}
}

// Init parses connection properties and creates a new Service Bus Queue client.
func (a *AzureServiceBusQueues) Init(metadata bindings.Metadata) (err error) {
	a.metadata, err = a.parseMetadata(metadata)
	if err != nil {
		return err
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	a.clearShutdown()

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

	// set the same default message time to live as suggested in Azure Portal to 14 days (otherwise it will be 10675199 days)
	if !ok {
		ttl = AzureServiceBusDefaultMessageTimeToLive
	}

	m.ttl = ttl

	// Queue names are case-insensitive and are forced to lowercase. This mimics the Azure portal's behavior.
	m.QueueName = strings.ToLower(m.QueueName)

	return &m, nil
}

func (a *AzureServiceBusQueues) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (a *AzureServiceBusQueues) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	sender, err := a.client.NewSender(a.metadata.QueueName, nil)
	if err != nil {
		return nil, err
	}
	defer sender.Close(ctx)

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

func (a *AzureServiceBusQueues) Read(handler func(context.Context, *bindings.ReadResponse) ([]byte, error)) error {
	// Connections need to retry forever with a maximum backoff of 5 minutes and exponential scaling.
	connConfig := retry.DefaultConfig()
	connConfig.Policy = retry.PolicyExponential
	connConfig.MaxInterval = 5 * time.Minute
	connBackoff := connConfig.NewBackOffWithContext(a.ctx)

	for !a.isShutdown() {
		receiver := a.attemptConnectionForever(connBackoff)
		if receiver == nil {
			a.logger.Errorf("Failed to connect to Azure Service Bus Queue.")
			continue
		}

		msgs, err := receiver.ReceiveMessages(a.ctx, 10, nil)
		if err != nil {
			a.logger.Warnf("Error reading from Azure Service Bus Queue binding: %s", err.Error())
		}

		// Blocks until the connection is closed
		for _, msg := range msgs {
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

			err = receiver.CompleteMessage(a.ctx, msg, nil)
			if err != nil {
				a.logger.Warnf("Error completing message: %s", err.Error())
				continue
			}
		}

		// Disconnect (gracefully) before attempting to re-connect (unless we're shutting down)
		// Use a background context here because a.ctx may be canceled already at this stage
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := receiver.Close(ctx); err != nil {
			// Log only
			a.logger.Warnf("Error closing receiver of Azure Service Bus Queue binding: %s", err.Error())
		}
		cancel()
	}
	return nil
}

func (a *AzureServiceBusQueues) abandonMessage(receiver *servicebus.Receiver, msg *servicebus.ReceivedMessage) {
	ctx, cancel := context.WithTimeout(a.ctx, 30*time.Second)
	err := receiver.AbandonMessage(ctx, msg, nil)
	if err != nil {
		// Log only
		a.logger.Warnf("Error abandoning message: %s", err.Error())
	}
	cancel()
}

func (a *AzureServiceBusQueues) attemptConnectionForever(backoff backoff.BackOff) *servicebus.Receiver {
	var receiver *servicebus.Receiver
	retry.NotifyRecover(
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
	return receiver
}

func (a *AzureServiceBusQueues) Close() error {
	defer a.cancel()
	a.logger.Info("Shutdown called!")
	a.setShutdown()
	return nil
}

func (a *AzureServiceBusQueues) setShutdown() {
	atomic.CompareAndSwapInt32(&a.shutdownSignal, 0, 1)
}

func (a *AzureServiceBusQueues) clearShutdown() {
	atomic.CompareAndSwapInt32(&a.shutdownSignal, 1, 0)
}

func (a *AzureServiceBusQueues) isShutdown() bool {
	val := atomic.LoadInt32(&a.shutdownSignal)
	return val == 1
}
