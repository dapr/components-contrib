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

	servicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	admin "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
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
	adminClient    *admin.Client
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
func (a *AzureServiceBusQueues) Init(metadata bindings.Metadata) error {
	meta, err := a.parseMetadata(metadata)
	if err != nil {
		return err
	}
	userAgent := "dapr-" + logger.DaprVersion
	a.metadata = meta

	var client *servicebus.Client
	var adminClient *admin.Client
	if a.metadata.ConnectionString != "" {
		client, err = servicebus.NewClientFromConnectionString(a.metadata.ConnectionString, &servicebus.ClientOptions{
			ApplicationID: userAgent,
		})

		if err != nil {
			return err
		}

		adminClient, err = admin.NewClientFromConnectionString(a.metadata.ConnectionString, &admin.ClientOptions{})
		if err != nil {
			return err
		}
	} else {
		// Initialization code
		settings, sErr := azauth.NewEnvironmentSettings(azauth.AzureServiceBusResourceName, metadata.Properties)
		if sErr != nil {
			return sErr
		}

		token, tErr := settings.GetTokenCredential()
		if tErr != nil {
			return tErr
		}

		client, err = servicebus.NewClient(a.metadata.NamespaceName, token, &servicebus.ClientOptions{
			ApplicationID: userAgent,
		})

		if err != nil {
			return err
		}

		adminClient, err = admin.NewClient(a.metadata.NamespaceName, token, &admin.ClientOptions{})
		if err != nil {
			return err
		}
	}
	a.client = client
	a.adminClient = adminClient

	ctx := context.Background()

	_, err = adminClient.GetQueue(ctx, a.metadata.QueueName, nil)
	if err != nil {
		var ttl time.Duration
		var ok bool
		ttl, ok, err = contrib_metadata.TryGetTTL(metadata.Properties)
		if err != nil {
			return err
		}

		if !ok {
			ttl = a.metadata.ttl
		}

		properties := &admin.QueueProperties{}
		properties.DefaultMessageTimeToLive = &ttl

		_, err := adminClient.CreateQueue(ctx, a.metadata.QueueName, properties, nil)
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

func (a *AzureServiceBusQueues) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	sender, err := a.client.NewSender(a.metadata.QueueName, nil)
	if err != nil {
		return nil, err
	}
	defer sender.Close(context.Background())

	msg := &servicebus.Message{Body: req.Data}
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

	return nil, sender.SendMessage(ctx, msg)
}

func (a *AzureServiceBusQueues) Read(handler func(*bindings.ReadResponse) ([]byte, error)) error {
	// Connections need to retry forever with a maximum backoff of 5 minutes and exponential scaling.
	connConfig := retry.DefaultConfig()
	connConfig.Policy = retry.PolicyExponential
	connConfig.MaxInterval, _ = time.ParseDuration("5m")
	connBackoff := connConfig.NewBackOffWithContext(a.ctx)

	for !a.isShutdown() {
		receiver := a.attemptConnectionForever(connBackoff)

		if receiver == nil {
			a.logger.Errorf("Failed to connect to Azure Service Bus Queue.")
			continue
		}
		defer receiver.Close(context.Background())

		msgs, err := receiver.ReceiveMessages(a.ctx, 10, nil)
		if err != nil {
			a.logger.Warnf("Error reading from Azure Service Bus Queue binding: %s", err.Error())
		}

		for _, msg := range msgs {
			body, err := msg.Body()
			if err != nil {
				a.logger.Warnf("Error reading message body: %s", err.Error())
				receiver.AbandonMessage(a.ctx, msg, nil)
			}

			_, err = handler(&bindings.ReadResponse{
				Data:     body,
				Metadata: map[string]string{id: msg.MessageID, correlationID: *msg.CorrelationID, label: *msg.Subject},
			})
			if err == nil {
				return receiver.CompleteMessage(a.ctx, msg)
			}

			receiver.AbandonMessage(a.ctx, msg, nil)
		}
	}
	return nil
}

func (a *AzureServiceBusQueues) attemptConnectionForever(backoff backoff.BackOff) *servicebus.Receiver {
	var receiver *servicebus.Receiver
	retry.NotifyRecover(func() error {
		clientAttempt, err := a.client.NewReceiverForQueue(a.metadata.QueueName, nil)
		if err != nil {
			return err
		}
		receiver = clientAttempt
		return nil
	}, backoff,
		func(err error, d time.Duration) {
			a.logger.Debugf("Failed to connect to Azure Service Bus Queue Binding with error: %s", err.Error())
		},
		func() {
			a.logger.Debug("Successfully reconnected to Azure Service Bus.")
			backoff.Reset()
		})
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
