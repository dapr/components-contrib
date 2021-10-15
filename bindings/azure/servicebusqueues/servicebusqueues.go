// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package servicebusqueues

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	servicebus "github.com/Azure/azure-service-bus-go"

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
	metadata *serviceBusQueuesMetadata
	ns       *servicebus.Namespace
	queue    *servicebus.QueueEntity
	readLock *sync.RWMutex
	shutdown bool
	logger   logger.Logger
	ctx      context.Context
	cancel   context.CancelFunc
}

type serviceBusQueuesMetadata struct {
	ConnectionString string `json:"connectionString"`
	QueueName        string `json:"queueName"`
	ttl              time.Duration
}

// NewAzureServiceBusQueues returns a new AzureServiceBusQueues instance.
func NewAzureServiceBusQueues(logger logger.Logger) *AzureServiceBusQueues {
	return &AzureServiceBusQueues{readLock: &sync.RWMutex{}, logger: logger}
}

// Init parses connection properties and creates a new Service Bus Queue client.
func (a *AzureServiceBusQueues) Init(metadata bindings.Metadata) error {
	meta, err := a.parseMetadata(metadata)
	if err != nil {
		return err
	}
	userAgent := "dapr-" + logger.DaprVersion
	a.metadata = meta

	ns, err := servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(a.metadata.ConnectionString),
		servicebus.NamespaceWithUserAgent(userAgent))
	if err != nil {
		return err
	}
	a.ns = ns

	qm := ns.NewQueueManager()

	ctx := context.Background()

	queues, err := qm.List(ctx)
	if err != nil {
		return err
	}

	var entity *servicebus.QueueEntity
	for _, q := range queues {
		if q.Name == a.metadata.QueueName {
			entity = q

			break
		}
	}

	// Create queue if it does not exist
	if entity == nil {
		var ttl time.Duration
		var ok bool
		ttl, ok, err = contrib_metadata.TryGetTTL(metadata.Properties)
		if err != nil {
			return err
		}

		if !ok {
			ttl = a.metadata.ttl
		}

		entity, err = qm.Put(ctx, a.metadata.QueueName, servicebus.QueueEntityWithMessageTimeToLive(&ttl))
		if err != nil {
			return err
		}
	}
	a.queue = entity

	a.setShutdown(false)

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

	client, err := a.ns.NewQueue(a.queue.Name)
	if err != nil {
		return nil, err
	}
	defer client.Close(context.Background())

	msg := servicebus.NewMessage(req.Data)
	if val, ok := req.Metadata[id]; ok && val != "" {
		msg.ID = val
	}
	if val, ok := req.Metadata[correlationID]; ok && val != "" {
		msg.CorrelationID = val
	}

	ttl, ok, err := contrib_metadata.TryGetTTL(req.Metadata)
	if err != nil {
		return nil, err
	}

	if ok {
		msg.TTL = &ttl
	}

	return nil, client.Send(ctx, msg)
}

func (a *AzureServiceBusQueues) Read(handler func(*bindings.ReadResponse) ([]byte, error)) error {
	var sbHandler servicebus.HandlerFunc = func(ctx context.Context, msg *servicebus.Message) error {
		_, err := handler(&bindings.ReadResponse{
			Data:     msg.Data,
			Metadata: map[string]string{id: msg.ID, correlationID: msg.CorrelationID, label: msg.Label},
		})
		if err == nil {
			return msg.Complete(ctx)
		}

		return msg.Abandon(ctx)
	}

	reconnCtx, reconnCancel := context.WithCancel(context.Background())
	defer reconnCancel()

	// Simple retry for error handling (like a malformed message).
	defaultConfig := retry.DefaultConfig()
	simpleBackoff := defaultConfig.NewBackOffWithContext(reconnCtx)

	// Connections need to retry forever with a maximum backoff of 5 minutes and exponential scaling.
	connConfig := retry.DefaultConfig()
	connConfig.Policy = retry.PolicyExponential
	connConfig.MaxInterval, _ = time.ParseDuration("5m")
	connBackoff := connConfig.NewBackOffWithContext(reconnCtx)

	return retry.NotifyRecover(func() error {
		for !a.getShutdown() {
			var client *servicebus.Queue
			retry.NotifyRecover(func() error {
				clientAttempt, err := a.ns.NewQueue(a.queue.Name)
				if err != nil {
					return err
				}
				client = clientAttempt
				return nil
			}, connBackoff,
				func(err error, d time.Duration) {
					a.logger.Debugf("Failed to connect to Azure Service Bus Queue Binding with error: %s", err.Error())
				},
				func() {
					a.logger.Debug("Successfully reconnected to Azure Service Bus.")
					connBackoff.Reset()
				})

			if client == nil {
				a.logger.Errorf("Failed to connect to Azure Service Bus Queue.")
			}
			defer client.Close(context.Background())

			if err := client.Receive(a.ctx, sbHandler); err != nil {
				a.logger.Warnf("Error reading from Azure Service Bus Queue binding: %s", err.Error())
			}
		}
		return nil
	}, simpleBackoff,
		func(err error, d time.Duration) {
			a.logger.Debugf("Failed to read from Azure Service Bus Queue Binding with error: %s", err.Error())
		},
		func() { /* Reading from the queue never returns unless an error is encountered. */ })
}

func (a *AzureServiceBusQueues) Close() error {
	defer a.cancel()
	a.logger.Info("Shutdown called!")
	a.setShutdown(true)
	return nil
}

func (a *AzureServiceBusQueues) setShutdown(val bool) {
	a.readLock.Lock()
	defer a.readLock.Unlock()
	a.shutdown = val
}

func (a *AzureServiceBusQueues) getShutdown() bool {
	a.readLock.RLock()
	defer a.readLock.RUnlock()
	return a.shutdown
}
