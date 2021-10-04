// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package servicebusqueues

import (
	"context"
	"encoding/json"
	"time"

	servicebus "github.com/Azure/azure-service-bus-go"

	"github.com/dapr/components-contrib/bindings"
	contrib_metadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
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
	client   *servicebus.Queue

	logger logger.Logger
}

type serviceBusQueuesMetadata struct {
	ConnectionString string `json:"connectionString"`
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

	ns, err := servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(a.metadata.ConnectionString),
		servicebus.NamespaceWithUserAgent(userAgent))
	if err != nil {
		return err
	}

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

	client, err := ns.NewQueue(entity.Name)
	if err != nil {
		return err
	}
	a.client = client

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

	return &m, nil
}

func (a *AzureServiceBusQueues) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (a *AzureServiceBusQueues) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

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

	return nil, a.client.Send(ctx, msg)
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

	if err := a.client.Receive(context.Background(), sbHandler); err != nil {
		return err
	}

	return nil
}

func (a *AzureServiceBusQueues) Close() error {
	return a.client.Close(context.Background())
}
