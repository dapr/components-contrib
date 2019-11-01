// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package azureservicebus

import (
	"errors"
	"fmt"
	"context"
	"time"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/Azure/azure-service-bus-go"
	"github.com/lithammer/shortuuid"
)

const (
	connString = "connectionString"
	consumerID = "consumerID"
	maxDeliveryCount = 5
)

type azureServiceBus struct {
	metadata metadata
	namespace *servicebus.Namespace
	topicManager *servicebus.TopicManager
}

type subscription interface {
	Close(ctx context.Context) error
	Receive(ctx context.Context, handler servicebus.Handler) error 
}

// NewAzureServiceBus returns a new Azure ServiceBus pub-sub implementation
func NewAzureServiceBus() pubsub.PubSub {
	return &azureServiceBus{}
}

func parseAzureServiceBusMetadata(meta pubsub.Metadata) (metadata, error) {
	m := metadata{}
	if val, ok := meta.Properties[connString]; ok && val != "" {
		m.ConnectionString = val
	} else {
		return m, errors.New("azure serivce bus error: missing connection string")
	}
	if val, ok := meta.Properties[consumerID]; ok && val != "" {
		m.ConsumerID = val
	} else {
		// default
		u := shortuuid.New()
		m.ConsumerID = fmt.Sprintf("dapr-%s", u)
	}

	return m, nil
}

func (a *azureServiceBus) Init(metadata pubsub.Metadata) error {
	m, err := parseAzureServiceBusMetadata(metadata)
	if err != nil {
		return err
	}

	a.metadata = m 
	a.namespace, err = servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(a.metadata.ConnectionString))
	if err != nil {
		return err
	}

	a.topicManager = a.namespace.NewTopicManager()
	return nil
}

func (a *azureServiceBus) Publish(req *pubsub.PublishRequest) error {
	a.ensureTopic(req.Topic)

	sender, err := a.namespace.NewTopic(req.Topic)

	var ctx context.Context
	ctx, _ = context.WithTimeout(context.Background(), time.Second * 60)

	err = sender.Send(ctx, servicebus.NewMessage(req.Data))
	if err != nil {
		return err
	}
	return nil
}

func (a *azureServiceBus) getHandlerFunc(topic string, handler func(msg *pubsub.NewMessage) error) func (ctx context.Context, message *servicebus.Message) error {
	return func (ctx context.Context, message *servicebus.Message) error {
		msg := &pubsub.NewMessage{
			Data: message.Data,
			Topic: topic,
		}
		err := handler(msg)
		if err != nil {
			if message.DeliveryCount >= maxDeliveryCount {
				return message.DeadLetter(ctx, fmt.Errorf(("service bus error: poision message %s"), message.ID))
			}
			return message.Abandon(ctx)
		}
		return message.Complete(ctx)
	}
}

func (a *azureServiceBus) Subscribe(req pubsub.SubscribeRequest, handler func(msg *pubsub.NewMessage) error) error {
	subID := a.metadata.ConsumerID
	a.ensureSubscription(subID, req.Topic)
	topic, err := a.namespace.NewTopic(req.Topic)
	if err != nil {
		return fmt.Errorf("service bus error: could not instantiate topic %s", req.Topic)
	}

	var sub subscription
	sub, err = topic.NewSubscription(subID)
	if err != nil {
		return fmt.Errorf("service bus error: could not instantiate subscription %s for topic %s", subID, req.Topic)
	}

	sbHandlerFunc := servicebus.HandlerFunc(a.getHandlerFunc(req.Topic, handler))

	ctx := context.Background() // infinite context
	go a.handleSubscriptionMessages(ctx, sub, sbHandlerFunc)

	return nil
}

func (a *azureServiceBus) handleSubscriptionMessages(ctx context.Context, sub subscription, handlerFunc servicebus.HandlerFunc) {
	for {
		if err := sub.Receive(ctx, handlerFunc); err != nil {
			// TODO: handle message handling error...
			fmt.Printf("%s", err)
		}
	}
}

func (a *azureServiceBus) ensureTopic(topic string) error {
	var getCtx context.Context
	getCtx, _ = context.WithTimeout(context.Background(), time.Second * 60)
	topicEntity, err := a.topicManager.Get(getCtx, topic)
	if err != nil && !servicebus.IsErrNotFound(err) {
		return fmt.Errorf("service bus error: could not get topic %s", topic)
	}

	if topicEntity == nil {
		var putCtx context.Context
		putCtx, _ = context.WithTimeout(context.Background(), time.Second * 60)
		topicEntity, err = a.topicManager.Put(putCtx, topic)
		if err != nil {
			return fmt.Errorf("service bus error: could not put topic %s", topic)
		}
	}
	return nil
}

func (a *azureServiceBus) ensureSubscription(name string, topic string) error {
	subscriptionManager, err := a.namespace.NewSubscriptionManager(topic)
	if err != nil {
		return err
	}
	var getCtx context.Context
	getCtx, _ = context.WithTimeout(context.Background(), time.Second * 60)
	subEntity, err := subscriptionManager.Get(getCtx, name)
	if err != nil && !servicebus.IsErrNotFound(err) {
		return fmt.Errorf("service bus error: could not get subscription %s", name)
	}

	if subEntity == nil {
		var putCtx context.Context
		putCtx, _ = context.WithTimeout(context.Background(), time.Second * 60)
		subEntity, err = subscriptionManager.Put(putCtx, name)
		if err != nil {
			return fmt.Errorf("service bus error: could not put subscription %s", name)
		}
	}
	return nil
}