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

	log "github.com/Sirupsen/logrus"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/Azure/azure-service-bus-go"
	"github.com/lithammer/shortuuid"
)

const (
	connString = "connectionString"
	consumerID = "consumerID"
	maxDeliveryCount = 5
	timeoutInSec = 60
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
	ctx, _ = context.WithTimeout(context.Background(), time.Second * timeoutInSec)

	err = sender.Send(ctx, servicebus.NewMessage(req.Data))
	if err != nil {
		return err
	}
	return nil
}

func (a *azureServiceBus) getHandlerFunc(topic string, handler func(msg *pubsub.NewMessage) error) func (ctx context.Context, message *servicebus.Message) error {
	return func (ctx context.Context, message *servicebus.Message) error {
		// TODO: are there any conditions where we should return an error?
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

	ctx := context.Background()
	go a.handleSubscriptionMessages(ctx, req.Topic, sub, sbHandlerFunc)

	return nil
}

func (a *azureServiceBus) handleSubscriptionMessages(ctx context.Context, topic string, sub subscription, handlerFunc servicebus.HandlerFunc) {
	for {
		if err := sub.Receive(ctx, handlerFunc); err != nil {
			log.Errorf("service bus error: error receiving from topic %s: %s", topic, err)
			return
		}
	}
}

func (a *azureServiceBus) ensureTopic(topic string) error {
	getCtx, getCancel := context.WithTimeout(context.Background(), time.Second * timeoutInSec)
	defer getCancel()

	if a.topicManager == nil {
		return fmt.Errorf("service bus error: init() has not been called")
	}
	topicEntity, err := a.topicManager.Get(getCtx, topic)
	if err != nil && !servicebus.IsErrNotFound(err) {
		return fmt.Errorf("service bus error: could not get topic %s", topic)
	}

	if topicEntity == nil {
		putCtx, putCancel := context.WithTimeout(context.Background(), time.Second * timeoutInSec)
		defer putCancel()
		topicEntity, err = a.topicManager.Put(putCtx, topic)
		if err != nil {
			return fmt.Errorf("service bus error: could not put topic %s", topic)
		}
	}
	return nil
}

func (a *azureServiceBus) ensureSubscription(name string, topic string) error {
	a.ensureTopic(topic) // TODO: should we create the topic if it doesn't exist?!

	subscriptionManager, err := a.namespace.NewSubscriptionManager(topic)
	if err != nil {
		return err
	}
	getCtx, getCancel := context.WithTimeout(context.Background(), time.Second * timeoutInSec)
	defer getCancel()
	subEntity, err := subscriptionManager.Get(getCtx, name)
	if err != nil && !servicebus.IsErrNotFound(err) {
		return fmt.Errorf("service bus error: could not get subscription %s", name)
	}

	if subEntity == nil {
		putCtx, putCancel := context.WithTimeout(context.Background(), time.Second * timeoutInSec)
		defer putCancel()
		subEntity, err = subscriptionManager.Put(putCtx, name)
		if err != nil {
			return fmt.Errorf("service bus error: could not put subscription %s", name)
		}
	}
	return nil
}