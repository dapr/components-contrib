// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package azureservicebus

import (
	"errors"
	"fmt"
	"context"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/Azure/azure-service-bus-go"
	"github.com/lithammer/shortuuid"
)

const (
	connString = "connectionString"
	subscriberID = "subscriberId"
)

type azureServiceBus struct {
	metadata metadata
	namespace *servicebus.Namespace
	topicManager *servicebus.TopicManager
}

// NewAzureServiceBus returns a new Azure ServiceBus pub-sub implementation
func NewAzureServiceBus() pubsub.PubSub {
	return &azureServiceBus{}
}

func parseAzureServiceBusMetadata(meta pubsub.Metadata) (metadata, error) {
	m := metadata{}
	if val, ok := meta.Properties[connString]; ok && val != "" {
		m.connectionString = val
	} else {
		return m, errors.New("azure serivce bus error: missing connection string")
	}
	if val, ok := meta.Properties[subscriberID]; ok && val != "" {
		m.subscriberID = val
	} else {
		// default
		u := shortuuid.New()
		m.subscriberID = fmt.Sprintf("dapr-%s", u)
	}

	return m, nil
}

func (a *azureServiceBus) Init(metadata pubsub.Metadata) error {
	m, err := parseAzureServiceBusMetadata(metadata)
	if err != nil {
		return err
	}

	a.metadata = m 
	a.namespace, err = servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(a.metadata.connectionString))
	if err != nil {
		return err
	}

	a.topicManager = a.namespace.NewTopicManager()
	return nil
}

func (a *azureServiceBus) Publish(req *pubsub.PublishRequest) error {
	a.ensureTopic(req.Topic)

	sender, err := a.namespace.NewTopic(req.Topic, nil)

	err = sender.Send(context.TODO(), servicebus.NewMessage(req.Data))
	if err != nil {
		return err
	}
	return nil
}

func (a *azureServiceBus) Subscribe(req pubsub.SubscribeRequest, handler func(msg *pubsub.NewMessage) error) error {
	subID := a.metadata.subscriberID
	a.ensureSubscription(subID, req.Topic)
	topic, err := a.namespace.NewTopic(req.Topic)
	if err != nil {
		return fmt.Errorf("service bus error: could not Instantiate topic %s", req.Topic)
	}

	sub, err := topic.NewSubscription(subID, nil)
	if err != nil {
		return fmt.Errorf("service bus error: could not Instantiate subscription %s for topic %s", subID, req.Topic)
	}

	sbHandlerFunc := servicebus.HandlerFunc(func (ctx context.Context, message *servicebus.Message) error {
		msg := &pubsub.NewMessage{
			Data: message.Data,
			Topic: req.Topic,
		}
		err := handler(msg)
		if err != nil {
			message.Abandon(context.TODO())
			return fmt.Errorf("service bus error: could not handle message from topic %s", msg.Topic)
		}
		message.Complete(context.TODO())
		return nil
	})

	go a.handleSubscriptionMessages(sub, sbHandlerFunc)

	return nil
}

func (a *azureServiceBus) handleSubscriptionMessages(sub *servicebus.Subscription, handlerFunc servicebus.HandlerFunc) {
	defer sub.Close(context.TODO())
	for {
		sub.Receive(context.TODO(), handlerFunc)
	}
}

func (a *azureServiceBus) ensureTopic(topic string) error {
	topicEntity, err := a.topicManager.Get(context.TODO(), topic)
	if err != nil && !servicebus.IsErrNotFound(err) {
		return fmt.Errorf("service bus error: could not get topic %s", topic)
	}
	if topicEntity == nil {
		topicEntity, err = a.topicManager.Put(context.TODO(), topic, nil)
		if err != nil {
			return fmt.Errorf("service bus error: could not put topic %s", topic)
		}
	}
	return nil
}

func (a *azureServiceBus) ensureSubscription(name string, topic string) error {
	subscriptionManager, err := a.namespace.NewSubscriptionManager(topic)
	subEntity, err := subscriptionManager.Get(context.TODO(), name)
	if err != nil && !servicebus.IsErrNotFound(err) {
		return fmt.Errorf("service bus error: could not get subscription %s", name)
	}

	if subEntity == nil {
		subEntity, err = subscriptionManager.Put(context.TODO(), name, nil)
		if err != nil {
			return fmt.Errorf("service bus error: could not put subscription %s", name)
		}
	}
	return nil
}