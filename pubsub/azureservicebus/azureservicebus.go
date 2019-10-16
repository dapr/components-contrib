// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package azureservicebus

import (
	"errors"
	"fmt"
	"time"
	"context"

	log "github.com/Sirupsen/logrus"

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

func (s *azureServiceBus) Init(metadata pubsub.Metadata) error {
	m, err := parseAzureServiceBusMetadata(metadata)
	if err != nil {
		return err
	}

	s.metadata = m 
	s.namespace, err = servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(s.metadata.connectionString))
	if err != nil {
		return err
	}

	s.topicManager = s.namespace.NewTopicManager()
	return nil
}

func (s *azureServiceBus) Publish(req *pubsub.PublishRequest) error {
	// Ensure topic exists
	s.makeTopic(req.Topic)

	// Instantiate topic client
	sender, err := s.namespace.NewTopic(req.Topic, nil)

	// Send message to topic
	err = sender.Send(context.TODO(), servicebus.NewMessage(req.Data))
	if err != nil {
		return err
	}
	return nil
}

func (s *azureServiceBus) Subscribe(req pubsub.SubscribeRequest, handler func(msg *pubsub.NewMessage) error) error {
	// Ensure subscription exists
	subID := s.metadata.subscriberID
	s.makeSubscription(subID, req.Topic)

	// Instantiate topic client
	topic, err := s.namespace.NewTopic(req.Topic)
	if err != nil {
		return fmt.Errorf("service bus error: could not Instantiate topic %s", req.Topic)
	}
	sub, err := topic.NewSubscription(subID, nil)
	if err != nil {
		return fmt.Errorf("service bus error: could not Instantiate subscription %s for topic %s", subID, req.Topic)
	}

	// Wrapper handler for service bus messages
	servicebusHandlerFunc := servicebus.HandlerFunc(func (ctx context.Context, message *servicebus.Message) error {
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

	// Handle each subscriptions messages using an isolated go routine
	go s.handleSubscriptionMessages(sub, servicebusHandlerFunc)

	return nil
}

func (s *azureServiceBus) handleSubscriptionMessages(sub *servicebus.Subscription, handlerFunc servicebus.HandlerFunc) {
	for {
		sub.Receive(context.TODO(), handlerFunc)
	}
}

func (s *azureServiceBus) makeTopic(topic string) error {
	// Try to get the topic first...
	topicEntity, err := s.topicManager.Get(context.TODO(), topic)
	if err != nil && !servicebus.IsErrNotFound(err) {
		return fmt.Errorf("service bus error: could not get topic %s", topic)
	}
	// If topic doesn't exist, create it
	if topicEntity == nil {
		topicEntity, err = s.topicManager.Put(context.TODO(), topic, nil)
		if err != nil {
			return fmt.Errorf("service bus error: could not put topic %s", topic)
		}
	}
	return nil
}

func (s *azureServiceBus) makeSubscription(name string, topic string) error {
	subscriptionManager, err := s.namespace.NewSubscriptionManager(topic)
	// Try to get the subscription first...
	subEntity, err := subscriptionManager.Get(context.TODO(), name)
	if err != nil && !servicebus.IsErrNotFound(err) {
		return fmt.Errorf("service bus error: could not get subscription %s", name)
	}
	// If subscription doesn't exist, create it
	if subEntity == nil {
		subEntity, err = subscriptionManager.Put(context.TODO(), name, nil)
		if err != nil {
			return fmt.Errorf("service bus error: could not put subscription %s", name)
		}
	}
	return nil
}