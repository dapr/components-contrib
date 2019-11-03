// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package azureservicebus

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	servicebus "github.com/Azure/azure-service-bus-go"
	log "github.com/Sirupsen/logrus"
	"github.com/dapr/components-contrib/pubsub"
)

const (
	connStringKey       = "connectionString"
	consumerIDKey       = "consumerID"
	maxDeliveryCountKey = "maxDeliveryCount"
	timeoutInSecKey     = "timeoutInSec"

	defaultMaxDeliveryCount = 10
	defaultTimeoutInSec     = 60
)

type azureServiceBus struct {
	metadata     metadata
	namespace    *servicebus.Namespace
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
	if val, ok := meta.Properties[connStringKey]; ok && val != "" {
		m.ConnectionString = val
	} else {
		return m, errors.New("azure serivce bus error: missing connection string")
	}

	if val, ok := meta.Properties[consumerIDKey]; ok && val != "" {
		m.ConsumerID = val
	} else {
		return m, errors.New("azure service bus error: missing consumerID")
	}

	useDefault := true
	if val, ok := meta.Properties[maxDeliveryCountKey]; ok && val != "" {
		var err error
		m.MaxDeliveryCount, err = strconv.Atoi(val)
		if err == nil {
			useDefault = false
		}
	}
	if useDefault {
		m.MaxDeliveryCount = defaultMaxDeliveryCount
	}

	useDefault = true
	if val, ok := meta.Properties[timeoutInSecKey]; ok && val != "" {
		var err error
		m.TimeoutInSec, err = strconv.Atoi(val)
		if err == nil {
			useDefault = false
		}
	}
	if useDefault {
		m.TimeoutInSec = defaultTimeoutInSec
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
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(a.metadata.TimeoutInSec))
	defer cancel()

	err = sender.Send(ctx, servicebus.NewMessage(req.Data))
	if err != nil {
		return err
	}
	return nil
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

func (a *azureServiceBus) getHandlerFunc(topic string, handler func(msg *pubsub.NewMessage) error) func(ctx context.Context, message *servicebus.Message) error {
	return func(ctx context.Context, message *servicebus.Message) error {
		// TODO: are there any conditions where we should return an error?
		msg := &pubsub.NewMessage{
			Data:  message.Data,
			Topic: topic,
		}
		err := handler(msg)
		if err != nil {
			return message.Abandon(ctx)
		}
		return message.Complete(ctx)
	}
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
	entity, err := a.getTopicEntity(topic)
	if err != nil {
		return err
	}

	if entity == nil {
		err = a.createTopicEntity(topic)
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *azureServiceBus) ensureSubscription(name string, topic string) error {
	err := a.ensureTopic(topic) // TODO: should we create the topic if it doesn't exist?!
	if err != nil {
		return err
	}

	subManager, err := a.namespace.NewSubscriptionManager(topic)
	if err != nil {
		return err
	}

	entity, err := a.getSubscriptionEntity(subManager, topic, name)
	if err != nil {
		return err
	}

	if entity == nil {
		err = a.createSubscriptionEntity(subManager, topic, name)
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *azureServiceBus) getTopicEntity(topic string) (*servicebus.TopicEntity, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(a.metadata.TimeoutInSec))
	defer cancel()

	if a.topicManager == nil {
		return nil, fmt.Errorf("service bus error: init() has not been called")
	}
	topicEntity, err := a.topicManager.Get(ctx, topic)
	if err != nil && !servicebus.IsErrNotFound(err) {
		return nil, fmt.Errorf("service bus error: could not get topic %s", topic)
	}
	return topicEntity, nil
}

func (a *azureServiceBus) createTopicEntity(topic string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(a.metadata.TimeoutInSec))
	defer cancel()
	_, err := a.topicManager.Put(ctx, topic)
	if err != nil {
		return fmt.Errorf("service bus error: could not put topic %s", topic)
	}
	return nil
}

func (a *azureServiceBus) getSubscriptionEntity(mgr *servicebus.SubscriptionManager, topic, subscription string) (*servicebus.SubscriptionEntity, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(a.metadata.TimeoutInSec))
	defer cancel()
	entity, err := mgr.Get(ctx, subscription)
	if err != nil && !servicebus.IsErrNotFound(err) {
		return nil, fmt.Errorf("service bus error: could not get subscription %s", subscription)
	}
	return entity, nil
}

func (a *azureServiceBus) createSubscriptionEntity(mgr *servicebus.SubscriptionManager, topic, subscription string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(a.metadata.TimeoutInSec))
	defer cancel()
	_, err := mgr.Put(ctx, subscription, subscriptionManagementOptionsWithMaxDeliveryCount(a.metadata.MaxDeliveryCount))
	if err != nil {
		return fmt.Errorf("service bus error: could not put subscription %s", subscription)
	}
	return nil
}

func subscriptionManagementOptionsWithMaxDeliveryCount(maxDeliveryCount int) servicebus.SubscriptionManagementOption {
	return func(d *servicebus.SubscriptionDescription) error {
		mdc := int32(maxDeliveryCount)
		d.MaxDeliveryCount = &mdc
		return nil
	}
}
