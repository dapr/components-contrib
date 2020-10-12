package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	gcppubsub "cloud.google.com/go/pubsub"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/logger"
	"google.golang.org/api/option"
)

const (
	errorMessagePrefix = "gcp pubsub error:"
	consumerID         = "consumerID"
)

// GCPPubSub type
type GCPPubSub struct {
	client   *gcppubsub.Client
	metadata *metadata
	logger   logger.Logger
}

// NewGCPPubSub returns a new GCPPubSub instance
func NewGCPPubSub(logger logger.Logger) pubsub.PubSub {
	return &GCPPubSub{logger: logger}
}

// Init parses metadata and creates a new Pub Sub client
func (g *GCPPubSub) Init(meta pubsub.Metadata) error {
	b, err := g.parseMetadata(meta)
	if err != nil {
		return err
	}

	var pubsubMeta metadata
	err = json.Unmarshal(b, &pubsubMeta)
	if err != nil {
		return err
	}
	clientOptions := option.WithCredentialsJSON(b)
	ctx := context.Background()
	pubsubClient, err := gcppubsub.NewClient(ctx, pubsubMeta.ProjectID, clientOptions)
	if err != nil {
		return fmt.Errorf("%s error creating pubsub client: %s", errorMessagePrefix, err)
	}

	if val, ok := meta.Properties[consumerID]; ok && val != "" {
		pubsubMeta.ConsumerID = val
	} else {
		return fmt.Errorf("%s missing consumerID", errorMessagePrefix)
	}

	g.client = pubsubClient
	g.metadata = &pubsubMeta

	return nil
}

func (g *GCPPubSub) parseMetadata(metadata pubsub.Metadata) ([]byte, error) {
	b, err := json.Marshal(metadata.Properties)

	return b, err
}

// Publish the topic to GCP Pubsub
func (g *GCPPubSub) Publish(req *pubsub.PublishRequest) error {
	if !g.metadata.DisableEntityManagement {
		err := g.ensureTopic(req.Topic)
		if err != nil {
			return fmt.Errorf("%s could not get valid topic %s, %s", errorMessagePrefix, req.Topic, err)
		}
	}

	ctx := context.Background()
	topic := g.getTopic(req.Topic)

	_, err := topic.Publish(ctx, &gcppubsub.Message{
		Data: req.Data,
	}).Get((ctx))

	return err
}

// Subscribe to the GCP Pubsub topic
func (g *GCPPubSub) Subscribe(req pubsub.SubscribeRequest, daprHandler func(msg *pubsub.NewMessage) error) error {
	if !g.metadata.DisableEntityManagement {
		topicErr := g.ensureTopic(req.Topic)
		if topicErr != nil {
			return fmt.Errorf("%s could not get valid topic %s, %s", errorMessagePrefix, req.Topic, topicErr)
		}

		subError := g.ensureSubscription(g.metadata.ConsumerID, req.Topic)
		if subError != nil {
			return fmt.Errorf("%s could not get valid subscription %s, %s", errorMessagePrefix, g.metadata.ConsumerID, subError)
		}
	}

	topic := g.getTopic(req.Topic)
	sub := g.getSubscription(g.metadata.ConsumerID)

	go g.handleSubscriptionMessages(topic, sub, daprHandler)

	return nil
}

func (g *GCPPubSub) handleSubscriptionMessages(topic *gcppubsub.Topic, sub *gcppubsub.Subscription, daprHandler func(msg *pubsub.NewMessage) error) error {
	err := sub.Receive(context.Background(), func(ctx context.Context, m *gcppubsub.Message) {
		msg := &pubsub.NewMessage{
			Data:  m.Data,
			Topic: topic.ID(),
		}

		err := daprHandler(msg)

		if err == nil {
			m.Ack()
		}
	})

	return err
}

func (g *GCPPubSub) ensureTopic(topic string) error {
	entity := g.getTopic(topic)
	exists, err := entity.Exists(context.Background())
	if !exists {
		_, err = g.client.CreateTopic(context.Background(), topic)
	}

	return err
}

func (g *GCPPubSub) getTopic(topic string) *gcppubsub.Topic {
	return g.client.Topic(topic)
}

func (g *GCPPubSub) ensureSubscription(subscription string, topic string) error {
	err := g.ensureTopic(topic)
	if err != nil {
		return err
	}

	entity := g.getSubscription(subscription)
	exists, subErr := entity.Exists(context.Background())
	if !exists {
		_, subErr = g.client.CreateSubscription(context.Background(), g.metadata.ConsumerID,
			gcppubsub.SubscriptionConfig{Topic: g.getTopic(topic)})
	}

	return subErr
}

func (g *GCPPubSub) getSubscription(subscription string) *gcppubsub.Subscription {
	return g.client.Subscription(subscription)
}

func (g *GCPPubSub) Close() error {
	return g.client.Close()
}
