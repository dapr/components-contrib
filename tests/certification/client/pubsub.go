package client

import (
	"context"
	"encoding/json"

	pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	"github.com/pkg/errors"
)

// PublishEvent publishes data onto specific pubsub topic.
func (c *GRPCClient) PublishEvent(ctx context.Context, pubsubName, topicName string, data []byte) error {
	if pubsubName == "" {
		return errors.New("pubsubName name required")
	}
	if topicName == "" {
		return errors.New("topic name required")
	}

	envelop := &pb.PublishEventRequest{
		PubsubName: pubsubName,
		Topic:      topicName,
		Data:       data,
	}

	_, err := c.protoClient.PublishEvent(c.withAuthToken(ctx), envelop)
	if err != nil {
		return errors.Wrapf(err, "error publishing event unto %s topic", topicName)
	}

	return nil
}

func (c *GRPCClient) PublishEventfromCustomContent(ctx context.Context, pubsubName, topicName string, data interface{}) error {
	return c.PublishEventFromCustomContent(ctx, pubsubName, topicName, data)
}

// PublishEventfromCustomContent serializes an struct and publishes its contents as data (JSON) onto topic in specific pubsub component.
func (c *GRPCClient) PublishEventFromCustomContent(ctx context.Context, pubsubName, topicName string, data interface{}) error {
	if pubsubName == "" {
		return errors.New("pubsubName name required")
	}
	if topicName == "" {
		return errors.New("topic name required")
	}

	bytes, err := json.Marshal(data)

	if err != nil {
		return errors.WithMessage(err, "error serializing input struct")
	}

	envelop := &pb.PublishEventRequest{
		PubsubName:      pubsubName,
		Topic:           topicName,
		Data:            bytes,
		DataContentType: "application/json",
	}

	_, err = c.protoClient.PublishEvent(c.withAuthToken(ctx), envelop)

	if err != nil {
		return errors.Wrapf(err, "error publishing event unto %s topic", topicName)
	}

	return nil
}
