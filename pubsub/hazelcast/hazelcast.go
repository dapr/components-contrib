package hazelcast

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/hazelcast/hazelcast-go-client"
	hazelcastCore "github.com/hazelcast/hazelcast-go-client/core"
)

const (
	hazelcastServers = "hazelcastServers"
)

type Hazelcast struct {
	client hazelcast.Client
	logger logger.Logger
}

// NewHazelcastPubSub returns a new hazelcast pub-sub implementation
func NewHazelcastPubSub(logger logger.Logger) pubsub.PubSub {
	return &Hazelcast{logger: logger}
}

func parseHazelcastMetadata(meta pubsub.Metadata) (metadata, error) {
	m := metadata{}
	if val, ok := meta.Properties[hazelcastServers]; ok && val != "" {
		m.hazelcastServers = val
	} else {
		return m, errors.New("hazelcast error: missing hazelcast servers")
	}

	return m, nil
}

func (p *Hazelcast) Init(metadata pubsub.Metadata) error {
	m, err := parseHazelcastMetadata(metadata)
	if err != nil {
		return err
	}

	hzConfig := hazelcast.NewConfig()

	servers := m.hazelcastServers
	hzConfig.NetworkConfig().AddAddress(strings.Split(servers, ",")...)

	p.client, err = hazelcast.NewClientWithConfig(hzConfig)
	if err != nil {
		return fmt.Errorf("hazelcast error: failed to create new client, %v", err)
	}

	return nil
}

func (p *Hazelcast) Publish(req *pubsub.PublishRequest) error {
	topic, err := p.client.GetTopic(req.Topic)
	if err != nil {
		return fmt.Errorf("hazelcast error: failed to get topic for %s", req.Topic)
	}

	if err = topic.Publish(req.Data); err != nil {
		return fmt.Errorf("hazelcast error: failed to publish data, %v", err)
	}

	return nil
}

func (p *Hazelcast) Subscribe(req pubsub.SubscribeRequest, handler func(msg *pubsub.NewMessage) error) error {
	topic, err := p.client.GetTopic(req.Topic)
	if err != nil {
		return fmt.Errorf("hazelcast error: failed to get topic for %s", req.Topic)
	}

	_, err = topic.AddMessageListener(&hazelcastMessageListener{topic.Name(), handler})
	if err != nil {
		return fmt.Errorf("hazelcast error: failed to add new listener, %v", err)
	}

	return nil
}

type hazelcastMessageListener struct {
	topicName     string
	pubsubHandler func(msg *pubsub.NewMessage) error
}

func (l *hazelcastMessageListener) OnMessage(message hazelcastCore.Message) error {
	msg, ok := message.MessageObject().([]byte)
	if !ok {
		return errors.New("hazelcast error: cannot cast message to byte array")
	}

	return l.handleMessageObject(msg)
}

func (l *hazelcastMessageListener) handleMessageObject(message []byte) error {
	pubsubMsg := &pubsub.NewMessage{
		Data:  message,
		Topic: l.topicName,
	}

	return l.pubsubHandler(pubsubMsg)
}
