/*
Copyright 2021 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package hazelcast

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/hazelcast/hazelcast-go-client"
	hazelcastCore "github.com/hazelcast/hazelcast-go-client/core"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

const (
	hazelcastServers           = "hazelcastServers"
	hazelcastBackOffMaxRetries = "backOffMaxRetries"
)

type Hazelcast struct {
	client   hazelcast.Client
	logger   logger.Logger
	metadata metadata
}

// NewHazelcastPubSub returns a new hazelcast pub-sub implementation.
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

	if val, ok := meta.Properties[hazelcastBackOffMaxRetries]; ok && val != "" {
		backOffMaxRetriesInt, err := strconv.Atoi(val)
		if err != nil {
			return m, fmt.Errorf("hazelcast error: invalid backOffMaxRetries %s, %v", val, err)
		}
		m.backOffMaxRetries = backOffMaxRetriesInt
	}

	return m, nil
}

func (p *Hazelcast) Init(metadata pubsub.Metadata) error {
	p.logger.Warnf("DEPRECATION NOTICE: Component pubsub.hazelcast has been deprecated and will be removed in a future Dapr release.")

	m, err := parseHazelcastMetadata(metadata)
	if err != nil {
		return err
	}

	p.metadata = m
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

func (p *Hazelcast) Subscribe(subscribeCtx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	topic, err := p.client.GetTopic(req.Topic)
	if err != nil {
		return fmt.Errorf("hazelcast error: failed to get topic for %s", req.Topic)
	}

	listenerID, err := topic.AddMessageListener(&hazelcastMessageListener{
		p:             p,
		ctx:           subscribeCtx,
		topicName:     topic.Name(),
		pubsubHandler: handler,
	})
	if err != nil {
		return fmt.Errorf("hazelcast error: failed to add new listener, %v", err)
	}

	// Wait for context cancelation then remove the listener
	go func() {
		<-subscribeCtx.Done()
		topic.RemoveMessageListener(listenerID)
	}()

	return nil
}

func (p *Hazelcast) Close() error {
	p.client.Shutdown()

	return nil
}

func (p *Hazelcast) Features() []pubsub.Feature {
	return nil
}

type hazelcastMessageListener struct {
	p             *Hazelcast
	ctx           context.Context
	topicName     string
	pubsubHandler pubsub.Handler
}

func (l *hazelcastMessageListener) OnMessage(message hazelcastCore.Message) error {
	msg, ok := message.MessageObject().([]byte)
	if !ok {
		return errors.New("hazelcast error: cannot cast message to byte array")
	}

	if err := l.handleMessageObject(msg); err != nil {
		l.p.logger.Error("Failure processing Hazelcast message")

		return err
	}

	return nil
}

func (l *hazelcastMessageListener) handleMessageObject(message []byte) error {
	pubsubMsg := pubsub.NewMessage{
		Data:  message,
		Topic: l.topicName,
	}

	// TODO: See https://github.com/dapr/components-contrib/issues/1808
	// This component has built-in retries because Hazelcast doesn't support N/ACK for pubsub (it delivers messages "once" and not "at least once")
	var b backoff.BackOff = backoff.NewConstantBackOff(5 * time.Second)
	b = backoff.WithContext(b, l.ctx)
	if l.p.metadata.backOffMaxRetries >= 0 {
		b = backoff.WithMaxRetries(b, uint64(l.p.metadata.backOffMaxRetries))
	}

	return retry.NotifyRecover(func() error {
		l.p.logger.Debug("Processing Hazelcast message")

		return l.pubsubHandler(l.ctx, &pubsubMsg)
	}, b, func(err error, d time.Duration) {
		l.p.logger.Error("Error processing Hazelcast message. Retrying...")
	}, func() {
		l.p.logger.Info("Successfully processed Hazelcast message after it previously failed")
	})
}
