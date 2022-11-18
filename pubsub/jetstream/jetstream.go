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

package jetstream

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

type jetstreamPubSub struct {
	nc   *nats.Conn
	jsc  nats.JetStreamContext
	l    logger.Logger
	meta metadata

	backOffConfig retry.Config
}

func NewJetStream(logger logger.Logger) pubsub.PubSub {
	return &jetstreamPubSub{l: logger}
}

func (js *jetstreamPubSub) Init(metadata pubsub.Metadata) error {
	var err error
	js.meta, err = parseMetadata(metadata)
	if err != nil {
		return err
	}

	var opts []nats.Option
	opts = append(opts, nats.Name(js.meta.name))

	// Set nats.UserJWT options when jwt and seed key is provided.
	if js.meta.jwt != "" && js.meta.seedKey != "" {
		opts = append(opts, nats.UserJWT(func() (string, error) {
			return js.meta.jwt, nil
		}, func(nonce []byte) ([]byte, error) {
			return sigHandler(js.meta.seedKey, nonce)
		}))
	} else if js.meta.tlsClientCert != "" && js.meta.tlsClientKey != "" {
		js.l.Debug("Configure nats for tls client authentication")
		opts = append(opts, nats.ClientCert(js.meta.tlsClientCert, js.meta.tlsClientKey))
	} else if js.meta.token != "" {
		js.l.Debug("Configure nats for token authentication")
		opts = append(opts, nats.Token(js.meta.token))
	}

	js.nc, err = nats.Connect(js.meta.natsURL, opts...)
	if err != nil {
		return err
	}
	js.l.Debugf("Connected to nats at %s", js.meta.natsURL)

	js.jsc, err = js.nc.JetStream()
	if err != nil {
		return err
	}

	// Default retry configuration is used if no backOff properties are set.
	if err := retry.DecodeConfigWithPrefix(
		&js.backOffConfig,
		metadata.Properties,
		"backOff"); err != nil {
		return err
	}

	js.l.Debug("JetStream initialization complete")

	return nil
}

func (js *jetstreamPubSub) Features() []pubsub.Feature {
	return nil
}

func (js *jetstreamPubSub) Publish(req *pubsub.PublishRequest) error {
	var opts []nats.PubOpt
	var msgID string

	event, err := pubsub.FromCloudEvent(req.Data, "", "", "", "")
	if err != nil {
		js.l.Debugf("error unmarshalling cloudevent: %v", err)
	} else {
		// Use the cloudevent id as the Nats-MsgId for deduplication
		if id, ok := event["id"].(string); ok {
			msgID = id
			opts = append(opts, nats.MsgId(msgID))
		}
	}

	if msgID == "" {
		js.l.Warn("empty message ID, Jetstream deduplication will not be possible")
	}

	js.l.Debugf("Publishing to topic %v id: %s", req.Topic, msgID)
	_, err = js.jsc.Publish(req.Topic, req.Data, opts...)

	return err
}

func (js *jetstreamPubSub) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	var consumerConfig nats.ConsumerConfig

	consumerConfig.DeliverSubject = nats.NewInbox()

	if v := js.meta.durableName; v != "" {
		consumerConfig.Durable = v
	}

	if v := js.meta.startTime; !v.IsZero() {
		consumerConfig.OptStartTime = &v
	} else if v := js.meta.startSequence; v > 0 {
		consumerConfig.OptStartSeq = v
	} else if js.meta.deliverAll {
		consumerConfig.DeliverPolicy = nats.DeliverAllPolicy
	} else {
		consumerConfig.DeliverPolicy = nats.DeliverLastPolicy
	}

	if js.meta.flowControl {
		consumerConfig.FlowControl = true
	}

	if js.meta.ackWait != 0 {
		consumerConfig.AckWait = js.meta.ackWait
	}
	if js.meta.maxDeliver != 0 {
		consumerConfig.MaxDeliver = js.meta.maxDeliver
	}
	if len(js.meta.backOff) != 0 {
		consumerConfig.BackOff = js.meta.backOff
	}
	if js.meta.maxAckPending != 0 {
		consumerConfig.MaxAckPending = js.meta.maxAckPending
	}
	if js.meta.replicas != 0 {
		consumerConfig.Replicas = js.meta.replicas
	}
	if js.meta.memoryStorage {
		consumerConfig.MemoryStorage = true
	}
	if js.meta.rateLimit != 0 {
		consumerConfig.RateLimit = js.meta.rateLimit
	}
	if js.meta.hearbeat != 0 {
		consumerConfig.Heartbeat = js.meta.hearbeat
	}

	natsHandler := func(m *nats.Msg) {
		jsm, err := m.Metadata()
		if err != nil {
			// If we get an error, then we don't have a valid JetStream
			// message.
			js.l.Error(err)

			return
		}

		js.l.Debugf("Processing JetStream message %s/%d", m.Subject, jsm.Sequence)
		err = handler(ctx, &pubsub.NewMessage{
			Topic: req.Topic,
			Data:  m.Data,
			Metadata: map[string]string{
				"Topic": m.Subject,
			},
		})
		if err != nil {
			js.l.Errorf("Error processing JetStream message %s/%d: %v", m.Subject, jsm.Sequence, err)

			nakErr := m.Nak()
			if nakErr != nil {
				js.l.Errorf("Error while sending NAK for JetStream message %s/%d: %v", m.Subject, jsm.Sequence, nakErr)
			}

			return
		}

		err = m.Ack()
		if err != nil {
			js.l.Errorf("Error while sending ACK for JetStream message %s/%d: %v", m.Subject, jsm.Sequence, err)
		}
	}

	var err error
	streamName := js.meta.streamName
	if streamName == "" {
		streamName, err = js.jsc.StreamNameBySubject(req.Topic)
		if err != nil {
			return err
		}
	}
	var subscription *nats.Subscription

	consumerInfo, err := js.jsc.AddConsumer(streamName, &consumerConfig)
	if err != nil {
		return err
	}

	if queue := js.meta.queueGroupName; queue != "" {
		js.l.Debugf("nats: subscribed to subject %s with queue group %s",
			req.Topic, js.meta.queueGroupName)
		subscription, err = js.jsc.QueueSubscribe(req.Topic, queue, natsHandler, nats.Bind(streamName, consumerInfo.Name))
	} else {
		js.l.Debugf("nats: subscribed to subject %s", req.Topic)
		subscription, err = js.jsc.Subscribe(req.Topic, natsHandler, nats.Bind(streamName, consumerInfo.Name))
	}
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		err := subscription.Unsubscribe()
		if err != nil {
			js.l.Warnf("nats: error while unsubscribing from topic %s: %v", req.Topic, err)
		}
	}()

	return nil
}

func (js *jetstreamPubSub) Close() error {
	return js.nc.Drain()
}

// Handle nats signature request for challenge response authentication.
func sigHandler(seedKey string, nonce []byte) ([]byte, error) {
	kp, err := nkeys.FromSeed([]byte(seedKey))
	if err != nil {
		return nil, err
	}
	// Wipe our key on exit.
	defer kp.Wipe()

	sig, _ := kp.Sign(nonce)
	return sig, nil
}
