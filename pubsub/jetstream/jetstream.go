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
	"errors"
	"time"

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

	ctx           context.Context
	ctxCancel     context.CancelFunc
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

	js.ctx, js.ctxCancel = context.WithCancel(context.Background())

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
	js.l.Debugf("Publishing topic %v with data: %v", req.Topic, req.Data)
	_, err := js.jsc.Publish(req.Topic, req.Data)

	return err
}

func (js *jetstreamPubSub) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	var opts []nats.SubOpt

	if v := js.meta.durableName; v != "" {
		opts = append(opts, nats.Durable(v))
	}

	if v := js.meta.startTime; !v.IsZero() {
		opts = append(opts, nats.StartTime(v))
	} else if v := js.meta.startSequence; v > 0 {
		opts = append(opts, nats.StartSequence(v))
	} else if js.meta.deliverAll {
		opts = append(opts, nats.DeliverAll())
	} else {
		opts = append(opts, nats.DeliverLast())
	}

	if js.meta.flowControl {
		opts = append(opts, nats.EnableFlowControl())
	}

	natsHandler := func(m *nats.Msg) {
		jsm, err := m.Metadata()
		if err != nil {
			// If we get an error, then we don't have a valid JetStream
			// message.
			js.l.Error(err)

			return
		}

		operation := func() error {
			js.l.Debugf("Processing JetStream message %s/%d", m.Subject,
				jsm.Sequence)
			opErr := handler(js.ctx, &pubsub.NewMessage{
				Topic: req.Topic,
				Data:  m.Data,
				Metadata: map[string]string{
					"Topic": m.Subject,
				},
			})
			if opErr != nil {
				return opErr
			}

			return m.Ack()
		}
		notify := func(nerr error, d time.Duration) {
			js.l.Errorf("Error processing JetStream message: %s/%d. Retrying...",
				m.Subject, jsm.Sequence)
		}
		recovered := func() {
			js.l.Infof("Successfully processed JetStream message after it previously failed: %s/%d",
				m.Subject, jsm.Sequence)
		}
		backOff := js.backOffConfig.NewBackOffWithContext(js.ctx)

		err = retry.NotifyRecover(operation, backOff, notify, recovered)
		if err != nil && !errors.Is(err, context.Canceled) {
			js.l.Errorf("Error processing message and retries are exhausted:  %s/%d.",
				m.Subject, jsm.Sequence)
		}
	}

	var err error
	if queue := js.meta.queueGroupName; queue != "" {
		js.l.Debugf("nats: subscribed to subject %s with queue group %s",
			req.Topic, js.meta.queueGroupName)
		_, err = js.jsc.QueueSubscribe(req.Topic, queue, natsHandler, opts...)
	} else {
		js.l.Debugf("nats: subscribed to subject %s", req.Topic)
		_, err = js.jsc.Subscribe(req.Topic, natsHandler, opts...)
	}

	return err
}

func (js *jetstreamPubSub) Close() error {
	js.ctxCancel()

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
