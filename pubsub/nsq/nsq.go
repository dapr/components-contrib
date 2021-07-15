// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------
package nsq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"

	"github.com/nsqio/go-nsq"
)

const (
	splitter           = ","
	defaultMaxInflight = 200
	defaultConcurrency = 1
)

type subscriber struct {
	topic string

	c *nsq.Consumer

	// handler so we can resubcribe
	h nsq.HandlerFunc
}

type nsqPubSub struct {
	current  int
	settings Settings

	pubs []*nsq.Producer
	subs []*subscriber

	logger logger.Logger
	config *nsq.Config

	ctx           context.Context
	cancel        context.CancelFunc
	backOffConfig retry.Config
}

// NewNSQPubSub returns a new NATS pub-sub implementation
func NewNSQPubSub(logger logger.Logger) pubsub.PubSub {
	return &nsqPubSub{logger: logger}
}

func (n *nsqPubSub) Init(metadata pubsub.Metadata) error {
	n.settings = Settings{
		MaxInFlight: defaultMaxInflight,
		Concurrency: defaultConcurrency,
	}

	err := n.settings.Decode(metadata.Properties)
	if err != nil {
		return fmt.Errorf("init nsq config error: %w", err)
	}

	if err = n.settings.Validate(); err != nil {
		return err
	}

	n.config = n.settings.ToNSQConfig()
	producers := make([]*nsq.Producer, 0, len(n.settings.nsqds))

	// create producers
	for _, addr := range n.settings.nsqds {
		p, err := nsq.NewProducer(addr, n.config)
		if err != nil {
			return err
		}
		if err = p.Ping(); err != nil {
			return err
		}
		producers = append(producers, p)

		n.logger.Debugf("connected to nsq producer at %s", addr)
	}

	n.pubs = producers

	n.ctx, n.cancel = context.WithCancel(context.Background())
	// Default retry configuration is used if no
	// backOff properties are set.
	if err := retry.DecodeConfigWithPrefix(
		&n.backOffConfig,
		metadata.Properties,
		"backOff"); err != nil {
		return fmt.Errorf("retry configuration error: %w", err)
	}

	return nil
}

func (n *nsqPubSub) Publish(req *pubsub.PublishRequest) error {
	n.logger.Debugf("[nsq] publish to %s", req.Topic)

	err := n.pubs[n.current].Publish(req.Topic, req.Data)
	if err != nil {
		for i, pub := range n.pubs {
			if i == n.current {
				continue
			}
			if err = pub.Publish(req.Topic, req.Data); err != nil {
				continue
			}
			n.current = i
		}
	}

	if err != nil {
		n.logger.Errorf("error send message topic:%s err: %v", req.Topic, err)
	}

	return err
}

func (n *nsqPubSub) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	var settings Settings
	err := settings.Decode(req.Metadata)
	if err != nil {
		return fmt.Errorf("subscribe nsq config error: %w", err)
	}

	c, err := nsq.NewConsumer(req.Topic, settings.Channel, n.config)
	if err != nil {
		return err
	}

	h := nsq.HandlerFunc(func(nm *nsq.Message) error {
		b := n.backOffConfig.NewBackOffWithContext(n.ctx)

		rerr := retry.NotifyRecover(func() error {
			n.logger.Debugf("[nsq] Processing msg %s on %s", nm.ID, req.Topic)
			herr := handler(n.ctx, &pubsub.NewMessage{Topic: req.Topic, Data: nm.Body})
			if herr != nil {
				n.logger.Errorf("nsq error: fail to send message to dapr application. topic:%s message-id:%s err:%v ", req.Topic, nm.ID, herr)
			}

			return herr
		}, b, func(err error, d time.Duration) {
			n.logger.Errorf("nsq error: fail to processing message. topic:%s message-id:%s. Retrying...", req.Topic, nm.ID)
		}, func() {
			n.logger.Infof("nsq successfully processed message after it previously failed. topic:%s message-id:%s.", req.Topic, nm.ID)
		})

		if rerr != nil && !errors.Is(rerr, context.Canceled) {
			n.logger.Errorf("nsq error: processing message and retries are exhausted. topic:%s message-id:%s.", req.Topic, nm.ID)
		}

		return rerr
	})

	c.AddConcurrentHandlers(h, settings.Concurrency)

	if len(n.settings.lookups) > 0 {
		err = c.ConnectToNSQLookupds(n.settings.lookups)
		n.logger.Debugf("connected to nsq consumer at %v", n.settings.lookups)
	} else {
		err = c.ConnectToNSQDs(n.settings.nsqds)
		n.logger.Debugf("connected to nsq consumer at %v", n.settings.nsqds)
	}

	if err != nil {
		return err
	}

	sub := &subscriber{
		c:     c,
		topic: req.Topic,
		h:     h,
	}

	n.subs = append(n.subs, sub)

	return nil
}

func (n *nsqPubSub) Close() error {
	n.cancel()

	// stop the producers
	for _, p := range n.pubs {
		p.Stop()
	}

	// stop the consumers
	for _, c := range n.subs {
		c.c.Stop()

		if len(n.settings.lookups) > 0 {
			// disconnect from all lookupd
			for _, addr := range n.settings.lookups {
				c.c.DisconnectFromNSQLookupd(addr)
			}
		} else {
			// disconnect from all nsq brokers
			for _, addr := range n.settings.nsqds {
				c.c.DisconnectFromNSQD(addr)
			}
		}
	}

	return nil
}

func (n *nsqPubSub) Features() []pubsub.Feature {
	return nil
}
