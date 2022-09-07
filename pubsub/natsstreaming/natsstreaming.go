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

/*
Package natsstreaming implements NATS Streaming pubsub component
*/
package natsstreaming

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	nats "github.com/nats-io/nats.go"
	stan "github.com/nats-io/stan.go"
	"github.com/nats-io/stan.go/pb"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

// compulsory options.
const (
	natsURL                = "natsURL"
	natsStreamingClusterID = "natsStreamingClusterID"
)

// subscription options (optional).
const (
	durableSubscriptionName = "durableSubscriptionName"
	startAtSequence         = "startAtSequence"
	startWithLastReceived   = "startWithLastReceived"
	deliverAll              = "deliverAll"
	deliverNew              = "deliverNew"
	startAtTimeDelta        = "startAtTimeDelta"
	startAtTime             = "startAtTime"
	startAtTimeFormat       = "startAtTimeFormat"
	ackWaitTime             = "ackWaitTime"
	maxInFlight             = "maxInFlight"
)

// valid values for subscription options.
const (
	subscriptionTypeQueueGroup = "queue"
	subscriptionTypeTopic      = "topic"
	startWithLastReceivedTrue  = "true"
	deliverAllTrue             = "true"
	deliverNewTrue             = "true"
)

const (
	consumerID       = "consumerID" // passed in by Dapr runtime
	subscriptionType = "subscriptionType"
)

type natsStreamingPubSub struct {
	pubsub.DefaultBulkMessager
	metadata         metadata
	natStreamingConn stan.Conn

	logger logger.Logger

	backOffConfig retry.Config
}

// NewNATSStreamingPubSub returns a new NATS Streaming pub-sub implementation.
func NewNATSStreamingPubSub(logger logger.Logger) pubsub.PubSub {
	return &natsStreamingPubSub{logger: logger}
}

func parseNATSStreamingMetadata(meta pubsub.Metadata) (metadata, error) {
	m := metadata{}
	if val, ok := meta.Properties[natsURL]; ok && val != "" {
		m.natsURL = val
	} else {
		return m, errors.New("nats-streaming error: missing nats URL")
	}
	if val, ok := meta.Properties[natsStreamingClusterID]; ok && val != "" {
		m.natsStreamingClusterID = val
	} else {
		return m, errors.New("nats-streaming error: missing nats streaming cluster ID")
	}

	if val, ok := meta.Properties[subscriptionType]; ok {
		if val == subscriptionTypeTopic || val == subscriptionTypeQueueGroup {
			m.subscriptionType = val
		} else {
			return m, errors.New("nats-streaming error: valid value for subscriptionType is topic or queue")
		}
	}

	if val, ok := meta.Properties[consumerID]; ok && val != "" {
		m.natsQueueGroupName = val
	} else {
		return m, errors.New("nats-streaming error: missing queue group name")
	}

	if val, ok := meta.Properties[durableSubscriptionName]; ok && val != "" {
		m.durableSubscriptionName = val
	}

	if val, ok := meta.Properties[ackWaitTime]; ok && val != "" {
		dur, err := time.ParseDuration(meta.Properties[ackWaitTime])
		if err != nil {
			return m, fmt.Errorf("nats-streaming error %s ", err)
		}
		m.ackWaitTime = dur
	}
	if val, ok := meta.Properties[maxInFlight]; ok && val != "" {
		max, err := strconv.ParseUint(meta.Properties[maxInFlight], 10, 64)
		if err != nil {
			return m, fmt.Errorf("nats-streaming error in parsemetadata for maxInFlight: %s ", err)
		}
		if max < 1 {
			return m, errors.New("nats-streaming error: maxInFlight should be equal to or more than 1")
		}
		m.maxInFlight = max
	}

	//nolint:nestif
	// subscription options - only one can be used
	if val, ok := meta.Properties[startAtSequence]; ok && val != "" {
		// nats streaming accepts a uint64 as sequence
		seq, err := strconv.ParseUint(meta.Properties[startAtSequence], 10, 64)
		if err != nil {
			return m, fmt.Errorf("nats-streaming error %s ", err)
		}
		if seq < 1 {
			return m, errors.New("nats-streaming error: startAtSequence should be equal to or more than 1")
		}
		m.startAtSequence = seq
	} else if val, ok := meta.Properties[startWithLastReceived]; ok {
		// only valid value is true
		if val == startWithLastReceivedTrue {
			m.startWithLastReceived = val
		} else {
			return m, errors.New("nats-streaming error: valid value for startWithLastReceived is true")
		}
	} else if val, ok := meta.Properties[deliverAll]; ok {
		// only valid value is true
		if val == deliverAllTrue {
			m.deliverAll = val
		} else {
			return m, errors.New("nats-streaming error: valid value for deliverAll is true")
		}
	} else if val, ok := meta.Properties[deliverNew]; ok {
		// only valid value is true
		if val == deliverNewTrue {
			m.deliverNew = val
		} else {
			return m, errors.New("nats-streaming error: valid value for deliverNew is true")
		}
	} else if val, ok := meta.Properties[startAtTimeDelta]; ok && val != "" {
		dur, err := time.ParseDuration(meta.Properties[startAtTimeDelta])
		if err != nil {
			return m, fmt.Errorf("nats-streaming error %s ", err)
		}
		m.startAtTimeDelta = dur
	} else if val, ok := meta.Properties[startAtTime]; ok && val != "" {
		m.startAtTime = val
		if val, ok := meta.Properties[startAtTimeFormat]; ok && val != "" {
			m.startAtTimeFormat = val
		} else {
			return m, errors.New("nats-streaming error: missing value for startAtTimeFormat")
		}
	}

	c, err := pubsub.Concurrency(meta.Properties)
	if err != nil {
		return m, fmt.Errorf("nats-streaming error: can't parse %s: %s", pubsub.ConcurrencyKey, err)
	}

	m.concurrencyMode = c
	return m, nil
}

func (n *natsStreamingPubSub) Init(metadata pubsub.Metadata) error {
	m, err := parseNATSStreamingMetadata(metadata)
	if err != nil {
		return err
	}
	n.metadata = m
	clientID := genRandomString(20)
	opts := []nats.Option{nats.Name(clientID)}
	natsConn, err := nats.Connect(m.natsURL, opts...)
	if err != nil {
		return fmt.Errorf("nats-streaming: error connecting to nats server at %s: %s", m.natsURL, err)
	}
	natStreamingConn, err := stan.Connect(m.natsStreamingClusterID, clientID, stan.NatsConn(natsConn))
	if err != nil {
		return fmt.Errorf("nats-streaming: error connecting to nats streaming server %s: %s", m.natsStreamingClusterID, err)
	}
	n.logger.Debugf("connected to natsstreaming at %s", m.natsURL)

	// Default retry configuration is used if no
	// backOff properties are set.
	if err := retry.DecodeConfigWithPrefix(
		&n.backOffConfig,
		metadata.Properties,
		"backOff"); err != nil {
		return err
	}

	n.natStreamingConn = natStreamingConn

	return nil
}

func (n *natsStreamingPubSub) Publish(req *pubsub.PublishRequest) error {
	err := n.natStreamingConn.Publish(req.Topic, req.Data)
	if err != nil {
		return fmt.Errorf("nats-streaming: error from publish: %s", err)
	}

	return nil
}

func (n *natsStreamingPubSub) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	natStreamingsubscriptionOptions, err := n.subscriptionOptions()
	if err != nil {
		return fmt.Errorf("nats-streaming: error getting subscription options %s", err)
	}

	natsMsgHandler := func(natsMsg *stan.Msg) {
		msg := pubsub.NewMessage{
			Topic: req.Topic,
			Data:  natsMsg.Data,
		}

		n.logger.Debugf("Processing NATS Streaming message %s/%d", natsMsg.Subject, natsMsg.Sequence)

		f := func() {
			herr := handler(ctx, &msg)
			if herr == nil {
				natsMsg.Ack()
			}
		}

		switch n.metadata.concurrencyMode {
		case pubsub.Single:
			f()
		case pubsub.Parallel:
			go f()
		}
	}

	var subscription stan.Subscription
	if n.metadata.subscriptionType == subscriptionTypeTopic {
		subscription, err = n.natStreamingConn.Subscribe(req.Topic, natsMsgHandler, natStreamingsubscriptionOptions...)
	} else if n.metadata.subscriptionType == subscriptionTypeQueueGroup {
		subscription, err = n.natStreamingConn.QueueSubscribe(req.Topic, n.metadata.natsQueueGroupName, natsMsgHandler, natStreamingsubscriptionOptions...)
	}

	if err != nil {
		return fmt.Errorf("nats-streaming: subscribe error %s", err)
	}

	go func() {
		<-ctx.Done()
		err := subscription.Unsubscribe()
		if err != nil {
			n.logger.Warnf("nats-streaming: error while unsubscribing from topic %s: %v", req.Topic, err)
		}
	}()

	if n.metadata.subscriptionType == subscriptionTypeTopic {
		n.logger.Debugf("nats-streaming: subscribed to subject %s", req.Topic)
	} else if n.metadata.subscriptionType == subscriptionTypeQueueGroup {
		n.logger.Debugf("nats-streaming: subscribed to subject %s with queue group %s", req.Topic, n.metadata.natsQueueGroupName)
	}

	return nil
}

func (n *natsStreamingPubSub) subscriptionOptions() ([]stan.SubscriptionOption, error) {
	var options []stan.SubscriptionOption

	if n.metadata.durableSubscriptionName != "" {
		options = append(options, stan.DurableName(n.metadata.durableSubscriptionName))
	}

	switch {
	case n.metadata.deliverNew == deliverNewTrue:
		options = append(options, stan.StartAt(pb.StartPosition_NewOnly)) //nolint:nosnakecase
	case n.metadata.startAtSequence >= 1: // messages index start from 1, this is a valid check
		options = append(options, stan.StartAtSequence(n.metadata.startAtSequence))
	case n.metadata.startWithLastReceived == startWithLastReceivedTrue:
		options = append(options, stan.StartWithLastReceived())
	case n.metadata.deliverAll == deliverAllTrue:
		options = append(options, stan.DeliverAllAvailable())
	case n.metadata.startAtTimeDelta > (1 * time.Nanosecond): // as long as its a valid time.Duration
		options = append(options, stan.StartAtTimeDelta(n.metadata.startAtTimeDelta))
	case n.metadata.startAtTime != "":
		if n.metadata.startAtTimeFormat != "" {
			startTime, err := time.Parse(n.metadata.startAtTimeFormat, n.metadata.startAtTime)
			if err != nil {
				return nil, err
			}
			options = append(options, stan.StartAtTime(startTime))
		}
	}

	// default is auto ACK. switching to manual ACK since processing errors need to be handled
	options = append(options, stan.SetManualAckMode())

	// check if set the ack options.
	if n.metadata.ackWaitTime > (1 * time.Nanosecond) {
		options = append(options, stan.AckWait(n.metadata.ackWaitTime))
	}
	if n.metadata.maxInFlight >= 1 {
		options = append(options, stan.MaxInflight(int(n.metadata.maxInFlight)))
	}

	return options, nil
}

const inputs = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"

// generates a random string of length 20.
func genRandomString(n int) string {
	b := make([]byte, n)
	s := rand.NewSource(int64(time.Now().Nanosecond()))
	for i := range b {
		b[i] = inputs[s.Int63()%int64(len(inputs))]
	}
	clientID := string(b)

	return clientID
}

func (n *natsStreamingPubSub) Close() error {
	return n.natStreamingConn.Close()
}

func (n *natsStreamingPubSub) Features() []pubsub.Feature {
	return nil
}
