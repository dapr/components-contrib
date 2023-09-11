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
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	nats "github.com/nats-io/nats.go"
	stan "github.com/nats-io/stan.go"
	"github.com/nats-io/stan.go/pb"

	"github.com/dapr/components-contrib/metadata"
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
	metadata         natsMetadata
	natStreamingConn stan.Conn

	logger logger.Logger

	backOffConfig retry.Config

	closed  atomic.Bool
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// NewNATSStreamingPubSub returns a new NATS Streaming pub-sub implementation.
func NewNATSStreamingPubSub(logger logger.Logger) pubsub.PubSub {
	return &natsStreamingPubSub{logger: logger, closeCh: make(chan struct{})}
}

func parseNATSStreamingMetadata(meta pubsub.Metadata) (natsMetadata, error) {
	m := natsMetadata{}

	var err error
	if err = metadata.DecodeMetadata(meta.Properties, &m); err != nil {
		return m, err
	}

	if m.NatsURL == "" {
		return m, errors.New("nats-streaming error: missing nats URL")
	}
	if m.NatsStreamingClusterID == "" {
		return m, errors.New("nats-streaming error: missing nats streaming cluster ID")
	}

	switch m.SubscriptionType {
	case subscriptionTypeTopic, subscriptionTypeQueueGroup, "":
		// valid values
	default:
		return m, errors.New("nats-streaming error: valid value for subscriptionType is topic or queue")
	}

	if m.NatsQueueGroupName == "" {
		return m, errors.New("nats-streaming error: missing queue group name")
	}

	if m.MaxInFlight != nil && *m.MaxInFlight < 1 {
		return m, errors.New("nats-streaming error: maxInFlight should be equal to or more than 1")
	}

	//nolint:nestif
	// subscription options - only one can be used

	// helper function to reset mutually exclusive options
	clearValues := func(m *natsMetadata, indexToKeep int) {
		if indexToKeep != 0 {
			m.StartAtSequence = nil
		}
		if indexToKeep != 1 {
			m.StartWithLastReceived = ""
		}
		if indexToKeep != 2 {
			m.DeliverAll = ""
		}
		if indexToKeep != 3 {
			m.DeliverNew = ""
		}
		if indexToKeep != 4 {
			m.StartAtTime = ""
		}
		if indexToKeep != 4 {
			m.StartAtTimeFormat = ""
		}
	}

	switch {
	case m.StartAtSequence != nil:
		if *m.StartAtSequence < 1 {
			return m, errors.New("nats-streaming error: startAtSequence should be equal to or more than 1")
		}
		clearValues(&m, 0)
	case m.StartWithLastReceived != "":
		if m.StartWithLastReceived != startWithLastReceivedTrue {
			return m, errors.New("nats-streaming error: valid value for startWithLastReceived is true")
		}
		clearValues(&m, 1)
	case m.DeliverAll != "":
		if m.DeliverAll != deliverAllTrue {
			return m, errors.New("nats-streaming error: valid value for deliverAll is true")
		}
		clearValues(&m, 2)
	case m.DeliverNew != "":
		if m.DeliverNew != deliverNewTrue {
			return m, errors.New("nats-streaming error: valid value for deliverNew is true")
		}
		clearValues(&m, 3)
	case m.StartAtTime != "":
		if m.StartAtTimeFormat == "" {
			return m, errors.New("nats-streaming error: missing value for startAtTimeFormat")
		}
		clearValues(&m, 4)
	}

	m.ConcurrencyMode, err = pubsub.Concurrency(meta.Properties)
	if err != nil {
		return m, fmt.Errorf("nats-streaming error: can't parse %s: %s", pubsub.ConcurrencyKey, err)
	}
	return m, nil
}

func (n *natsStreamingPubSub) Init(_ context.Context, metadata pubsub.Metadata) error {
	n.logger.Warn("⚠️ The NATS Streaming PubSub component is deprecated due to the deprecation of NATS Server, and will be removed from Dapr 1.13")

	m, err := parseNATSStreamingMetadata(metadata)
	if err != nil {
		return err
	}
	n.metadata = m
	clientID := genRandomString(20)
	opts := []nats.Option{nats.Name(clientID)}
	natsConn, err := nats.Connect(m.NatsURL, opts...)
	if err != nil {
		return fmt.Errorf("nats-streaming: error connecting to nats server at %s: %s", m.NatsURL, err)
	}
	natStreamingConn, err := stan.Connect(m.NatsStreamingClusterID, clientID, stan.NatsConn(natsConn))
	if err != nil {
		return fmt.Errorf("nats-streaming: error connecting to nats streaming server %s: %s", m.NatsStreamingClusterID, err)
	}
	n.logger.Debugf("connected to natsstreaming at %s", m.NatsURL)

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

func (n *natsStreamingPubSub) Publish(_ context.Context, req *pubsub.PublishRequest) error {
	if n.closed.Load() {
		return errors.New("component is closed")
	}

	err := n.natStreamingConn.Publish(req.Topic, req.Data)
	if err != nil {
		return fmt.Errorf("nats-streaming: error from publish: %s", err)
	}

	return nil
}

func (n *natsStreamingPubSub) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if n.closed.Load() {
		return errors.New("component is closed")
	}

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

		switch n.metadata.ConcurrencyMode {
		case pubsub.Single:
			f()
		case pubsub.Parallel:
			n.wg.Add(1)
			go func() {
				defer n.wg.Done()
				f()
			}()
		}
	}

	var subscription stan.Subscription
	if n.metadata.SubscriptionType == subscriptionTypeTopic {
		subscription, err = n.natStreamingConn.Subscribe(req.Topic, natsMsgHandler, natStreamingsubscriptionOptions...)
	} else if n.metadata.SubscriptionType == subscriptionTypeQueueGroup {
		subscription, err = n.natStreamingConn.QueueSubscribe(req.Topic, n.metadata.NatsQueueGroupName, natsMsgHandler, natStreamingsubscriptionOptions...)
	}

	if err != nil {
		return fmt.Errorf("nats-streaming: subscribe error %s", err)
	}

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		select {
		case <-ctx.Done():
		case <-n.closeCh:
		}
		err := subscription.Unsubscribe()
		if err != nil {
			n.logger.Warnf("nats-streaming: error while unsubscribing from topic %s: %v", req.Topic, err)
		}
	}()

	if n.metadata.SubscriptionType == subscriptionTypeTopic {
		n.logger.Debugf("nats-streaming: subscribed to subject %s", req.Topic)
	} else if n.metadata.SubscriptionType == subscriptionTypeQueueGroup {
		n.logger.Debugf("nats-streaming: subscribed to subject %s with queue group %s", req.Topic, n.metadata.NatsQueueGroupName)
	}

	return nil
}

func (n *natsStreamingPubSub) subscriptionOptions() ([]stan.SubscriptionOption, error) {
	var options []stan.SubscriptionOption

	if n.metadata.DurableSubscriptionName != "" {
		options = append(options, stan.DurableName(n.metadata.DurableSubscriptionName))
	}

	switch {
	case n.metadata.DeliverNew == deliverNewTrue:
		options = append(options, stan.StartAt(pb.StartPosition_NewOnly)) //nolint:nosnakecase
	case n.metadata.StartAtSequence != nil && *n.metadata.StartAtSequence >= 1: // messages index start from 1, this is a valid check
		options = append(options, stan.StartAtSequence(*n.metadata.StartAtSequence))
	case n.metadata.StartWithLastReceived == startWithLastReceivedTrue:
		options = append(options, stan.StartWithLastReceived())
	case n.metadata.DeliverAll == deliverAllTrue:
		options = append(options, stan.DeliverAllAvailable())
	case n.metadata.StartAtTimeDelta > (1 * time.Nanosecond): // as long as its a valid time.Duration
		options = append(options, stan.StartAtTimeDelta(n.metadata.StartAtTimeDelta))
	case n.metadata.StartAtTime != "":
		if n.metadata.StartAtTimeFormat != "" {
			startTime, err := time.Parse(n.metadata.StartAtTimeFormat, n.metadata.StartAtTime)
			if err != nil {
				return nil, err
			}
			options = append(options, stan.StartAtTime(startTime))
		}
	}

	// default is auto ACK. switching to manual ACK since processing errors need to be handled
	options = append(options, stan.SetManualAckMode())

	// check if set the ack options.
	if n.metadata.AckWaitTime > (1 * time.Nanosecond) {
		options = append(options, stan.AckWait(n.metadata.AckWaitTime))
	}
	if n.metadata.MaxInFlight != nil && *n.metadata.MaxInFlight >= 1 {
		options = append(options, stan.MaxInflight(int(*n.metadata.MaxInFlight)))
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
	defer n.wg.Wait()
	if n.closed.CompareAndSwap(false, true) {
		close(n.closeCh)
	}

	return n.natStreamingConn.Close()
}

func (n *natsStreamingPubSub) Features() []pubsub.Feature {
	return nil
}

// GetComponentMetadata returns the metadata of the component.
func (n *natsStreamingPubSub) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := natsMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.PubSubType)
	return
}
