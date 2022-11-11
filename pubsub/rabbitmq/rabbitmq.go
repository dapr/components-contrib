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

package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/dapr/kit/logger"

	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
)

const (
	fanoutExchangeKind              = "fanout"
	logMessagePrefix                = "rabbitmq pub/sub:"
	errorMessagePrefix              = "rabbitmq pub/sub error:"
	errorChannelNotInitialized      = "channel not initialized"
	errorChannelConnection          = "channel/connection is not open"
	defaultDeadLetterExchangeFormat = "dlx-%s"
	defaultDeadLetterQueueFormat    = "dlq-%s"

	publishMaxRetries       = 3
	publishRetryWaitSeconds = 2

	argQueueMode          = "x-queue-mode"
	argMaxLength          = "x-max-length"
	argMaxLengthBytes     = "x-max-length-bytes"
	argDeadLetterExchange = "x-dead-letter-exchange"
	queueModeLazy         = "lazy"
	reqMetadataRoutingKey = "routingKey"
)

// RabbitMQ allows sending/receiving messages in pub/sub format.
type rabbitMQ struct {
	connection        rabbitMQConnectionBroker
	channel           rabbitMQChannelBroker
	channelMutex      sync.RWMutex
	connectionCount   int
	metadata          *metadata
	declaredExchanges map[string]bool
	ctx               context.Context
	cancel            context.CancelFunc

	connectionDial func(uri string) (rabbitMQConnectionBroker, rabbitMQChannelBroker, error)

	logger logger.Logger
}

// interface used to allow unit testing.
//
//nolint:interfacebloat
type rabbitMQChannelBroker interface {
	PublishWithContext(ctx context.Context, exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing) error
	PublishWithDeferredConfirmWithContext(ctx context.Context, exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error)
	QueueDeclare(name string, durable bool, autoDelete bool, exclusive bool, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name string, key string, exchange string, noWait bool, args amqp.Table) error
	Consume(queue string, consumer string, autoAck bool, exclusive bool, noLocal bool, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Nack(tag uint64, multiple bool, requeue bool) error
	Ack(tag uint64, multiple bool) error
	ExchangeDeclare(name string, kind string, durable bool, autoDelete bool, internal bool, noWait bool, args amqp.Table) error
	Qos(prefetchCount, prefetchSize int, global bool) error
	Confirm(noWait bool) error
	Close() error
	IsClosed() bool
}

// interface used to allow unit testing.
type rabbitMQConnectionBroker interface {
	Close() error
}

// NewRabbitMQ creates a new RabbitMQ pub/sub.
func NewRabbitMQ(logger logger.Logger) pubsub.PubSub {
	return &rabbitMQ{
		declaredExchanges: make(map[string]bool),
		logger:            logger,
		connectionDial:    dial,
	}
}

func dial(uri string) (rabbitMQConnectionBroker, rabbitMQChannelBroker, error) {
	conn, err := amqp.Dial(uri)
	if err != nil {
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	return conn, ch, nil
}

// Init does metadata parsing and connection creation.
func (r *rabbitMQ) Init(metadata pubsub.Metadata) error {
	meta, err := createMetadata(metadata, r.logger)
	if err != nil {
		return err
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())

	r.metadata = meta

	r.reconnect(0)
	// We do not return error on reconnect because it can cause problems if init() happens
	// right at the restart window for service. So, we try it now but there is logic in the
	// code to reconnect as many times as needed.
	return nil
}

func (r *rabbitMQ) reconnect(connectionCount int) error {
	r.channelMutex.Lock()
	defer r.channelMutex.Unlock()

	if r.isStopped() {
		// Do not reconnect on stopped service.
		return errors.New("cannot connect after component is stopped")
	}

	r.logger.Infof("%s connectionCount: current=%d reference=%d", logMessagePrefix, r.connectionCount, connectionCount)
	if connectionCount != r.connectionCount {
		// Reconnection request is old.
		r.logger.Infof("%s stale reconnect attempt", logMessagePrefix)

		return nil
	}

	err := r.reset()
	if err != nil {
		return err
	}

	r.connection, r.channel, err = r.connectionDial(r.metadata.connectionURI())
	if err != nil {
		r.reset()

		return err
	}

	if r.metadata.publisherConfirm {
		err = r.channel.Confirm(false)
		if err != nil {
			r.reset()

			return err
		}
	}

	r.connectionCount++

	r.logger.Infof("%s connected with connectionCount=%d", logMessagePrefix, r.connectionCount)

	return nil
}

func (r *rabbitMQ) publishSync(req *pubsub.PublishRequest) (rabbitMQChannelBroker, int, error) {
	r.channelMutex.Lock()
	defer r.channelMutex.Unlock()

	if r.channel == nil {
		return r.channel, r.connectionCount, errors.New(errorChannelNotInitialized)
	}

	if err := r.ensureExchangeDeclared(r.channel, req.Topic, r.metadata.exchangeKind); err != nil {
		r.logger.Errorf("%s publishing to %s failed in ensureExchangeDeclared: %v", logMessagePrefix, req.Topic, err)

		return r.channel, r.connectionCount, err
	}
	routingKey := ""
	if val, ok := req.Metadata[reqMetadataRoutingKey]; ok && val != "" {
		routingKey = val
	}

	ttl, ok, err := contribMetadata.TryGetTTL(req.Metadata)
	if err != nil {
		r.logger.Warnf("%s publishing to %s failed parse TryGetTTL: %v, it is ignored.", logMessagePrefix, req.Topic, err)
	}
	var expiration string
	if ok {
		// RabbitMQ expects the duration in ms
		expiration = strconv.FormatInt(ttl.Milliseconds(), 10)
	} else if r.metadata.defaultQueueTTL != nil {
		expiration = strconv.FormatInt(r.metadata.defaultQueueTTL.Milliseconds(), 10)
	}

	confirm, err := r.channel.PublishWithDeferredConfirmWithContext(r.ctx, req.Topic, routingKey, false, false, amqp.Publishing{
		ContentType:  "text/plain",
		Body:         req.Data,
		DeliveryMode: r.metadata.deliveryMode,
		Expiration:   expiration,
	})
	if err != nil {
		r.logger.Errorf("%s publishing to %s failed in channel.Publish: %v", logMessagePrefix, req.Topic, err)

		return r.channel, r.connectionCount, err
	}

	// confirm will be nil if are not requesting publish confirmations
	if confirm != nil {
		// Blocks until the server confirms
		ok := confirm.Wait()
		if !ok {
			err = fmt.Errorf("did not receive confirmation of publishing")
			r.logger.Errorf("%s publishing to %s failed: %v", logMessagePrefix, req.Topic, err)
		}
	}

	return r.channel, r.connectionCount, nil
}

func (r *rabbitMQ) Publish(req *pubsub.PublishRequest) error {
	r.logger.Debugf("%s publishing message to %s", logMessagePrefix, req.Topic)

	attempt := 0
	for {
		attempt++
		channel, connectionCount, err := r.publishSync(req)
		if err == nil {
			return nil
		}
		if attempt >= publishMaxRetries {
			r.logger.Errorf("%s publishing failed: %v", logMessagePrefix, err)
			return err
		}
		if mustReconnect(channel, err) {
			r.logger.Warnf("%s publisher is reconnecting in %s ...", logMessagePrefix, r.metadata.reconnectWait.String())
			time.Sleep(r.metadata.reconnectWait)
			r.reconnect(connectionCount)
		} else {
			r.logger.Warnf("%s publishing attempt (%d/%d) failed: %v", logMessagePrefix, attempt, publishMaxRetries, err)
			time.Sleep(publishRetryWaitSeconds * time.Second)
		}
	}
}

func (r *rabbitMQ) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if r.metadata.consumerID == "" {
		return errors.New("consumerID is required for subscriptions")
	}

	queueName := fmt.Sprintf("%s-%s", r.metadata.consumerID, req.Topic)
	r.logger.Infof("%s subscribe to topic/queue '%s/%s'", logMessagePrefix, req.Topic, queueName)

	// Do not set a timeout on the context, as we're just waiting for the first ack; we're using a semaphore instead
	ackCh := make(chan struct{}, 1)
	defer close(ackCh)
	go r.subscribeForever(ctx, req, queueName, handler, ackCh)

	// Wait for the ack for 1 minute or return an error
	select {
	case <-time.After(time.Minute):
		return fmt.Errorf("failed to subscribe to %s", queueName)
	case <-ackCh:
		return nil
	}
}

// this function call should be wrapped by channelMutex.
func (r *rabbitMQ) prepareSubscription(channel rabbitMQChannelBroker, req pubsub.SubscribeRequest, queueName string) (*amqp.Queue, error) {
	err := r.ensureExchangeDeclared(channel, req.Topic, r.metadata.exchangeKind)
	if err != nil {
		r.logger.Errorf("%s prepareSubscription for topic/queue '%s/%s' failed in ensureExchangeDeclared: %v", logMessagePrefix, req.Topic, queueName, err)

		return nil, err
	}

	r.logger.Infof("%s declaring queue '%s'", logMessagePrefix, queueName)
	var args amqp.Table
	if r.metadata.enableDeadLetter {
		// declare dead letter exchange
		dlxName := fmt.Sprintf(defaultDeadLetterExchangeFormat, queueName)
		dlqName := fmt.Sprintf(defaultDeadLetterQueueFormat, queueName)
		err = r.ensureExchangeDeclared(channel, dlxName, fanoutExchangeKind)
		if err != nil {
			r.logger.Errorf("%s prepareSubscription for topic/queue '%s/%s' failed in ensureExchangeDeclared: %v", logMessagePrefix, req.Topic, dlqName, err)

			return nil, err
		}
		var q amqp.Queue
		dlqArgs := r.metadata.formatQueueDeclareArgs(nil)
		// dead letter queue use lazy mode, keeping as many messages as possible on disk to reduce RAM usage
		dlqArgs[argQueueMode] = queueModeLazy
		q, err = channel.QueueDeclare(dlqName, true, r.metadata.deleteWhenUnused, false, false, dlqArgs)
		if err != nil {
			r.logger.Errorf("%s prepareSubscription for topic/queue '%s/%s' failed in channel.QueueDeclare: %v", logMessagePrefix, req.Topic, dlqName, err)

			return nil, err
		}
		err = channel.QueueBind(q.Name, "", dlxName, false, nil)
		if err != nil {
			r.logger.Errorf("%s prepareSubscription for topic/queue '%s/%s' failed in channel.QueueBind: %v", logMessagePrefix, req.Topic, dlqName, err)

			return nil, err
		}
		r.logger.Infof("%s declared dead letter exchange for queue '%s' bind dead letter queue '%s' to dead letter exchange '%s'", logMessagePrefix, queueName, dlqName, dlxName)
		args = amqp.Table{argDeadLetterExchange: dlxName}
	}
	args = r.metadata.formatQueueDeclareArgs(args)
	q, err := channel.QueueDeclare(queueName, r.metadata.durable, r.metadata.deleteWhenUnused, false, false, args)
	if err != nil {
		r.logger.Errorf("%s prepareSubscription for topic/queue '%s/%s' failed in channel.QueueDeclare: %v", logMessagePrefix, req.Topic, queueName, err)

		return nil, err
	}

	if r.metadata.prefetchCount > 0 {
		r.logger.Infof("%s setting prefetch count to %s", logMessagePrefix, strconv.Itoa(int(r.metadata.prefetchCount)))
		err = channel.Qos(int(r.metadata.prefetchCount), 0, false)
		if err != nil {
			r.logger.Errorf("%s prepareSubscription for topic/queue '%s/%s' failed in channel.Qos: %v", logMessagePrefix, req.Topic, queueName, err)

			return nil, err
		}
	}

	metadataRoutingKey := ""
	if val, ok := req.Metadata[reqMetadataRoutingKey]; ok && val != "" {
		metadataRoutingKey = val
	}
	routingKeys := strings.Split(metadataRoutingKey, ",")
	for i := 0; i < len(routingKeys); i++ {
		routingKey := routingKeys[i]
		r.logger.Debugf("%s binding queue '%s' to exchange '%s' with routing key '%s'", logMessagePrefix, q.Name, req.Topic, routingKey)
		err = channel.QueueBind(q.Name, routingKey, req.Topic, false, nil)
		if err != nil {
			r.logger.Errorf("%s prepareSubscription for topic/queue '%s/%s' failed in channel.QueueBind: %v", logMessagePrefix, req.Topic, queueName, err)

			return nil, err
		}
	}

	return &q, nil
}

func (r *rabbitMQ) ensureSubscription(req pubsub.SubscribeRequest, queueName string) (rabbitMQChannelBroker, int, *amqp.Queue, error) {
	r.channelMutex.RLock()
	defer r.channelMutex.RUnlock()

	if r.channel == nil {
		return nil, r.connectionCount, nil, errors.New(errorChannelNotInitialized)
	}

	q, err := r.prepareSubscription(r.channel, req, queueName)

	return r.channel, r.connectionCount, q, err
}

func (r *rabbitMQ) subscribeForever(ctx context.Context, req pubsub.SubscribeRequest, queueName string, handler pubsub.Handler, ackCh chan struct{}) {
	for {
		var (
			err             error
			errFuncName     string
			connectionCount int
			channel         rabbitMQChannelBroker
			q               *amqp.Queue
			msgs            <-chan amqp.Delivery
		)
		for {
			channel, connectionCount, q, err = r.ensureSubscription(req, queueName)
			if err != nil {
				errFuncName = "ensureSubscription"
				break
			}

			msgs, err = channel.Consume(
				q.Name,
				queueName,          // consumerId
				r.metadata.autoAck, // autoAck
				false,              // exclusive
				false,              // noLocal
				false,              // noWait
				nil,
			)
			if err != nil {
				errFuncName = "channel.Consume"
				break
			}

			// one-time notification on successful subscribe
			if ackCh != nil {
				ackCh <- struct{}{}
				ackCh = nil
			}

			err = r.listenMessages(ctx, channel, msgs, req.Topic, handler)
			if err != nil {
				errFuncName = "listenMessages"
				break
			}
		}

		if err == context.Canceled || err == context.DeadlineExceeded {
			// Subscription context was canceled
			r.logger.Infof("%s subscription for %s has context canceled", logMessagePrefix, queueName)
			return
		}

		if r.isStopped() {
			r.logger.Infof("%s subscriber for %s is stopped", logMessagePrefix, queueName)
			return
		}

		// print the error if the subscriber is running.
		if err != nil {
			r.logger.Errorf("%s error in subscriber for %s in %s: %v", logMessagePrefix, queueName, errFuncName, err)
		}

		if mustReconnect(channel, err) {
			r.logger.Warnf("%s subscriber is reconnecting in %s ...", logMessagePrefix, r.metadata.reconnectWait.String())
			time.Sleep(r.metadata.reconnectWait)
			r.reconnect(connectionCount)
		}
	}
}

func (r *rabbitMQ) listenMessages(ctx context.Context, channel rabbitMQChannelBroker, msgCh <-chan amqp.Delivery, topic string, handler pubsub.Handler) error {
	var err error
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, more := <-msgCh:
			// Handle case of channel closed
			if !more {
				r.logger.Debugf("%s subscriber channel closed for topic %s", logMessagePrefix, topic)
				return nil
			}

			switch r.metadata.concurrency {
			case pubsub.Single:
				err = r.handleMessage(ctx, d, topic, handler)
			case pubsub.Parallel:
				go func(d amqp.Delivery) {
					err = r.handleMessage(ctx, d, topic, handler)
				}(d)
			}
			if err != nil && mustReconnect(channel, err) {
				return err
			}
		}
	}
}

func (r *rabbitMQ) handleMessage(ctx context.Context, d amqp.Delivery, topic string, handler pubsub.Handler) error {
	pubsubMsg := &pubsub.NewMessage{
		Data:  d.Body,
		Topic: topic,
	}

	err := handler(ctx, pubsubMsg)

	if err != nil {
		r.logger.Errorf("%s handling message from topic '%s', %s", errorMessagePrefix, topic, err)

		if !r.metadata.autoAck {
			// if message is not auto acked we need to ack/nack
			r.logger.Debugf("%s nacking message '%s' from topic '%s', requeue=%t", logMessagePrefix, d.MessageId, topic, r.metadata.requeueInFailure)
			if err = d.Nack(false, r.metadata.requeueInFailure); err != nil {
				r.logger.Errorf("%s error nacking message '%s' from topic '%s', %s", logMessagePrefix, d.MessageId, topic, err)
			}
		}
	} else if !r.metadata.autoAck {
		// if message is not auto acked we need to ack/nack
		r.logger.Debugf("%s acking message '%s' from topic '%s'", logMessagePrefix, d.MessageId, topic)
		if err = d.Ack(false); err != nil {
			r.logger.Errorf("%s error acking message '%s' from topic '%s', %s", logMessagePrefix, d.MessageId, topic, err)
		}
	}

	return err
}

// this function call should be wrapped by channelMutex.
func (r *rabbitMQ) ensureExchangeDeclared(channel rabbitMQChannelBroker, exchange, exchangeKind string) error {
	if !r.containsExchange(exchange) {
		r.logger.Debugf("%s declaring exchange '%s' of kind '%s'", logMessagePrefix, exchange, exchangeKind)
		err := channel.ExchangeDeclare(exchange, exchangeKind, true, false, false, false, nil)
		if err != nil {
			r.logger.Errorf("%s ensureExchangeDeclared: channel.ExchangeDeclare failed: %v", logMessagePrefix, err)

			return err
		}

		r.putExchange(exchange)
	}

	return nil
}

// this function call should be wrapped by channelMutex.
func (r *rabbitMQ) containsExchange(exchange string) bool {
	_, exists := r.declaredExchanges[exchange]

	return exists
}

// this function call should be wrapped by channelMutex.
func (r *rabbitMQ) putExchange(exchange string) {
	r.declaredExchanges[exchange] = true
}

// this function call should be wrapped by channelMutex.
func (r *rabbitMQ) reset() (err error) {
	if len(r.declaredExchanges) > 0 {
		r.declaredExchanges = make(map[string]bool)
	}

	if r.channel != nil {
		if err = r.channel.Close(); err != nil {
			r.logger.Errorf("%s reset: channel.Close() failed: %v", logMessagePrefix, err)
		}
		r.channel = nil
	}
	if r.connection != nil {
		if err2 := r.connection.Close(); err2 != nil {
			r.logger.Errorf("%s reset: connection.Close() failed: %v", logMessagePrefix, err2)
			if err == nil {
				err = err2
			}
		}
		r.connection = nil
	}

	return
}

func (r *rabbitMQ) isStopped() bool {
	return r.ctx.Err() != nil
}

func (r *rabbitMQ) Close() error {
	r.channelMutex.Lock()
	defer r.channelMutex.Unlock()

	r.cancel()
	err := r.reset()

	return err
}

func (r *rabbitMQ) Features() []pubsub.Feature {
	return []pubsub.Feature{pubsub.FeatureMessageTTL}
}

func mustReconnect(channel rabbitMQChannelBroker, err error) bool {
	if channel == nil {
		return true
	}

	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), errorChannelConnection)
}
