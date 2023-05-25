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
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
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
	argMaxPriority        = "x-max-priority"
	queueModeLazy         = "lazy"
	reqMetadataRoutingKey = "routingKey"
)

// RabbitMQ allows sending/receiving messages in pub/sub format.
type rabbitMQ struct {
	connection        rabbitMQConnectionBroker
	channel           rabbitMQChannelBroker
	channelMutex      sync.RWMutex
	connectionCount   int
	metadata          *rabbitmqMetadata
	declaredExchanges map[string]bool

	connectionDial func(protocol, uri string, tlsCfg *tls.Config, externalSasl bool) (rabbitMQConnectionBroker, rabbitMQChannelBroker, error)
	closeCh        chan struct{}
	closed         atomic.Bool
	wg             sync.WaitGroup

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
		closeCh:           make(chan struct{}),
	}
}

func dial(protocol, uri string, tlsCfg *tls.Config, externalSasl bool) (rabbitMQConnectionBroker, rabbitMQChannelBroker, error) {
	var (
		conn *amqp.Connection
		ch   *amqp.Channel
		err  error
	)

	if protocol == protocolAMQPS {
		if externalSasl {
			conn, err = amqp.DialTLS_ExternalAuth(uri, tlsCfg)
		} else {
			conn, err = amqp.DialTLS(uri, tlsCfg)
		}
	} else {
		conn, err = amqp.Dial(uri)
	}
	if err != nil {
		return nil, nil, err
	}

	ch, err = conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	return conn, ch, nil
}

// Init does metadata parsing and connection creation.
func (r *rabbitMQ) Init(_ context.Context, metadata pubsub.Metadata) error {
	meta, err := createMetadata(metadata, r.logger)
	if err != nil {
		return err
	}

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

	tlsCfg, err := pubsub.ConvertTLSPropertiesToTLSConfig(r.metadata.TLSProperties)
	if err != nil {
		return err
	}

	r.connection, r.channel, err = r.connectionDial(r.metadata.internalProtocol, r.metadata.connectionURI(), tlsCfg, r.metadata.SaslExternal)
	if err != nil {
		r.reset()

		return err
	}

	if r.metadata.PublisherConfirm {
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

func (r *rabbitMQ) publishSync(ctx context.Context, req *pubsub.PublishRequest) (rabbitMQChannelBroker, int, error) {
	r.channelMutex.Lock()
	defer r.channelMutex.Unlock()

	if r.channel == nil {
		return r.channel, r.connectionCount, errors.New(errorChannelNotInitialized)
	}

	if err := r.ensureExchangeDeclared(r.channel, req.Topic, r.metadata.ExchangeKind, r.metadata.Durable, r.metadata.DeleteWhenUnused); err != nil {
		r.logger.Errorf("%s publishing to %s failed in ensureExchangeDeclared: %v", logMessagePrefix, req.Topic, err)

		return r.channel, r.connectionCount, err
	}
	routingKey := ""
	if val, ok := req.Metadata[reqMetadataRoutingKey]; ok && val != "" {
		routingKey = val
	}

	ttl, ok, err := metadata.TryGetTTL(req.Metadata)
	if err != nil {
		r.logger.Warnf("%s publishing to %s failed to parse TryGetTTL: %v, it is ignored.", logMessagePrefix, req.Topic, err)
	}
	var expiration string
	if ok {
		// RabbitMQ expects the duration in ms
		expiration = strconv.FormatInt(ttl.Milliseconds(), 10)
	} else if r.metadata.DefaultQueueTTL != nil {
		expiration = strconv.FormatInt(r.metadata.DefaultQueueTTL.Milliseconds(), 10)
	}

	p := amqp.Publishing{
		ContentType:  "text/plain",
		Body:         req.Data,
		DeliveryMode: r.metadata.DeliveryMode,
		Expiration:   expiration,
	}

	priority, ok, err := metadata.TryGetPriority(req.Metadata)
	if err != nil {
		r.logger.Warnf("%s publishing to %s failed to parse priority: %v, it is ignored.", logMessagePrefix, req.Topic, err)
	}

	if ok {
		p.Priority = priority
	}

	confirm, err := r.channel.PublishWithDeferredConfirmWithContext(ctx, req.Topic, routingKey, false, false, p)
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

func (r *rabbitMQ) Publish(ctx context.Context, req *pubsub.PublishRequest) error {
	if r.closed.Load() {
		return errors.New("component is closed")
	}

	r.logger.Debugf("%s publishing message to %s", logMessagePrefix, req.Topic)

	attempt := 0
	for {
		attempt++
		channel, connectionCount, err := r.publishSync(ctx, req)
		if err == nil {
			return nil
		}
		if attempt >= publishMaxRetries {
			r.logger.Errorf("%s publishing failed: %v", logMessagePrefix, err)
			return err
		}
		if mustReconnect(channel, err) {
			r.logger.Warnf("%s publisher is reconnecting in %s ...", logMessagePrefix, r.metadata.ReconnectWait.String())
			select {
			case <-time.After(r.metadata.ReconnectWait):
			case <-ctx.Done():
				return nil
			}

			r.reconnect(connectionCount)
		} else {
			r.logger.Warnf("%s publishing attempt (%d/%d) failed: %v", logMessagePrefix, attempt, publishMaxRetries, err)
			select {
			case <-time.After(publishRetryWaitSeconds * time.Second):
			case <-ctx.Done():
				return nil
			}
		}
	}
}

func (r *rabbitMQ) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if r.closed.Load() {
		return errors.New("component is closed")
	}

	if r.metadata.ConsumerID == "" {
		return errors.New("consumerID is required for subscriptions")
	}

	queueName := fmt.Sprintf("%s-%s", r.metadata.ConsumerID, req.Topic)
	r.logger.Infof("%s subscribe to topic/queue '%s/%s'", logMessagePrefix, req.Topic, queueName)

	// Do not set a timeout on the context, as we're just waiting for the first ack; we're using a semaphore instead
	ackCh := make(chan struct{}, 1)
	defer close(ackCh)

	subctx, cancel := context.WithCancel(ctx)
	r.wg.Add(2)
	go func() {
		defer r.wg.Done()
		r.subscribeForever(subctx, req, queueName, handler, ackCh)
	}()
	go func() {
		defer r.wg.Done()
		defer cancel()
		select {
		case <-subctx.Done():
		case <-r.closeCh:
		}
	}()

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
	err := r.ensureExchangeDeclared(channel, req.Topic, r.metadata.ExchangeKind, r.metadata.Durable, r.metadata.DeleteWhenUnused)
	if err != nil {
		r.logger.Errorf("%s prepareSubscription for topic/queue '%s/%s' failed in ensureExchangeDeclared: %v", logMessagePrefix, req.Topic, queueName, err)

		return nil, err
	}

	r.logger.Infof("%s declaring queue '%s'", logMessagePrefix, queueName)
	var args amqp.Table
	if r.metadata.EnableDeadLetter {
		// declare dead letter exchange
		dlxName := fmt.Sprintf(defaultDeadLetterExchangeFormat, queueName)
		dlqName := fmt.Sprintf(defaultDeadLetterQueueFormat, queueName)
		// dead letter exchange is always durable
		err = r.ensureExchangeDeclared(channel, dlxName, fanoutExchangeKind, true, r.metadata.DeleteWhenUnused)
		if err != nil {
			r.logger.Errorf("%s prepareSubscription for topic/queue '%s/%s' failed in ensureExchangeDeclared: %v", logMessagePrefix, req.Topic, dlqName, err)

			return nil, err
		}
		var q amqp.Queue
		dlqArgs := r.metadata.formatQueueDeclareArgs(nil)
		// dead letter queue use lazy mode, keeping as many messages as possible on disk to reduce RAM usage
		dlqArgs[argQueueMode] = queueModeLazy
		q, err = channel.QueueDeclare(dlqName, true, r.metadata.DeleteWhenUnused, false, false, dlqArgs)
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

	// use priority queue if configured on subscription
	if val, ok := req.Metadata[metadataMaxPriority]; ok && val != "" {
		parsedVal, pErr := strconv.ParseUint(val, 10, 0)
		if pErr != nil {
			r.logger.Errorf("%s prepareSubscription error: can't parse maxPriority %s value on subscription metadata for topic/queue `%s/%s`: %s", logMessagePrefix, val, req.Topic, queueName, pErr)
			return nil, pErr
		}

		mp := uint8(parsedVal)
		if parsedVal > 255 {
			mp = math.MaxUint8
		}

		args[argMaxPriority] = mp
	}

	q, err := channel.QueueDeclare(queueName, r.metadata.Durable, r.metadata.DeleteWhenUnused, false, false, args)
	if err != nil {
		r.logger.Errorf("%s prepareSubscription for topic/queue '%s/%s' failed in channel.QueueDeclare: %v", logMessagePrefix, req.Topic, queueName, err)

		return nil, err
	}

	if r.metadata.PrefetchCount > 0 {
		r.logger.Infof("%s setting prefetch count to %s", logMessagePrefix, strconv.Itoa(int(r.metadata.PrefetchCount)))
		err = channel.Qos(int(r.metadata.PrefetchCount), 0, false)
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
				r.metadata.AutoAck, // autoAck
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
			r.logger.Warnf("%s subscriber is reconnecting in %s ...", logMessagePrefix, r.metadata.ReconnectWait.String())
			select {
			case <-time.After(r.metadata.ReconnectWait):
			case <-ctx.Done():
				r.logger.Infof("%s subscription for %s has context canceled", logMessagePrefix, queueName)
				return
			}
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

			switch r.metadata.Concurrency {
			case pubsub.Single:
				err = r.handleMessage(ctx, d, topic, handler)
				if err != nil && mustReconnect(channel, err) {
					return err
				}
			case pubsub.Parallel:
				r.wg.Add(1)
				go func(d amqp.Delivery) {
					defer r.wg.Done()
					if err := r.handleMessage(ctx, d, topic, handler); err != nil {
						r.logger.Errorf("%s error handling message: %v", logMessagePrefix, err)
					}
				}(d)
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

		if !r.metadata.AutoAck {
			// if message is not auto acked we need to ack/nack
			r.logger.Debugf("%s nacking message '%s' from topic '%s', requeue=%t", logMessagePrefix, d.MessageId, topic, r.metadata.RequeueInFailure)
			if err = d.Nack(false, r.metadata.RequeueInFailure); err != nil {
				r.logger.Errorf("%s error nacking message '%s' from topic '%s', %s", logMessagePrefix, d.MessageId, topic, err)
			}
		}
	} else if !r.metadata.AutoAck {
		// if message is not auto acked we need to ack/nack
		r.logger.Debugf("%s acking message '%s' from topic '%s'", logMessagePrefix, d.MessageId, topic)
		if err = d.Ack(false); err != nil {
			r.logger.Errorf("%s error acking message '%s' from topic '%s', %s", logMessagePrefix, d.MessageId, topic, err)
		}
	}

	return err
}

// this function call should be wrapped by channelMutex.
func (r *rabbitMQ) ensureExchangeDeclared(channel rabbitMQChannelBroker, exchange, exchangeKind string, durable bool, autoDelete bool) error {
	if !r.containsExchange(exchange) {
		r.logger.Debugf("%s declaring exchange '%s' of kind '%s'", logMessagePrefix, exchange, exchangeKind)
		err := channel.ExchangeDeclare(exchange, exchangeKind, durable, autoDelete, false, false, nil)
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
	return r.closed.Load()
}

// Close closes the rabbitMQ connection. Blocks until all go routines are done.
func (r *rabbitMQ) Close() error {
	r.channelMutex.Lock()
	defer r.channelMutex.Unlock()

	if r.closed.CompareAndSwap(false, true) {
		close(r.closeCh)
	}

	defer r.wg.Wait()

	return r.reset()
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

// GetComponentMetadata returns the metadata of the component.
func (r *rabbitMQ) GetComponentMetadata() map[string]string {
	metadataStruct := rabbitmqMetadata{}
	metadataInfo := map[string]string{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.PubSubType)
	return metadataInfo
}
