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

	"github.com/streadway/amqp"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

const (
	fanoutExchangeKind              = "fanout"
	logMessagePrefix                = "rabbitmq pub/sub:"
	errorMessagePrefix              = "rabbitmq pub/sub error:"
	errorChannelNotInitialized      = "channel not initialized"
	errorChannelConnection          = "channel/connection is not open"
	defaultDeadLetterExchangeFormat = "dlx-%s"
	defaultDeadLetterQueueFormat    = "dlq-%s"

	metadataHostKey              = "host"
	metadataConsumerIDKey        = "consumerID"
	metadataDurable              = "durable"
	metadataDeleteWhenUnusedKey  = "deletedWhenUnused"
	metadataAutoAckKey           = "autoAck"
	metadataDeliveryModeKey      = "deliveryMode"
	metadataRequeueInFailureKey  = "requeueInFailure"
	metadataReconnectWaitSeconds = "reconnectWaitSeconds"
	metadataEnableDeadLetter     = "enableDeadLetter"
	metadataMaxLen               = "maxLen"
	metadataMaxLenBytes          = "maxLenBytes"

	defaultReconnectWaitSeconds = 3
	publishMaxRetries           = 3
	publishRetryWaitSeconds     = 2
	metadataPrefetchCount       = "prefetchCount"

	argQueueMode          = "x-queue-mode"
	argMaxLength          = "x-max-length"
	argMaxLengthBytes     = "x-max-length-bytes"
	argDeadLetterExchange = "x-dead-letter-exchange"
	queueModeLazy         = "lazy"
)

// RabbitMQ allows sending/receiving messages in pub/sub format.
type rabbitMQ struct {
	connection        rabbitMQConnectionBroker
	channel           rabbitMQChannelBroker
	channelMutex      sync.RWMutex
	connectionCount   int
	stopped           bool
	metadata          *metadata
	declaredExchanges map[string]bool

	connectionDial func(host string) (rabbitMQConnectionBroker, rabbitMQChannelBroker, error)

	logger        logger.Logger
	backOffConfig retry.Config
	ctx           context.Context
	cancel        context.CancelFunc
}

// interface used to allow unit testing.
type rabbitMQChannelBroker interface {
	Publish(exchange string, key string, mandatory bool, immediate bool, msg amqp.Publishing) error
	QueueDeclare(name string, durable bool, autoDelete bool, exclusive bool, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name string, key string, exchange string, noWait bool, args amqp.Table) error
	Consume(queue string, consumer string, autoAck bool, exclusive bool, noLocal bool, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Nack(tag uint64, multiple bool, requeue bool) error
	Ack(tag uint64, multiple bool) error
	ExchangeDeclare(name string, kind string, durable bool, autoDelete bool, internal bool, noWait bool, args amqp.Table) error
	Qos(prefetchCount, prefetchSize int, global bool) error
	Close() error
}

// interface used to allow unit testing.
type rabbitMQConnectionBroker interface {
	Close() error
}

type ErrorCollection struct {
	errors []error
	mux    sync.Mutex
}

func (c *ErrorCollection) Append(e error, stopCh chan struct{}) {
	c.mux.Lock()
	if len(c.errors) == 0 {
		stopCh <- struct{}{}
		close(stopCh)
	}
	c.errors = append(c.errors, e)
	c.mux.Unlock()
}

func (c *ErrorCollection) GetErrors() []error {
	c.mux.Lock()
	defer c.mux.Unlock()
	return c.errors
}

// NewRabbitMQ creates a new RabbitMQ pub/sub.
func NewRabbitMQ(logger logger.Logger) pubsub.PubSub {
	return &rabbitMQ{
		declaredExchanges: make(map[string]bool),
		stopped:           false,
		logger:            logger,
		connectionDial:    dial,
	}
}

func dial(host string) (rabbitMQConnectionBroker, rabbitMQChannelBroker, error) {
	conn, err := amqp.Dial(host)
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
	meta, err := createMetadata(metadata)
	if err != nil {
		return err
	}

	// Default retry configuration is used if no backOff properties are set.
	// backOff max retry config is set to 0, which means not to retry by default.
	r.backOffConfig = retry.DefaultConfigWithNoRetry()
	if err := retry.DecodeConfigWithPrefix(
		&r.backOffConfig,
		metadata.Properties,
		"backOff"); err != nil {
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

	return r.doReconnect(connectionCount)
}

// this function call should be wrapped by channelMutex.
func (r *rabbitMQ) doReconnect(connectionCount int) error {
	if r.stopped {
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

	r.connection, r.channel, err = r.connectionDial(r.metadata.host)
	if err != nil {
		r.reset()

		return err
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

	if err := r.ensureExchangeDeclared(r.channel, req.Topic); err != nil {
		r.logger.Errorf("%s publishing to %s failed in ensureExchangeDeclared: %v", logMessagePrefix, req.Topic, err)

		return r.channel, r.connectionCount, err
	}

	if err := r.channel.Publish(req.Topic, "", false, false, amqp.Publishing{
		ContentType:  "text/plain",
		Body:         req.Data,
		DeliveryMode: r.metadata.deliveryMode,
	}); err != nil {
		r.logger.Errorf("%s publishing to %s failed in channel.Publish: %v", logMessagePrefix, req.Topic, err)

		return r.channel, r.connectionCount, err
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
		if mustReconnect(channel, []error{err}) {
			r.logger.Warnf("%s publisher is reconnecting in %s ...", logMessagePrefix, r.metadata.reconnectWait.String())
			time.Sleep(r.metadata.reconnectWait)
			r.reconnect(connectionCount)
		} else {
			r.logger.Warnf("%s publishing attempt (%d/%d) failed: %v", logMessagePrefix, attempt, publishMaxRetries, err)
			time.Sleep(publishRetryWaitSeconds * time.Second)
		}
	}
}

func (r *rabbitMQ) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if r.metadata.consumerID == "" {
		return errors.New("consumerID is required for subscriptions")
	}

	queueName := fmt.Sprintf("%s-%s", r.metadata.consumerID, req.Topic)
	r.logger.Infof("%s subscribe to topic/queue '%s/%s'", logMessagePrefix, req.Topic, queueName)

	ackCh := make(chan struct{}, 1)
	ctx, cancel := context.WithTimeout(r.ctx, time.Minute)
	defer cancel()

	go r.subscribeForever(req, queueName, handler, ackCh)

	select {
	case <-ctx.Done():
		return fmt.Errorf("failed to subscribe to %s", queueName)
	case <-ackCh:
		return nil
	}
}

// this function call should be wrapped by channelMutex.
func (r *rabbitMQ) prepareSubscription(channel rabbitMQChannelBroker, req pubsub.SubscribeRequest, queueName string) (*amqp.Queue, error) {
	err := r.ensureExchangeDeclared(channel, req.Topic)
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
		err = r.ensureExchangeDeclared(channel, dlxName)
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

	r.logger.Infof("%s binding queue '%s' to exchange '%s'", logMessagePrefix, q.Name, req.Topic)
	err = channel.QueueBind(q.Name, "", req.Topic, false, nil)
	if err != nil {
		r.logger.Errorf("%s prepareSubscription for topic/queue '%s/%s' failed in channel.QueueBind: %v", logMessagePrefix, req.Topic, queueName, err)

		return nil, err
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

func (r *rabbitMQ) subscribeForever(req pubsub.SubscribeRequest, queueName string, handler pubsub.Handler, ackCh chan struct{}) {
	// one-time notification on successful subscribe
	var subscribed bool

	for {
		var (
			errs            []error
			errFuncName     string
			connectionCount int
			channel         rabbitMQChannelBroker
			q               *amqp.Queue
			msgs            <-chan amqp.Delivery
		)
		for {
			var err error
			channel, connectionCount, q, err = r.ensureSubscription(req, queueName)
			if err != nil {
				errs = append(errs, err)
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
				errs = append(errs, err)
				errFuncName = "channel.Consume"
				break
			}

			if !subscribed {
				subscribed = true
				ackCh <- struct{}{}
				ackCh = nil
			}

			errs = r.listenMessages(channel, msgs, req.Topic, handler)
			if len(errs) != 0 {
				errFuncName = "listenMessages"
				break
			}
		}

		if r.isStopped() {
			r.logger.Infof("%s subscriber for %s is stopped", logMessagePrefix, queueName)

			return
		}

		// print errors if the subscriber is running.
		if len(errs) != 0 {
			for i := range errs {
				r.logger.Errorf("%s error in subscriber for %s in %s: %v", logMessagePrefix, queueName, errFuncName, errs[i])
			}
		}

		if mustReconnect(channel, errs) {
			r.logger.Warnf("%s subscriber is reconnecting in %s ...", logMessagePrefix, r.metadata.reconnectWait.String())
			time.Sleep(r.metadata.reconnectWait)
			r.reconnect(connectionCount)
		}
	}
}

func (r *rabbitMQ) listenMessages(channel rabbitMQChannelBroker, msgs <-chan amqp.Delivery, topic string, handler pubsub.Handler) []error {
	switch r.metadata.concurrency {
	case pubsub.Single:
		for d := range msgs {
			if e := r.handleMessage(channel, d, topic, handler); e != nil {
				if mustReconnect(channel, []error{e}) {
					return []error{e}
				}
			}
		}
	case pubsub.Parallel:
		ec := ErrorCollection{errors: []error{}}
		stopCh := make(chan struct{})
		var wg sync.WaitGroup
		for {
			select {
			case <-stopCh:
				wg.Wait()
				return ec.GetErrors()
			case d := <-msgs:
				wg.Add(1)
				go func(channel rabbitMQChannelBroker, d amqp.Delivery, topic string, handler pubsub.Handler) {
					if e := r.handleMessage(channel, d, topic, handler); e != nil {
						ec.Append(e, stopCh)
					}
					wg.Done()
				}(channel, d, topic, handler)
			}
		}
	}

	return nil
}

func (r *rabbitMQ) handleMessage(channel rabbitMQChannelBroker, d amqp.Delivery, topic string, handler pubsub.Handler) error {
	pubsubMsg := &pubsub.NewMessage{
		Data:  d.Body,
		Topic: topic,
	}

	b := r.backOffConfig.NewBackOffWithContext(r.ctx)
	err := retry.NotifyRecover(func() error {
		return handler(r.ctx, pubsubMsg)
	}, b, func(err error, d time.Duration) {
		r.logger.Errorf("%s error handling message from topic '%s', %s", logMessagePrefix, topic, err)
	}, func() {
		r.logger.Infof("%s successfully processed message after it previously failed from topic '%s'", logMessagePrefix, topic)
	})

	//nolint:nestif
	// if message is not auto acked we need to ack/nack
	if !r.metadata.autoAck {
		if err != nil {
			requeue := r.metadata.requeueInFailure && !d.Redelivered

			r.logger.Debugf("%s nacking message '%s' from topic '%s', requeue=%t", logMessagePrefix, d.MessageId, topic, requeue)
			if err = d.Nack(false, requeue); err != nil {
				r.logger.Errorf("%s error nacking message '%s' from topic '%s', %s", logMessagePrefix, d.MessageId, topic, err)
			}
		} else {
			r.logger.Debugf("%s acking message '%s' from topic '%s'", logMessagePrefix, d.MessageId, topic)
			if err = d.Ack(false); err != nil {
				r.logger.Errorf("%s error acking message '%s' from topic '%s', %s", logMessagePrefix, d.MessageId, topic, err)
			}
		}
	}

	return err
}

// this function call should be wrapped by channelMutex.
func (r *rabbitMQ) ensureExchangeDeclared(channel rabbitMQChannelBroker, exchange string) error {
	if !r.containsExchange(exchange) {
		r.logger.Debugf("%s declaring exchange '%s' of kind '%s'", logMessagePrefix, exchange, fanoutExchangeKind)
		err := channel.ExchangeDeclare(exchange, fanoutExchangeKind, true, false, false, false, nil)
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
	r.channelMutex.RLock()
	defer r.channelMutex.RUnlock()

	return r.stopped
}

func (r *rabbitMQ) Close() error {
	r.channelMutex.Lock()
	defer r.channelMutex.Unlock()

	err := r.reset()
	r.stopped = true
	r.cancel()

	return err
}

func (r *rabbitMQ) Features() []pubsub.Feature {
	return nil
}

func mustReconnect(channel rabbitMQChannelBroker, errs []error) bool {
	if channel == nil {
		return true
	}

	if len(errs) == 0 {
		return false
	}

	for _, err := range errs {
		if strings.Contains(err.Error(), errorChannelConnection) {
			return true
		}
	}

	return false
}
