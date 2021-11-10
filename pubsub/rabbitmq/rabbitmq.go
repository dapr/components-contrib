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
	errorChannelConnection          = "channel/connection is not open"
	errorUnexpectedCommand          = "unexpected command received"
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

	defaultReconnectWaitSeconds = 10
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

func (r *rabbitMQ) getChannel() (rabbitMQChannelBroker, int) {
	r.channelMutex.RLock()
	defer r.channelMutex.RUnlock()

	return r.channel, r.connectionCount
}

func (r *rabbitMQ) reconnect(connectionCount int) error {
	r.channelMutex.Lock()
	defer r.channelMutex.Unlock()

	return r.doReconnect(connectionCount)
}

func (r *rabbitMQ) doReconnect(connectionCount int) error {
	if r.stopped {
		// Do not reconnect on stopped service.
		return errors.New("cannot connect after component is stopped")
	}

	if connectionCount != r.connectionCount {
		// Reconnection request is old.
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

	r.logger.Infof("%s connected", logMessagePrefix)

	return nil
}

func (r *rabbitMQ) getChannelOrReconnect() (rabbitMQChannelBroker, int, error) {
	r.channelMutex.Lock()
	defer r.channelMutex.Unlock()

	if r.channel != nil {
		return r.channel, r.connectionCount, nil
	}

	r.logger.Warnf("%s reconnecting ...", logMessagePrefix)
	err := r.doReconnect(r.connectionCount)

	return r.channel, r.connectionCount, err
}

func (r *rabbitMQ) Publish(req *pubsub.PublishRequest) error {
	channel, connectionCount, err := r.getChannelOrReconnect()
	if err != nil {
		return err
	}

	err = r.ensureExchangeDeclared(channel, req.Topic)
	if err != nil {
		return err
	}

	r.logger.Debugf("%s publishing message to topic '%s'", logMessagePrefix, req.Topic)

	err = channel.Publish(req.Topic, "", false, false, amqp.Publishing{
		ContentType:  "text/plain",
		Body:         req.Data,
		DeliveryMode: r.metadata.deliveryMode,
	})

	if err != nil {
		if mustReconnect(channel, err) {
			r.logger.Warnf("%s pubsub publisher for %s is reconnecting ...", logMessagePrefix, req.Topic)
			r.reconnect(connectionCount)
		}

		return err
	}

	return nil
}

func (r *rabbitMQ) Subscribe(req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if r.metadata.consumerID == "" {
		return errors.New("consumerID is required for subscriptions")
	}

	queueName := fmt.Sprintf("%s-%s", r.metadata.consumerID, req.Topic)

	// // By the time Subscribe exits, the subscription should be active.
	if _, _, _, err := r.ensureSubscription(req, queueName); err != nil {
		return err
	}

	go r.subscribeForever(req, queueName, handler)

	return nil
}

func (r *rabbitMQ) prepareSubscription(channel rabbitMQChannelBroker, req pubsub.SubscribeRequest, queueName string) (*amqp.Queue, error) {
	err := r.ensureExchangeDeclared(channel, req.Topic)
	if err != nil {
		return nil, err
	}

	r.logger.Debugf("%s declaring queue '%s'", logMessagePrefix, queueName)
	var args amqp.Table
	if r.metadata.enableDeadLetter {
		// declare dead letter exchange
		dlxName := fmt.Sprintf(defaultDeadLetterExchangeFormat, queueName)
		dlqName := fmt.Sprintf(defaultDeadLetterQueueFormat, queueName)
		err = r.ensureExchangeDeclared(channel, dlxName)
		if err != nil {
			return nil, err
		}
		var q amqp.Queue
		dlqArgs := r.metadata.formatQueueDeclareArgs(nil)
		// dead letter queue use lazy mode, keeping as many messages as possible on disk to reduce RAM usage
		dlqArgs[argQueueMode] = queueModeLazy
		q, err = channel.QueueDeclare(dlqName, true, r.metadata.deleteWhenUnused, false, false, dlqArgs)
		if err != nil {
			return nil, err
		}
		err = channel.QueueBind(q.Name, "", dlxName, false, nil)
		if err != nil {
			return nil, err
		}
		r.logger.Debugf("declared dead letter exchange for queue '%s' bind dead letter queue '%s' to dead letter exchange '%s'", queueName, dlqName, dlxName)
		args = amqp.Table{argDeadLetterExchange: dlxName}
	}
	args = r.metadata.formatQueueDeclareArgs(args)
	q, err := channel.QueueDeclare(queueName, r.metadata.durable, r.metadata.deleteWhenUnused, false, false, args)
	if err != nil {
		return nil, err
	}

	if r.metadata.prefetchCount > 0 {
		r.logger.Debugf("setting prefetch count to %s", strconv.Itoa(int(r.metadata.prefetchCount)))
		err = channel.Qos(int(r.metadata.prefetchCount), 0, false)
		if err != nil {
			return nil, err
		}
	}

	r.logger.Debugf("%s binding queue '%s' to exchange '%s'", logMessagePrefix, q.Name, req.Topic)
	err = channel.QueueBind(q.Name, "", req.Topic, false, nil)
	if err != nil {
		return nil, err
	}

	return &q, nil
}

func (r *rabbitMQ) ensureSubscription(req pubsub.SubscribeRequest,
	queueName string) (rabbitMQChannelBroker, int, *amqp.Queue, error) {
	channel, connectionCount := r.getChannel()
	if channel == nil {
		return nil, 0, nil, errors.New("channel not initialized")
	}

	q, err := r.prepareSubscription(channel, req, queueName)

	return channel, connectionCount, q, err
}

func (r *rabbitMQ) subscribeForever(req pubsub.SubscribeRequest, queueName string, handler pubsub.Handler) {
	for {
		var (
			err             error
			connectionCount int
			channel         rabbitMQChannelBroker
			q               *amqp.Queue
			msgs            <-chan amqp.Delivery
		)
		for {
			channel, connectionCount, q, err = r.ensureSubscription(req, queueName)
			if err != nil {
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
				break
			}

			err = r.listenMessages(channel, msgs, req.Topic, handler)
			if err != nil {
				break
			}
		}

		if r.isStopped() {
			return
		}

		r.logger.Errorf("%s error in subscription for %s: %v", logMessagePrefix, queueName, err)

		if mustReconnect(channel, err) {
			time.Sleep(r.metadata.reconnectWait)
			r.logger.Warnf("%s pubsub subscription for %s is reconnecting ...", logMessagePrefix, queueName)
			r.reconnect(connectionCount)
		}
	}
}

func (r *rabbitMQ) listenMessages(channel rabbitMQChannelBroker, msgs <-chan amqp.Delivery, topic string, handler pubsub.Handler) error {
	var err error
	for d := range msgs {
		switch r.metadata.concurrency {
		case pubsub.Single:
			err = r.handleMessage(channel, d, topic, handler)
		case pubsub.Parallel:
			go func(channel rabbitMQChannelBroker, d amqp.Delivery, topic string, handler pubsub.Handler) {
				err = r.handleMessage(channel, d, topic, handler)
			}(channel, d, topic, handler)
		}
		if (err != nil) && mustReconnect(channel, err) {
			return err
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

func (r *rabbitMQ) ensureExchangeDeclared(channel rabbitMQChannelBroker, exchange string) error {
	if !r.containsExchange(exchange) {
		r.logger.Debugf("%s declaring exchange '%s' of kind '%s'", logMessagePrefix, exchange, fanoutExchangeKind)
		err := channel.ExchangeDeclare(exchange, fanoutExchangeKind, true, false, false, false, nil)
		if err != nil {
			return err
		}

		r.putExchange(exchange)
	}

	return nil
}

func (r *rabbitMQ) containsExchange(exchange string) bool {
	r.channelMutex.RLock()
	defer r.channelMutex.RUnlock()

	_, exists := r.declaredExchanges[exchange]

	return exists
}

func (r *rabbitMQ) putExchange(exchange string) {
	r.channelMutex.Lock()
	defer r.channelMutex.Unlock()

	r.declaredExchanges[exchange] = true
}

func (r *rabbitMQ) reset() (err error) {
	if len(r.declaredExchanges) > 0 {
		r.declaredExchanges = make(map[string]bool)
	}

	if r.channel != nil {
		err = r.channel.Close()
		r.channel = nil
	}
	if r.connection != nil {
		err2 := r.connection.Close()
		r.connection = nil
		if err == nil {
			err = err2
		}
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

func mustReconnect(channel rabbitMQChannelBroker, err error) bool {
	if channel == nil {
		return true
	}

	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), errorChannelConnection) || strings.Contains(err.Error(), errorUnexpectedCommand)
}
