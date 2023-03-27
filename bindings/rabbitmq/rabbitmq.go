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
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/internal/utils"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const (
	host                       = "host"
	queueName                  = "queueName"
	exclusive                  = "exclusive"
	durable                    = "durable"
	deleteWhenUnused           = "deleteWhenUnused"
	prefetchCount              = "prefetchCount"
	maxPriority                = "maxPriority"
	reconnectWaitSecondsKey    = "reconnectWaitInSeconds"
	rabbitMQQueueMessageTTLKey = "x-message-ttl"
	rabbitMQMaxPriorityKey     = "x-max-priority"
	defaultBase                = 10
	defaultBitSize             = 0

	errorChannelConnection = "channel/connection is not open"
	defaultReconnectWait   = 5 * time.Second
)

var errClosed = errors.New("component is stopped")

// RabbitMQ allows sending/receiving data to/from RabbitMQ.
type RabbitMQ struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	metadata   rabbitMQMetadata
	logger     logger.Logger
	queue      amqp.Queue
	closed     atomic.Bool
	closeCh    chan struct{}
	wg         sync.WaitGroup

	// used for reconnect
	channelMutex             sync.RWMutex
	notifyRabbitChannelClose chan *amqp.Error
}

// Metadata is the rabbitmq config.
type rabbitMQMetadata struct {
	Host             string `json:"host"`
	QueueName        string `json:"queueName"`
	Exclusive        bool   `json:"exclusive,string"`
	Durable          bool   `json:"durable,string"`
	DeleteWhenUnused bool   `json:"deleteWhenUnused,string"`
	PrefetchCount    int    `json:"prefetchCount"`
	MaxPriority      *uint8 `json:"maxPriority"` // Priority Queue deactivated if nil
	reconnectWait    time.Duration
	defaultQueueTTL  *time.Duration
}

// NewRabbitMQ returns a new rabbitmq instance.
func NewRabbitMQ(logger logger.Logger) bindings.InputOutputBinding {
	return &RabbitMQ{
		logger:  logger,
		closeCh: make(chan struct{}),
	}
}

// Init does metadata parsing and connection creation.
func (r *RabbitMQ) Init(_ context.Context, metadata bindings.Metadata) error {
	err := r.parseMetadata(metadata)
	if err != nil {
		return err
	}

	err = r.connect()
	if err != nil {
		return err
	}
	r.notifyRabbitChannelClose = make(chan *amqp.Error, 1)
	r.channel.NotifyClose(r.notifyRabbitChannelClose)
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		r.reconnectWhenNecessary()
	}()
	return nil
}

func (r *RabbitMQ) reconnectWhenNecessary() {
	for {
		select {
		case <-r.closeCh:
			return
		case e := <-r.notifyRabbitChannelClose:
			// If this error can not be recovered, first wait and then retry.
			if e != nil && !e.Recover {
				select {
				case <-time.After(r.metadata.reconnectWait):
				case <-r.closeCh:
					return
				}
			}
			r.channelMutex.Lock()
			if r.connection != nil && !r.connection.IsClosed() {
				ch, err := r.connection.Channel()
				if err == nil {
					r.notifyRabbitChannelClose = make(chan *amqp.Error, 1)
					ch.NotifyClose(r.notifyRabbitChannelClose)
					r.channel = ch
					r.channelMutex.Unlock()
					continue
				}
				// if encounter err fallback to reconnect connection
			}
			r.channelMutex.Unlock()
			// keep trying to reconnect
			for {
				err := r.connect()
				if err == nil {
					break
				}
				if err == errClosed {
					return
				}
				r.logger.Warnf("Reconnect failed: %v", err)

				select {
				case <-time.After(r.metadata.reconnectWait):
				case <-r.closeCh:
					return
				}
			}
		}
	}
}

func dial(uri string) (conn *amqp.Connection, ch *amqp.Channel, err error) {
	conn, err = amqp.Dial(uri)
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

func (r *RabbitMQ) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (r *RabbitMQ) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	// check if connection channel to rabbitmq is open
	r.channelMutex.RLock()
	ch := r.channel
	r.channelMutex.RUnlock()
	if ch == nil {
		return nil, errors.New(errorChannelConnection)
	}
	pub := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         req.Data,
	}

	contentType, ok := contribMetadata.TryGetContentType(req.Metadata)
	if ok {
		pub.ContentType = contentType
	}

	// The default time to live has been set in the queue
	// We allow overriding on each call, by setting a value in request metadata
	ttl, ok, err := contribMetadata.TryGetTTL(req.Metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to get TTL: %w", err)
	}
	if ok {
		// RabbitMQ expects the duration in ms
		pub.Expiration = strconv.FormatInt(ttl.Milliseconds(), 10)
	}

	priority, ok, err := contribMetadata.TryGetPriority(req.Metadata)
	if err != nil {
		return nil, err
	}
	if ok {
		pub.Priority = priority
	}

	err = ch.PublishWithContext(ctx, "", r.metadata.QueueName, false, false, pub)
	if err != nil {
		return nil, fmt.Errorf("failed to publish message: %w", err)
	}

	return nil, nil
}

func (r *RabbitMQ) parseMetadata(metadata bindings.Metadata) error {
	m := rabbitMQMetadata{reconnectWait: defaultReconnectWait}

	if val := metadata.Properties[host]; val != "" {
		m.Host = val
	} else {
		return errors.New("missing host address")
	}

	if val := metadata.Properties[queueName]; val != "" {
		m.QueueName = val
	} else {
		return errors.New("missing queue Name")
	}

	if val := metadata.Properties[durable]; val != "" {
		m.Durable = utils.IsTruthy(val)
	}

	if val := metadata.Properties[deleteWhenUnused]; val != "" {
		m.DeleteWhenUnused = utils.IsTruthy(val)
	}

	if val := metadata.Properties[prefetchCount]; val != "" {
		parsedVal, err := strconv.ParseInt(val, defaultBase, defaultBitSize)
		if err != nil {
			return fmt.Errorf("can't parse prefetchCount field: %s", err)
		}
		m.PrefetchCount = int(parsedVal)
	}

	if val := metadata.Properties[exclusive]; val != "" {
		m.Exclusive = utils.IsTruthy(val)
	}

	if val := metadata.Properties[maxPriority]; val != "" {
		parsedVal, err := strconv.ParseUint(val, defaultBase, defaultBitSize)
		if err != nil {
			return fmt.Errorf("can't parse maxPriority field: %s", err)
		}

		maxPriority := uint8(parsedVal)
		if parsedVal > 255 {
			// Overflow
			maxPriority = math.MaxUint8
		}

		m.MaxPriority = &maxPriority
	}

	if val := metadata.Properties[reconnectWaitSecondsKey]; val != "" {
		if intVal, err := strconv.Atoi(val); err == nil && intVal > 0 {
			m.reconnectWait = time.Duration(intVal) * time.Second
		}
	}

	ttl, ok, err := contribMetadata.TryGetTTL(metadata.Properties)
	if err != nil {
		return fmt.Errorf("failed to parse TTL: %w", err)
	}
	if ok {
		m.defaultQueueTTL = &ttl
	}

	r.metadata = m

	return nil
}

func (r *RabbitMQ) declareQueue(channel *amqp.Channel) (amqp.Queue, error) {
	args := amqp.Table{}
	if r.metadata.defaultQueueTTL != nil {
		// Value in ms
		ttl := *r.metadata.defaultQueueTTL / time.Millisecond
		args[rabbitMQQueueMessageTTLKey] = int(ttl)
	}

	if r.metadata.MaxPriority != nil {
		args[rabbitMQMaxPriorityKey] = *r.metadata.MaxPriority
	}

	return channel.QueueDeclare(r.metadata.QueueName, r.metadata.Durable, r.metadata.DeleteWhenUnused, r.metadata.Exclusive, false, args)
}

func (r *RabbitMQ) Read(ctx context.Context, handler bindings.Handler) error {
	if r.closed.Load() {
		return errors.New("binding already closed")
	}

	readCtx, cancel := context.WithCancel(ctx)
	r.wg.Add(2)
	go func() {
		select {
		case <-r.closeCh:
			// nop
		case <-readCtx.Done():
			// nop
		}
		r.wg.Done()
		cancel()
	}()
	go func() {
		// unless closed, keep trying to read and handle messages forever
		defer r.wg.Done()
		for {
			var (
				msgs              <-chan amqp.Delivery
				err               error
				declaredQueueName string
				ch                *amqp.Channel
			)
			r.channelMutex.RLock()
			declaredQueueName = r.queue.Name
			ch = r.channel
			r.channelMutex.RUnlock()

			if ch != nil {
				msgs, err = ch.Consume(
					declaredQueueName,
					"",
					false,
					false,
					false,
					false,
					nil,
				)
				if err == nil {
					// all good, handle messages
					r.handleMessage(readCtx, handler, msgs, ch)
				} else {
					r.logger.Errorf("Error consuming messages from queue [%s]: %v", r.queue.Name, err)
				}
			}

			// something went wrong, wait for reconnect
			select {
			case <-time.After(r.metadata.reconnectWait):
				continue
			case <-readCtx.Done():
				r.logger.Info("Input binding closed, stop fetching message")
				return
			}
		}
	}()
	return nil
}

// handleMessage handles incoming messages from RabbitMQ
func (r *RabbitMQ) handleMessage(ctx context.Context, handler bindings.Handler, msgCh <-chan amqp.Delivery, ch *amqp.Channel) {
	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-msgCh:
			if !ok {
				r.logger.Info("Input binding channel closed")
				return
			}
			_, err := handler(ctx, &bindings.ReadResponse{
				Data: d.Body,
			})
			if err != nil {
				ch.Nack(d.DeliveryTag, false, true)
			} else {
				ch.Ack(d.DeliveryTag, false)
			}
		}
	}
}

func (r *RabbitMQ) Close() error {
	if r.closed.CompareAndSwap(false, true) {
		close(r.closeCh)
	}
	defer r.wg.Wait()
	r.channelMutex.Lock()
	defer r.channelMutex.Unlock()
	return r.reset()
}

func (r *RabbitMQ) connect() error {
	if r.closed.Load() {
		// Do not reconnect on stopped service.
		return errClosed
	}

	conn, ch, err := dial(r.metadata.Host)
	if err != nil {
		return err
	}

	ch.Qos(r.metadata.PrefetchCount, 0, true)
	q, err := r.declareQueue(ch)
	if err != nil {
		return err
	}

	r.notifyRabbitChannelClose = make(chan *amqp.Error, 1)
	ch.NotifyClose(r.notifyRabbitChannelClose)

	r.channelMutex.Lock()
	// try to close the old channel and connection, ignore the error
	_ = r.reset() //nolint:errcheck
	r.connection, r.channel, r.queue = conn, ch, q
	r.channelMutex.Unlock()

	r.logger.Info("Connected to RabbitMQ")

	return nil
}

// reset the channel and the connection when encountered a connection error.
// this function call should be wrapped by channelMutex.
func (r *RabbitMQ) reset() (err error) {
	if r.channel != nil {
		if err = r.channel.Close(); err != nil {
			r.logger.Warnf("Reset: channel.Close() failed: %v", err)
		}
		r.channel = nil
	}

	if r.connection != nil {
		if err2 := r.connection.Close(); err2 != nil {
			r.logger.Warnf("Reset: connection.Close() failed: %v", err2)
			if err == nil {
				err = err2
			}
		}
		r.connection = nil
	}

	return err
}
