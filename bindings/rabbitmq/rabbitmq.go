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
	rabbitMQQueueMessageTTLKey = "x-message-ttl"
	rabbitMQMaxPriorityKey     = "x-max-priority"
	defaultBase                = 10
	defaultBitSize             = 0
)

// RabbitMQ allows sending/receiving data to/from RabbitMQ.
type RabbitMQ struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	metadata   rabbitMQMetadata
	logger     logger.Logger
	queue      amqp.Queue
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
	defaultQueueTTL  *time.Duration
}

// NewRabbitMQ returns a new rabbitmq instance.
func NewRabbitMQ(logger logger.Logger) bindings.InputOutputBinding {
	return &RabbitMQ{logger: logger}
}

// Init does metadata parsing and connection creation.
func (r *RabbitMQ) Init(metadata bindings.Metadata) error {
	err := r.parseMetadata(metadata)
	if err != nil {
		return err
	}

	conn, err := amqp.Dial(r.metadata.Host)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	ch.Qos(r.metadata.PrefetchCount, 0, true)
	r.connection = conn
	r.channel = ch

	q, err := r.declareQueue()
	if err != nil {
		return err
	}

	r.queue = q

	return nil
}

func (r *RabbitMQ) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (r *RabbitMQ) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	pub := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         req.Data,
	}

	contentType, ok := contribMetadata.TryGetContentType(req.Metadata)

	if ok {
		pub.ContentType = contentType
	}

	ttl, ok, err := contribMetadata.TryGetTTL(req.Metadata)
	if err != nil {
		return nil, err
	}

	// The default time to live has been set in the queue
	// We allow overriding on each call, by setting a value in request metadata
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

	err = r.channel.PublishWithContext(ctx, "", r.metadata.QueueName, false, false, pub)

	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (r *RabbitMQ) parseMetadata(metadata bindings.Metadata) error {
	m := rabbitMQMetadata{}

	if val, ok := metadata.Properties[host]; ok && val != "" {
		m.Host = val
	} else {
		return errors.New("rabbitMQ binding error: missing host address")
	}

	if val, ok := metadata.Properties[queueName]; ok && val != "" {
		m.QueueName = val
	} else {
		return errors.New("rabbitMQ binding error: missing queue Name")
	}

	if val, ok := metadata.Properties[durable]; ok && val != "" {
		m.Durable = utils.IsTruthy(val)
	}

	if val, ok := metadata.Properties[deleteWhenUnused]; ok && val != "" {
		m.DeleteWhenUnused = utils.IsTruthy(val)
	}

	if val, ok := metadata.Properties[prefetchCount]; ok && val != "" {
		parsedVal, err := strconv.ParseInt(val, defaultBase, defaultBitSize)
		if err != nil {
			return fmt.Errorf("rabbitMQ binding error: can't parse prefetchCount field: %s", err)
		}
		m.PrefetchCount = int(parsedVal)
	}

	if val, ok := metadata.Properties[exclusive]; ok && val != "" {
		m.Exclusive = utils.IsTruthy(val)
	}

	if val, ok := metadata.Properties[maxPriority]; ok && val != "" {
		parsedVal, err := strconv.ParseUint(val, defaultBase, defaultBitSize)
		if err != nil {
			return fmt.Errorf("rabbitMQ binding error: can't parse maxPriority field: %s", err)
		}

		maxPriority := uint8(parsedVal)
		if parsedVal > 255 {
			// Overflow
			maxPriority = math.MaxUint8
		}

		m.MaxPriority = &maxPriority
	}

	ttl, ok, err := contribMetadata.TryGetTTL(metadata.Properties)
	if err != nil {
		return err
	}

	if ok {
		m.defaultQueueTTL = &ttl
	}

	r.metadata = m

	return nil
}

func (r *RabbitMQ) declareQueue() (amqp.Queue, error) {
	args := amqp.Table{}
	if r.metadata.defaultQueueTTL != nil {
		// Value in ms
		ttl := *r.metadata.defaultQueueTTL / time.Millisecond
		args[rabbitMQQueueMessageTTLKey] = int(ttl)
	}

	if r.metadata.MaxPriority != nil {
		args[rabbitMQMaxPriorityKey] = *r.metadata.MaxPriority
	}

	return r.channel.QueueDeclare(r.metadata.QueueName, r.metadata.Durable, r.metadata.DeleteWhenUnused, r.metadata.Exclusive, false, args)
}

func (r *RabbitMQ) Read(ctx context.Context, handler bindings.Handler) error {
	msgs, err := r.channel.Consume(
		r.queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	go func() {
		var err error
		for {
			select {
			case <-ctx.Done():
				return
			case d := <-msgs:
				_, err = handler(ctx, &bindings.ReadResponse{
					Data: d.Body,
				})
				if err != nil {
					r.channel.Nack(d.DeliveryTag, false, true)
				} else {
					r.channel.Ack(d.DeliveryTag, false)
				}
			}
		}
	}()

	return nil
}
