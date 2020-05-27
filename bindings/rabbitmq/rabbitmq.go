// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package rabbitmq

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/streadway/amqp"
)

const (
	rabbitMQQueueMessageTTLKey = "x-message-ttl"
)

// RabbitMQ allows sending/receiving data to/from RabbitMQ
type RabbitMQ struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	metadata   rabbitMQMetadata
	logger     logger.Logger
	queue      amqp.Queue
}

// Metadata is the rabbitmq config
type rabbitMQMetadata struct {
	QueueName        string `json:"queueName"`
	Host             string `json:"host"`
	Durable          bool   `json:"durable,string"`
	DeleteWhenUnused bool   `json:"deleteWhenUnused,string"`
	defaultQueueTTL  *time.Duration
}

// NewRabbitMQ returns a new rabbitmq instance
func NewRabbitMQ(logger logger.Logger) *RabbitMQ {
	return &RabbitMQ{logger: logger}
}

// Init does metadata parsing and connection creation
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

	r.connection = conn
	r.channel = ch

	q, err := r.declareQueue()
	if err != nil {
		return err
	}

	r.queue = q

	return nil
}

func (r *RabbitMQ) Operations() []bindings.OperationType {
	return []bindings.OperationType{bindings.CreateOperation}
}

func (r *RabbitMQ) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	pub := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         req.Data,
	}

	ttl, ok, err := bindings.TryGetTTL(req.Metadata)
	if err != nil {
		return nil, err
	}

	// The default time to live has been set in the queue
	// We allow overriding on each call, by setting a value in request metadata
	if ok {
		// RabbitMQ expects the duration in ms
		pub.Expiration = strconv.FormatInt(ttl.Milliseconds(), 10)
	}

	err = r.channel.Publish("", r.metadata.QueueName, false, false, pub)

	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (r *RabbitMQ) parseMetadata(metadata bindings.Metadata) error {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return err
	}

	var m rabbitMQMetadata
	err = json.Unmarshal(b, &m)
	if err != nil {
		return err
	}

	ttl, ok, err := bindings.TryGetTTL(metadata.Properties)
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

	return r.channel.QueueDeclare(r.metadata.QueueName, r.metadata.Durable, r.metadata.DeleteWhenUnused, false, false, args)
}

func (r *RabbitMQ) Read(handler func(*bindings.ReadResponse) error) error {
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

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			err := handler(&bindings.ReadResponse{
				Data: d.Body,
			})
			if err == nil {
				r.channel.Ack(d.DeliveryTag, false)
			}
		}
	}()

	<-forever
	return nil
}
