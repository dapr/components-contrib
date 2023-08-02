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
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"math"
	"net/url"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dapr/components-contrib/internal/utils"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
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
	caCert                     = "caCert"
	clientCert                 = "clientCert"
	clientKey                  = "clientKey"
	externalSasl               = "saslExternal"
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
	Host             string         `mapstructure:"host"`
	QueueName        string         `mapstructure:"queueName"`
	Exclusive        bool           `mapstructure:"exclusive"`
	Durable          bool           `mapstructure:"durable"`
	DeleteWhenUnused bool           `mapstructure:"deleteWhenUnused"`
	PrefetchCount    int            `mapstructure:"prefetchCount"`
	MaxPriority      *uint8         `mapstructure:"maxPriority"` // Priority Queue deactivated if nil
	ReconnectWait    time.Duration  `mapstructure:"reconnectWaitInSeconds"`
	DefaultQueueTTL  *time.Duration `mapstructure:"ttlInSeconds"`
	CaCert           string         `mapstructure:"caCert"`
	ClientCert       string         `mapstructure:"clientCert"`
	ClientKey        string         `mapstructure:"clientKey"`
	ExternalSasl     bool           `mapstructure:"externalSasl"`
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
				case <-time.After(r.metadata.ReconnectWait):
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
				case <-time.After(r.metadata.ReconnectWait):
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

func dialTLS(uri string, tlsConfig *tls.Config, externalAuth bool) (conn *amqp.Connection, ch *amqp.Channel, err error) {
	if externalAuth {
		conn, err = amqp.DialTLS_ExternalAuth(uri, tlsConfig)
	} else {
		conn, err = amqp.DialTLS(uri, tlsConfig)
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
		Headers:      make(amqp.Table, len(req.Metadata)),
	}

	for k, v := range req.Metadata {
		pub.Headers[k] = v
	}

	contentType, ok := metadata.TryGetContentType(req.Metadata)
	if ok {
		pub.ContentType = contentType
	}

	// The default time to live has been set in the queue
	// We allow overriding on each call, by setting a value in request metadata
	ttl, ok, err := metadata.TryGetTTL(req.Metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to get TTL: %w", err)
	}
	if ok {
		// RabbitMQ expects the duration in ms
		pub.Expiration = strconv.FormatInt(ttl.Milliseconds(), 10)
	}

	priority, ok, err := metadata.TryGetPriority(req.Metadata)
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

func (r *RabbitMQ) parseMetadata(meta bindings.Metadata) error {
	m := rabbitMQMetadata{
		ReconnectWait: defaultReconnectWait,
	}

	metadata.DecodeMetadata(meta.Properties, &m)

	if m.Host == "" {
		return errors.New("missing host address")
	}

	if m.QueueName == "" {
		return errors.New("missing queue Name")
	}

	if val := meta.Properties[maxPriority]; val != "" {
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

	if val, ok := meta.Properties[caCert]; ok && val != "" {
		if !isValidPEM(val) {
			return errors.New("invalid ca certificate")
		}
		m.CaCert = val
	}
	if val, ok := meta.Properties[clientCert]; ok && val != "" {
		if !isValidPEM(val) {
			return errors.New("invalid client certificate")
		}
		m.ClientCert = val
	}
	if val, ok := meta.Properties[clientKey]; ok && val != "" {
		if !isValidPEM(val) {
			return errors.New("invalid client certificate key")
		}
		m.ClientKey = val
	}

	if val, ok := meta.Properties[externalSasl]; ok && val != "" {
		m.ExternalSasl = utils.IsTruthy(val)
	}

	if val, ok := meta.Properties[caCert]; ok && val != "" {
		if !isValidPEM(val) {
			return errors.New("invalid ca certificate")
		}
		m.CaCert = val
	}
	if val, ok := meta.Properties[clientCert]; ok && val != "" {
		if !isValidPEM(val) {
			return errors.New("invalid client certificate")
		}
		m.ClientCert = val
	}
	if val, ok := meta.Properties[clientKey]; ok && val != "" {
		if !isValidPEM(val) {
			return errors.New("invalid client certificate key")
		}
		m.ClientKey = val
	}

	if val, ok := meta.Properties[externalSasl]; ok && val != "" {
		m.ExternalSasl = utils.IsTruthy(val)
	}

	ttl, ok, err := metadata.TryGetTTL(meta.Properties)
	if err != nil {
		return fmt.Errorf("failed to parse TTL: %w", err)
	}
	if ok {
		m.DefaultQueueTTL = &ttl
	}

	r.metadata = m
	return nil
}

func (r *RabbitMQ) declareQueue(channel *amqp.Channel) (amqp.Queue, error) {
	args := amqp.Table{}
	if r.metadata.DefaultQueueTTL != nil {
		// Value in ms
		ttl := *r.metadata.DefaultQueueTTL / time.Millisecond
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
			case <-time.After(r.metadata.ReconnectWait):
				continue
			case <-readCtx.Done():
				r.logger.Info("Input binding closed, stop fetching message")
				return
			}
		}
	}()
	return nil
}

func (r *RabbitMQ) newTLSConfig() *tls.Config {
	tlsConfig := new(tls.Config)

	if r.metadata.ClientCert != "" && r.metadata.ClientKey != "" {
		cert, err := tls.X509KeyPair([]byte(r.metadata.ClientCert), []byte(r.metadata.ClientKey))
		if err != nil {
			r.logger.Warnf("Unable to load client certificate and key pair. Err: %v", err)
			return tlsConfig
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if r.metadata.CaCert != "" {
		tlsConfig.RootCAs = x509.NewCertPool()
		if ok := tlsConfig.RootCAs.AppendCertsFromPEM([]byte(r.metadata.CaCert)); !ok {
			r.logger.Warnf("Unable to load CA certificate.")
		}
	}
	return tlsConfig
}

// isValidPEM validates the provided input has PEM formatted block.
func isValidPEM(val string) bool {
	block, _ := pem.Decode([]byte(val))

	return block != nil
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

			metadata := make(map[string]string)
			// Passthrough any custom metadata to the handler.
			for k, v := range d.Headers {
				if s, ok := v.(string); ok {
					// Escape the key and value to ensure they are valid URL query parameters.
					// This is necessary for them to be sent as HTTP Metadata.
					metadata[url.QueryEscape(k)] = url.QueryEscape(s)
				}
			}

			_, err := handler(ctx, &bindings.ReadResponse{
				Data:     d.Body,
				Metadata: metadata,
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
	var conn *amqp.Connection
	var ch *amqp.Channel
	var err error
	if r.metadata.ClientCert != "" && r.metadata.ClientKey != "" && r.metadata.CaCert != "" {
		tlsConfig := r.newTLSConfig()
		conn, ch, err = dialTLS(r.metadata.Host, tlsConfig, r.metadata.ExternalSasl)
	} else {
		conn, ch, err = dial(r.metadata.Host)
	}

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

func (r *RabbitMQ) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := rabbitMQMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}
