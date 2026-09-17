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

package amqp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	time "time"

	amqp "github.com/Azure/go-amqp"
	backoff "github.com/cenkalti/backoff/v4"

	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/retry"
)

const (
	publishRetryWaitSeconds = 2
	publishMaxRetries       = 3

	// defaultCloseTimeout bounds how long closing a link or session may block.
	defaultCloseTimeout = 5 * time.Second

	// reattachFloor is the minimum delay between two attempts to re-attach a
	// receiver link, so that a link failing immediately after it attaches
	// cannot spin against the broker.
	reattachFloor = time.Second
)

// amqpPubSub type allows sending and receiving data to/from an AMQP 1.0 broker
type amqpPubSub struct {
	// lifecycleMu serialises admitting a new subscription against Close. Without
	// it, Subscribe can call wg.Add after Close has started wg.Wait, which is a
	// WaitGroup misuse that panics the process.
	lifecycleMu sync.Mutex

	// connMu guards client and session. Operations that use the connection
	// hold it for reading, so that publishing is concurrent; only replacing a
	// dead connection takes it for writing.
	connMu  sync.RWMutex
	client  *amqp.Conn
	session *amqp.Session

	metadata      *metadata
	logger        logger.Logger
	backOffConfig retry.Config
	wg            sync.WaitGroup
	closed        atomic.Bool
	closeCh       chan struct{}

	// Address prefixes applied when the component configuration does not set
	// them. They are fixed by the constructor, because the component cannot
	// read back which registered type name the user declared.
	defaultTopicPrefix string
	defaultQueuePrefix string
}

// NewAMQPPubsub returns a new AMQP 1.0 pub/sub instance, registered as
// pubsub.amqp. It addresses topics and queues by name, which is what a broker
// such as ActiveMQ Artemis expects. To reach a broker that namespaces its
// destinations, set topicAddressPrefix and queueAddressPrefix to the prefixes
// that broker is configured with.
func NewAMQPPubsub(logger logger.Logger) pubsub.PubSub {
	return newAMQPPubsub(logger, genericAddressPrefix, genericAddressPrefix)
}

// NewSolaceAMQPPubsub returns an AMQP 1.0 pub/sub instance that defaults to the
// Solace addressing convention. It backs the deprecated pubsub.solace.amqp
// type, so that configurations written against it keep working unchanged.
// New configurations must use pubsub.amqp.
func NewSolaceAMQPPubsub(logger logger.Logger) pubsub.PubSub {
	return newAMQPPubsub(logger, solaceTopicAddressPrefix, solaceQueueAddressPrefix)
}

func newAMQPPubsub(logger logger.Logger, defaultTopicPrefix, defaultQueuePrefix string) pubsub.PubSub {
	return &amqpPubSub{
		logger:             logger,
		closeCh:            make(chan struct{}),
		defaultTopicPrefix: defaultTopicPrefix,
		defaultQueuePrefix: defaultQueuePrefix,
	}
}

// Init parses the metadata and creates a new Pub Sub Client.
func (a *amqpPubSub) Init(ctx context.Context, metadata pubsub.Metadata) error {
	amqpMeta, err := parseAMQPMetaData(metadata, a.logger, a.defaultTopicPrefix, a.defaultQueuePrefix)
	if err != nil {
		return err
	}

	// Reconnect backoff, tunable with the usual backOff* metadata keys. The
	// default is a constant retry every 5 seconds.
	backOffConfig := retry.DefaultConfig()
	if err = retry.DecodeConfigWithPrefix(&backOffConfig, metadata.Properties, "backOff"); err != nil {
		return err
	}

	client, session, err := a.connect(ctx, amqpMeta)
	if err != nil {
		return err
	}

	a.connMu.Lock()
	a.metadata, a.backOffConfig = amqpMeta, backOffConfig
	a.client, a.session = client, session
	a.connMu.Unlock()

	return nil
}

// currentMetadata returns the parsed configuration, or nil before Init.
func (a *amqpPubSub) currentMetadata() *metadata {
	a.connMu.RLock()
	defer a.connMu.RUnlock()

	return a.metadata
}

// closeReceiver detaches a receiver link, bounded by defaultCloseTimeout.
func (a *amqpPubSub) closeReceiver(receiver *amqp.Receiver, address string) {
	closeCtx, cancel := context.WithTimeout(context.Background(), defaultCloseTimeout)
	defer cancel()

	if err := receiver.Close(closeCtx); err != nil {
		a.logger.Warnf("failed to close the receiver link for %s: %v", address, err)
	}
}

// currentSession returns the session operations must use. It is nil only
// before a successful Init.
func (a *amqpPubSub) currentSession() *amqp.Session {
	a.connMu.RLock()
	defer a.connMu.RUnlock()

	return a.session
}

// renewSession replaces stale with a freshly dialled connection and session.
//
// go-amqp 1.0.5 exposes no liveness signal on a Conn or a Session, so a dead
// connection is only discovered when an operation fails. Callers pass the
// session they were using: if another caller has already reconnected, the new
// session is returned and no second dial happens, so a broker restart costs
// one dial rather than one per in-flight operation.
func (a *amqpPubSub) renewSession(ctx context.Context, stale *amqp.Session) (*amqp.Session, error) {
	a.connMu.Lock()
	defer a.connMu.Unlock()

	if a.closed.Load() {
		return nil, errors.New("component is closed")
	}

	if a.metadata == nil {
		return nil, errors.New("component is not initialized")
	}

	// Defer to another caller's reconnect only if it actually produced a
	// session. A failed attempt also leaves a.session different from stale,
	// because it is set to nil, and this caller must dial rather than hand
	// back a nil session for the caller to dereference.
	if a.session != nil && a.session != stale {
		return a.session, nil
	}

	a.closeConnLocked()

	client, session, err := a.connect(ctx, a.metadata)
	if err != nil {
		return nil, err
	}

	a.client, a.session = client, session
	a.logger.Infof("Reconnected to %s", a.metadata.URL)

	return session, nil
}

// closeConnLocked tears down the current session and connection. The caller
// must hold connMu for writing.
func (a *amqpPubSub) closeConnLocked() {
	if a.session != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), defaultCloseTimeout)
		if err := a.session.Close(closeCtx); err != nil {
			a.logger.Warnf("failed to close the AMQP session: %v", err)
		}
		cancel()
		a.session = nil
	}

	if a.client != nil {
		// Conn.Close takes no context and blocks until the writer goroutine
		// drains, so a wedged peer would hang this call, and with it connMu
		// and every later reconnect. Bound it.
		client := a.client
		done := make(chan error, 1)
		go func() { done <- client.Close() }()

		select {
		case err := <-done:
			if err != nil {
				a.logger.Warnf("failed to close the AMQP connection: %v", err)
			}
		case <-time.After(defaultCloseTimeout):
			a.logger.Warnf("timed out closing the AMQP connection, abandoning it")
		}

		a.client = nil
	}
}

// Publish the topic to amqp pubsub
func (a *amqpPubSub) Publish(ctx context.Context, req *pubsub.PublishRequest) error {
	if a.closed.Load() {
		return pubsub.NewTerminalError(errors.New("component is closed"))
	}

	if req.Topic == "" {
		return pubsub.NewTerminalError(errors.New("topic name is empty"))
	}

	md := a.currentMetadata()
	if md == nil {
		return pubsub.NewTerminalError(errors.New("component is not initialized"))
	}

	address := md.addressFor(req.Topic)
	if address == "" {
		return pubsub.NewTerminalError(fmt.Errorf("topic %q maps to an empty AMQP address", req.Topic))
	}

	m := amqp.NewMessage(req.Data)

	// If the request has ttl specified, put it on the message header
	ttlProp := req.Metadata["ttlInSeconds"]
	if ttlProp != "" {
		ttlInSeconds, err := strconv.Atoi(ttlProp)
		if err != nil {
			a.logger.Warnf("Invalid ttl received from message %d", ttlInSeconds)
		} else {
			m.Header.TTL = time.Second * time.Duration(ttlInSeconds)
		}
	}

	// Publish the message, retrying a bounded number of times before giving up.
	// Every attempt opens its own link. Once a link is done, go-amqp returns
	// the same error from every Send without touching the network, so retrying
	// on the link that just failed can never succeed.
	var err error
	for attempt := 0; ; attempt++ {
		err = a.publishOnce(ctx, address, m)
		if err == nil {
			return nil
		}

		if attempt >= publishMaxRetries || a.closed.Load() {
			break
		}

		a.logger.Warnf("Failed to publish a message to %s, retrying: %v", address, err)

		select {
		case <-time.After(publishRetryWaitSeconds * time.Second):
		case <-ctx.Done():
			return pubsub.NewRetriableError(ctx.Err())
		}
	}

	return pubsub.NewRetriableError(err)
}

// publishOnce opens a sender link, sends one message and closes the link again.
// The link is per publish, so it has to be closed here; otherwise every
// published message leaks a link on the broker.
func (a *amqpPubSub) publishOnce(ctx context.Context, address string, m *amqp.Message) error {
	sender, err := a.newSender(ctx, address)
	if err != nil {
		return err
	}

	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), defaultCloseTimeout)
		defer cancel()
		if cerr := sender.Close(closeCtx); cerr != nil {
			a.logger.Warnf("failed to close the sender link for %s: %v", address, cerr)
		}
	}()

	return sender.Send(ctx, m, nil)
}

// sessionOrRenew returns the current session, dialling a new one if an earlier
// reconnect attempt failed and left none behind.
func (a *amqpPubSub) sessionOrRenew(ctx context.Context) (*amqp.Session, error) {
	if session := a.currentSession(); session != nil {
		return session, nil
	}

	return a.renewSession(ctx, nil)
}

// newSender attaches a sender link, reconnecting once if the session it used
// turns out to be dead.
func (a *amqpPubSub) newSender(ctx context.Context, address string) (*amqp.Sender, error) {
	session, err := a.sessionOrRenew(ctx)
	if err != nil {
		return nil, err
	}

	sender, err := session.NewSender(ctx, address, nil)
	if err == nil {
		return sender, nil
	}

	session, rerr := a.renewSession(ctx, session)
	if rerr != nil {
		return nil, errors.Join(err, rerr)
	}

	return session.NewSender(ctx, address, nil)
}

// newReceiver attaches a receiver link, reconnecting once if the session it
// used turns out to be dead.
func (a *amqpPubSub) newReceiver(ctx context.Context, address string) (*amqp.Receiver, error) {
	session, err := a.sessionOrRenew(ctx)
	if err != nil {
		return nil, err
	}

	receiver, err := session.NewReceiver(ctx, address, nil)
	if err == nil {
		return receiver, nil
	}

	session, rerr := a.renewSession(ctx, session)
	if rerr != nil {
		return nil, errors.Join(err, rerr)
	}

	return session.NewReceiver(ctx, address, nil)
}

func (a *amqpPubSub) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if a.closed.Load() {
		return errors.New("component is closed")
	}

	if a.currentMetadata() == nil {
		return errors.New("component is not initialized")
	}

	address := a.currentMetadata().addressFor(req.Topic)
	if address == "" {
		return fmt.Errorf("topic %q maps to an empty AMQP address", req.Topic)
	}

	// Attach once here, so that an unreachable broker or an address the broker
	// rejects fails the Subscribe call rather than retrying in the background.
	receiver, err := a.newReceiver(ctx, address)
	if err != nil {
		a.logger.Errorf("Unable to create a receiver for %s: %v", address, err)
		return err
	}

	// Admitting the subscription has to be atomic with Close marking the
	// component closed. Otherwise wg.Add can run while Close is inside
	// wg.Wait, which panics, and the subscription leaks past Close.
	a.lifecycleMu.Lock()
	if a.closed.Load() {
		a.lifecycleMu.Unlock()
		a.closeReceiver(receiver, address)

		return errors.New("component is closed")
	}
	a.wg.Add(2)
	a.lifecycleMu.Unlock()

	a.logger.Infof("Attempting to subscribe to %s", address)
	subCtx, cancel := context.WithCancel(ctx)
	go func() {
		defer a.wg.Done()
		defer cancel()
		select {
		case <-a.closeCh:
		case <-subCtx.Done():
		}
	}()
	go func() {
		defer a.wg.Done()
		a.subscribeForever(subCtx, receiver, handler, req.Topic, address)
	}()

	return nil
}

// subscribeForever delivers messages for the lifetime of the subscription. If
// the receiver link fails, for example because the broker restarted, it
// re-attaches with backoff instead of ending the subscription.
// topic is the Dapr topic name the messages are delivered under, address is the
// AMQP address the receiver link is attached to.
func (a *amqpPubSub) subscribeForever(ctx context.Context, receiver *amqp.Receiver, handler pubsub.Handler, topic string, address string) {
	b := a.reconnectBackOff(ctx)

	for {
		a.deliver(ctx, receiver, handler, topic, address)

		if ctx.Err() != nil || a.closed.Load() {
			return
		}

		// Wait before re-attaching. A link that attaches and then fails
		// immediately would otherwise spin against the broker, because the
		// retry helper runs its first attempt with no delay.
		if !a.wait(ctx, reattachFloor) {
			return
		}

		var err error
		receiver, err = a.reattach(ctx, address, b)
		if err != nil {
			a.logger.Errorf("Ending the subscription to %s: %v", address, err)
			return
		}
	}
}

// reconnectBackOff returns the backoff used between re-attach attempts.
//
// The configured interval is honoured, but the retry never gives up. The
// runtime never calls Subscribe again, so a subscription that stops retrying
// stops consuming for the lifetime of the process. That is a worse outcome
// than retrying for a long time, and it would be silent.
func (a *amqpPubSub) reconnectBackOff(ctx context.Context) backoff.BackOff {
	cfg := a.backOffConfig
	cfg.MaxRetries = -1
	cfg.MaxElapsedTime = 0

	return cfg.NewBackOffWithContext(ctx)
}

// wait sleeps for d, reporting false if the subscription ended first.
func (a *amqpPubSub) wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C:
		return !a.closed.Load()
	case <-ctx.Done():
		return false
	case <-a.closeCh:
		return false
	}
}

// reattach re-opens a receiver link, retrying with backoff until it succeeds,
// the context is cancelled or the component closes.
func (a *amqpPubSub) reattach(ctx context.Context, address string, b backoff.BackOff) (*amqp.Receiver, error) {
	return retry.NotifyRecoverWithData(
		func() (*amqp.Receiver, error) {
			if a.closed.Load() {
				return nil, backoff.Permanent(errors.New("component is closed"))
			}

			return a.newReceiver(ctx, address)
		},
		b,
		func(err error, d time.Duration) {
			a.logger.Warnf("Failed to re-attach the receiver for %s, retrying in %v: %v", address, d, err)
		},
		func() {
			a.logger.Infof("Re-attached the receiver for %s", address)
		},
	)
}

// deliver pumps messages from one receiver link until the context is cancelled
// or the link fails. It closes the link before returning.
func (a *amqpPubSub) deliver(ctx context.Context, receiver *amqp.Receiver, handler pubsub.Handler, topic string, address string) {
	defer func() {
		a.logger.Infof("closing receiver for %s", address)
		a.closeReceiver(receiver, address)
	}()

	for {
		// Receive next message
		msg, err := receiver.Receive(ctx, nil)
		if err != nil {
			if ctx.Err() != nil {
				// The subscription is being torn down.
				return
			}
			// Receive only fails on a cancelled context or on a link that is
			// done for good, in which case it returns the same error
			// immediately every time. Returning here rather than continuing
			// avoids spinning on it, and lets the caller re-attach.
			a.logger.Errorf("The receiver link for %s failed: %v", address, err)
			return
		}

		if msg == nil {
			continue
		}

		if err = handler(ctx, newPubsubMessage(topic, msg)); err != nil {
			a.logger.Errorf("Error processing message from %s: %v", address, err)

			// Release rather than reject. In AMQP 1.0 the rejected outcome
			// tells the broker the message is invalid, so it is dead-lettered
			// or discarded and never comes back. A handler error in Dapr means
			// "deliver this again", which is the released outcome.
			if err = receiver.ReleaseMessage(ctx, msg); err != nil {
				a.logger.Errorf("failed to release a message from %s: %v", address, err)
			} else {
				a.logger.Debugf("released a message for redelivery")
			}

			continue
		}

		if err = receiver.AcceptMessage(ctx, msg); err != nil {
			a.logger.Errorf("failed to acknowledge a message from %s: %v", address, err)
		} else {
			a.logger.Debugf("ACKed a message")
		}
	}
}

// newPubsubMessage converts a message received from the broker into a Dapr
// pub/sub message delivered under the topic the subscription was created for.
func newPubsubMessage(topic string, msg *amqp.Message) *pubsub.NewMessage {
	data := msg.GetData()

	// if data is empty, then check the value field for data
	if len(data) == 0 {
		data = []byte(fmt.Sprint(msg.Value))
	}

	return &pubsub.NewMessage{
		Data:  data,
		Topic: topic,
	}
}

// connect dials the broker and opens a session on the connection. It returns
// both, because closing only the session leaves the socket open.
func (a *amqpPubSub) connect(ctx context.Context, md *metadata) (*amqp.Conn, *amqp.Session, error) {
	uri, err := url.Parse(md.URL)
	if err != nil {
		return nil, nil, err
	}

	clientOpts := a.createClientOptions(uri, md)

	a.logger.Infof("Attempting to connect to %s", md.URL)
	client, err := amqp.Dial(ctx, md.URL, &clientOpts)
	if err != nil {
		return nil, nil, fmt.Errorf("%s dialing AMQP server: %w", errorMsgPrefix, err)
	}

	// Open a session
	session, err := client.NewSession(ctx, nil)
	if err != nil {
		if cerr := client.Close(); cerr != nil {
			a.logger.Warnf("failed to close the connection after a failed session: %v", cerr)
		}
		return nil, nil, fmt.Errorf("%s creating AMQP session: %w", errorMsgPrefix, err)
	}

	return client, session, nil
}

func (a *amqpPubSub) newTLSConfig(md *metadata) *tls.Config {
	tlsConfig := new(tls.Config)

	if md.ClientCert != "" && md.ClientKey != "" {
		cert, err := tls.X509KeyPair([]byte(md.ClientCert), []byte(md.ClientKey))
		if err != nil {
			a.logger.Warnf("unable to load client certificate and key pair. Err: %v", err)

			return tlsConfig
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if md.CaCert != "" {
		tlsConfig.RootCAs = x509.NewCertPool()
		if ok := tlsConfig.RootCAs.AppendCertsFromPEM([]byte(md.CaCert)); !ok {
			a.logger.Warnf("unable to load ca certificate.")
		}
	}

	return tlsConfig
}

func (a *amqpPubSub) createClientOptions(uri *url.URL, md *metadata) amqp.ConnOptions {
	var opts amqp.ConnOptions

	scheme := uri.Scheme

	switch scheme {
	case "amqp":
		if md.Anonymous {
			opts.SASLType = amqp.SASLTypeAnonymous()
		} else {
			opts.SASLType = amqp.SASLTypePlain(md.Username, md.Password)
		}
	case "amqps":
		opts.SASLType = amqp.SASLTypePlain(md.Username, md.Password)
		opts.TLSConfig = a.newTLSConfig(md)
	}

	return opts
}

// Close the session
func (a *amqpPubSub) Close() error {
	a.lifecycleMu.Lock()
	if a.closed.CompareAndSwap(false, true) {
		close(a.closeCh)
	}
	a.lifecycleMu.Unlock()

	// Let the subscription goroutines observe closeCh and release their links
	// before the session underneath them is torn down.
	a.wg.Wait()

	a.connMu.Lock()
	defer a.connMu.Unlock()

	// Init may have failed before a connection was established.
	a.closeConnLocked()

	return nil
}

// Features lists what this component guarantees.
//
// FeatureSubscribeWildcards is deliberately not declared. Wildcard syntax is
// broker-specific: Solace matches with "*" and ">", ActiveMQ Artemis with "*"
// and "#" over a "." delimiter. The feature is a single boolean with no syntax
// dimension, so a component that points at any AMQP 1.0 broker cannot promise
// it. metadata.yaml has never declared it either.
func (a *amqpPubSub) Features() []pubsub.Feature {
	return []pubsub.Feature{pubsub.FeatureMessageTTL}
}

// GetComponentMetadata returns the metadata of the component.
func (a *amqpPubSub) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := metadata{}
	_ = contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.PubSubType)
	return
}
