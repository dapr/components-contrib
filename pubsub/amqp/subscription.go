/*
Copyright 2026 The Dapr Authors
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
	"errors"
	"fmt"
	"strings"
	"time"

	amqp "github.com/Azure/go-amqp"
	backoff "github.com/cenkalti/backoff/v4"

	kitmd "github.com/dapr/kit/metadata"
)

const (
	// Source capabilities that ActiveMQ Artemis and other brokers read on a
	// receiver link. "topic" asks for a subscription on a multicast address,
	// "shared" lets several links consume from one subscription.
	capabilityTopic  = "topic"
	capabilityShared = "shared"

	// selectorFilterName is the key and descriptor name of the JMS selector
	// filter, the same one amqp.NewSelectorFilter writes.
	selectorFilterName = "apache.org:selector-filter:string"

	// recreateRefusal is how Artemis describes the amqp:illegal-state error
	// it refuses an attach with, see selectorChange.
	recreateRefusal = "Unable to recreate subscription"

	// linkNameSeparator is reserved in subscription names. Artemis cuts a
	// shared subscription name at the first "|", which JMS clients use to tell
	// several links to one subscription apart, and it does so on the close
	// path for every subscription. A name that contains it would not map to
	// the queue it created.
	linkNameSeparator = "|"
)

// subscriptionOptions are the per-subscription metadata keys. They come from the
// subscription, not from the component, so that one component can carry
// several subscriptions with different settings.
type subscriptionOptions struct {
	// SubscriptionName is the link name. A broker such as ActiveMQ Artemis
	// names the subscription queue "<containerID>.<subscriptionName>".
	SubscriptionName string

	// Durable keeps the subscription, and the messages it holds, while no
	// consumer is attached.
	Durable bool

	// Shared lets every replica that uses the same subscription name consume
	// from one subscription, so that each message reaches one replica.
	Shared bool

	// Selector is a JMS selector the broker applies to the subscription, for
	// example "type = 'order'". Messages that do not match never enter it.
	Selector string

	// AllowSelectorChange lets the broker recreate a durable subscription
	// whose selector changed, and drop the messages it holds. Without it the
	// subscription ends when the broker reports the change, see
	// selectorChange.
	AllowSelectorChange bool
}

// parseSubscriptionOptions reads the subscription options from the metadata of
// a subscribe request. Keys are matched without regard to case, and unknown
// keys are ignored, because the same map carries runtime keys such as
// rawPayload.
func parseSubscriptionOptions(md map[string]string) (subscriptionOptions, error) {
	var opts subscriptionOptions
	if err := kitmd.DecodeMetadata(md, &opts); err != nil {
		return opts, fmt.Errorf("invalid subscription metadata: %w", err)
	}

	opts.SubscriptionName = strings.TrimSpace(opts.SubscriptionName)
	opts.Selector = strings.TrimSpace(opts.Selector)

	return opts, nil
}

// named reports whether the subscription has a name of its own. A named
// subscription gets its own connection, see attachNamed.
func (o subscriptionOptions) named() bool {
	return o.SubscriptionName != ""
}

// validate checks the options against the topic they are used with.
func (o subscriptionOptions) validate(md *metadata, topic string) error {
	if !o.named() {
		if o.Durable || o.Shared {
			return errors.New("durable and shared subscriptions need a subscriptionName")
		}

		return nil
	}

	if strings.Contains(o.SubscriptionName, linkNameSeparator) {
		return fmt.Errorf("subscriptionName %q must not contain %q", o.SubscriptionName, linkNameSeparator)
	}

	// A queue is consumed directly, not through a subscription, so a
	// subscription name has nothing to name there.
	if strings.HasPrefix(topic, queueScheme) || hasPrefix(topic, md.QueueAddressPrefix) {
		return fmt.Errorf("subscriptionName cannot be used with the queue %q", topic)
	}

	if md.containerID() == "" {
		return errors.New("a named subscription needs a container ID: set containerID or consumerID on the component")
	}

	return nil
}

// receiverOptions returns the options of the receiver link, or nil when the
// subscription sets none, which keeps the broker defaults.
func (o subscriptionOptions) receiverOptions() *amqp.ReceiverOptions {
	if !o.named() && o.Selector == "" {
		return nil
	}

	opts := &amqp.ReceiverOptions{}
	if o.Selector != "" {
		opts.Filters = []amqp.LinkFilter{amqp.NewSelectorFilter(o.Selector)}
	}

	if !o.named() {
		return opts
	}

	// The un-prefixed Durability, ExpiryPolicy and Capabilities fields of
	// ReceiverOptions describe the target. A subscription is the source.
	opts.Name = o.SubscriptionName
	opts.SourceCapabilities = []string{capabilityTopic}
	if o.Shared {
		opts.SourceCapabilities = append(opts.SourceCapabilities, capabilityShared)
	}

	if o.Durable {
		// Never expire. With the link-detach policy, Artemis deletes the
		// subscription on any detach, including a reconnect.
		opts.SourceDurability = amqp.DurabilityUnsettledState
		opts.SourceExpiryPolicy = amqp.ExpiryPolicyNever
	}

	return opts
}

// messageReceiver is the part of *amqp.Receiver that delivery uses. It lets the
// delivery loop run in a test without a broker.
type messageReceiver interface {
	Receive(ctx context.Context, opts *amqp.ReceiveOptions) (*amqp.Message, error)
	AcceptMessage(ctx context.Context, msg *amqp.Message) error
	ModifyMessage(ctx context.Context, msg *amqp.Message, opts *amqp.ModifyMessageOptions) error
}

// subscriptionLink is one attached receiver, and the way to let go of it.
type subscriptionLink struct {
	receiver messageReceiver
	release  func()

	// named is set for a named subscription, see attachNamed.
	named bool

	// sub are the options the link was attached with.
	sub subscriptionOptions
}

// attacher returns how a subscription attaches its receiver link, and the
// function that frees what attaching it claimed. A named subscription claims
// its name here, so the caller must call release once the subscription ends.
func (a *amqpPubSub) attacher(md *metadata, address string, sub subscriptionOptions) (attach func(context.Context) (*subscriptionLink, error), release func(), err error) {
	if !sub.named() {
		return func(ctx context.Context) (*subscriptionLink, error) {
			return a.attachShared(ctx, address, sub)
		}, func() {}, nil
	}

	if err = a.claimSubscriptionName(sub.SubscriptionName); err != nil {
		return nil, nil, err
	}

	return func(ctx context.Context) (*subscriptionLink, error) {
			return a.attachNamed(ctx, md, address, sub)
		}, func() {
			a.releaseSubscriptionName(sub.SubscriptionName)
		}, nil
}

// attachShared attaches a receiver on the shared connection. Releasing it
// detaches the link.
func (a *amqpPubSub) attachShared(ctx context.Context, address string, sub subscriptionOptions) (*subscriptionLink, error) {
	receiver, err := a.newReceiver(ctx, address, sub.receiverOptions())
	if err != nil {
		return nil, err
	}

	if err = checkSelector(sub, receiver.LinkSourceFilterValue(selectorFilterName), address); err != nil {
		a.closeReceiver(receiver, address)

		return nil, err
	}

	return &subscriptionLink{
		receiver: receiver,
		release:  func() { a.closeReceiver(receiver, address) },
	}, nil
}

// attachNamed attaches a named subscription on a connection of its own.
//
// Releasing it closes that connection and never the link. go-amqp can only
// detach a link with closed=true, and a broker such as ActiveMQ Artemis deletes
// a durable subscription, with every message it holds, when its last consumer
// closes the link that way. Ending the connection leaves the subscription in
// place. A shared connection cannot be ended for one subscription, and the
// container ID that names the subscription is set when a connection opens.
func (a *amqpPubSub) attachNamed(ctx context.Context, md *metadata, address string, sub subscriptionOptions) (*subscriptionLink, error) {
	dialCtx, cancel := a.dialContext(ctx)
	defer cancel()

	client, session, err := a.connectAs(dialCtx, md, md.containerID())
	if err != nil {
		return nil, err
	}

	receiver, err := session.NewReceiver(dialCtx, address, sub.receiverOptions())
	if err != nil {
		a.closeClient(client)

		return nil, sub.selectorChange(err, address)
	}

	if err = checkSelector(sub, receiver.LinkSourceFilterValue(selectorFilterName), address); err != nil {
		a.closeClient(client)

		return nil, err
	}

	return &subscriptionLink{
		receiver: receiver,
		release:  func() { a.closeClient(client) },
		named:    true,
		sub:      sub,
	}, nil
}

// selectorChangeError ends a durable subscription that the broker holds with
// another selector, or address, than the one it was asked for.
type selectorChangeError struct {
	sub     subscriptionOptions
	address string
	cause   error
}

func (e *selectorChangeError) Error() string {
	return fmt.Sprintf("the broker holds subscription %q with another address or selector than %s and %q, and would drop the messages it holds to recreate it: use a new subscriptionName, or set allowSelectorChange to recreate it (%v)",
		e.sub.SubscriptionName, e.address, e.sub.Selector, e.cause)
}

func (e *selectorChangeError) Unwrap() error {
	return e.cause
}

// selectorChange turns the broker refusing to attach a durable subscription it
// holds with another selector, or address, into a selectorChangeError. Any
// other error, and this one once the subscription allows the change, is
// returned as is.
//
// ActiveMQ Artemis compares the selector on every attach to a durable
// subscription. With no consumer attached it recreates the subscription,
// drops the messages it holds and reports nothing. With consumers attached, as
// during a rolling update, it refuses the attach with this error instead.
// Retrying the attach until the old consumers detach, which the re-attach loop
// would do, has the subscription recreated the same way, so the refusal is
// the one point at which the loss can be stopped.
func (o subscriptionOptions) selectorChange(err error, address string) error {
	if err == nil || o.AllowSelectorChange {
		return err
	}

	// Artemis answers the attach and detaches right after, so the refusal
	// arrives from the first Receive as a LinkError. A broker that answers
	// the attach with no terminus at all makes it the attach error, a bare
	// Error. Neither type unwraps to the other.
	var protoErr *amqp.Error
	var linkErr *amqp.LinkError
	if errors.As(err, &linkErr) {
		protoErr = linkErr.RemoteErr
	} else if !errors.As(err, &protoErr) {
		return err
	}

	if protoErr == nil || protoErr.Condition != amqp.ErrCondIllegalState || !strings.Contains(protoErr.Description, recreateRefusal) {
		return err
	}

	return &selectorChangeError{sub: o, address: address, cause: err}
}

// checkSelector fails a subscription whose selector the broker did not echo
// back on attach. A broker that does not support selectors may attach without
// the filter and deliver every message.
//
// Artemis echoes the filter it was asked for, even when it then refuses the
// link, so this catches a dropped filter but not a refused one. A refusal
// arrives as the first Receive error instead.
func checkSelector(sub subscriptionOptions, echoed any, address string) error {
	if sub.Selector == "" || echoed != nil {
		return nil
	}

	return fmt.Errorf("the broker attached to %s without the selector %q", address, sub.Selector)
}

// reattachDelay returns how long to wait before re-attaching a link that
// failed.
//
// A broker such as ActiveMQ Artemis accepts some attaches and refuses the
// subscription right after, for example when a second consumer joins a durable
// subscription that is not shared, or when another selector is in use. The
// re-attach then succeeds every time and only the first Receive fails. A named
// subscription whose link failed before it delivered anything therefore waits
// for refused, which grows across such failures and resets once a message
// arrives. Every other link waits reattachFloor.
func reattachDelay(named, received bool, refused backoff.BackOff) time.Duration {
	if !named || received {
		refused.Reset()

		return reattachFloor
	}

	return max(reattachFloor, refused.NextBackOff())
}

// claimSubscriptionName reserves a subscription name for one subscription of
// this component. Two subscriptions with one name would map to one broker
// queue on two addresses, which the broker refuses or resolves by recreating
// the queue.
func (a *amqpPubSub) claimSubscriptionName(name string) error {
	a.namesMu.Lock()
	defer a.namesMu.Unlock()

	if _, ok := a.names[name]; ok {
		return fmt.Errorf("subscriptionName %q is already in use by another subscription of this component", name)
	}

	if a.names == nil {
		a.names = make(map[string]struct{})
	}
	a.names[name] = struct{}{}

	return nil
}

// releaseSubscriptionName frees a name claimed by claimSubscriptionName.
func (a *amqpPubSub) releaseSubscriptionName(name string) {
	a.namesMu.Lock()
	defer a.namesMu.Unlock()

	delete(a.names, name)
}
