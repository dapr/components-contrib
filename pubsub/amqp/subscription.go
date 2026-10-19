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

	amqp "github.com/Azure/go-amqp"

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

	// A subscription is a copy of a topic. A queue is consumed directly, so a
	// subscription name on it has nothing to name.
	if strings.HasPrefix(topic, queueScheme) || hasPrefix(topic, md.QueueAddressPrefix) {
		return fmt.Errorf("subscriptionName cannot be used with the queue %q", topic)
	}

	if md.containerID() == "" {
		return errors.New("a named subscription needs a containerID or a consumerID")
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

// subscriptionLink is one attached receiver, and the way to let go of it.
type subscriptionLink struct {
	receiver *amqp.Receiver
	release  func()
}

// attachShared attaches a receiver on the shared connection. Releasing it
// detaches the link.
func (a *amqpPubSub) attachShared(ctx context.Context, address string, opts *amqp.ReceiverOptions) (*subscriptionLink, error) {
	receiver, err := a.newReceiver(ctx, address, opts)
	if err != nil {
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

		return nil, err
	}

	// A broker that does not support selectors may attach without the filter
	// and deliver every message. Artemis echoes the filter it applied, even
	// when it then refuses the link, so this catches a dropped filter but not
	// a refused one. A refusal arrives as the first Receive error instead.
	if sub.Selector != "" && receiver.LinkSourceFilterValue(selectorFilterName) == nil {
		a.closeClient(client)

		return nil, fmt.Errorf("the broker attached the subscription %q to %s without its selector", sub.SubscriptionName, address)
	}

	return &subscriptionLink{
		receiver: receiver,
		release:  func() { a.closeClient(client) },
	}, nil
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
