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

package daprmq

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"

	mq "github.com/olitomlinson/dapr-mq/sdks/go"

	"github.com/dapr/components-contrib/pubsub"
)

// deliveryCountKey carries a message's delivery count to the app, as Service Bus does.
const deliveryCountKey = "metadata.DeliveryCount"

// queueStream is the part of an SDK Consume stream the component uses, so tests can fake it.
type queueStream interface {
	Receive() (streamDelivery, error)
	Close() error
}

type streamDelivery interface {
	LockID() string
	Item() json.RawMessage
	DeliveryCount() int
	Ack() error
	Nack() error
	DeadLetter() error
}

type sdkStream struct{ stream *mq.QueueStream }

func (s sdkStream) Receive() (streamDelivery, error) {
	d, err := s.stream.Receive()
	if err != nil {
		return nil, err
	}
	return sdkDelivery{d}, nil
}

func (s sdkStream) Close() error { return s.stream.Close() }

type sdkDelivery struct{ d *mq.QueueDelivery }

func (d sdkDelivery) LockID() string        { return d.d.LockID }
func (d sdkDelivery) Item() json.RawMessage { return d.d.Item }
func (d sdkDelivery) DeliveryCount() int    { return d.d.DeliveryCount }
func (d sdkDelivery) Ack() error            { return d.d.Ack() }
func (d sdkDelivery) Nack() error           { return d.d.Nack() }
func (d sdkDelivery) DeadLetter() error     { return d.d.DeadLetter() }

// consume receives over a Consume stream until ctx ends, reopening the stream with backoff
// whenever it breaks. The server keeps up to maxActiveMessages delivered and renews their locks.
func (d *PubSub) consume(ctx context.Context, topic, queueID string, handler pubsub.Handler) {
	// Strict order needs one message in flight: a later message held while an earlier one is
	// nacked back into place would overtake it.
	strict := !d.md.AllowCompetingConsumers || d.md.ConcurrencyMode == pubsub.Single
	options := &mq.ConsumeOptions{
		PrefetchCount:           d.md.MaxActiveMessages,
		LockTTL:                 d.md.LockTTL,
		AllowCompetingConsumers: d.md.AllowCompetingConsumers,
		OnSettleFailed: func(lockID string, err error) {
			d.logger.Warnf("daprmq: settling %s on %s failed: %v", lockID, queueID, err)
		},
	}
	if strict {
		options.PrefetchCount = 1
	}

	bo := newBackOff()
	for ctx.Err() == nil {
		// The stream outlives ctx: on unsubscribe, handlers still running settle on it before it closes.
		stream, err := d.openStream(context.WithoutCancel(ctx), queueID, options)
		if err != nil {
			d.logger.Warnf("daprmq: opening a stream on %s failed: %v", queueID, err)
			wait(ctx, bo.NextBackOff())
			continue
		}

		delivered, err := d.serveStream(ctx, topic, queueID, stream, handler, strict)
		if ctx.Err() != nil {
			return
		}
		if delivered {
			bo.Reset()
		}
		d.logger.Warnf("daprmq: stream on %s ended, reopening: %v", queueID, err)
		wait(ctx, bo.NextBackOff())
	}
}

// serveStream hands each delivery to the handler until the stream ends or ctx does. When ctx ends,
// handlers already running settle first, then the stream is closed and the server returns every
// delivery that was never handled. It reports whether anything was delivered, and why the stream
// ended if it ended on its own.
func (d *PubSub) serveStream(ctx context.Context, topic, queueID string, stream queueStream, handler pubsub.Handler, strict bool) (bool, error) {
	received := make(chan streamDelivery)
	ended := make(chan error, 1)
	go func() {
		defer close(received)
		for {
			delivery, err := stream.Receive()
			if err != nil {
				ended <- err
				return
			}
			received <- delivery
		}
	}()

	var sem chan struct{}
	if !strict && d.md.MaxConcurrentHandlers > 0 {
		sem = make(chan struct{}, d.md.MaxConcurrentHandlers)
	}
	var handlers sync.WaitGroup
	delivered := false

	for {
		select {
		case delivery, ok := <-received:
			if !ok {
				handlers.Wait()
				return delivered, <-ended
			}
			delivered = true
			if ctx.Err() != nil {
				continue // left unsettled: the server returns it when the stream closes
			}
			if strict {
				d.handleStreamDelivery(ctx, topic, queueID, delivery, handler)
				continue
			}
			if sem != nil {
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					continue
				}
			}
			handlers.Add(1)
			go func() {
				defer handlers.Done()
				if sem != nil {
					defer func() { <-sem }()
				}
				d.handleStreamDelivery(ctx, topic, queueID, delivery, handler)
			}()

		case <-ctx.Done():
			handlers.Wait()
			_ = stream.Close()
			for range received {
				// Drain until the server ends the stream; these are returned to the queue.
			}
			return delivered, nil
		}
	}
}

// handleStreamDelivery calls the handler and settles the delivery on its stream.
func (d *PubSub) handleStreamDelivery(ctx context.Context, topic, queueID string, delivery streamDelivery, handler pubsub.Handler) {
	msg, err := unwrap(delivery.Item(), topic)
	if err != nil {
		d.logger.Errorf("daprmq: dead-lettering undecodable message from %s: %v", queueID, err)
		if dlErr := delivery.DeadLetter(); dlErr != nil {
			d.logger.Warnf("daprmq: dead-letter %s failed: %v", delivery.LockID(), dlErr)
		}
		return
	}
	if msg.Metadata == nil {
		msg.Metadata = map[string]string{}
	}
	msg.Metadata[deliveryCountKey] = strconv.Itoa(delivery.DeliveryCount())

	handlerErr := ctx.Err()
	if handlerErr == nil {
		handlerErr = handler(ctx, msg)
	}

	if handlerErr == nil {
		err = delivery.Ack()
	} else {
		d.paceRetry(ctx)
		err = delivery.Nack()
	}
	if err != nil {
		d.logger.Warnf("daprmq: settling %s failed: %v", delivery.LockID(), err)
	}
}
