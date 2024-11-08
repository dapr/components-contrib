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

package inmemory

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dapr/components-contrib/common/eventbus"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

type bus struct {
	bus     eventbus.Bus
	log     logger.Logger
	closed  atomic.Bool
	closeCh chan struct{}
	wg      sync.WaitGroup
}

func New(logger logger.Logger) pubsub.PubSub {
	return &bus{
		log:     logger,
		closeCh: make(chan struct{}),
	}
}

func (a *bus) Close() error {
	if a.closed.CompareAndSwap(false, true) {
		close(a.closeCh)
	}
	a.wg.Wait()
	return nil
}

func (a *bus) Features() []pubsub.Feature {
	return []pubsub.Feature{pubsub.FeatureSubscribeWildcards}
}

func (a *bus) Init(_ context.Context, metadata pubsub.Metadata) error {
	a.bus = eventbus.New(true)

	return nil
}

func (a *bus) Publish(_ context.Context, req *pubsub.PublishRequest) error {
	if a.closed.Load() {
		return errors.New("component is closed")
	}

	a.bus.Publish(req.Topic, req.Data)

	return nil
}

func (a *bus) Subscribe(ctx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if a.closed.Load() {
		return errors.New("component is closed")
	}

	// For this component we allow built-in retries because it is backed by memory
	retryHandler := func(data []byte) {
		for range 10 {
			handleErr := handler(ctx, &pubsub.NewMessage{Data: data, Topic: req.Topic, Metadata: req.Metadata})
			if handleErr == nil {
				break
			}
			a.log.Error(handleErr)
			select {
			case <-time.After(100 * time.Millisecond):
				// Nop
			case <-ctx.Done():
				return
			}
		}
	}
	err := a.bus.SubscribeAsync(req.Topic, retryHandler, true)
	if err != nil {
		return err
	}

	// Unsubscribe when context is done
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		select {
		case <-ctx.Done():
		case <-a.closeCh:
		}
		err := a.bus.Unsubscribe(req.Topic, retryHandler)
		if err != nil {
			a.log.Errorf("error while unsubscribing from topic %s: %v", req.Topic, err)
		}
	}()

	return nil
}

// GetComponentMetadata returns the metadata of the component.
func (a *bus) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	return
}
