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

package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"

	"github.com/dapr/components-contrib/bindings"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const (
	id          = "id"
	publishTime = "publishTime"
	topic       = "topic"
)

// GCPPubSub is an input/output binding for GCP Pub Sub.
type GCPPubSub struct {
	client   *pubsub.Client
	metadata *pubsubMetadata
	logger   logger.Logger
	closed   atomic.Bool
	closeCh  chan struct{}
	wg       sync.WaitGroup
}

// NewGCPPubSub returns a new GCPPubSub instance.
func NewGCPPubSub(logger logger.Logger) bindings.InputOutputBinding {
	return &GCPPubSub{
		logger:  logger,
		closeCh: make(chan struct{}),
	}
}

// Init parses metadata and creates a new Pub Sub client.
func (g *GCPPubSub) Init(ctx context.Context, metadata bindings.Metadata) error {
	b, err := g.parseMetadata(metadata)
	if err != nil {
		return err
	}

	var pubsubMeta pubsubMetadata
	err = json.Unmarshal(b, &pubsubMeta)
	if err != nil {
		return err
	}
	clientOptions := option.WithCredentialsJSON(b)
	pubsubClient, err := pubsub.NewClient(ctx, pubsubMeta.ProjectID, clientOptions)
	if err != nil {
		return fmt.Errorf("error creating pubsub client: %s", err)
	}

	g.client = pubsubClient
	g.metadata = &pubsubMeta

	return nil
}

func (g *GCPPubSub) parseMetadata(metadata bindings.Metadata) ([]byte, error) {
	return json.Marshal(metadata.Properties)
}

func (g *GCPPubSub) Read(ctx context.Context, handler bindings.Handler) error {
	if g.closed.Load() {
		return errors.New("binding is closed")
	}
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		sub := g.client.Subscription(g.metadata.Subscription)
		err := sub.Receive(ctx, func(c context.Context, m *pubsub.Message) {
			_, err := handler(c, &bindings.ReadResponse{
				Data:     m.Data,
				Metadata: map[string]string{id: m.ID, publishTime: m.PublishTime.String()},
			})
			if err != nil {
				m.Nack()
				return
			}
			m.Ack()
		})
		if err != nil {
			g.logger.Errorf("error receiving messages: %v", err)
		}
	}()

	return nil
}

func (g *GCPPubSub) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (g *GCPPubSub) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	topicName := g.metadata.Topic
	if val, ok := req.Metadata[topic]; ok && val != "" {
		topicName = val
	}

	t := g.client.Topic(topicName)
	_, err := t.Publish(ctx, &pubsub.Message{
		Data: req.Data,
	}).Get(ctx)

	return nil, err
}

func (g *GCPPubSub) Close() error {
	if g.closed.CompareAndSwap(false, true) {
		close(g.closeCh)
	}
	defer g.wg.Wait()
	return g.client.Close()
}

// GetComponentMetadata returns the metadata of the component.
func (g *GCPPubSub) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := pubsubMetadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.BindingType)
	return
}
