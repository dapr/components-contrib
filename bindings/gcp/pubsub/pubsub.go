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
	kitmd "github.com/dapr/kit/metadata"
)

const (
	id          = "id"
	publishTime = "publishTime"
	topic       = "topic"
)

// GCPPubSub is an input/output binding for GCP Pub Sub.
type GCPPubSub struct {
	client   *pubsub.Client
	metadata *pubSubMetadata
	logger   logger.Logger
	closed   atomic.Bool
	closeCh  chan struct{}
	wg       sync.WaitGroup
}

// These JSON tags directly match the builtin auth provider metadata fields for GCP.
type pubSubMetadata struct {
	Topic        string `json:"topic" mapstructure:"topic"`
	Subscription string `json:"subscription" mapstructure:"subscription"`

	// Note: the mdignore is to ignore these fields on the metadata analyzer,
	// as these fields are parsed and used by the builtin auth provider,
	// so they are still captured in the metadata.yaml file and in parsing.
	Type                    string `json:"type" mapstructure:"type" mdignore:"true"`
	ProjectID               string `json:"projectID" mapstructure:"project_id" mapstructurealiases:"projectID" mdignore:"true"`
	PrivateKeyID            string `json:"privateKeyID" mapstructure:"private_key_id" mapstructurealiases:"privateKeyID" mdignore:"true"`
	PrivateKey              string `json:"privateKey" mapstructure:"private_key" mapstructurealiases:"privateKey" mdignore:"true"`
	ClientEmail             string `json:"clientEmail" mapstructure:"client_email" mapstructurealiases:"clientEmail" mdignore:"true"`
	ClientID                string `json:"clientID" mapstructure:"client_id" mapstructurealiases:"clientID" mdignore:"true"`
	AuthURI                 string `json:"authURI" mapstructure:"auth_uri" mapstructurealiases:"authURI" mdignore:"true"`
	TokenURI                string `json:"tokenURI" mapstructure:"token_uri" mapstructurealiases:"tokenURI" mdignore:"true"`
	AuthProviderX509CertURL string `json:"authProviderX509CertURL" mapstructure:"auth_provider_x509_cert_url" mapstructurealiases:"authProviderX509CertURL" mdignore:"true"`
	ClientX509CertURL       string `json:"clientX509CertURL" mapstructure:"client_x509_cert_url" mapstructurealiases:"clientX509CertURL" mdignore:"true"`
}

type GCPAuthJSON struct {
	ProjectID           string `json:"project_id"`
	PrivateKeyID        string `json:"private_key_id"`
	PrivateKey          string `json:"private_key"`
	ClientEmail         string `json:"client_email"`
	ClientID            string `json:"client_id"`
	AuthURI             string `json:"auth_uri"`
	TokenURI            string `json:"token_uri"`
	AuthProviderCertURL string `json:"auth_provider_x509_cert_url"`
	ClientCertURL       string `json:"client_x509_cert_url"`
	Type                string `json:"type"`
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
	var (
		pubsubMeta   pubSubMetadata
		pubsubClient *pubsub.Client
	)
	err := kitmd.DecodeMetadata(metadata.Properties, &pubsubMeta)
	if err != nil {
		return err
	}

	if pubsubMeta.PrivateKeyID != "" {
		authJSON := &GCPAuthJSON{
			ProjectID:           pubsubMeta.ProjectID,
			PrivateKeyID:        pubsubMeta.PrivateKeyID,
			PrivateKey:          pubsubMeta.PrivateKey,
			ClientEmail:         pubsubMeta.ClientEmail,
			ClientID:            pubsubMeta.ClientID,
			AuthURI:             pubsubMeta.AuthURI,
			TokenURI:            pubsubMeta.TokenURI,
			AuthProviderCertURL: pubsubMeta.AuthProviderX509CertURL,
			ClientCertURL:       pubsubMeta.ClientX509CertURL,
			Type:                pubsubMeta.Type,
		}
		gcpCompatibleJSON, _ := json.Marshal(authJSON)
		clientOptions := option.WithCredentialsJSON(gcpCompatibleJSON)
		pubsubClient, err = pubsub.NewClient(ctx, pubsubMeta.ProjectID, clientOptions)
		if err != nil {
			return fmt.Errorf("error creating pubsub client: %s", err)
		}
	} else {
		pubsubClient, err = pubsub.NewClient(ctx, pubsubMeta.ProjectID)
		if err != nil {
			return fmt.Errorf("error creating pubsub client: %s", err)
		}
	}

	g.client = pubsubClient
	g.metadata = &pubsubMeta

	return nil
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
	metadataStruct := pubSubMetadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.BindingType)
	return
}
