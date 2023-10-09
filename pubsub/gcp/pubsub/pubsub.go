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
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	gcppubsub "cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

const (
	errorMessagePrefix = "gcp pubsub error:"

	// Metadata keys.
	metadataProjectIDKey   = "projectId"
	metedataOrderingKeyKey = "orderingKey"

	// Defaults.
	defaultMaxReconnectionAttempts = 30
	defaultConnectionRecoveryInSec = 2
	defaultMaxDeliveryAttempts     = 5
)

// GCPPubSub type.
type GCPPubSub struct {
	client   *gcppubsub.Client
	metadata *metadata
	logger   logger.Logger

	closed  atomic.Bool
	closeCh chan struct{}
	wg      sync.WaitGroup
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

type WhatNow struct {
	Type string `json:"type"`
}

// NewGCPPubSub returns a new GCPPubSub instance.
func NewGCPPubSub(logger logger.Logger) pubsub.PubSub {
	return &GCPPubSub{logger: logger, closeCh: make(chan struct{})}
}

func createMetadata(pubSubMetadata pubsub.Metadata) (*metadata, error) {
	// TODO: Add the rest of the metadata here, add defaults where applicable
	result := metadata{
		DisableEntityManagement: false,
		Type:                    "service_account",
		MaxReconnectionAttempts: defaultMaxReconnectionAttempts,
		ConnectionRecoveryInSec: defaultConnectionRecoveryInSec,
		MaxDeliveryAttempts:     defaultMaxDeliveryAttempts,
	}

	err := contribMetadata.DecodeMetadata(pubSubMetadata.Properties, &result)
	if err != nil {
		return nil, err
	}

	if result.ProjectID == "" {
		return &result, fmt.Errorf("%s missing attribute %s", errorMessagePrefix, metadataProjectIDKey)
	}

	return &result, nil
}

// Init parses metadata and creates a new Pub Sub client.
func (g *GCPPubSub) Init(ctx context.Context, meta pubsub.Metadata) error {
	metadata, err := createMetadata(meta)
	if err != nil {
		return err
	}

	pubsubClient, err := g.getPubSubClient(ctx, metadata)
	if err != nil {
		return fmt.Errorf("%s error creating pubsub client: %w", errorMessagePrefix, err)
	}

	g.client = pubsubClient
	g.metadata = metadata

	return nil
}

func (g *GCPPubSub) getPubSubClient(ctx context.Context, metadata *metadata) (*gcppubsub.Client, error) {
	var pubsubClient *gcppubsub.Client
	var err error

	// context.Background is used here, as the context used to Dial the
	// server in the gRPC DialPool. Callers should always call `Close` on the
	// component to ensure all resources are released.
	if metadata.PrivateKeyID != "" {
		// TODO: validate that all auth json fields are filled
		authJSON := &GCPAuthJSON{
			ProjectID:           metadata.IdentityProjectID,
			PrivateKeyID:        metadata.PrivateKeyID,
			PrivateKey:          metadata.PrivateKey,
			ClientEmail:         metadata.ClientEmail,
			ClientID:            metadata.ClientID,
			AuthURI:             metadata.AuthURI,
			TokenURI:            metadata.TokenURI,
			AuthProviderCertURL: metadata.AuthProviderCertURL,
			ClientCertURL:       metadata.ClientCertURL,
			Type:                metadata.Type,
		}
		gcpCompatibleJSON, _ := json.Marshal(authJSON)
		g.logger.Debugf("Using explicit credentials for GCP")
		clientOptions := option.WithCredentialsJSON(gcpCompatibleJSON)
		pubsubClient, err = gcppubsub.NewClient(context.Background(), metadata.ProjectID, clientOptions)
		if err != nil {
			return pubsubClient, err
		}
	} else {
		g.logger.Debugf("Using implicit credentials for GCP")

		// The following allows the Google SDK to connect to
		// the GCP PubSub Emulator.
		// example: export PUBSUB_EMULATOR_HOST=localhost:8085
		// see: https://cloud.google.com/pubsub/docs/emulator#env
		if metadata.ConnectionEndpoint != "" {
			g.logger.Debugf("setting GCP PubSub Emulator environment variable to 'PUBSUB_EMULATOR_HOST=%s'", metadata.ConnectionEndpoint)
			os.Setenv("PUBSUB_EMULATOR_HOST", metadata.ConnectionEndpoint)
		}
		pubsubClient, err = gcppubsub.NewClient(context.Background(), metadata.ProjectID)
		if err != nil {
			return pubsubClient, err
		}
	}

	return pubsubClient, nil
}

// Publish the topic to GCP Pubsub.
func (g *GCPPubSub) Publish(ctx context.Context, req *pubsub.PublishRequest) error {
	if g.closed.Load() {
		return errors.New("component is closed")
	}

	if !g.metadata.DisableEntityManagement {
		err := g.ensureTopic(ctx, req.Topic)
		if err != nil {
			return fmt.Errorf("%s could not get valid topic %s, %s", errorMessagePrefix, req.Topic, err)
		}
	}

	topic := g.getTopic(req.Topic)

	msg := &gcppubsub.Message{
		Data: req.Data,
	}

	// If Message Ordering is enabled,
	// use the provided OrderingKey giving
	// preference to the OrderingKey at the request level
	if g.metadata.EnableMessageOrdering {
		topic.EnableMessageOrdering = g.metadata.EnableMessageOrdering
		msgOrderingKey := g.metadata.OrderingKey
		if req.Metadata != nil && req.Metadata[metedataOrderingKeyKey] != "" {
			msgOrderingKey = req.Metadata[metedataOrderingKeyKey]
		}
		msg.OrderingKey = msgOrderingKey
		g.logger.Infof("Message Ordering Key: %s", msg.OrderingKey)
	}
	_, err := topic.Publish(ctx, msg).Get(ctx)

	return err
}

// Subscribe to the GCP Pubsub topic.
func (g *GCPPubSub) Subscribe(parentCtx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if g.closed.Load() {
		return errors.New("component is closed")
	}

	if !g.metadata.DisableEntityManagement {
		topicErr := g.ensureTopic(parentCtx, req.Topic)
		if topicErr != nil {
			return fmt.Errorf("%s could not get valid topic - topic:%q, error: %v", errorMessagePrefix, req.Topic, topicErr)
		}

		subError := g.ensureSubscription(parentCtx, g.metadata.ConsumerID, req.Topic)
		if subError != nil {
			return fmt.Errorf("%s could not get valid subscription - consumerID:%q, error: %v", errorMessagePrefix, g.metadata.ConsumerID, subError)
		}
	}

	topic := g.getTopic(req.Topic)
	sub := g.getSubscription(BuildSubscriptionID(g.metadata.ConsumerID, req.Topic))

	subscribeCtx, cancel := context.WithCancel(parentCtx)
	g.wg.Add(2)
	go func() {
		defer g.wg.Done()
		defer cancel()
		select {
		case <-subscribeCtx.Done():
		case <-g.closeCh:
		}
	}()
	go func() {
		defer g.wg.Done()
		g.handleSubscriptionMessages(subscribeCtx, topic, sub, handler)
	}()

	return nil
}

func BuildSubscriptionID(consumerID, topic string) string {
	return fmt.Sprintf("%s-%s", consumerID, topic)
}

func (g *GCPPubSub) handleSubscriptionMessages(parentCtx context.Context, topic *gcppubsub.Topic, sub *gcppubsub.Subscription, handler pubsub.Handler) error {
	// Limit the number of attempted reconnects we make.
	reconnAttempts := make(chan struct{}, g.metadata.MaxReconnectionAttempts)
	for i := 0; i < g.metadata.MaxReconnectionAttempts; i++ {
		reconnAttempts <- struct{}{}
	}

	readReconnectAttemptsRemaining := func() int { return len(reconnAttempts) }

	// Periodically refill the reconnect attempts channel to avoid
	// exhausting all the refill attempts due to intermittent issues
	// occurring over a longer period of time.
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		reconnCtx, reconnCancel := context.WithCancel(parentCtx)
		defer reconnCancel()

		// Calculate refill interval relative to MaxReconnectionAttempts and ConnectionRecoveryInSec
		// in order to allow reconnection attempts to be exhausted in case of a permanent issue (e.g. wrong subscription name).
		refillInterval := time.Duration(2*g.metadata.MaxReconnectionAttempts*g.metadata.ConnectionRecoveryInSec) * time.Second
		for {
			select {
			case <-reconnCtx.Done():
				g.logger.Debugf("Reconnect context for subscription %s is done", sub.ID())
				return
			case <-time.After(refillInterval):
				attempts := readReconnectAttemptsRemaining()
				if attempts < g.metadata.MaxReconnectionAttempts {
					reconnAttempts <- struct{}{}
				}
				g.logger.Debugf("Number of reconnect attempts remaining for subscription %s: %d", sub.ID(), attempts)
			}
		}
	}()

	// Reconnect loop.
	var receiveErr error = nil
	for {
		receiveErr = sub.Receive(parentCtx, func(ctx context.Context, m *gcppubsub.Message) {
			msg := &pubsub.NewMessage{
				Data:  m.Data,
				Topic: topic.ID(),
			}

			err := handler(ctx, msg)

			if err == nil {
				m.Ack()
			} else {
				m.Nack()
			}
		})

		g.logger.Infof("Lost connection to subscription %s", sub.ID())
		// Exit out of reconnect loop if Receive method returns without error.
		if receiveErr == nil || receiveErr == context.Canceled {
			g.logger.Infof("Subscription was cancelled, not reconnecting.")
			break
		}

		// If we get to here - there was an error returned from the Receive method.
		g.logger.Warnf("Connection to subscription %s closed with error: %s", sub.ID(), receiveErr)
		reconnectionAttemptsRemaining := readReconnectAttemptsRemaining()
		if reconnectionAttemptsRemaining == 0 {
			g.logger.Errorf("Reconnection attempts exhausted for subscription %s, giving up after %d attempts.", sub.ID(), g.metadata.MaxReconnectionAttempts)
			break
		}

		g.logger.Infof("Sleeping for %d seconds before attempting to reconnect to subscription %s ... [%d/%d]", g.metadata.ConnectionRecoveryInSec, sub.ID(), g.metadata.MaxReconnectionAttempts-reconnectionAttemptsRemaining, g.metadata.MaxReconnectionAttempts)
		select {
		case <-time.After(time.Second * time.Duration(g.metadata.ConnectionRecoveryInSec)):
		case <-parentCtx.Done():
			break
		}

		<-reconnAttempts
	}

	return receiveErr
}

func (g *GCPPubSub) ensureTopic(parentCtx context.Context, topic string) error {
	entity := g.getTopic(topic)
	exists, err := entity.Exists(parentCtx)
	if err != nil {
		return err
	}

	if !exists {
		_, err = g.client.CreateTopic(parentCtx, topic)
		if status.Code(err) == codes.AlreadyExists {
			return nil
		}

		return err
	}

	return nil
}

func (g *GCPPubSub) getTopic(topic string) *gcppubsub.Topic {
	return g.client.Topic(topic)
}

func (g *GCPPubSub) ensureSubscription(parentCtx context.Context, subscription string, topic string) error {
	err := g.ensureTopic(parentCtx, topic)
	if err != nil {
		return err
	}

	managedSubscription := subscription + "-" + topic
	entity := g.getSubscription(managedSubscription)
	exists, subErr := entity.Exists(parentCtx)
	if !exists {
		subConfig := gcppubsub.SubscriptionConfig{
			AckDeadline:           20 * time.Second,
			Topic:                 g.getTopic(topic),
			EnableMessageOrdering: g.metadata.EnableMessageOrdering,
		}

		if g.metadata.DeadLetterTopic != "" {
			subErr = g.ensureTopic(parentCtx, g.metadata.DeadLetterTopic)
			if subErr != nil {
				return subErr
			}
			dlTopic := fmt.Sprintf("projects/%s/topics/%s", g.metadata.ProjectID, g.metadata.DeadLetterTopic)
			subConfig.DeadLetterPolicy = &gcppubsub.DeadLetterPolicy{
				DeadLetterTopic:     dlTopic,
				MaxDeliveryAttempts: g.metadata.MaxDeliveryAttempts,
			}
		}
		_, subErr = g.client.CreateSubscription(parentCtx, managedSubscription, subConfig)
		if subErr != nil {
			g.logger.Errorf("unable to create subscription (%s): %#v - %v ", managedSubscription, subConfig, subErr)
		}
	}

	return subErr
}

func (g *GCPPubSub) getSubscription(subscription string) *gcppubsub.Subscription {
	return g.client.Subscription(subscription)
}

func (g *GCPPubSub) Close() error {
	defer g.wg.Wait()
	if g.closed.CompareAndSwap(false, true) {
		close(g.closeCh)
	}
	return g.client.Close()
}

func (g *GCPPubSub) Features() []pubsub.Feature {
	return nil
}

// GetComponentMetadata returns the metadata of the component.
func (g *GCPPubSub) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := metadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.PubSubType)
	return
}
