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
	"fmt"
	"strconv"
	"time"

	gcppubsub "cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/kit/logger"
)

const (
	errorMessagePrefix = "gcp pubsub error:"

	// Metadata keys.
	metadataConsumerIDKey              = "consumerID"
	metadataTypeKey                    = "type"
	metadataProjectIDKey               = "projectId"
	metadataIdentityProjectIDKey       = "identityProjectId"
	metadataPrivateKeyIDKey            = "privateKeyId"
	metadataClientEmailKey             = "clientEmail"
	metadataClientIDKey                = "clientId"
	metadataAuthURIKey                 = "authUri"
	metadataTokenURIKey                = "tokenUri"
	metadataAuthProviderX509CertURLKey = "authProviderX509CertUrl"
	metadataClientX509CertURLKey       = "clientX509CertUrl"
	metadataPrivateKeyKey              = "privateKey"
	metadataDisableEntityManagementKey = "disableEntityManagement"
	metadataEnableMessageOrderingKey   = "enableMessageOrdering"
	metadataMaxReconnectionAttemptsKey = "maxReconnectionAttempts"
	metadataConnectionRecoveryInSecKey = "connectionRecoveryInSec"

	// Defaults.
	defaultMaxReconnectionAttempts = 30
	defaultConnectionRecoveryInSec = 2
)

// GCPPubSub type.
type GCPPubSub struct {
	pubsub.DefaultBatcher
	client        *gcppubsub.Client
	metadata      *metadata
	logger        logger.Logger
	publishCtx    context.Context
	publishCancel context.CancelFunc
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
	return &GCPPubSub{logger: logger}
}

func createMetadata(pubSubMetadata pubsub.Metadata) (*metadata, error) {
	// TODO: Add the rest of the metadata here, add defaults where applicable
	result := metadata{
		DisableEntityManagement: false,
		Type:                    "service_account",
	}

	if val, found := pubSubMetadata.Properties[metadataTypeKey]; found && val != "" {
		result.Type = val
	}

	if val, found := pubSubMetadata.Properties[metadataConsumerIDKey]; found && val != "" {
		result.consumerID = val
	}

	if val, found := pubSubMetadata.Properties[metadataIdentityProjectIDKey]; found && val != "" {
		result.IdentityProjectID = val
	}

	if val, found := pubSubMetadata.Properties[metadataProjectIDKey]; found && val != "" {
		result.ProjectID = val
	} else {
		return &result, fmt.Errorf("%s missing attribute %s", errorMessagePrefix, metadataProjectIDKey)
	}

	if val, found := pubSubMetadata.Properties[metadataPrivateKeyIDKey]; found && val != "" {
		result.PrivateKeyID = val
	}

	if val, found := pubSubMetadata.Properties[metadataClientEmailKey]; found && val != "" {
		result.ClientEmail = val
	}

	if val, found := pubSubMetadata.Properties[metadataClientIDKey]; found && val != "" {
		result.ClientID = val
	}

	if val, found := pubSubMetadata.Properties[metadataAuthURIKey]; found && val != "" {
		result.AuthURI = val
	}

	if val, found := pubSubMetadata.Properties[metadataTokenURIKey]; found && val != "" {
		result.TokenURI = val
	}

	if val, found := pubSubMetadata.Properties[metadataAuthProviderX509CertURLKey]; found && val != "" {
		result.AuthProviderCertURL = val
	}

	if val, found := pubSubMetadata.Properties[metadataClientX509CertURLKey]; found && val != "" {
		result.ClientCertURL = val
	}

	if val, found := pubSubMetadata.Properties[metadataPrivateKeyKey]; found && val != "" {
		result.PrivateKey = val
	}

	if val, found := pubSubMetadata.Properties[metadataDisableEntityManagementKey]; found && val != "" {
		if boolVal, err := strconv.ParseBool(val); err == nil {
			result.DisableEntityManagement = boolVal
		}
	}

	if val, found := pubSubMetadata.Properties[metadataEnableMessageOrderingKey]; found && val != "" {
		if boolVal, err := strconv.ParseBool(val); err == nil {
			result.EnableMessageOrdering = boolVal
		}
	}

	result.MaxReconnectionAttempts = defaultMaxReconnectionAttempts
	if val, ok := pubSubMetadata.Properties[metadataMaxReconnectionAttemptsKey]; ok && val != "" {
		var err error
		result.MaxReconnectionAttempts, err = strconv.Atoi(val)
		if err != nil {
			return &result, fmt.Errorf("%s invalid maxReconnectionAttempts %s, %s", errorMessagePrefix, val, err)
		}
	}

	result.ConnectionRecoveryInSec = defaultConnectionRecoveryInSec
	if val, ok := pubSubMetadata.Properties[metadataConnectionRecoveryInSecKey]; ok && val != "" {
		var err error
		result.ConnectionRecoveryInSec, err = strconv.Atoi(val)
		if err != nil {
			return &result, fmt.Errorf("%s invalid connectionRecoveryInSec %s, %s", errorMessagePrefix, val, err)
		}
	}

	return &result, nil
}

// Init parses metadata and creates a new Pub Sub client.
func (g *GCPPubSub) Init(meta pubsub.Metadata) error {
	metadata, err := createMetadata(meta)
	if err != nil {
		return err
	}

	pubsubClient, err := g.getPubSubClient(context.Background(), metadata)
	if err != nil {
		return fmt.Errorf("%s error creating pubsub client: %w", errorMessagePrefix, err)
	}

	g.client = pubsubClient
	g.metadata = metadata

	g.publishCtx, g.publishCancel = context.WithCancel(context.Background())

	return nil
}

func (g *GCPPubSub) getPubSubClient(ctx context.Context, metadata *metadata) (*gcppubsub.Client, error) {
	var pubsubClient *gcppubsub.Client
	var err error

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
		pubsubClient, err = gcppubsub.NewClient(ctx, metadata.ProjectID, clientOptions)
		if err != nil {
			return pubsubClient, err
		}
	} else {
		g.logger.Debugf("Using implicit credentials for GCP")
		pubsubClient, err = gcppubsub.NewClient(ctx, metadata.ProjectID)
		if err != nil {
			return pubsubClient, err
		}
	}

	return pubsubClient, nil
}

// Publish the topic to GCP Pubsub.
func (g *GCPPubSub) Publish(req *pubsub.PublishRequest) error {
	if !g.metadata.DisableEntityManagement {
		err := g.ensureTopic(g.publishCtx, req.Topic)
		if err != nil {
			return fmt.Errorf("%s could not get valid topic %s, %s", errorMessagePrefix, req.Topic, err)
		}
	}

	topic := g.getTopic(req.Topic)

	_, err := topic.Publish(g.publishCtx, &gcppubsub.Message{
		Data: req.Data,
	}).Get(g.publishCtx)

	return err
}

// Subscribe to the GCP Pubsub topic.
func (g *GCPPubSub) Subscribe(subscribeCtx context.Context, req pubsub.SubscribeRequest, handler pubsub.Handler) error {
	if !g.metadata.DisableEntityManagement {
		topicErr := g.ensureTopic(subscribeCtx, req.Topic)
		if topicErr != nil {
			return fmt.Errorf("%s could not get valid topic %s, %s", errorMessagePrefix, req.Topic, topicErr)
		}

		subError := g.ensureSubscription(subscribeCtx, g.metadata.consumerID, req.Topic)
		if subError != nil {
			return fmt.Errorf("%s could not get valid subscription %s, %s", errorMessagePrefix, g.metadata.consumerID, subError)
		}
	}

	topic := g.getTopic(req.Topic)
	sub := g.getSubscription(g.metadata.consumerID + "-" + req.Topic)

	go g.handleSubscriptionMessages(subscribeCtx, topic, sub, handler)

	return nil
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
	go func() {
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
		time.Sleep(time.Second * time.Duration(g.metadata.ConnectionRecoveryInSec))
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
		_, subErr = g.client.CreateSubscription(parentCtx, managedSubscription, gcppubsub.SubscriptionConfig{
			Topic:                 g.getTopic(topic),
			EnableMessageOrdering: g.metadata.EnableMessageOrdering,
		})
	}

	return subErr
}

func (g *GCPPubSub) getSubscription(subscription string) *gcppubsub.Subscription {
	return g.client.Subscription(subscription)
}

func (g *GCPPubSub) Close() error {
	g.publishCancel()
	return g.client.Close()
}

func (g *GCPPubSub) Features() []pubsub.Feature {
	return nil
}
