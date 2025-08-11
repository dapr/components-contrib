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
	kitmd "github.com/dapr/kit/metadata"
)

const (
	errorMessagePrefix = "gcp pubsub error:"

	// Metadata keys.
	metadataProjectIDKey   = "projectId"
	metedataOrderingKeyKey = "orderingKey"
	metadataAckDeadlineKey = "ackDeadline"

	// Defaults.
	defaultMaxReconnectionAttempts = 30
	defaultConnectionRecoveryInSec = 2
	defaultMaxDeliveryAttempts     = 5
	defaultAckDeadline             = 20 * time.Second
)

// GCPPubSub type.
type GCPPubSub struct {
	client   *gcppubsub.Client
	metadata *metadata
	logger   logger.Logger

	closed     atomic.Bool
	closeCh    chan struct{}
	wg         sync.WaitGroup
	topicCache map[string]cacheEntry
	lock       *sync.RWMutex
}

type cacheEntry struct {
	LastSync time.Time
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

const topicCacheRefreshInterval = 5 * time.Hour

// NewGCPPubSub returns a new GCPPubSub instance.
func NewGCPPubSub(logger logger.Logger) pubsub.PubSub {
	client := &GCPPubSub{
		logger:     logger,
		closeCh:    make(chan struct{}),
		topicCache: make(map[string]cacheEntry),
		lock:       &sync.RWMutex{},
	}
	return client
}

func (g *GCPPubSub) periodicCacheRefresh() {
	// Run this loop 5 times every topicCacheRefreshInterval, to be able to delete items that are stale
	ticker := time.NewTicker(topicCacheRefreshInterval / 5)
	defer ticker.Stop()

	for {
		select {
		case <-g.closeCh:
			return
		case <-ticker.C:
			g.lock.Lock()
			for key, entry := range g.topicCache {
				// Delete from the cache if the last sync was longer than topicCacheRefreshInterval
				if time.Since(entry.LastSync) > topicCacheRefreshInterval {
					delete(g.topicCache, key)
				}
			}
			g.lock.Unlock()
		}
	}
}

func createMetadata(pubSubMetadata pubsub.Metadata) (*metadata, error) {
	// TODO: Add the rest of the metadata here, add defaults where applicable
	result := metadata{
		DisableEntityManagement: false,
		Type:                    "service_account",
		MaxReconnectionAttempts: defaultMaxReconnectionAttempts,
		ConnectionRecoveryInSec: defaultConnectionRecoveryInSec,
		MaxDeliveryAttempts:     defaultMaxDeliveryAttempts,
		AckDeadline:             defaultAckDeadline,
	}

	err := kitmd.DecodeMetadata(pubSubMetadata.Properties, &result)
	if err != nil {
		return nil, err
	}

	if result.ProjectID == "" {
		return &result, fmt.Errorf("%s missing attribute %s", errorMessagePrefix, metadataProjectIDKey)
	}

	if result.AckDeadline <= 0 {
		return nil, fmt.Errorf("%s invalid AckDeadline %s. Value must be a positive Go duration string or integer", errorMessagePrefix, pubSubMetadata.Properties[metadataAckDeadlineKey])
	}

	return &result, nil
}

// Init parses metadata and creates a new Pub Sub client.
func (g *GCPPubSub) Init(ctx context.Context, meta pubsub.Metadata) error {
	metadata, err := createMetadata(meta)
	if err != nil {
		return err
	}

	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		g.periodicCacheRefresh()
	}()

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
	g.lock.RLock()
	_, topicExists := g.topicCache[req.Topic]
	g.lock.RUnlock()

	// We are not acquiring a write lock before calling ensureTopic, so there's the chance that ensureTopic be called multiple time
	// This is acceptable in our case, even is slightly wasteful, as ensureTopic is idempotent
	if !g.metadata.DisableEntityManagement && !topicExists {
		err := g.ensureTopic(ctx, req.Topic)
		if err != nil {
			return fmt.Errorf("%s could not get valid topic %s: %w", errorMessagePrefix, req.Topic, err)
		}
		g.lock.Lock()
		g.topicCache[req.Topic] = cacheEntry{
			LastSync: time.Now(),
		}
		g.lock.Unlock()
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
	g.lock.RLock()
	_, topicExists := g.topicCache[req.Topic]
	g.lock.RUnlock()

	// We are not acquiring a write lock before calling ensureTopic, so there's the chance that ensureTopic be called multiple times
	// This is acceptable in our case, even is slightly wasteful, as ensureTopic is idempotent
	if !g.metadata.DisableEntityManagement && !topicExists {
		topicErr := g.ensureTopic(parentCtx, req.Topic)
		if topicErr != nil {
			return fmt.Errorf("%s could not get valid topic - topic:%q, error: %w", errorMessagePrefix, req.Topic, topicErr)
		}
		g.lock.Lock()
		g.topicCache[req.Topic] = cacheEntry{
			LastSync: time.Now(),
		}
		g.lock.Unlock()

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
	for range g.metadata.MaxReconnectionAttempts {
		reconnAttempts <- struct{}{}
	}

	readReconnectAttemptsRemaining := func() int { return len(reconnAttempts) }

	// Apply configured limits for MaxOutstandingMessages, MaxOutstandingBytes, and MaxConcurrentConnections
	// NOTE: negative MaxOutstandingMessages and MaxOutstaningBytes values are allowed and indicate
	//  in the GCP pubsub library that no limit should be applied. Zero values result in the package
	//  default being used: 1000 messages and 1e9 (1G) bytes respectively.
	if g.metadata.MaxOutstandingMessages != 0 {
		sub.ReceiveSettings.MaxOutstandingMessages = g.metadata.MaxOutstandingMessages
	}
	if g.metadata.MaxOutstandingBytes != 0 {
		sub.ReceiveSettings.MaxOutstandingBytes = g.metadata.MaxOutstandingBytes
	}
	// NOTE: For MaxConcurrentConnections, negative values are not allowed so only override if the value is greater than 0
	if g.metadata.MaxConcurrentConnections > 0 {
		sub.ReceiveSettings.NumGoroutines = g.metadata.MaxConcurrentConnections
	}

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
	g.lock.RLock()
	_, topicOK := g.topicCache[topic]
	_, dlTopicOK := g.topicCache[g.metadata.DeadLetterTopic]
	g.lock.RUnlock()
	if !topicOK {
		g.lock.Lock()
		// Double-check if the topic still doesn't exist to avoid race condition
		if _, ok := g.topicCache[topic]; !ok {
			err := g.ensureTopic(parentCtx, topic)
			if err != nil {
				g.lock.Unlock()
				return err
			}
			g.topicCache[topic] = cacheEntry{
				LastSync: time.Now(),
			}
		}
		g.lock.Unlock()
	}

	managedSubscription := subscription + "-" + topic
	entity := g.getSubscription(managedSubscription)
	exists, subErr := entity.Exists(parentCtx)
	if !exists {
		subConfig := gcppubsub.SubscriptionConfig{
			AckDeadline:           g.metadata.AckDeadline,
			Topic:                 g.getTopic(topic),
			EnableMessageOrdering: g.metadata.EnableMessageOrdering,
		}

		if g.metadata.DeadLetterTopic != "" && !dlTopicOK {
			g.lock.Lock()
			// Double-check if the DeadLetterTopic still doesn't exist to avoid race condition
			if _, ok := g.topicCache[g.metadata.DeadLetterTopic]; !ok {
				subErr = g.ensureTopic(parentCtx, g.metadata.DeadLetterTopic)
				if subErr != nil {
					g.lock.Unlock()
					return subErr
				}
				g.topicCache[g.metadata.DeadLetterTopic] = cacheEntry{
					LastSync: time.Now(),
				}
			}
			g.lock.Unlock()
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
