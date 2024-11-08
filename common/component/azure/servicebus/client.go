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

package servicebus

import (
	"context"
	"fmt"
	"sync"
	"time"

	servicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	sbadmin "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"github.com/cenkalti/backoff/v4"
	"golang.org/x/exp/maps"

	azauth "github.com/dapr/components-contrib/common/authentication/azure"
	"github.com/dapr/kit/logger"
)

// Type that matches Client.EnsureTopic and Client.EnsureSubscription
type ensureFn func(context.Context, string) error

// Client contains the clients for Service Bus and methods to get senders and to create topics, subscriptions, queues.
type Client struct {
	client      *servicebus.Client
	adminClient *sbadmin.Client
	metadata    *Metadata
	lock        *sync.RWMutex
	senders     map[string]*servicebus.Sender
}

// NewClient creates a new Client object.
func NewClient(metadata *Metadata, rawMetadata map[string]string) (*Client, error) {
	client := &Client{
		metadata: metadata,
		lock:     &sync.RWMutex{},
		senders:  make(map[string]*servicebus.Sender),
	}

	clientOpts := &servicebus.ClientOptions{
		ApplicationID: "dapr-" + logger.DaprVersion,
		// TODO: Use the built-in retry in the SDK rather than our own on top of that
		/*RetryOptions: servicebus.RetryOptions{
			MaxRetries: int32(metadata.PublishMaxRetries),
			RetryDelay: time.Duration(metadata.PublishInitialRetryIntervalInMs) * time.Millisecond,
		},*/
	}

	if metadata.ConnectionString != "" {
		var err error
		client.client, err = servicebus.NewClientFromConnectionString(metadata.ConnectionString, clientOpts)
		if err != nil {
			return nil, err
		}

		if !metadata.DisableEntityManagement {
			client.adminClient, err = sbadmin.NewClientFromConnectionString(metadata.ConnectionString, nil)
			if err != nil {
				return nil, err
			}
		}
	} else {
		settings, err := azauth.NewEnvironmentSettings(rawMetadata)
		if err != nil {
			return nil, err
		}

		token, err := settings.GetTokenCredential()
		if err != nil {
			return nil, err
		}

		client.client, err = servicebus.NewClient(metadata.NamespaceName, token, clientOpts)
		if err != nil {
			return nil, err
		}

		if !metadata.DisableEntityManagement {
			client.adminClient, err = sbadmin.NewClient(metadata.NamespaceName, token, nil)
			if err != nil {
				return nil, err
			}
		}
	}

	return client, nil
}

// GetClient returns the azservicebus.Client object.
func (c *Client) GetClient() *servicebus.Client {
	return c.client
}

// GetSenderForTopic returns the sender for a queue or topic, or creates a new one if it doesn't exist
func (c *Client) GetSender(ctx context.Context, queueOrTopic string, ensureFn ensureFn) (*servicebus.Sender, error) {
	c.lock.RLock()
	sender, ok := c.senders[queueOrTopic]
	c.lock.RUnlock()
	if ok && sender != nil {
		return sender, nil
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	// Check again after acquiring a write lock in case another goroutine created the sender
	sender, ok = c.senders[queueOrTopic]
	if ok && sender != nil {
		return sender, nil
	}

	// Ensure the queue or topic exists, if needed
	if ensureFn != nil {
		// Ensure the queue or topic exists the first time it is referenced
		// This does nothing if DisableEntityManagement is true
		err := ensureFn(ctx, queueOrTopic)
		if err != nil {
			return nil, err
		}
	}

	// Create the sender
	sender, err := c.client.NewSender(queueOrTopic, nil)
	if err != nil {
		return nil, err
	}
	c.senders[queueOrTopic] = sender

	return sender, nil
}

// CloseSender closes a sender for a queue or topic.
func (c *Client) CloseSender(queueOrTopic string, log logger.Logger) {
	c.lock.Lock()
	sender, ok := c.senders[queueOrTopic]
	if ok {
		delete(c.senders, queueOrTopic)
	}
	c.lock.Unlock()

	if sender != nil {
		log.Info("Closing sender: " + queueOrTopic)
		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
		// Log only
		err := sender.Close(closeCtx)
		closeCancel()
		if err != nil {
			// Log only
			log.Warnf("Error closing sender %s: %v", queueOrTopic, err)
		}
		log.Debug("Closed sender: " + queueOrTopic)
	}
}

// CloseAllSenders closes all sender connections.
func (c *Client) CloseAllSenders(log logger.Logger) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Close all senders, up to 3 in parallel
	workersCh := make(chan bool, 3)
	for k, t := range c.senders {
		// Blocks if we have too many goroutines
		workersCh <- true
		go func(k string, t *servicebus.Sender) {
			log.Debug("Closing sender: " + k)
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.metadata.TimeoutInSec)*time.Second)
			err := t.Close(ctx)
			cancel()
			if err != nil {
				// Log only
				log.Warnf("Error closing sender %s: %v", k, err)
			}
			<-workersCh
		}(k, t)
	}
	for range cap(workersCh) {
		// Wait for all workers to be done
		workersCh <- true
	}
	close(workersCh)

	// Clear the map
	maps.Clear(c.senders)
}

// Close the client and every sender or consumer created by the connnection.
func (c *Client) Close(log logger.Logger) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.metadata.TimeoutInSec)*time.Second)
		err := c.client.Close(ctx)
		cancel()
		if err != nil {
			// log only
			log.Warnf("Error closing client: %v", err)
		}
		c.client = nil
	}

	// Clear the map of senders
	maps.Clear(c.senders)
}

// EnsureTopic creates the topic if it doesn't exist.
// Returns with nil error if the admin client doesn't exist.
func (c *Client) EnsureTopic(ctx context.Context, topic string) error {
	if c.adminClient == nil {
		return nil
	}

	shouldCreate, err := c.shouldCreateTopic(ctx, topic)
	if err != nil {
		return err
	}

	if shouldCreate {
		err = c.createTopic(ctx, topic)
		if err != nil {
			return err
		}
	}

	return nil
}

type SubscribeOptions struct {
	RequireSessions      bool
	MaxConcurrentSesions int
}

// EnsureSubscription creates the topic subscription if it doesn't exist.
// Returns with nil error if the admin client doesn't exist.
func (c *Client) EnsureSubscription(ctx context.Context, name string, topic string, opts SubscribeOptions) error {
	if c.adminClient == nil {
		return nil
	}

	err := c.EnsureTopic(ctx, topic)
	if err != nil {
		return err
	}

	shouldCreate, err := c.shouldCreateSubscription(ctx, topic, name, opts)
	if err != nil {
		return err
	}

	if shouldCreate {
		err = c.createSubscription(ctx, topic, name, opts)
		if err != nil {
			return err
		}
	}

	return nil
}

// EnsureTopic creates the queue if it doesn't exist.
// Returns with nil error if the admin client doesn't exist.
func (c *Client) EnsureQueue(ctx context.Context, queue string) error {
	if c.adminClient == nil {
		return nil
	}

	shouldCreate, err := c.shouldCreateQueue(ctx, queue)
	if err != nil {
		return err
	}

	if shouldCreate {
		err = c.createQueue(ctx, queue)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) shouldCreateTopic(parentCtx context.Context, topic string) (bool, error) {
	ctx, cancel := context.WithTimeout(parentCtx, time.Second*time.Duration(c.metadata.TimeoutInSec))
	defer cancel()

	res, err := c.adminClient.GetTopic(ctx, topic, nil)
	if err != nil {
		return false, fmt.Errorf("could not get topic %s: %w", topic, err)
	}
	if res == nil {
		// If res is nil, the topic does not exist
		return true, nil
	}
	return false, nil
}

func (c *Client) createTopic(parentCtx context.Context, topic string) error {
	ctx, cancel := context.WithTimeout(parentCtx, time.Second*time.Duration(c.metadata.TimeoutInSec))
	defer cancel()

	_, err := c.adminClient.CreateTopic(ctx, topic, nil)
	if err != nil {
		return fmt.Errorf("could not create topic %s: %w", topic, err)
	}
	return nil
}

func (c *Client) shouldCreateSubscription(parentCtx context.Context, topic, subscription string, opts SubscribeOptions) (bool, error) {
	ctx, cancel := context.WithTimeout(parentCtx, time.Second*time.Duration(c.metadata.TimeoutInSec))
	defer cancel()

	res, err := c.adminClient.GetSubscription(ctx, topic, subscription, nil)
	if err != nil {
		return false, fmt.Errorf("could not get subscription %s: %w", subscription, err)
	}
	if res == nil {
		// If res is nil, the subscription does not exist
		return true, nil
	}

	if notEqual(res.RequiresSession, &opts.RequireSessions) {
		return false, fmt.Errorf("subscription %s already exists but session requirement doesn't match", subscription)
	}

	return false, nil
}

func (c *Client) createSubscription(parentCtx context.Context, topic, subscription string, opts SubscribeOptions) error {
	ctx, cancel := context.WithTimeout(parentCtx, time.Second*time.Duration(c.metadata.TimeoutInSec))
	defer cancel()

	_, err := c.adminClient.CreateSubscription(ctx, topic, subscription, &sbadmin.CreateSubscriptionOptions{
		Properties: c.metadata.CreateSubscriptionProperties(opts),
	})
	if err != nil {
		return fmt.Errorf("could not create subscription %s: %w", subscription, err)
	}
	return nil
}

func (c *Client) shouldCreateQueue(parentCtx context.Context, queue string) (bool, error) {
	ctx, cancel := context.WithTimeout(parentCtx, time.Second*time.Duration(c.metadata.TimeoutInSec))
	defer cancel()

	res, err := c.adminClient.GetQueue(ctx, queue, nil)
	if err != nil {
		return false, fmt.Errorf("could not get queue %s: %w", queue, err)
	}
	if res == nil {
		// If res nil, the queue does not exist
		return true, nil
	}
	return false, nil
}

func (c *Client) createQueue(parentCtx context.Context, queue string) error {
	ctx, cancel := context.WithTimeout(parentCtx, time.Second*time.Duration(c.metadata.TimeoutInSec))
	defer cancel()

	_, err := c.adminClient.CreateQueue(ctx, queue, &sbadmin.CreateQueueOptions{
		Properties: c.metadata.CreateQueueProperties(),
	})
	if err != nil {
		return fmt.Errorf("could not create queue %s: %w", queue, err)
	}
	return nil
}

// ReconnectionBackoff returns the backoff for reconnecting in a subscription.
func (c *Client) ReconnectionBackoff() backoff.BackOff {
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = 0
	bo.InitialInterval = time.Duration(c.metadata.MinConnectionRecoveryInSec) * time.Second
	bo.MaxInterval = time.Duration(c.metadata.MaxConnectionRecoveryInSec) * time.Second
	return bo
}

func notEqual(a, b *bool) bool {
	if a == nil && b == nil {
		return false
	} else if a == nil || b == nil {
		return true
	}
	return *a != *b
}
