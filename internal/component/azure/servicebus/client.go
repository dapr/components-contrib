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
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	servicebus "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	sbadmin "github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"

	azauth "github.com/dapr/components-contrib/internal/authentication/azure"
	"github.com/dapr/kit/logger"
)

// Client contains the clients for Service Bus and methods to create topics, subscriptions, queues.
type Client struct {
	client      *servicebus.Client
	adminClient *sbadmin.Client
	metadata    *Metadata
}

// NewClient creates a new Client object.
func NewClient(metadata *Metadata, rawMetadata map[string]string) (*Client, error) {
	client := &Client{
		metadata: metadata,
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
		settings, err := azauth.NewEnvironmentSettings(azauth.AzureServiceBusResourceName, rawMetadata)
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
func (c *Client) GetClient() *azservicebus.Client {
	return c.client
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

// EnsureSubscription creates the topic subscription if it doesn't exist.
// Returns with nil error if the admin client doesn't exist.
func (c *Client) EnsureSubscription(ctx context.Context, name string, topic string) error {
	if c.adminClient == nil {
		return nil
	}

	err := c.EnsureTopic(ctx, topic)
	if err != nil {
		return err
	}

	shouldCreate, err := c.shouldCreateSubscription(ctx, topic, name)
	if err != nil {
		return err
	}

	if shouldCreate {
		err = c.createSubscription(ctx, topic, name)
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

func (c *Client) shouldCreateSubscription(parentCtx context.Context, topic, subscription string) (bool, error) {
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
	return false, nil
}

func (c *Client) createSubscription(parentCtx context.Context, topic, subscription string) error {
	ctx, cancel := context.WithTimeout(parentCtx, time.Second*time.Duration(c.metadata.TimeoutInSec))
	defer cancel()

	_, err := c.adminClient.CreateSubscription(ctx, topic, subscription, &sbadmin.CreateSubscriptionOptions{
		Properties: c.metadata.CreateSubscriptionProperties(),
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
