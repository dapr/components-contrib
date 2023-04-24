/*
Copyright 2023 The Dapr Authors
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

package pubsub_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
)

type topicManager struct {
	projectID string
	gcpClient *pubsub.Client
}
type TopicMessagePayload struct {
	Message  string
	TopicArn string
}
type DataMessage struct {
	Data  string `json:"data"`
	Topic string `json:"topic"`
}

type MessageFunc func(*DataMessage) error

func NewTopicManager(projectID string) (*topicManager, error) {
	tpm := &topicManager{
		projectID: projectID,
	}
	err := tpm.connect()
	if err != nil {
		return nil, err
	}
	return tpm, nil
}

func (tm *topicManager) connect() error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, tm.projectID)
	if err != nil {
		return fmt.Errorf("GCP pubsub.NewClient failed to connect: %v", err)
	}
	tm.gcpClient = client

	return nil
}

func (tm *topicManager) disconnect() error {
	return tm.gcpClient.Close()
}

func (tp *topicManager) GetMessages(topicID string, msgTimeout time.Duration, subID string, fn MessageFunc) (int, error) {
	ctx := context.Background()

	topic := tp.gcpClient.Topic(topicID)
	cfg := &pubsub.SubscriptionConfig{
		Topic: topic,
	}
	sub, err := getOrCreateSub(ctx, tp.gcpClient, subID, cfg)
	if err != nil {
		return -1, fmt.Errorf("getOrCreateSub: %v", err)
	}

	sub.ReceiveSettings.Synchronous = true
	sub.ReceiveSettings.MaxOutstandingMessages = 1

	ctxTimeout := msgTimeout * time.Second
	ctx, cancel := context.WithTimeout(ctx, ctxTimeout)
	defer cancel()
	numMgs := -1
	fmt.Printf("@@@ GetMessages- %s\n", ctxTimeout)
	err = sub.Receive(ctx, func(_ context.Context, msg *pubsub.Message) {
		defer msg.Ack()

		dm, e1 := extractDataMessage(msg)
		if e1 != nil {
			fmt.Printf("@@@ GetMessages- extractDataMessage error: %v", e1)
			err = e1
			return
		}

		if e1 := fn(dm); e1 != nil {
			fmt.Printf("@@@ GetMessages- calling FN error: %v", e1)
			err = e1
			return
		}
	})

	if err != nil {
		return -1, fmt.Errorf("sub.Receive: %v", err)
	}

	return numMgs, nil
}
func extractDataMessage(msg *pubsub.Message) (*DataMessage, error) {
	dm := DataMessage{}
	err := json.Unmarshal(msg.Data, &dm)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling message data: %v", err)
	}

	return &dm, nil
}

func getOrCreateSub(ctx context.Context, client *pubsub.Client, subID string, cfg *pubsub.SubscriptionConfig) (*pubsub.Subscription, error) {
	sub := client.Subscription(subID)
	ok, err := sub.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check if subscription exists: %v", err)
	}
	if !ok {
		sub, err = client.CreateSubscription(ctx, subID, *cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create subscription (%q): %v", subID, err)
		}
	}
	return sub, nil
}

func deleteSubscriptions(projectID string, subs []string) error {
	for _, s := range subs {
		fmt.Printf("Deleting subscription: %s\n", s)
		deleteSubscription(os.Stdout, projectID, s)
	}
	return nil
}

func deleteTopics(projectID string, topics []string) error {
	for _, t := range topics {
		fmt.Printf("Deleting topics: %s\n", t)
		deleteTopic(os.Stdout, projectID, t)
	}
	return nil
}

func deleteSubscription(w io.Writer, projectID, subID string) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("pubsub.NewClient: %v", err)
	}
	defer client.Close()

	sub := client.Subscription(subID)
	if err := sub.Delete(ctx); err != nil {
		return fmt.Errorf("subscription Delete failed: %v", err)
	}
	fmt.Fprintf(w, "Subscription %q deleted.", subID)
	return nil
}

func deleteTopic(w io.Writer, projectID, topicID string) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("pubsub.NewClient: %v", err)
	}
	defer client.Close()

	t := client.Topic(topicID)
	if err := t.Delete(ctx); err != nil {
		return fmt.Errorf("topic Delete failed: %v", err)
	}
	fmt.Fprintf(w, "Deleted topic: %v\n", t)
	return nil
}
