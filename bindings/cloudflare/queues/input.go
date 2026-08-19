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

package cfqueues

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/dapr/components-contrib/bindings"
)

// Base URL of the Cloudflare API. It's a variable so tests can point it to a local server.
var cfAPIBaseURL = "https://api.cloudflare.com/client/v4"

// Message as returned by the messages/pull endpoint.
type pullMessage struct {
	Body        json.RawMessage `json:"body"`
	ID          string          `json:"id"`
	TimestampMs int64           `json:"timestamp_ms"`
	Attempts    int             `json:"attempts"`
	LeaseID     string          `json:"lease_id"`
}

// Data returns the message body as it was published.
// Messages published as strings (including those sent by the output binding) are unwrapped,
// while structured bodies are passed through as JSON.
func (m pullMessage) Data() []byte {
	var str string
	if json.Unmarshal(m.Body, &str) == nil {
		return []byte(str)
	}
	return m.Body
}

type leaseRef struct {
	LeaseID string `json:"lease_id"`
}

type ackRequest struct {
	Acks    []leaseRef `json:"acks,omitempty"`
	Retries []leaseRef `json:"retries,omitempty"`
}

// Read messages from the queue with an HTTP pull consumer, invoking handler for each one.
func (q *CFQueues) Read(ctx context.Context, handler bindings.Handler) error {
	if q.closed.Load() {
		return errors.New("binding is closed")
	}

	// Pull consumers are served by the Cloudflare API rather than by the worker, so the
	// worker-only ("workerUrl") authentication profile cannot be used for the input binding.
	if q.metadata.CfAPIToken == "" || q.metadata.CfAccountID == "" {
		return fmt.Errorf("the input binding requires the metadata properties 'cfAPIToken' (with the 'queues#read' and 'queues#write' permissions) and 'cfAccountID'; see the documentation at %s", componentDocsURL)
	}

	queueID, err := q.resolveQueueID(ctx)
	if err != nil {
		return err
	}

	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		for {
			if ctx.Err() != nil || q.closed.Load() {
				return
			}

			count, err := q.pollOnce(ctx, queueID, handler)
			if err != nil && ctx.Err() == nil {
				q.logger.Errorf("Failed to receive messages from queue '%s': %v", q.metadata.QueueName, err)
			}

			// A full batch means there is likely more in the queue, so drain it before pausing.
			if err == nil && count >= q.metadata.BatchSize {
				continue
			}

			select {
			case <-ctx.Done():
				return
			case <-q.closeCh:
				return
			case <-time.After(q.metadata.PollingInterval):
			}
		}
	}()

	return nil
}

// Pulls one batch of messages, invokes the handler for each, then acknowledges the batch.
// Returns the number of messages that were pulled.
func (q *CFQueues) pollOnce(ctx context.Context, queueID string, handler bindings.Handler) (int, error) {
	pullBody := map[string]any{
		"batch_size":            q.metadata.BatchSize,
		"visibility_timeout_ms": q.metadata.VisibilityTimeout.Milliseconds(),
	}
	var pulled struct {
		Result struct {
			Messages []pullMessage `json:"messages"`
		} `json:"result"`
	}
	err := q.apiRequest(ctx, http.MethodPost, "/accounts/"+q.metadata.CfAccountID+"/queues/"+queueID+"/messages/pull", pullBody, &pulled)
	if err != nil {
		return 0, err
	}
	if len(pulled.Result.Messages) == 0 {
		return 0, nil
	}

	// Messages whose lease is neither acknowledged nor retried become visible again when their
	// visibility timeout expires, so delivery is at-least-once.
	ack := ackRequest{}
	for _, msg := range pulled.Result.Messages {
		if ctx.Err() != nil {
			break
		}
		_, hErr := handler(ctx, &bindings.ReadResponse{
			Data: msg.Data(),
			Metadata: map[string]string{
				"id":        msg.ID,
				"attempts":  strconv.Itoa(msg.Attempts),
				"timestamp": strconv.FormatInt(msg.TimestampMs, 10),
			},
		})
		if hErr != nil {
			q.logger.Errorf("Error processing message '%s' from queue '%s': %v", msg.ID, q.metadata.QueueName, hErr)
			ack.Retries = append(ack.Retries, leaseRef{LeaseID: msg.LeaseID})
			continue
		}
		ack.Acks = append(ack.Acks, leaseRef{LeaseID: msg.LeaseID})
	}

	if len(ack.Acks) == 0 && len(ack.Retries) == 0 {
		return len(pulled.Result.Messages), nil
	}
	err = q.apiRequest(ctx, http.MethodPost, "/accounts/"+q.metadata.CfAccountID+"/queues/"+queueID+"/messages/ack", ack, nil)
	return len(pulled.Result.Messages), err
}

// Returns the ID of the queue, looking it up by name unless it's set in the metadata.
func (q *CFQueues) resolveQueueID(ctx context.Context) (string, error) {
	if q.metadata.QueueID != "" {
		return q.metadata.QueueID, nil
	}

	for page := 1; ; page++ {
		var list struct {
			Result []struct {
				QueueID   string `json:"queue_id"`
				QueueName string `json:"queue_name"`
			} `json:"result"`
			ResultInfo struct {
				TotalPages int `json:"total_pages"`
			} `json:"result_info"`
		}
		err := q.apiRequest(ctx, http.MethodGet, fmt.Sprintf("/accounts/%s/queues?page=%d&per_page=100", q.metadata.CfAccountID, page), nil, &list)
		if err != nil {
			return "", fmt.Errorf("failed to list the queues in the account: %w", err)
		}
		for _, queue := range list.Result {
			if queue.QueueName == q.metadata.QueueName {
				return queue.QueueID, nil
			}
		}
		if len(list.Result) == 0 || page >= list.ResultInfo.TotalPages {
			return "", fmt.Errorf("queue '%s' was not found in account '%s'", q.metadata.QueueName, q.metadata.CfAccountID)
		}
	}
}

// Performs a request against the Cloudflare API, decoding the response into dest when it's not nil.
func (q *CFQueues) apiRequest(parentCtx context.Context, method string, path string, body any, dest any) error {
	var reqBody io.Reader
	if body != nil {
		enc, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("failed to encode the request body: %w", err)
		}
		reqBody = bytes.NewReader(enc)
	}

	ctx, cancel := context.WithTimeout(parentCtx, q.metadata.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, cfAPIBaseURL+path, reqBody)
	if err != nil {
		return fmt.Errorf("error creating network request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+q.metadata.CfAPIToken)
	req.Header.Set("Content-Type", "application/json")

	res, err := q.Client().Do(req)
	if err != nil {
		return fmt.Errorf("error invoking the Cloudflare API: %w", err)
	}
	defer func() {
		// Drain the body before closing it
		_, _ = io.ReadAll(res.Body)
		_ = res.Body.Close()
	}()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid response status code: %d", res.StatusCode)
	}

	if dest == nil {
		return nil
	}
	err = json.NewDecoder(res.Body).Decode(dest)
	if err != nil {
		return fmt.Errorf("invalid response format: %w", err)
	}
	return nil
}
