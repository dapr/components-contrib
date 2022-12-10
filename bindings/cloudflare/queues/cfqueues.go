/*
Copyright 2022 The Dapr Authors
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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

// Minimum version required for the running Worker.
const minWorkerVersion = 20221209

// CFQueues is a binding for publishing messages on Cloudflare Queues
type CFQueues struct {
	metadata componentMetadata
	client   *http.Client
	logger   logger.Logger
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewCFQueues returns a new CFQueues.
func NewCFQueues(logger logger.Logger) bindings.OutputBinding {
	return &CFQueues{logger: logger}
}

// Init the component.
func (q *CFQueues) Init(metadata bindings.Metadata) error {
	err := mapstructure.Decode(metadata.Properties, &q.metadata)
	if err != nil {
		return fmt.Errorf("failed to parse metadata: %w", err)
	}
	err = q.metadata.Validate()
	if err != nil {
		return fmt.Errorf("metadata is invalid: %w", err)
	}

	q.ctx, q.cancel = context.WithCancel(context.Background())

	q.client = &http.Client{
		Timeout: time.Second * 30,
	}

	// TODO: Automatically create or update the worker
	ok, err := q.checkWorker()
	if err != nil {
		q.logger.Errorf("The component could not be initialized because of an error: %v", err)
		return err
	}
	if !ok {
		q.logger.Errorf("The worker is running but it's on an old version and needs to be upgraded")
		return errors.New("worker needs to be upgraded")
	}

	return nil
}

type infoEndpointResponse struct {
	Version string   `json:"version"`
	Queues  []string `json:"queues"`
}

// Check the worker to ensure it's available and it's using a supported version.
// In case the worker needs to be updated, the method returns false and no error
func (q *CFQueues) checkWorker() (bool, error) {
	ctx, cancel := context.WithTimeout(q.ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", q.metadata.WorkerURL+".well-known/dapr/info", nil)
	if err != nil {
		return false, fmt.Errorf("error creating network request: %w", err)
	}
	res, err := q.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("error invoking the worker: %w", err)
	}
	defer func() {
		// Drain the body before closing it
		_, _ = io.ReadAll(res.Body)
		res.Body.Close()
	}()
	if res.StatusCode != http.StatusOK {
		return false, fmt.Errorf("invalid response status code: %d", err)
	}

	var data infoEndpointResponse
	err = json.NewDecoder(res.Body).Decode(&data)
	if err != nil {
		return false, fmt.Errorf("invalid response from the worker: %w", err)
	}

	version, _ := strconv.Atoi(data.Version)
	if version < minWorkerVersion {
		// Return no error indicating that the version is too low
		return false, nil
	}

	return true, nil
}

// Operations returns the supported operations for this binding.
func (q CFQueues) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation, "publish"}
}

// Invoke the output binding.
func (q *CFQueues) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	switch req.Operation {
	case bindings.CreateOperation, "publish":
		return q.invokePublish(ctx, req)
	default:
		return nil, fmt.Errorf("unsupported operation: %s", req.Operation)
	}
}

// Handler for invoke operations for publishing messages to the Workers Queue
func (q *CFQueues) invokePublish(ctx context.Context, ir *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	md := publishRequestMetadata{}
	err := md.FromMetadata(ir.Metadata)
	if err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", q.metadata.WorkerURL+"publish/"+md.QueueName, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating network request: %w", err)
	}
	res, err := q.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error invoking the worker: %w", err)
	}
	defer func() {
		// Drain the body before closing it
		_, _ = io.ReadAll(res.Body)
		res.Body.Close()
	}()
	if res.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("invalid response status code: %d", err)
	}

	return nil, nil
}

// Close the component
func (q *CFQueues) Close() error {
	if q.cancel != nil {
		q.cancel()
		q.cancel = nil
	}
	return nil
}

type componentMetadata struct {
	WorkerURL string `mapstructure:"workerUrl"`
}

// Validate the metadata object
func (m *componentMetadata) Validate() error {
	if m.WorkerURL == "" {
		return errors.New("property 'workerUrl' is required")
	}
	u, err := url.Parse(m.WorkerURL)
	if err != nil {
		return fmt.Errorf("invalid property 'workerUrl': %w", err)
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return errors.New("invalid property 'workerUrl': unsupported scheme")
	}
	// Re-set the URL to make sure it's sanitized
	m.WorkerURL = u.String()
	if !strings.HasSuffix(m.WorkerURL, "/") {
		m.WorkerURL += "/"
	}

	return nil
}

type publishRequestMetadata struct {
	QueueName string
}

var queueNameValidation = regexp.MustCompile("^([a-zA-Z0-9_\\-\\.]+)$")

func (m *publishRequestMetadata) FromMetadata(md map[string]string) error {
	if len(md) == 0 {
		return errors.New("metata property 'queue' is required")
	}

	m.QueueName = md["queue"]
	if m.QueueName == "" {
		return errors.New("metata property 'queue' is required")
	}
	if !queueNameValidation.MatchString(m.QueueName) {
		return errors.New("metadata property 'queue' is invalid")
	}

	return nil
}
