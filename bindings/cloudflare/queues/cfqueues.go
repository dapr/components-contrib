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
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"

	"golang.org/x/exp/slices"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/internal/component/cloudflare/workers"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// Link to the documentation for the component
const componentDocsURL = "https://docs.dapr.io/reference/components-reference/supported-bindings/cloudflare-queues/"

// CFQueues is a binding for publishing messages on Cloudflare Queues
type CFQueues struct {
	*workers.Base
	metadata componentMetadata
}

// NewCFQueues returns a new CFQueues.
func NewCFQueues(logger logger.Logger) bindings.OutputBinding {
	q := &CFQueues{
		Base: &workers.Base{},
	}
	q.SetLogger(logger)
	return q
}

// Init the component.
func (q *CFQueues) Init(_ context.Context, metadata bindings.Metadata) error {
	// Decode the metadata
	err := contribMetadata.DecodeMetadata(metadata.Properties, &q.metadata)
	if err != nil {
		return fmt.Errorf("failed to parse metadata: %w", err)
	}
	err = q.metadata.Validate()
	if err != nil {
		return fmt.Errorf("metadata is invalid: %w", err)
	}
	q.SetMetadata(&q.metadata.BaseMetadata)

	// Init the base component
	workerBindings := []workers.CFBinding{
		{Type: "queue", Name: q.metadata.QueueName, QueueName: &q.metadata.QueueName},
	}
	infoResponseValidate := func(data *workers.InfoEndpointResponse) error {
		if !slices.Contains(data.Queues, q.metadata.QueueName) {
			return fmt.Errorf("the worker is not bound to the queue '%s'; please re-deploy the worker with the correct bindings per instructions in the documentation at %s", q.metadata.QueueName, componentDocsURL)
		}
		return nil
	}
	return q.Base.Init(workerBindings, componentDocsURL, infoResponseValidate)
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
func (q *CFQueues) invokePublish(parentCtx context.Context, ir *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	token, err := q.metadata.CreateToken()
	if err != nil {
		return nil, fmt.Errorf("failed to create authorization token: %w", err)
	}

	ctx, cancel := context.WithTimeout(parentCtx, q.metadata.Timeout)
	defer cancel()

	d, err := strconv.Unquote(string(ir.Data))
	if err == nil {
		ir.Data = []byte(d)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, q.metadata.WorkerURL+"queues/"+q.metadata.QueueName, bytes.NewReader(ir.Data))
	if err != nil {
		return nil, fmt.Errorf("error creating network request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	res, err := q.Client().Do(req)
	if err != nil {
		return nil, fmt.Errorf("error invoking the worker: %w", err)
	}
	defer func() {
		// Drain the body before closing it
		_, _ = io.ReadAll(res.Body)
		res.Body.Close()
	}()
	if res.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("invalid response status code: %d", res.StatusCode)
	}

	return nil, nil
}

// Close the component
func (q *CFQueues) Close() error {
	err := q.Base.Close()
	if err != nil {
		return err
	}
	return nil
}

// GetComponentMetadata returns the metadata of the component.
func (q *CFQueues) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := componentMetadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.BindingType)
	return
}
