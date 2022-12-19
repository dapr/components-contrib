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

package cfkv

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"time"

	"github.com/mitchellh/mapstructure"
	"golang.org/x/exp/slices"

	"github.com/dapr/components-contrib/internal/component/cloudflare/workers"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

// Link to the documentation for the component
// TODO: Add link to docs
const componentDocsURL = "https://TODO"

// CFWorkersKV is a state store backed by Cloudflare Workers KV.
type CFWorkersKV struct {
	*workers.Base
	state.DefaultBulkStore
	metadata componentMetadata
}

// NewCFWorkersKV returns a new CFWorkersKV.
func NewCFWorkersKV(logger logger.Logger) state.Store {
	q := &CFWorkersKV{
		Base: &workers.Base{},
	}
	q.DefaultBulkStore = state.NewDefaultBulkStore(q)
	q.SetLogger(logger)
	return q
}

// Init the component.
func (q *CFWorkersKV) Init(metadata state.Metadata) error {
	// Decode the metadata
	err := mapstructure.Decode(metadata.Properties, &q.metadata)
	if err != nil {
		return fmt.Errorf("failed to parse metadata: %w", err)
	}
	err = q.metadata.Validate()
	if err != nil {
		return fmt.Errorf("metadata is invalid: %w", err)
	}
	q.SetMetadata(&q.metadata.BaseMetadata)

	// Init the base component
	workerBindings := []workers.Binding{
		{Type: "kv_namespace", Name: q.metadata.KVNamespaceID, KVNamespaceID: &q.metadata.KVNamespaceID},
	}
	infoResponseValidate := func(data *workers.InfoEndpointResponse) error {
		if !slices.Contains(data.KV, q.metadata.KVNamespaceID) {
			return fmt.Errorf("the worker is not bound to the namespace with ID '%s'; please re-deploy the worker with the correct bindings per instructions in the documentation at %s", q.metadata.KVNamespaceID, componentDocsURL)
		}
		return nil
	}
	return q.Base.Init(workerBindings, componentDocsURL, infoResponseValidate)
}

func (q *CFWorkersKV) GetComponentMetadata() map[string]string {
	metadataStruct := componentMetadata{}
	metadataInfo := map[string]string{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo)
	return metadataInfo
}

// Features returns the features supported by this state store.
func (q CFWorkersKV) Features() []state.Feature {
	return []state.Feature{}
}

func (q *CFWorkersKV) Delete(parentCtx context.Context, stateReq *state.DeleteRequest) error {
	token, err := q.metadata.CreateToken()
	if err != nil {
		return fmt.Errorf("failed to create authorization token: %w", err)
	}

	ctx, cancel := context.WithTimeout(parentCtx, 30*time.Second)
	defer cancel()

	u := q.metadata.WorkerURL + "kv/" + q.metadata.KVNamespaceID + "/" + url.PathEscape(stateReq.Key)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return fmt.Errorf("error creating network request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	res, err := q.Client().Do(req)
	if err != nil {
		return fmt.Errorf("error invoking the worker: %w", err)
	}
	defer func() {
		// Drain the body before closing it
		_, _ = io.ReadAll(res.Body)
		res.Body.Close()
	}()
	if res.StatusCode != http.StatusNoContent {
		return fmt.Errorf("invalid response status code: %d", res.StatusCode)
	}
	return nil
}

func (q *CFWorkersKV) Get(parentCtx context.Context, stateReq *state.GetRequest) (*state.GetResponse, error) {
	token, err := q.metadata.CreateToken()
	if err != nil {
		return nil, fmt.Errorf("failed to create authorization token: %w", err)
	}

	ctx, cancel := context.WithTimeout(parentCtx, 30*time.Second)
	defer cancel()

	u := q.metadata.WorkerURL + "kv/" + q.metadata.KVNamespaceID + "/" + url.PathEscape(stateReq.Key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
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
	if res.StatusCode == http.StatusNotFound {
		return &state.GetResponse{}, nil
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("invalid response status code: %d", res.StatusCode)
	}

	// Read the response
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response data: %w", err)
	}

	return &state.GetResponse{
		Data: data,
	}, nil
}

func (q *CFWorkersKV) Set(parentCtx context.Context, stateReq *state.SetRequest) error {
	token, err := q.metadata.CreateToken()
	if err != nil {
		return fmt.Errorf("failed to create authorization token: %w", err)
	}

	ctx, cancel := context.WithTimeout(parentCtx, 30*time.Second)
	defer cancel()

	u := q.metadata.WorkerURL + "kv/" + q.metadata.KVNamespaceID + "/" + url.PathEscape(stateReq.Key)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(q.marshalData(stateReq.Value)))
	if err != nil {
		return fmt.Errorf("error creating network request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	res, err := q.Client().Do(req)
	if err != nil {
		return fmt.Errorf("error invoking the worker: %w", err)
	}
	defer func() {
		// Drain the body before closing it
		_, _ = io.ReadAll(res.Body)
		res.Body.Close()
	}()
	if res.StatusCode != http.StatusCreated {
		return fmt.Errorf("invalid response status code: %d", res.StatusCode)
	}
	return nil
}

func (q *CFWorkersKV) marshalData(value any) []byte {
	switch x := value.(type) {
	case []byte:
		return x
	default:
		b, _ := json.Marshal(x)
		return b
	}
}

// Close the component
func (q *CFWorkersKV) Close() error {
	err := q.Base.Close()
	if err != nil {
		return err
	}
	return nil
}
