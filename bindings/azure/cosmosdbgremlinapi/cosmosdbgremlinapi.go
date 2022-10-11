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

package cosmosdbgremlinapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	gremcos "github.com/supplyon/gremcos"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

const (
	queryOperation bindings.OperationKind = "query"

	// keys from request's Data.
	commandGremlinKey = "gremlin"

	// keys from response's Data.
	respGremlinKey   = "gremlin"
	respOpKey        = "operation"
	respStartTimeKey = "start-time"
	respEndTimeKey   = "end-time"
	respDurationKey  = "duration"
)

// CosmosDBGremlinAPI allows performing state operations on collections.
type CosmosDBGremlinAPI struct {
	metadata *cosmosDBGremlinAPICredentials
	client   *gremcos.Cosmos
	logger   logger.Logger
}

type cosmosDBGremlinAPICredentials struct {
	URL       string `json:"url"`
	MasterKey string `json:"masterKey"`
	Username  string `json:"username"`
}

// NewCosmosDBGremlinAPI returns a new CosmosDBGremlinAPI instance.
func NewCosmosDBGremlinAPI(logger logger.Logger) bindings.OutputBinding {
	return &CosmosDBGremlinAPI{logger: logger}
}

// Init performs CosmosDBGremlinAPI connection parsing and connecting.
func (c *CosmosDBGremlinAPI) Init(metadata bindings.Metadata) error {
	c.logger.Debug("Initializing Cosmos Graph DB binding")

	m, err := c.parseMetadata(metadata)
	if err != nil {
		return err
	}
	c.metadata = m
	client, err := gremcos.New(c.metadata.URL,
		gremcos.WithAuth(c.metadata.Username, c.metadata.MasterKey),
	)
	if err != nil {
		return errors.New("CosmosDBGremlinAPI Error: failed to create the Cosmos Graph DB connector")
	}

	c.client = &client

	return nil
}

func (c *CosmosDBGremlinAPI) parseMetadata(metadata bindings.Metadata) (*cosmosDBGremlinAPICredentials, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var creds cosmosDBGremlinAPICredentials
	err = json.Unmarshal(b, &creds)
	if err != nil {
		return nil, err
	}

	return &creds, nil
}

func (c *CosmosDBGremlinAPI) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{queryOperation}
}

func (c *CosmosDBGremlinAPI) Invoke(_ context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var jsonPoint map[string]interface{}
	err := json.Unmarshal(req.Data, &jsonPoint)
	if err != nil {
		return nil, errors.New("CosmosDBGremlinAPI Error: Cannot convert request data")
	}

	gq := fmt.Sprintf("%s", jsonPoint[commandGremlinKey])

	if gq == "" {
		return nil, errors.New("CosmosDBGremlinAPI Error: missing data - gremlin query not set")
	}
	startTime := time.Now()
	resp := &bindings.InvokeResponse{
		Metadata: map[string]string{
			respOpKey:        string(req.Operation),
			respGremlinKey:   gq,
			respStartTimeKey: startTime.Format(time.RFC3339Nano),
		},
	}
	d, err := (*c.client).Execute(gq)
	if err != nil {
		return nil, errors.New("CosmosDBGremlinAPI Error:error excuting gremlin")
	}
	if len(d) > 0 {
		resp.Data = d[0].Result.Data
	}
	endTime := time.Now()
	resp.Metadata[respEndTimeKey] = endTime.Format(time.RFC3339Nano)
	resp.Metadata[respDurationKey] = endTime.Sub(startTime).String()

	return resp, nil
}
