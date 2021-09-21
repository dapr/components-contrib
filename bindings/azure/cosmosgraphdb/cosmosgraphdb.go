// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package cosmosgraphdb

import (
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

// CosmosGraphDB allows performing state operations on collections.
type CosmosGraphDB struct {
	metadata *cosmosGraphDBCredentials
	client   *gremcos.Cosmos
	logger   logger.Logger
}

type cosmosGraphDBCredentials struct {
	URL       string `json:"url"`
	MasterKey string `json:"masterKey"`
	Username  string `json:"username"`
}

// NewCosmosGraphDB returns a new CosmosGraphDB instance.
func NewCosmosGraphDB(logger logger.Logger) *CosmosGraphDB {
	return &CosmosGraphDB{logger: logger}
}

// Init performs CosmosDB connection parsing and connecting.
func (c *CosmosGraphDB) Init(metadata bindings.Metadata) error {
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
		return errors.New("CosmosGraphDB Error: failed to create the Cosmos Graph DB connector")
	}

	c.client = client

	return nil
}

func (c *CosmosGraphDB) parseMetadata(metadata bindings.Metadata) (*cosmosGraphDBCredentials, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var creds cosmosGraphDBCredentials
	err = json.Unmarshal(b, &creds)
	if err != nil {
		return nil, err
	}

	return &creds, nil
}

func (c *CosmosGraphDB) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{queryOperation}
}

func (c *CosmosGraphDB) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var jsonPoint map[string]interface{}
	err := json.Unmarshal(req.Data, &jsonPoint)
	if err != nil {
		return nil, errors.New("CosmosGraphDB Error: Cannot convert request data")
	}

	gq := fmt.Sprintf("%s", jsonPoint[commandGremlinKey])

	if gq == "" {
		return nil, errors.New("CosmosGraphDB Error: missing data - gremlin query not set")
	}
	startTime := time.Now().UTC()
	resp := &bindings.InvokeResponse{
		Metadata: map[string]string{
			respOpKey:        string(req.Operation),
			respGremlinKey:   gq,
			respStartTimeKey: startTime.Format(time.RFC3339Nano),
		},
	}
	d, err := c.client.Execute(gq)
	if err != nil {
		return nil, errors.New("CosmosGraphDB Error:error excuting gremlin")
	}
	if len(d) > 0 {
		resp.Data = d[0].Result.Data
	}
	endTime := time.Now().UTC()
	resp.Metadata[respEndTimeKey] = endTime.Format(time.RFC3339Nano)
	resp.Metadata[respDurationKey] = endTime.Sub(startTime).String()

	return resp, nil
}
