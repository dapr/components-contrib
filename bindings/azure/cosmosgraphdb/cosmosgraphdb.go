// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package cosmosgraphdb

import (
	"encoding/json"
	"errors"

	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	gremcos "github.com/supplyon/gremcos"
)

const (
	queryOperation bindings.OperationKind = "query"

	// keys from request's metadata
	commandGremlinKey = "gremlin"

	// keys from response's metadata
	respGremlinKey   = "gremlin"
	respOpKey        = "operation"
	respStartTimeKey = "start-time"
	respEndTimeKey   = "end-time"
	respDurationKey  = "duration"
)

// CosmosGraphDB allows performing state operations on collections
type CosmosGraphDB struct {
	metadata *cosmosGraphDBCredentials
	client   *gremcos.Cosmos
	logger   logger.Logger
}

type cosmosGraphDBCredentials struct {
	URL       string `json:"url"`
	MasterKey string `json:"masterKey"`
	Username  string `json:"username"`
	//NumMaxActiveConnections string `json:"NumMaxActiveConnections"`
	//ConnectionIdleTimeout   string `json:"ConnectionIdleTimeout"`
}

// NewCosmosGraphDB returns a new CosmosGraphDB instance
func NewCosmosGraphDB(logger logger.Logger) *CosmosGraphDB {
	return &CosmosGraphDB{logger: logger}
}

// Init performs CosmosDB connection parsing and connecting
func (c *CosmosGraphDB) Init(metadata bindings.Metadata) error {
	m, err := c.parseMetadata(metadata)
	if err != nil {
		return errors.New("failed to create the Cosmos Graph DB connector - metadata error")
	}
	c.metadata = m
	client, err := gremcos.New(c.metadata.URL,
		gremcos.WithAuth(c.metadata.Username, c.metadata.MasterKey),
		//gremcos.NumMaxActiveConnections(m.NumMaxActiveConnections),
		//gremcos.ConnectionIdleTimeout(time.Second* int(m.ConnectionIdleTimeout)),
		//gremcos.MetricsPrefix("CosmosGraphDB"),
	)

	if err != nil {
		return errors.New("failed to create the Cosmos Graph DB connector")
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

	// if !c.client.IsConnected() {
	// 	return nil, errors.New("cosmosGraphDb is not connected")
	// }

	if req == nil {
		return nil, errors.New("invoke request required")
	}

	if req.Metadata == nil {
		return nil, errors.New("metadata required")
	}
	// m.logger.Debugf("operation: %v", req.Operation)

	gq, ok := req.Metadata[commandGremlinKey]
	if !ok || gq == "" {
		return nil, errors.New("required metadata not set: gremlin")
	}
	startTime := time.Now().UTC()
	resp := &bindings.InvokeResponse{
		Metadata: map[string]string{
			respOpKey:        string(req.Operation),
			respGremlinKey:   string(gq),
			respStartTimeKey: startTime.Format(time.RFC3339Nano),
		},
	}
	d, err := c.client.Execute(gq)
	if err != nil {
		return nil, errors.New("error excuting gremlin")
	}
	resp.Data = d[0].Result.Data

	endTime := time.Now().UTC()
	resp.Metadata[respEndTimeKey] = endTime.Format(time.RFC3339Nano)
	resp.Metadata[respDurationKey] = endTime.Sub(startTime).String()

	return resp, nil
}
