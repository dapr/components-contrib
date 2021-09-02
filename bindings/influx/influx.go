// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package influx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/influxdata/influxdb-client-go/api"
)

// Influx allows writing to InfluxDB
type Influx struct {
	metadata *influxMetadata
	client   influxdb2.Client
	writeAPI api.WriteAPIBlocking
	logger   logger.Logger
}

type influxMetadata struct {
	URL    string `json:"url"`
	Token  string `json:"token"`
	Org    string `json:"org"`
	Bucket string `json:"bucket"`
}

// NewInflux returns a new kafka binding instance
func NewInflux(logger logger.Logger) *Influx {
	return &Influx{logger: logger}
}

// Init does metadata parsing and connection establishment
func (i *Influx) Init(metadata bindings.Metadata) error {
	influxMeta, err := i.getInfluxMetadata(metadata)
	if err != nil {
		return err
	}

	i.metadata = influxMeta
	if i.metadata.URL == "" {
		return errors.New("Influx Error: URL required")
	}

	if i.metadata.Token == "" {
		return errors.New("Influx Error: Token required")
	}

	if i.metadata.Org == "" {
		return errors.New("Influx Error: Org required")
	}

	if i.metadata.Bucket == "" {
		return errors.New("Influx Error: Bucket required")
	}

	client := influxdb2.NewClient(i.metadata.URL, i.metadata.Token)
	i.client = client
	i.writeAPI = i.client.WriteAPIBlocking(i.metadata.Org, i.metadata.Bucket)

	return nil
}

// GetInfluxMetadata returns new Influx metadata
func (i *Influx) getInfluxMetadata(metadata bindings.Metadata) (*influxMetadata, error) {
	b, err := json.Marshal(metadata.Properties)
	if err != nil {
		return nil, err
	}

	var iMetadata influxMetadata
	err = json.Unmarshal(b, &iMetadata)
	if err != nil {
		return nil, err
	}

	return &iMetadata, nil
}

// Operations returns supported operations
func (i *Influx) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

// Invoke called on supported operations
func (i *Influx) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var jsonPoint map[string]interface{}
	err := json.Unmarshal(req.Data, &jsonPoint)
	if err != nil {
		return nil, errors.New("Influx Error: Cannot convert request data")
	}

	line := fmt.Sprintf("%s,%s %s", jsonPoint["measurement"], jsonPoint["tags"], jsonPoint["values"])

	// write the point
	err = i.writeAPI.WriteRecord(context.Background(), line)
	if err != nil {
		return nil, errors.New("Influx Error: Cannot write point")
	}

	return nil, nil
}

func (i *Influx) Close() error {
	i.client.Close()
	i.writeAPI = nil

	return nil
}
