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

package influx

import (
	"context"
	"encoding/json"
	"fmt"

	influxdb2 "github.com/influxdata/influxdb-client-go"
	"github.com/influxdata/influxdb-client-go/api"
	"github.com/pkg/errors"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const queryOperation bindings.OperationKind = "query"

const (
	rawQueryKey     = "raw"
	respOperatorKey = "operation"
)

var (
	ErrInvalidRequestData      = errors.New("Influx Error: Cannot convert request data")
	ErrCannotWriteRecord       = errors.New("Influx Error: Cannot write point")
	ErrInvalidRequestOperation = errors.Errorf("invalid operation type. Expected %s or %s", queryOperation, bindings.CreateOperation)
	ErrMetadataMissing         = errors.New("metadata required")
	ErrMetadataRawNotFound     = errors.Errorf("required metadata not set: %s", rawQueryKey)
)

// Influx allows writing to InfluxDB.
type Influx struct {
	metadata *influxMetadata
	client   influxdb2.Client
	writeAPI api.WriteAPIBlocking
	queryAPI api.QueryAPI
	logger   logger.Logger
}

type influxMetadata struct {
	URL    string `json:"url"`
	Token  string `json:"token"`
	Org    string `json:"org"`
	Bucket string `json:"bucket"`
}

// NewInflux returns a new kafka binding instance.
func NewInflux(logger logger.Logger) bindings.OutputBinding {
	return &Influx{logger: logger}
}

// Init does metadata parsing and connection establishment.
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
	i.queryAPI = i.client.QueryAPI(i.metadata.Org)

	return nil
}

// GetInfluxMetadata returns new Influx metadata.
func (i *Influx) getInfluxMetadata(meta bindings.Metadata) (*influxMetadata, error) {
	var iMetadata influxMetadata
	err := metadata.DecodeMetadata(meta.Properties, &iMetadata)
	if err != nil {
		return nil, err
	}

	return &iMetadata, nil
}

// Operations returns supported operations.
func (i *Influx) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation, queryOperation}
}

// Invoke called on supported operations.
func (i *Influx) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	switch req.Operation {
	case bindings.CreateOperation:
		var jsonPoint map[string]interface{}
		err := json.Unmarshal(req.Data, &jsonPoint)
		if err != nil {
			return nil, ErrInvalidRequestData
		}

		line := fmt.Sprintf("%s,%s %s", jsonPoint["measurement"], jsonPoint["tags"], jsonPoint["values"])

		// write the point
		err = i.writeAPI.WriteRecord(ctx, line)
		if err != nil {
			return nil, ErrCannotWriteRecord
		}
		return nil, nil
	case queryOperation:
		if req.Metadata == nil {
			return nil, ErrMetadataMissing
		}

		s, ok := req.Metadata[rawQueryKey]
		if !ok || s == "" {
			return nil, ErrMetadataRawNotFound
		}

		res, err := i.queryAPI.QueryRaw(ctx, s, influxdb2.DefaultDialect())
		if err != nil {
			return nil, errors.Wrap(err, "do query influx err")
		}

		resp := &bindings.InvokeResponse{
			Data: []byte(res),
			Metadata: map[string]string{
				respOperatorKey: string(req.Operation),
				rawQueryKey:     s,
			},
		}
		return resp, nil
	default:
		i.logger.Warnf("unsupported operation invoked")
		return nil, ErrInvalidRequestOperation
	}
}

func (i *Influx) Close() error {
	i.client.Close()
	i.writeAPI = nil
	i.queryAPI = nil
	return nil
}
