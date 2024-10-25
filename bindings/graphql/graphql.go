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

package graphql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	graphql "github.com/machinebox/graphql"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (

	// keys from request's metadata.
	commandQuery    = "query"
	commandMutation = "mutation"

	// keys from response's metadata.
	respOpKey        = "operation"
	respStartTimeKey = "start-time"
	respEndTimeKey   = "end-time"
	respDurationKey  = "duration"

	QueryOperation    bindings.OperationKind = "query"
	MutationOperation bindings.OperationKind = "mutation"
)

type graphQLMetadata struct {
	Endpoint string `mapstructure:"endpoint"`
}

// GraphQL represents GraphQL output bindings.
type GraphQL struct {
	client *graphql.Client
	header map[string]string
	logger logger.Logger
}

// NewGraphQL returns a new GraphQL binding instance.
func NewGraphQL(logger logger.Logger) bindings.OutputBinding {
	return &GraphQL{logger: logger}
}

// Init initializes the GraphQL binding.
func (gql *GraphQL) Init(_ context.Context, meta bindings.Metadata) error {
	gql.logger.Debug("GraphQL Error: Initializing GraphQL binding")

	m := graphQLMetadata{}
	err := kitmd.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return err
	}

	if m.Endpoint == "" {
		return errors.New("GraphQL Error: Missing GraphQL URL")
	}

	// Connect to GraphQL Server
	client := graphql.NewClient(m.Endpoint)

	gql.client = client
	gql.header = make(map[string]string)
	for k, v := range meta.Properties {
		if strings.HasPrefix(k, "header:") {
			gql.header[strings.TrimPrefix(k, "header:")] = v
		}
	}

	return nil
}

// Operations returns list of operations supported by GraphQL binding.
func (gql *GraphQL) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		QueryOperation,
		MutationOperation,
	}
}

// Invoke handles all invoke operations.
func (gql *GraphQL) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	if req == nil {
		return nil, errors.New("GraphQL Error: Invoke request required")
	}

	if req.Metadata == nil {
		return nil, errors.New("GraphQL Error: Metadata required")
	}
	gql.logger.Debugf("operation: %v", req.Operation)

	startTime := time.Now()

	resp := &bindings.InvokeResponse{
		Metadata: map[string]string{
			respOpKey:        string(req.Operation),
			respStartTimeKey: startTime.Format(time.RFC3339Nano),
		},
		Data: []byte{},
	}

	var graphqlResponse interface{}

	switch req.Operation { //nolint:exhaustive
	case QueryOperation:
		if err := gql.runRequest(ctx, commandQuery, req, &graphqlResponse); err != nil {
			return nil, err
		}

	case MutationOperation:
		if err := gql.runRequest(ctx, commandMutation, req, &graphqlResponse); err != nil {
			return nil, err
		}

	default:
		return nil, fmt.Errorf("GraphQL Error: invalid operation type: %s. Expected %s or %s",
			req.Operation, QueryOperation, MutationOperation)
	}

	b, err := json.Marshal(graphqlResponse)
	if err != nil {
		return nil, fmt.Errorf("GraphQL Error: %w", err)
	}

	resp.Data = b

	endTime := time.Now()
	resp.Metadata[respEndTimeKey] = endTime.Format(time.RFC3339Nano)
	resp.Metadata[respDurationKey] = endTime.Sub(startTime).String()

	return resp, nil
}

func (gql *GraphQL) runRequest(ctx context.Context, requestKey string, req *bindings.InvokeRequest, response interface{}) error {
	requestString, ok := req.Metadata[requestKey]
	if !ok || requestString == "" {
		return fmt.Errorf("GraphQL Error: required %q not set", requestKey)
	}

	// Check that the command is either a query or mutation based on the first keyword.
	requestString = strings.TrimSpace(requestString)
	re := regexp.MustCompile(`(?m)` + requestKey + `\b`)
	matches := re.FindAllStringIndex(requestString, 1)
	if len(matches) != 1 || matches[0][0] != 0 {
		return fmt.Errorf("GraphQL Error: command is not a %s", requestKey)
	}

	request := graphql.NewRequest(requestString)

	for headerKey, headerValue := range gql.header {
		request.Header.Set(headerKey, headerValue)
	}

	for k, v := range req.Metadata {
		if strings.HasPrefix(k, "header:") {
			request.Header.Set(strings.TrimPrefix(k, "header:"), v)
		} else if strings.HasPrefix(k, "variable:") {
			request.Var(strings.TrimPrefix(k, "variable:"), v)
		}
	}

	if err := gql.client.Run(ctx, request, response); err != nil {
		return fmt.Errorf("GraphQL Error: %w", err)
	}

	return nil
}

// GetComponentMetadata returns the metadata of the component.
func (gql *GraphQL) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := graphQLMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}

func (gql *GraphQL) Close() error {
	return nil
}
