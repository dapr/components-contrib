// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	graphql "github.com/machinebox/graphql"
)

const (
	// configurations to connect to GraphQL
	connectionEndPointKey = "endpoint"

	// keys from request's metadata
	commandQuery    = "query"
	commandMutation = "mutation"

	// keys from response's metadata
	respOpKey        = "operation"
	respStartTimeKey = "start-time"
	respEndTimeKey   = "end-time"
	respDurationKey  = "duration"
)

// GraphQL represents GraphQL output bindings
type GraphQL struct {
	client *graphql.Client
	header map[string]string
	logger logger.Logger
}

var _ = bindings.OutputBinding(&GraphQL{})

// NewHausura returns a new GraphQL binding instance
func NewGraphQL(logger logger.Logger) *GraphQL {
	var gql GraphQL
	gql.logger = logger

	return &gql
}

// Init initializes the GraphQL binding
func (gql *GraphQL) Init(metadata bindings.Metadata) error {
	gql.logger.Debug("GraphQL Error: Initializing GraphQL binding")

	p := metadata.Properties
	ep, ok := p[connectionEndPointKey]
	if !ok || ep == "" {
		return fmt.Errorf("GraphQL Error: Missing GraphQL URL")
	}

	// Connect to GraphQL Server
	client := graphql.NewClient(ep)

	gql.client = client
	gql.header = make(map[string]string)
	for k, v := range p {
		if k != connectionEndPointKey {
			gql.header[k] = v
		}
	}

	return nil
}

// Operations returns list of operations supported by GraphQL binding
func (gql *GraphQL) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.QueryOperation,
		bindings.MutationOperation,
	}
}

// Invoke handles all invoke operations
func (gql *GraphQL) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("GraphQL Error: Invoke request required")
	}

	if req.Metadata == nil {
		return nil, fmt.Errorf("GraphQL Error: Metadata required")
	}
	gql.logger.Debugf("operation: %v", req.Operation)

	q, okq := req.Metadata[commandQuery]
	m, okm := req.Metadata[commandMutation]
	if (!okq || q == "") && (!okm || m == "") {
		return nil, fmt.Errorf("GraphQL Error: Required metadata not set: %s or %s", commandQuery, commandMutation)
	}

	startTime := time.Now().UTC()

	resp := &bindings.InvokeResponse{
		Metadata: map[string]string{
			respOpKey:        string(req.Operation),
			respStartTimeKey: startTime.Format(time.RFC3339Nano),
		},
		Data: []byte{},
	}

	var graphqlResponse interface{}

	switch req.Operation {
	case bindings.QueryOperation:
		if !okq || q == "" {
			return nil, fmt.Errorf("GraphQL Error: Required query not set")
		}

		graphqlRequest := graphql.NewRequest(req.Metadata[commandQuery])

		for headerKey, headerValue := range gql.header {
			graphqlRequest.Header.Set(headerKey, headerValue)
		}

		if err := gql.client.Run(context.Background(), graphqlRequest, &graphqlResponse); err != nil {
			return nil, fmt.Errorf("GraphQL Error: %w", err)
		}

		b, err := json.Marshal(graphqlResponse)
		if err != nil {
			return nil, fmt.Errorf("GraphQL Error: %w", err)
		}

		resp.Data = b

	case bindings.MutationOperation:
		if !okm || m == "" {
			return nil, fmt.Errorf("GraphQL Error: Required mutation not set")
		}

		graphqlRequest := graphql.NewRequest(req.Metadata[commandMutation])

		for headerKey, headerValue := range gql.header {
			graphqlRequest.Header.Set(headerKey, headerValue)
		}

		if err := gql.client.Run(context.Background(), graphqlRequest, &graphqlResponse); err != nil {
			return nil, fmt.Errorf("GraphQL Error: %w", err)
		}

		b, err := json.Marshal(graphqlResponse)
		if err != nil {
			return nil, fmt.Errorf("GraphQL Error: %w", err)
		}

		resp.Data = b

	case bindings.CreateOperation:
		fallthrough

	case bindings.GetOperation:
		fallthrough

	case bindings.DeleteOperation:
		fallthrough

	case bindings.ListOperation:
		fallthrough

	default:
		return nil, fmt.Errorf("GraphQL Error: invalid operation type: %s. Expected %s or %s",
			req.Operation, bindings.QueryOperation, bindings.MutationOperation)
	}

	endTime := time.Now().UTC()
	resp.Metadata[respEndTimeKey] = endTime.Format(time.RFC3339Nano)
	resp.Metadata[respDurationKey] = endTime.Sub(startTime).String()

	return resp, nil
}
