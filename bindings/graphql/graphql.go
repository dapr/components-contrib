// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
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

	QueryOperation    bindings.OperationKind = "query"
	MutationOperation bindings.OperationKind = "mutation"
)

// GraphQL represents GraphQL output bindings
type GraphQL struct {
	client *graphql.Client
	header map[string]string
	logger logger.Logger
}

var _ = bindings.OutputBinding(&GraphQL{})

// NewGraphQL returns a new GraphQL binding instance
func NewGraphQL(logger logger.Logger) *GraphQL {
	return &GraphQL{logger: logger}
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
		if strings.HasPrefix(k, "header:") {
			gql.header[strings.TrimPrefix(k, "header:")] = v
		}
	}

	return nil
}

// Operations returns list of operations supported by GraphQL binding
func (gql *GraphQL) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		QueryOperation,
		MutationOperation,
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

	startTime := time.Now().UTC()

	resp := &bindings.InvokeResponse{
		Metadata: map[string]string{
			respOpKey:        string(req.Operation),
			respStartTimeKey: startTime.Format(time.RFC3339Nano),
		},
		Data: []byte{},
	}

	var graphqlResponse interface{}

	switch req.Operation { // nolint: exhaustive
	case QueryOperation:
		if err := gql.runRequest(commandQuery, req, &graphqlResponse); err != nil {
			return nil, err
		}

	case MutationOperation:
		if err := gql.runRequest(commandMutation, req, &graphqlResponse); err != nil {
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

	endTime := time.Now().UTC()
	resp.Metadata[respEndTimeKey] = endTime.Format(time.RFC3339Nano)
	resp.Metadata[respDurationKey] = endTime.Sub(startTime).String()

	return resp, nil
}

func (gql *GraphQL) runRequest(requestKey string, req *bindings.InvokeRequest, response interface{}) error {
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
		}
	}

	if err := gql.client.Run(context.Background(), request, response); err != nil {
		return fmt.Errorf("GraphQL Error: %w", err)
	}

	return nil
}
