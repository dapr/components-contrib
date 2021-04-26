// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package hasura

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
	// list of operations.
	queryOperation    bindings.OperationKind = "query"
	mutationOperation bindings.OperationKind = "mutation"

	// configurations to connect to Hasura
	connectionURLKey          = "url"
	connectionURLAccessKeyKey = "accesskey"

	// keys from request's metadata
	commandQuery    = "query"
	commandMutation = "mutation"

	// keys from response's metadata
	respOpKey        = "operation"
	respStartTimeKey = "start-time"
	respEndTimeKey   = "end-time"
	respDurationKey  = "duration"
)

// Hasura represents Hasura output bindings
type Hasura struct {
	client    *graphql.Client
	accesskey string
	logger    logger.Logger
}

var _ = bindings.OutputBinding(&Hasura{})

// NewHausura returns a new Hasura binding instance
func NewHasura(logger logger.Logger) *Hasura {
	return &Hasura{logger: logger}
}

// Init initializes the Hasura binding
func (h *Hasura) Init(metadata bindings.Metadata) error {
	h.logger.Debug("Hasura Error: Initializing Hasura binding")

	p := metadata.Properties
	url, ok := p[connectionURLKey]
	if !ok || url == "" {
		return fmt.Errorf("Hasura Error: Missing Hasura URL")
	}

	accesskey, ok := p[connectionURLAccessKeyKey]
	if !ok || accesskey == "" {
		return fmt.Errorf("Hasura Error: Missing Hasura Access Key")
	}

	// Connect to GraphQL Server
	client := graphql.NewClient(url)

	h.client = client
	h.accesskey = accesskey

	return nil
}

// Operations returns list of operations supported by Hasura binding
func (h *Hasura) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		queryOperation,
		mutationOperation,
	}
}

// Invoke handles all invoke operations
func (h *Hasura) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("Hasura Error: Invoke request required")
	}

	if req.Metadata == nil {
		return nil, fmt.Errorf("Hasura Error: Metadata required")
	}
	h.logger.Debugf("operation: %v", req.Operation)

	q, okq := req.Metadata[commandQuery]
	m, okm := req.Metadata[commandMutation]
	if (!okq || q == "") && (!okm || m == "") {
		return nil, fmt.Errorf("Hasura Error: Required metadata not set: %s or %s", commandQuery, commandMutation)
	}

	startTime := time.Now().UTC()

	resp := &bindings.InvokeResponse{
		Metadata: map[string]string{
			respOpKey:        string(req.Operation),
			respStartTimeKey: startTime.Format(time.RFC3339Nano),
		},
	}

	var graphqlResponse interface{}

	switch req.Operation {
	case queryOperation:
		if !okq || q == "" {
			return nil, fmt.Errorf("Hasura Error: Required query not set")
		}

		graphqlRequest := graphql.NewRequest(req.Metadata[commandQuery])
		graphqlRequest.Header.Set("x-hasura-access-key", h.accesskey)

		if err := h.client.Run(context.Background(), graphqlRequest, &graphqlResponse); err != nil {
			return nil, fmt.Errorf("Hasura Error: %v", err)
		}

		fmt.Println(graphqlResponse)
		b, err := json.Marshal(graphqlResponse)
		if err != nil {
			return nil, fmt.Errorf("Hasura Error: %v", err)
		}
		fmt.Println(string(b))

		resp.Data = b

	case mutationOperation:
		if !okm || m == "" {
			return nil, fmt.Errorf("Hasura Error: Required mutation not set")
		}

		graphqlRequest := graphql.NewRequest(req.Metadata[commandMutation])
		graphqlRequest.Header.Set("x-hasura-access-key", h.accesskey)

		if err := h.client.Run(context.Background(), graphqlRequest, &graphqlResponse); err != nil {
			return nil, fmt.Errorf("Hasura Error: %v", err)
		}

		b, err := json.Marshal(graphqlResponse)
		if err != nil {
			return nil, fmt.Errorf("Hasura Error: %v", err)
		}

		resp.Data = b

	default:
		return nil, fmt.Errorf("Hasura Error: invalid operation type: %s. Expected %s or %s",
			req.Operation, queryOperation, mutationOperation)
	}

	endTime := time.Now().UTC()
	resp.Metadata[respEndTimeKey] = endTime.Format(time.RFC3339Nano)
	resp.Metadata[respDurationKey] = endTime.Sub(startTime).String()

	return resp, nil
}
