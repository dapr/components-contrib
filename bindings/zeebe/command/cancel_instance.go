// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package command

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dapr/components-contrib/bindings"
)

var ErrMissingWorkflowInstanceKey = errors.New("workflowInstanceKey is a required attribute")

type cancelInstancePayload struct {
	WorkflowInstanceKey *int64 `json:"workflowInstanceKey"`
}

func (z *ZeebeCommand) cancelInstance(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var payload cancelInstancePayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, err
	}

	if payload.WorkflowInstanceKey == nil {
		return nil, ErrMissingWorkflowInstanceKey
	}

	_, err = z.client.NewCancelInstanceCommand().
		WorkflowInstanceKey(*payload.WorkflowInstanceKey).
		Send(context.Background())
	if err != nil {
		return nil, fmt.Errorf("cannot cancel instance for workflow instance key %d: %w", payload.WorkflowInstanceKey, err)
	}

	return &bindings.InvokeResponse{}, nil
}
