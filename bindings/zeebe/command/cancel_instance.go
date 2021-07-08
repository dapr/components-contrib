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

var ErrMissingProcessInstanceKey = errors.New("processInstanceKey is a required attribute")

type cancelInstancePayload struct {
	ProcessInstanceKey *int64 `json:"processInstanceKey"`
}

func (z *ZeebeCommand) cancelInstance(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var payload cancelInstancePayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, err
	}

	if payload.ProcessInstanceKey == nil {
		return nil, ErrMissingProcessInstanceKey
	}

	_, err = z.client.NewCancelInstanceCommand().
		ProcessInstanceKey(*payload.ProcessInstanceKey).
		Send(context.Background())
	if err != nil {
		return nil, fmt.Errorf("cannot cancel instance for process instance key %d: %w", payload.ProcessInstanceKey, err)
	}

	return &bindings.InvokeResponse{}, nil
}
