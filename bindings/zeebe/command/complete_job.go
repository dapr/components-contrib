// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package command

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/camunda-cloud/zeebe/clients/go/pkg/commands"

	"github.com/dapr/components-contrib/bindings"
)

type completeJobPayload struct {
	JobKey    *int64      `json:"jobKey"`
	Variables interface{} `json:"variables"`
}

func (z *ZeebeCommand) completeJob(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var payload completeJobPayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, err
	}

	if payload.JobKey == nil {
		return nil, ErrMissingJobKey
	}

	cmd1 := z.client.NewCompleteJobCommand()
	cmd2 := cmd1.JobKey(*payload.JobKey)
	var cmd3 commands.DispatchCompleteJobCommand = cmd2
	if payload.Variables != nil {
		cmd3, err = cmd2.VariablesFromObject(payload.Variables)
		if err != nil {
			return nil, err
		}
	}

	_, err = cmd3.Send(context.Background())
	if err != nil {
		return nil, fmt.Errorf("cannot complete job for key %d: %w", payload.JobKey, err)
	}

	return &bindings.InvokeResponse{}, nil
}
