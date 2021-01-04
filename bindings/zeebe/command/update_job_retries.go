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
	"github.com/zeebe-io/zeebe/clients/go/pkg/commands"
)

type updateJobRetriesPayload struct {
	JobKey  *int64 `json:"jobKey"`
	Retries *int32 `json:"retries"`
}

func (z *ZeebeCommand) updateJobRetries(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var payload updateJobRetriesPayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, err
	}

	if payload.JobKey == nil {
		return nil, errors.New(missingJobKeyErrorMsg)
	}

	cmd1 := z.client.NewUpdateJobRetriesCommand().JobKey(*payload.JobKey)
	var cmd2 commands.DispatchUpdateJobRetriesCommand = cmd1
	if payload.Retries != nil {
		cmd2 = cmd1.Retries(*payload.Retries)
	}

	_, err = cmd2.Send(context.Background())
	if err != nil {
		return nil, fmt.Errorf("cannot uodate job retries for key %d: %s", payload.JobKey, err)
	}

	return &bindings.InvokeResponse{}, nil
}
