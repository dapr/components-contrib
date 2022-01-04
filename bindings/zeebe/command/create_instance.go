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

	"github.com/camunda-cloud/zeebe/clients/go/pkg/commands"

	"github.com/dapr/components-contrib/bindings"
)

var (
	ErrAmbiguousCreationVars = errors.New("either 'bpmnProcessId' or 'processDefinitionKey' must be passed, not both at the same time")
	ErrMissingCreationVars   = errors.New("either 'bpmnProcessId' or 'processDefinitionKey' must be passed")
)

type createInstancePayload struct {
	BpmnProcessID        string      `json:"bpmnProcessId"`
	ProcessDefinitionKey *int64      `json:"processDefinitionKey"`
	Version              *int32      `json:"version"`
	Variables            interface{} `json:"variables"`
}

func (z *ZeebeCommand) createInstance(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var payload createInstancePayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, err
	}

	cmd1 := z.client.NewCreateInstanceCommand()
	var cmd2 commands.CreateInstanceCommandStep2
	var cmd3 commands.CreateInstanceCommandStep3
	var errorDetail string

	if payload.BpmnProcessID != "" { //nolint:nestif
		if payload.ProcessDefinitionKey != nil {
			return nil, ErrAmbiguousCreationVars
		}

		cmd2 = cmd1.BPMNProcessId(payload.BpmnProcessID)
		if payload.Version != nil {
			cmd3 = cmd2.Version(*payload.Version)
			errorDetail = fmt.Sprintf("bpmnProcessId %s and version %d", payload.BpmnProcessID, payload.Version)
		} else {
			cmd3 = cmd2.LatestVersion()
			errorDetail = fmt.Sprintf("bpmnProcessId %s and lates version", payload.BpmnProcessID)
		}
	} else if payload.ProcessDefinitionKey != nil {
		cmd3 = cmd1.ProcessDefinitionKey(*payload.ProcessDefinitionKey)
		errorDetail = fmt.Sprintf("processDefinitionKey %d", payload.ProcessDefinitionKey)
	} else {
		return nil, ErrMissingCreationVars
	}

	if payload.Variables != nil {
		cmd3, err = cmd3.VariablesFromObject(payload.Variables)
		if err != nil {
			return nil, err
		}
	}

	response, err := cmd3.Send(context.Background())
	if err != nil {
		return nil, fmt.Errorf("cannot create instane for %s: %w", errorDetail, err)
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal response to json: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}
