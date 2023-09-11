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

package command

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/camunda/zeebe/clients/go/v8/pkg/commands"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
)

var (
	ErrAmbiguousCreationVars = errors.New("either 'bpmnProcessId' or 'processDefinitionKey' must be passed, not both at the same time")
	ErrMissingCreationVars   = errors.New("either 'bpmnProcessId' or 'processDefinitionKey' must be passed")
)

type createInstancePayload struct {
	BpmnProcessID        string            `json:"bpmnProcessId"`
	ProcessDefinitionKey *int64            `json:"processDefinitionKey"`
	Version              *int32            `json:"version"`
	Variables            interface{}       `json:"variables"`
	WithResult           bool              `json:"withResult"`
	FetchVariables       []string          `json:"fetchVariables"`
	RequestTimeout       metadata.Duration `json:"requestTimeout"`
}

func (z *ZeebeCommand) createInstance(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
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

	var response interface{}
	// The request timeout has only an affect if WithResult is used. Using WithResult means that the operation is
	// synchronous instead of asynchronous, and the request timeout defines how long the client should wait for the
	// workflow/process to finish to get the result.
	//
	// From a code perspective, there are two Send methods in the Zeebe client. One if WithResult was used and
	// which extracts the request timeout from the context and another one which will not use any timeout.
	if payload.WithResult && payload.RequestTimeout.Duration != time.Duration(0) {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, payload.RequestTimeout.Duration)
		defer cancel()
		response, err = cmd3.WithResult().FetchVariables(payload.FetchVariables...).Send(ctxWithTimeout)
	} else if payload.WithResult {
		response, err = cmd3.WithResult().FetchVariables(payload.FetchVariables...).Send(ctx)
	} else {
		response, err = cmd3.Send(ctx)
	}

	if err != nil {
		return nil, fmt.Errorf("cannot create instance for %s: %w", errorDetail, err)
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal response to json: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}
