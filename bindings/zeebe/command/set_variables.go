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

	"github.com/dapr/components-contrib/bindings"
)

var (
	ErrMissingElementInstanceKey = errors.New("elementInstanceKey is a required attribute")
	ErrMissingVariables          = errors.New("variables is a required attribute")
)

type setVariablesPayload struct {
	ElementInstanceKey *int64      `json:"elementInstanceKey"`
	Local              bool        `json:"local"`
	Variables          interface{} `json:"variables"`
}

func (z *ZeebeCommand) setVariables(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var payload setVariablesPayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, err
	}

	if payload.ElementInstanceKey == nil {
		return nil, ErrMissingElementInstanceKey
	}

	if payload.Variables == nil {
		return nil, ErrMissingVariables
	}

	cmd, err := z.client.NewSetVariablesCommand().
		ElementInstanceKey(*payload.ElementInstanceKey).
		VariablesFromObject(payload.Variables)
	if err != nil {
		return nil, err
	}

	response, err := cmd.Local(payload.Local).Send(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot set variables for element instance key %d: %w", payload.ElementInstanceKey, err)
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal response to json: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}
