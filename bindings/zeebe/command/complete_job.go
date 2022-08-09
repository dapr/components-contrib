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
	"fmt"

	"github.com/camunda/zeebe/clients/go/v8/pkg/commands"

	"github.com/dapr/components-contrib/bindings"
)

type completeJobPayload struct {
	JobKey    *int64      `json:"jobKey"`
	Variables interface{} `json:"variables"`
}

func (z *ZeebeCommand) completeJob(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
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

	_, err = cmd3.Send(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot complete job for key %d: %w", payload.JobKey, err)
	}

	return &bindings.InvokeResponse{}, nil
}
