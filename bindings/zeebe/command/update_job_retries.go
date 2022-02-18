/*
Copyright 2022 The Dapr Authors
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

	"github.com/camunda-cloud/zeebe/clients/go/pkg/commands"

	"github.com/dapr/components-contrib/bindings"
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
		return nil, ErrMissingJobKey
	}

	cmd1 := z.client.NewUpdateJobRetriesCommand().JobKey(*payload.JobKey)
	var cmd2 commands.DispatchUpdateJobRetriesCommand = cmd1
	if payload.Retries != nil {
		cmd2 = cmd1.Retries(*payload.Retries)
	}

	_, err = cmd2.Send(context.Background())
	if err != nil {
		return nil, fmt.Errorf("cannot uodate job retries for key %d: %w", payload.JobKey, err)
	}

	return &bindings.InvokeResponse{}, nil
}
