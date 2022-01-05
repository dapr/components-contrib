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
