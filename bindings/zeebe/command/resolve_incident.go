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
	"errors"
	"fmt"

	"github.com/dapr/components-contrib/bindings"
)

var ErrMissingIncidentKey = errors.New("incidentKey is a required attribute")

type resolveIncidentPayload struct {
	IncidentKey *int64 `json:"incidentKey"`
}

func (z *ZeebeCommand) resolveIncident(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var payload resolveIncidentPayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, err
	}

	if payload.IncidentKey == nil {
		return nil, ErrMissingIncidentKey
	}

	_, err = z.client.NewResolveIncidentCommand().
		IncidentKey(*payload.IncidentKey).
		Send(context.Background())
	if err != nil {
		return nil, fmt.Errorf("cannot resolve incident for key %d: %w", payload.IncidentKey, err)
	}

	return &bindings.InvokeResponse{}, nil
}
