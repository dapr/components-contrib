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
