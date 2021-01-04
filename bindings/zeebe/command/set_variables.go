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

const (
	// errors
	missingElementInstanceKeyErrorMsg = "elementInstanceKey is a required attribute"
	missingVariablesErrorMsg          = "variables is a required attribute"
)

type setVariablesPayload struct {
	ElementInstanceKey *int64      `json:"elementInstanceKey"`
	Local              bool        `json:"local"`
	Variables          interface{} `json:"variables"`
}

func (z *ZeebeCommand) setVariables(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var payload setVariablesPayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, err
	}

	if payload.ElementInstanceKey == nil {
		return nil, errors.New(missingElementInstanceKeyErrorMsg)
	}

	if payload.Variables == nil {
		return nil, errors.New(missingVariablesErrorMsg)
	}

	cmd, err := z.client.NewSetVariablesCommand().
		ElementInstanceKey(*payload.ElementInstanceKey).
		VariablesFromObject(payload.Variables)
	if err != nil {
		return nil, err
	}

	response, err := cmd.Local(payload.Local).Send(context.Background())
	if err != nil {
		return nil, fmt.Errorf("cannot set variables for element instance key %d: %s", payload.ElementInstanceKey, err)
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal response to json: %s", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}
