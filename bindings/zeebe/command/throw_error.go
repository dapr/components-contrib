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

var ErrMissingErrorCode = errors.New("errorCode is a required attribute")

type throwErrorPayload struct {
	JobKey       *int64 `json:"jobKey"`
	ErrorCode    string `json:"errorCode"`
	ErrorMessage string `json:"errorMessage"`
}

func (z *ZeebeCommand) throwError(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var payload throwErrorPayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, err
	}

	if payload.JobKey == nil {
		return nil, ErrMissingJobKey
	}

	if payload.ErrorCode == "" {
		return nil, ErrMissingErrorCode
	}

	cmd := z.client.NewThrowErrorCommand().
		JobKey(*payload.JobKey).
		ErrorCode(payload.ErrorCode)

	if payload.ErrorMessage != "" {
		cmd = cmd.ErrorMessage(payload.ErrorMessage)
	}

	_, err = cmd.Send(context.Background())
	if err != nil {
		return nil, fmt.Errorf("cannot throw error for job key %d: %w", payload.JobKey, err)
	}

	return &bindings.InvokeResponse{}, nil
}
