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
	missingRetriesErrorMsg = "retries is a required attribute"
)

type failJobPayload struct {
	JobKey       *int64 `json:"jobKey"`
	Retries      *int32 `json:"retries"`
	ErrorMessage string `json:"errorMessage"`
}

func (z *ZeebeCommand) failJob(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var payload failJobPayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, err
	}

	if payload.JobKey == nil {
		return nil, errors.New(missingJobKeyErrorMsg)
	}

	if payload.Retries == nil {
		return nil, errors.New(missingRetriesErrorMsg)
	}

	cmd := z.client.NewFailJobCommand().
		JobKey(*payload.JobKey).
		Retries(*payload.Retries)

	if payload.ErrorMessage != "" {
		cmd = cmd.ErrorMessage(payload.ErrorMessage)
	}

	_, err = cmd.Send(context.Background())
	if err != nil {
		return nil, fmt.Errorf("cannot fail job for key %d: %s", payload.JobKey, err)
	}

	return &bindings.InvokeResponse{}, nil
}
