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
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
)

const (
	// errors
	missingJobTypeErrorMsg           = "jobType is a required attribute"
	missingMaxJobsToActivateErrorMsg = "maxJobsToActivate is a required attribute"
)

type activateJobsPayload struct {
	JobType           string            `json:"jobType"`
	MaxJobsToActivate *int32            `json:"maxJobsToActivate"`
	Timeout           metadata.Duration `json:"timeout"`
	WorkerName        string            `json:"workerName"`
	FetchVariables    []string          `json:"fetchVariables"`
}

func (z *ZeebeCommand) activateJobs(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var payload activateJobsPayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, err
	}

	if payload.JobType == "" {
		return nil, errors.New(missingJobTypeErrorMsg)
	}

	if payload.MaxJobsToActivate == nil {
		return nil, errors.New(missingMaxJobsToActivateErrorMsg)
	}

	cmd := z.client.NewActivateJobsCommand().
		JobType(payload.JobType).
		MaxJobsToActivate(*payload.MaxJobsToActivate)

	if payload.Timeout.Duration != time.Duration(0) {
		cmd = cmd.Timeout(payload.Timeout.Duration)
	}

	if payload.WorkerName != "" {
		cmd = cmd.WorkerName(payload.WorkerName)
	}

	if payload.FetchVariables != nil {
		cmd = cmd.FetchVariables(payload.FetchVariables...)
	}

	ctx := context.Background()
	response, err := cmd.Send(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot activate jobs for type %s: %s", payload.JobType, err)
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal response to json: %s", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}
