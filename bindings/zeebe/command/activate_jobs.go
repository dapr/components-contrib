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
	"time"

	"github.com/camunda/zeebe/clients/go/v8/pkg/entities"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/metadata"
)

var (
	ErrMissingJobType           = errors.New("jobType is a required attribute")
	ErrMissingMaxJobsToActivate = errors.New("maxJobsToActivate is a required attribute")
)

type activateJobsPayload struct {
	JobType           string             `json:"jobType"`
	MaxJobsToActivate *int32             `json:"maxJobsToActivate"`
	Timeout           *metadata.Duration `json:"timeout,omitempty"`
	WorkerName        string             `json:"workerName"`
	FetchVariables    []string           `json:"fetchVariables"`
	RequestTimeout    *metadata.Duration `json:"requestTimeout,omitempty"`
}

// UnmarshalJSON implements json.Unmarshaler to provide field-specific error messages for
// duration fields. Both timeout and requestTimeout accept either a Go duration string
// (e.g. "30s", "5m", "1h30m") or a plain integer representing nanoseconds.
func (p *activateJobsPayload) UnmarshalJSON(data []byte) error {
	var shadow struct {
		JobType           string           `json:"jobType"`
		MaxJobsToActivate *int32           `json:"maxJobsToActivate"`
		Timeout           *json.RawMessage `json:"timeout,omitempty"`
		WorkerName        string           `json:"workerName"`
		FetchVariables    []string         `json:"fetchVariables"`
		RequestTimeout    *json.RawMessage `json:"requestTimeout,omitempty"`
	}
	if err := json.Unmarshal(data, &shadow); err != nil {
		return err
	}

	p.JobType = shadow.JobType
	p.MaxJobsToActivate = shadow.MaxJobsToActivate
	p.WorkerName = shadow.WorkerName
	p.FetchVariables = shadow.FetchVariables

	if shadow.Timeout != nil {
		var d metadata.Duration
		if err := d.UnmarshalJSON(*shadow.Timeout); err != nil {
			return fmt.Errorf("invalid value %s for field 'timeout' (expected a Go duration string, e.g. \"30s\", \"5m\", \"1h30m\", or a plain integer nanoseconds value): %w", string(*shadow.Timeout), err)
		}
		p.Timeout = &d
	}

	if shadow.RequestTimeout != nil {
		var d metadata.Duration
		if err := d.UnmarshalJSON(*shadow.RequestTimeout); err != nil {
			return fmt.Errorf("invalid value %s for field 'requestTimeout' (expected a Go duration string, e.g. \"30s\", \"5m\", \"1h30m\", or a plain integer nanoseconds value): %w", string(*shadow.RequestTimeout), err)
		}
		p.RequestTimeout = &d
	}

	return nil
}

func (z *ZeebeCommand) activateJobs(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var payload activateJobsPayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, err
	}

	if payload.JobType == "" {
		return nil, ErrMissingJobType
	}

	if payload.MaxJobsToActivate == nil {
		return nil, ErrMissingMaxJobsToActivate
	}

	cmd := z.client.NewActivateJobsCommand().
		JobType(payload.JobType).
		MaxJobsToActivate(*payload.MaxJobsToActivate)

	if payload.Timeout != nil && payload.Timeout.Duration != time.Duration(0) {
		cmd = cmd.Timeout(payload.Timeout.Duration)
	}

	if payload.WorkerName != "" {
		cmd = cmd.WorkerName(payload.WorkerName)
	}

	if payload.FetchVariables != nil {
		cmd = cmd.FetchVariables(payload.FetchVariables...)
	}

	var response []entities.Job
	if payload.RequestTimeout != nil && payload.RequestTimeout.Duration != time.Duration(0) {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, payload.RequestTimeout.Duration)
		defer cancel()
		response, err = cmd.Send(ctxWithTimeout)
	} else {
		response, err = cmd.Send(ctx)
	}

	if err != nil {
		return nil, fmt.Errorf("cannot activate jobs for type %s: %w", payload.JobType, err)
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal response to json: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}
