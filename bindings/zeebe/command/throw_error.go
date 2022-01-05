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
