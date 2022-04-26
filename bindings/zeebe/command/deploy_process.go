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

const (
	// metadata.
	fileName = "fileName"
)

var ErrMissingFileName = errors.New("fileName is a required attribute")

func (z *ZeebeCommand) deployProcess(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var deployFileName string

	if val, ok := req.Metadata[fileName]; ok && val != "" {
		deployFileName = val
	} else {
		return nil, ErrMissingFileName
	}

	response, err := z.client.NewDeployProcessCommand().
		AddResource(req.Data, deployFileName).
		Send(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot deploy process with fileName %s: %w", deployFileName, err)
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal response to json: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}
