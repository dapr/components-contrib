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
	// metadata.
	fileName = "fileName"
)

var ErrMissingFileName = errors.New("fileName is a required attribute")

func (z *ZeebeCommand) deployProcess(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var deployFileName string

	if val, ok := req.Metadata[fileName]; ok && val != "" {
		deployFileName = val
	} else {
		return nil, ErrMissingFileName
	}

	response, err := z.client.NewDeployProcessCommand().
		AddResource(req.Data, deployFileName).
		Send(context.Background())
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
