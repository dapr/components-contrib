// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package command

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dapr/components-contrib/bindings"
)

func (z *ZeebeCommand) topology() (*bindings.InvokeResponse, error) {
	response, err := z.client.NewTopologyCommand().Send(context.Background())
	if err != nil {
		return nil, fmt.Errorf("cannot get zeebe toplogy: %s", err)
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal response to json: %s", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}
