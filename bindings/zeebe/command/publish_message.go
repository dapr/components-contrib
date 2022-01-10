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

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
)

var ErrMissingMessageName = errors.New("messageName is a required attribute")

type publishMessagePayload struct {
	MessageName    string            `json:"messageName"`
	CorrelationKey string            `json:"correlationKey"`
	MessageID      string            `json:"messageId"`
	TimeToLive     metadata.Duration `json:"timeToLive"`
	Variables      interface{}       `json:"variables"`
}

func (z *ZeebeCommand) publishMessage(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var payload publishMessagePayload
	err := json.Unmarshal(req.Data, &payload)
	if err != nil {
		return nil, err
	}

	if payload.MessageName == "" {
		return nil, ErrMissingMessageName
	}

	cmd := z.client.NewPublishMessageCommand().
		MessageName(payload.MessageName).
		CorrelationKey(payload.CorrelationKey)

	if payload.MessageID != "" {
		cmd = cmd.MessageId(payload.MessageID)
	}

	if payload.TimeToLive.Duration != time.Duration(0) {
		cmd = cmd.TimeToLive(payload.TimeToLive.Duration)
	}

	if payload.Variables != nil {
		cmd, err = cmd.VariablesFromObject(payload.Variables)
		if err != nil {
			return nil, err
		}
	}

	ctx := context.Background()
	response, err := cmd.Send(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot publish message with name %s: %w", payload.MessageName, err)
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal response to json: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: jsonResponse,
	}, nil
}
