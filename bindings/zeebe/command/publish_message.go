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
	"strings"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/metadata"
)

var ErrMissingMessageName = errors.New("messageName is a required attribute")

type publishMessagePayload struct {
	MessageName    string             `json:"messageName"`
	CorrelationKey string             `json:"correlationKey"`
	MessageID      string             `json:"messageId"`
	TimeToLive     *metadata.Duration `json:"timeToLive,omitempty"`
	Variables      interface{}        `json:"variables"`
}

// UnmarshalJSON implements json.Unmarshaler to provide field-specific error messages for
// duration fields. The timeToLive field accepts either a Go duration string (e.g. "30s", "5m", "1h30m")
// or a plain integer representing nanoseconds.
func (p *publishMessagePayload) UnmarshalJSON(data []byte) error {
	// Use a shadow struct with raw JSON for duration fields so we can provide better errors.
	var shadow struct {
		MessageName    string           `json:"messageName"`
		CorrelationKey string           `json:"correlationKey"`
		MessageID      string           `json:"messageId"`
		TimeToLive     *json.RawMessage `json:"timeToLive,omitempty"`
		Variables      interface{}      `json:"variables"`
	}
	if err := json.Unmarshal(data, &shadow); err != nil {
		return err
	}

	p.MessageName = shadow.MessageName
	p.CorrelationKey = shadow.CorrelationKey
	p.MessageID = shadow.MessageID
	p.Variables = shadow.Variables

	if shadow.TimeToLive != nil {
		var d metadata.Duration
		if err := d.UnmarshalJSON(*shadow.TimeToLive); err != nil {
			rawVal := strings.Trim(string(*shadow.TimeToLive), "\"")
			return fmt.Errorf("invalid value %q for field 'timeToLive' (expected a Go duration string, e.g. \"30s\", \"5m\", \"1h30m\", or a plain integer nanoseconds value): %w", rawVal, err)
		}
		p.TimeToLive = &d
	}

	return nil
}

func (z *ZeebeCommand) publishMessage(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
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

	if payload.TimeToLive != nil && payload.TimeToLive.Duration != time.Duration(0) {
		cmd = cmd.TimeToLive(payload.TimeToLive.Duration)
	}

	if payload.Variables != nil {
		cmd, err = cmd.VariablesFromObject(payload.Variables)
		if err != nil {
			return nil, err
		}
	}

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
