//go:build e2etests
// +build e2etests

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
	"testing"

	"github.com/camunda/zeebe/clients/go/v8/pkg/pb"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/zeebe/command"
	"github.com/dapr/components-contrib/tests/e2e/bindings/zeebe"
	"github.com/stretchr/testify/assert"
)

func TestPublishMessage(t *testing.T) {
	t.Parallel()

	cmd, err := zeebe.Command()
	assert.NoError(t, err)

	t.Run("publish a message only with message name", func(t *testing.T) {
		t.Parallel()

		data, err := json.Marshal(map[string]interface{}{
			"messageName": "some-message",
		})
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: command.PublishMessageOperation}
		res, err := cmd.Invoke(context.Background(), req)
		assert.NoError(t, err)

		variableResponse := &pb.PublishMessageResponse{}
		err = json.Unmarshal(res.Data, variableResponse)
		assert.NoError(t, err)
		assert.NotEqual(t, 0, variableResponse.Key)
		assert.Nil(t, res.Metadata)
	})

	t.Run("publish a message with a correlation ID", func(t *testing.T) {
		t.Parallel()

		data, err := json.Marshal(map[string]interface{}{
			"messageName":    "some-message",
			"correlationKey": "some-correlation-key",
		})
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: command.PublishMessageOperation}
		res, err := cmd.Invoke(context.Background(), req)
		assert.NoError(t, err)

		variableResponse := &pb.PublishMessageResponse{}
		err = json.Unmarshal(res.Data, variableResponse)
		assert.NoError(t, err)
		assert.NotEqual(t, 0, variableResponse.Key)
		assert.Nil(t, res.Metadata)
	})

	t.Run("publish a message with a time to live", func(t *testing.T) {
		t.Parallel()

		data, err := json.Marshal(map[string]interface{}{
			"messageName": "some-message",
			"timeToLive":  "5m",
		})
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: command.PublishMessageOperation}
		res, err := cmd.Invoke(context.Background(), req)
		assert.NoError(t, err)

		variableResponse := &pb.PublishMessageResponse{}
		err = json.Unmarshal(res.Data, variableResponse)
		assert.NoError(t, err)
		assert.NotEqual(t, 0, variableResponse.Key)
		assert.Nil(t, res.Metadata)
	})

	t.Run("publish a message with variables", func(t *testing.T) {
		t.Parallel()

		data, err := json.Marshal(map[string]interface{}{
			"messageName": "some-message",
			"variables": map[string]interface{}{
				"foo": "bar",
			},
		})
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: command.PublishMessageOperation}
		res, err := cmd.Invoke(context.Background(), req)
		assert.NoError(t, err)

		variableResponse := &pb.PublishMessageResponse{}
		err = json.Unmarshal(res.Data, variableResponse)
		assert.NoError(t, err)
		assert.NotEqual(t, 0, variableResponse.Key)
		assert.Nil(t, res.Metadata)
	})

	t.Run("publish a message with all params", func(t *testing.T) {
		t.Parallel()

		data, err := json.Marshal(map[string]interface{}{
			"messageName":    "some-message",
			"correlationKey": "some-correlation-key",
			"timeToLive":     "5m",
			"variables": map[string]interface{}{
				"foo": "bar",
			},
		})
		assert.NoError(t, err)

		req := &bindings.InvokeRequest{Data: data, Operation: command.PublishMessageOperation}
		res, err := cmd.Invoke(context.Background(), req)
		assert.NoError(t, err)

		variableResponse := &pb.PublishMessageResponse{}
		err = json.Unmarshal(res.Data, variableResponse)
		assert.NoError(t, err)
		assert.NotEqual(t, 0, variableResponse.Key)
		assert.Nil(t, res.Metadata)
	})
}
