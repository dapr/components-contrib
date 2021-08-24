// +build e2etests

// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package command

import (
	"encoding/json"
	"testing"

	"github.com/camunda-cloud/zeebe/clients/go/pkg/pb"
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
		res, err := cmd.Invoke(req)
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
		res, err := cmd.Invoke(req)
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
		res, err := cmd.Invoke(req)
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
		res, err := cmd.Invoke(req)
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
		res, err := cmd.Invoke(req)
		assert.NoError(t, err)

		variableResponse := &pb.PublishMessageResponse{}
		err = json.Unmarshal(res.Data, variableResponse)
		assert.NoError(t, err)
		assert.NotEqual(t, 0, variableResponse.Key)
		assert.Nil(t, res.Metadata)
	})
}
