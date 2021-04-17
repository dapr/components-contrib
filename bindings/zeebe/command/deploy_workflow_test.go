// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package command

import (
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestDeployWorkflow(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("fileName is mandatory", func(t *testing.T) {
		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: deployWorkflowOperation}
		_, err := message.Invoke(req)
		assert.Error(t, err, ErrMissingFileName)
	})

	t.Run("return error if file type recognition doesn't work because of missing file extension", func(t *testing.T) {
		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: deployWorkflowOperation, Metadata: map[string]string{
			"fileName": "test",
		}}
		_, err := message.Invoke(req)
		assert.Error(t, err, ErrMissingFileType)
	})

	t.Run("return error if file type isn't supported", func(t *testing.T) {
		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: deployWorkflowOperation, Metadata: map[string]string{
			"fileName": "test.bpmn",
			"fileType": "unsupported",
		}}
		_, err := message.Invoke(req)
		assert.Error(t, err, ErrInvalidFileType)
	})
}
