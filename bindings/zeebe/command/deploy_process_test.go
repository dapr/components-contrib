// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package command

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestDeployProcess(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("fileName is mandatory", func(t *testing.T) {
		cmd := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: DeployProcessOperation}
		_, err := cmd.Invoke(req)
		assert.Error(t, err, ErrMissingFileName)
	})
}
