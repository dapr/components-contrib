// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package command

import (
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

func TestDeployProcess(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("fileName is mandatory", func(t *testing.T) {
		message := ZeebeCommand{logger: testLogger}
		req := &bindings.InvokeRequest{Operation: deployProcessOperation}
		_, err := message.Invoke(req)
		assert.Error(t, err, ErrMissingFileName)
	})
}
