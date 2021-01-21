package bindings

import (
	"fmt"
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/tests/conformance/utils"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/sets"
)

type TestConfig struct {
	utils.CommonConfig
}

func NewTestConfig(name string, allOperations bool, operations []string) TestConfig {
	return TestConfig{
		CommonConfig: utils.CommonConfig{
			ComponentType: "output-binding",
			ComponentName: name,
			AllOperations: allOperations,
			Operations:    sets.NewString(operations...),
		},
	}
}

func ConformanceTests(t *testing.T, props map[string]string, binding bindings.OutputBinding, config TestConfig) {
	if config.CommonConfig.HasOperation("init") {
		t.Run(config.GetTestName("init"), func(t *testing.T) {
			err := binding.Init(bindings.Metadata{
				Properties: props,
			})
			assert.NoError(t, err, "expected no error setting up binding")
		})
	}

	if config.CommonConfig.HasOperation("operations") {
		t.Run(config.GetTestName("operations"), func(t *testing.T) {
			ops := binding.Operations()
			for _, op := range ops {
				assert.True(t, config.HasOperation(string(op)), fmt.Sprintf("Operation missing from conformance test config: %v", op))
			}
		})
	}

	req := bindings.InvokeRequest{
		Data: []byte("Test Data"),
		Metadata: map[string]string{
			"key": "test-key",
		},
	}

	createPerformed := false
	// Order matters here, we use the result of the create in other validations.
	if config.HasOperation(string(bindings.CreateOperation)) {
		t.Run(config.GetTestName("create"), func(t *testing.T) {
			req.Operation = bindings.CreateOperation
			_, err := binding.Invoke(&req)
			assert.Nil(t, err, "expected no error invoking output binding")
			createPerformed = true
		})
	}

	if config.HasOperation(string(bindings.GetOperation)) {
		t.Run(config.GetTestName("get"), func(t *testing.T) {
			req.Operation = bindings.GetOperation
			resp, err := binding.Invoke(&req)
			assert.Nil(t, err, "expected no error invoking output binding")
			assert.NotNil(t, resp)

			if createPerformed {
				assert.Equal(t, req.Data, resp.Data)
			}
		})
	}

	if config.HasOperation(string(bindings.ListOperation)) {
		t.Run(config.GetTestName("list"), func(t *testing.T) {
			req.Operation = bindings.GetOperation
			_, err := binding.Invoke(&req)
			assert.Nil(t, err, "expected no error invoking output binding")
		})
	}

	if config.HasOperation(string(bindings.DeleteOperation)) {
		t.Run(config.GetTestName("delete"), func(t *testing.T) {
			req.Operation = bindings.DeleteOperation
			_, err := binding.Invoke(&req)
			assert.Nil(t, err, "expected no error invoking output binding")

			if createPerformed && config.HasOperation(string(bindings.GetOperation)) {
				req.Operation = bindings.GetOperation
				resp, err := binding.Invoke(&req)
				assert.Nil(t, err, "expected no error invoking output binding")
				assert.NotNil(t, resp)
				assert.Nil(t, resp.Data)
			}
		})
	}
}
