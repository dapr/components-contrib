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
	metadata map[string]string
}

func NewTestConfig(name string, allOperations bool, operations []string, config map[string]string) TestConfig {
	return TestConfig{
		CommonConfig: utils.CommonConfig{
			ComponentType: "output-binding",
			ComponentName: name,
			AllOperations: allOperations,
			Operations:    sets.NewString(operations...),
		},
		metadata: config,
	}
}

func (tc *TestConfig) createInvokeRequest() bindings.InvokeRequest {
	// There is a possiblity that the metadata map might be modified by the Invoke function(eg: azure blobstorage).
	// So we are making a copy of the config metadata map and setting the Metadata field before each request
	return bindings.InvokeRequest{
		Data:     []byte("Test Data"),
		Metadata: tc.CopyMap(tc.metadata),
	}
}

func ConformanceTests(t *testing.T, props map[string]string, binding bindings.OutputBinding, config TestConfig) {
	// Init
	t.Run("init", func(t *testing.T) {
		err := binding.Init(bindings.Metadata{
			Properties: props,
		})
		assert.NoError(t, err, "expected no error setting up binding")
	})

	// Operations
	if config.HasOperation("operations") {
		t.Run("operations", func(t *testing.T) {
			ops := binding.Operations()
			for _, op := range ops {
				assert.True(t, config.HasOperation(string(op)), fmt.Sprintf("Operation missing from conformance test config: %v", op))
			}
		})
	}

	// CreateOperations
	createPerformed := false
	// Order matters here, we use the result of the create in other validations.
	if config.HasOperation(string(bindings.CreateOperation)) {
		t.Run("create", func(t *testing.T) {
			req := config.createInvokeRequest()
			req.Operation = bindings.CreateOperation
			_, err := binding.Invoke(&req)
			assert.Nil(t, err, "expected no error invoking output binding")
			createPerformed = true
		})
	}

	// GetOperation
	if config.HasOperation(string(bindings.GetOperation)) {
		t.Run("get", func(t *testing.T) {
			req := config.createInvokeRequest()
			req.Operation = bindings.GetOperation
			resp, err := binding.Invoke(&req)
			assert.Nil(t, err, "expected no error invoking output binding")
			if createPerformed {
				assert.Equal(t, req.Data, resp.Data)
			}
		})
	}

	// ListOperation
	if config.HasOperation(string(bindings.ListOperation)) {
		t.Run("list", func(t *testing.T) {
			req := config.createInvokeRequest()
			req.Operation = bindings.GetOperation
			_, err := binding.Invoke(&req)
			assert.Nil(t, err, "expected no error invoking output binding")
		})
	}

	// DeleteOperation
	if config.HasOperation(string(bindings.DeleteOperation)) {
		t.Run("delete", func(t *testing.T) {
			req := config.createInvokeRequest()
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
