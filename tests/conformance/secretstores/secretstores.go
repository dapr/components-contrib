package secretstores

import (
	"os"
	"testing"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/components-contrib/tests/conformance/utils"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/sets"
)

// creating this struct so that it can be expanded later
type TestConfig struct {
	utils.CommonConfig
}

func NewTestConfig(name string, allOperations bool, operations []string) TestConfig {
	tc := TestConfig{
		CommonConfig: utils.CommonConfig{
			ComponentType: "secretstores",
			ComponentName: name,
			AllOperations: allOperations,
			Operations:    sets.NewString(operations...),
		},
	}

	return tc
}

func ConformanceTests(t *testing.T, props map[string]string, store secretstores.SecretStore, config TestConfig) {
	// TODO add support for metadata
	// For local env var based component test
	os.Setenv("conftestsecret", "abcd")
	defer os.Unsetenv("conftestsecret")
	if config.HasOperation("init") {
		t.Run(config.GetTestName("init"), func(t *testing.T) {
			err := store.Init(secretstores.Metadata{
				Properties: props,
			})
			assert.NoError(t, err, "expected no error on initializing store")
		})
	}
	if config.HasOperation("get") {
		getSecretRequest := secretstores.GetSecretRequest{
			Name: "conftestsecret",
		}
		getSecretResponse := secretstores.GetSecretResponse{
			Data: map[string]string{
				"conftestsecret": "abcd",
			},
		}

		t.Run("secretstores/"+config.ComponentName+"/get", func(t *testing.T) {
			resp, err := store.GetSecret(getSecretRequest)
			assert.NoError(t, err, "expected no error on getting secret %v", getSecretRequest)
			assert.NotNil(t, resp, "expected value to be returned")
			assert.NotNil(t, resp.Data, "expected value to be returned")
			assert.Equal(t, getSecretResponse.Data, resp.Data, "expected values to be equal")
		})
	}

	if config.HasOperation("bulkget") {
		bulkReq := secretstores.BulkGetSecretRequest{}
		bulkResponse := secretstores.GetSecretResponse{
			Data: map[string]string{
				"conftestsecret": "abcd",
				"secondsecret":   "efgh",
			},
		}

		t.Run(config.GetTestName("bulkget"), func(t *testing.T) {
			resp, err := store.BulkGetSecret(bulkReq)
			assert.NoError(t, err, "expected no error on getting secret %v", bulkReq)
			assert.NotNil(t, resp, "expected value to be returned")
			assert.NotNil(t, resp.Data, "expected value to be returned")
			assert.Equal(t, bulkResponse.Data, resp.Data, "expected values to be equal")
		})
	}
}
