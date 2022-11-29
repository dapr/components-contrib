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

package vault_test

import (
	"fmt"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/go-sdk/client"
	"github.com/stretchr/testify/assert"
)

//
// Aux. functions for testing key presence
//

func testKeyValuesInSecret(currentGrpcPort int, secretStoreName string, secretName string, keyValueMap map[string]string, maybeVersionID ...string) flow.Runnable {
	return func(ctx flow.Context) error {
		daprClient, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer daprClient.Close()

		metadata := map[string]string{}
		if len(maybeVersionID) > 0 {
			metadata["version_id"] = maybeVersionID[0]
		}

		res, err := daprClient.GetSecret(ctx, secretStoreName, secretName, metadata)
		assert.NoError(ctx.T, err)
		assert.NotNil(ctx.T, res)

		for key, valueExpected := range keyValueMap {
			valueInSecret, exists := res[key]
			assert.True(ctx.T, exists, "expected key not found in key")
			assert.Equal(ctx.T, valueExpected, valueInSecret)
		}
		return nil
	}
}

func testSecretIsNotFound(currentGrpcPort int, secretStoreName string, secretName string) flow.Runnable {
	return func(ctx flow.Context) error {
		daprClient, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer daprClient.Close()

		emptyOpt := map[string]string{}

		_, err = daprClient.GetSecret(ctx, secretStoreName, secretName, emptyOpt)
		assert.Error(ctx.T, err)

		return nil
	}
}

func testDefaultSecretIsFound(currentGrpcPort int, secretStoreName string) flow.Runnable {
	return testKeyValuesInSecret(currentGrpcPort, secretStoreName, "multiplekeyvaluessecret", map[string]string{
		"first":  "1",
		"second": "2",
		"third":  "3",
	})
}

func testComponentIsNotWorking(targetComponentName string, currentGrpcPort int) flow.Runnable {
	return testSecretIsNotFound(currentGrpcPort, targetComponentName, "multiplekeyvaluessecret")
}

func testGetBulkSecretsWorksAndFoundKeys(currentGrpcPort int, secretStoreName string) flow.Runnable {
	return func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		emptyOpt := map[string]string{}

		res, err := client.GetBulkSecret(ctx, secretStoreName, emptyOpt)
		assert.NoError(ctx.T, err)
		assert.NotNil(ctx.T, res)
		assert.NotEmpty(ctx.T, res)

		for k, v := range res {
			ctx.Logf("💡 %s", k)
			for i, j := range v {
				ctx.Logf("💡\t %s : %s", i, j)
			}
		}

		return nil
	}
}
