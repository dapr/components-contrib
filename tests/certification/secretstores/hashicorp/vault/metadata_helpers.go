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
	"context"
	"fmt"

	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/go-sdk/client"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/assert"
)

//
// Helper methods for checking component registration and availability of its features
//

func testComponentFound(targetComponentName string, currentGrpcPort int) flow.Runnable {
	return func(ctx flow.Context) error {
		componentFound, _ := getComponentCapabilities(ctx, currentGrpcPort, targetComponentName)
		assert.True(ctx.T, componentFound, "Component was expected to be found but it was missing.")
		return nil
	}
}

// Due to https://github.com/dapr/dapr/issues/5487 we cannot perform negative tests
// for the component presence against the metadata registry.
// Instead, we turned testComponentNotFound into a simpler negative test that ensures a good key cannot be found
func testComponentNotFound(targetComponentName string, currentGrpcPort int) flow.Runnable {
	// TODO(tmacam) once https://github.com/dapr/dapr/issues/5487 is fixed, uncomment the code bellow
	return testSecretIsNotFound(currentGrpcPort, targetComponentName, "multiplekeyvaluessecret")

	//return func(ctx flow.Context) error {
	//	// Find the component
	//	componentFound, _ := getComponentCapabilities(ctx, currentGrpcPort, targetComponentName)
	//	assert.False(ctx.T, componentFound, "Component was expected to be missing but it was found.")
	//	return nil
	//}
}

func testComponentDoesNotHaveFeature(currentGrpcPort int, targetComponentName string, targetCapability secretstores.Feature) flow.Runnable {
	return testComponentAndFeaturePresence(currentGrpcPort, targetComponentName, targetCapability, false)
}

func testComponentHasFeature(currentGrpcPort int, targetComponentName string, targetCapability secretstores.Feature) flow.Runnable {
	return testComponentAndFeaturePresence(currentGrpcPort, targetComponentName, targetCapability, true)
}

func testComponentAndFeaturePresence(currentGrpcPort int, targetComponentName string, targetCapability secretstores.Feature, expectedToBeFound bool) flow.Runnable {
	return func(ctx flow.Context) error {
		componentFound, capabilities := getComponentCapabilities(ctx, currentGrpcPort, targetComponentName)

		assert.True(ctx.T, componentFound, "Component was expected to be found but it was missing.")

		targetCapabilityAsString := string(targetCapability)
		// Find capability
		capabilityFound := false
		for _, cap := range capabilities {
			if cap == targetCapabilityAsString {
				capabilityFound = true
				break
			}
		}
		assert.Equal(ctx.T, expectedToBeFound, capabilityFound)

		return nil
	}
}

func getComponentCapabilities(ctx flow.Context, currentGrpcPort int, targetComponentName string) (found bool, capabilities []string) {
	daprClient, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
	if err != nil {
		panic(err)
	}
	defer daprClient.Close()

	clientCtx := context.Background()

	resp, err := daprClient.GrpcClient().GetMetadata(clientCtx, &empty.Empty{})
	assert.NoError(ctx.T, err)
	assert.NotNil(ctx.T, resp)
	assert.NotNil(ctx.T, resp.GetRegisteredComponents())

	// Find the component
	for _, component := range resp.GetRegisteredComponents() {
		if component.GetName() == targetComponentName {
			ctx.Logf("component found=%s", component)
			return true, component.GetCapabilities()
		}
	}
	return false, []string{}
}
