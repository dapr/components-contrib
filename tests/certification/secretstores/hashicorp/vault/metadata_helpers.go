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
	"github.com/dapr/dapr/pkg/proto/runtime/v1"
	"github.com/dapr/go-sdk/client"
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

	resp, err := daprClient.GrpcClient().GetMetadata(clientCtx, new(runtime.GetMetadataRequest))
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
