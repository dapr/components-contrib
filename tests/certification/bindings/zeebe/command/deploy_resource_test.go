/*
Copyright 2023 The Dapr Authors
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

package command_test

import (
	"strconv"
	"testing"
	"time"

	zeebe_test "github.com/dapr/components-contrib/tests/certification/bindings/zeebe"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/stretchr/testify/assert"
)

func TestDeployResourceOperation(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(2)
	grpcPort := ports[0]
	httpPort := ports[1]

	id := zeebe_test.TestID()

	testDeployBpmn := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		deployment, err := zeebe_test.DeployResource(
			client,
			ctx,
			zeebe_test.TestProcessFile,
			1,
			zeebe_test.IDModifier(id))

		assert.NoError(t, err)
		assert.Equal(t, id, deployment.Deployments[0].Metadata.Process.BpmnProcessId)
		assert.Equal(t, int32(1), deployment.Deployments[0].Metadata.Process.Version)
		assert.NotNil(t, deployment.Deployments[0].Metadata.Process.ProcessDefinitionKey)
		assert.Equal(t, zeebe_test.TestProcessFile, deployment.Deployments[0].Metadata.Process.ResourceName)

		return nil
	}

	testDeployDmn := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		deployment, err := zeebe_test.DeployResource(
			client,
			ctx,
			zeebe_test.TestDmnFile,
			2,
			zeebe_test.IDModifier(id))

		assert.NoError(t, err)
		assert.Equal(t, id, deployment.Deployments[0].Metadata.Decision.DmnDecisionId)
		assert.Equal(t, "Test", deployment.Deployments[0].Metadata.Decision.DmnDecisionName)
		assert.Equal(t, int32(1), deployment.Deployments[0].Metadata.Decision.Version)
		assert.NotNil(t, deployment.Deployments[0].Metadata.Decision.DecisionKey)
		assert.Equal(t, "Definitions_0c98xne", deployment.Deployments[0].Metadata.Decision.DmnDecisionRequirementsId)
		assert.NotNil(t, deployment.Deployments[0].Metadata.Decision.DecisionRequirementsKey)

		assert.Equal(t, "Definitions_0c98xne", deployment.Deployments[1].Metadata.DecisionRequirements.DmnDecisionRequirementsId)
		assert.NotNil(t, deployment.Deployments[1].Metadata.DecisionRequirements.DecisionRequirementsKey)
		assert.Equal(t, int32(1), deployment.Deployments[1].Metadata.DecisionRequirements.Version)
		assert.NotNil(t, deployment.Deployments[1].Metadata.DecisionRequirements.DecisionRequirementsKey)
		assert.Equal(t, zeebe_test.TestDmnFile, deployment.Deployments[1].Metadata.DecisionRequirements.ResourceName)

		return nil
	}

	testDeployBpmnWithNewVersion := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		deployment, err := zeebe_test.DeployResource(
			client,
			ctx,
			zeebe_test.TestProcessFile,
			1,
			zeebe_test.IDModifier(id),
			// changing the name results in a new version
			zeebe_test.NameModifier(id))

		assert.NoError(t, err)
		assert.Equal(t, id, deployment.Deployments[0].Metadata.Process.BpmnProcessId)
		assert.Equal(t, int32(2), deployment.Deployments[0].Metadata.Process.Version)
		assert.NotNil(t, deployment.Deployments[0].Metadata.Process.ProcessDefinitionKey)
		assert.Equal(t, zeebe_test.TestProcessFile, deployment.Deployments[0].Metadata.Process.ResourceName)

		return nil
	}

	flow.New(t, "Test deploy resource operation").
		Step(dockercompose.Run("zeebe", zeebe_test.DockerComposeYaml)).
		Step("Waiting for Zeebe Readiness...", retry.Do(time.Second*3, 10, zeebe_test.CheckZeebeConnection)).
		Step(sidecar.Run(zeebe_test.SidecarName,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithResourcesPath("components/standard"),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
			)...,
		)).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Deploy a BPMN resource", testDeployBpmn).
		Step("Deploy a DMN resource", testDeployDmn).
		Step("Deploy a BPMN with new version", testDeployBpmnWithNewVersion).
		Run()

	flow.New(t, "Test deploy resource operation with TLS").
		Step("Provide key and cert", zeebe_test.ProvideKeyAndCert("../certs")).
		Step(dockercompose.Run("zeebe", zeebe_test.DockerComposeTlsYaml)).
		Step("Waiting for Zeebe Readiness...", retry.Do(time.Second*3, 10, zeebe_test.CheckZeebeConnectionTls("../certs/cert.pem"))).
		Step(sidecar.Run(zeebe_test.SidecarName,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithResourcesPath("components/tlsEnabled"),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
			)...,
		)).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Deploy a BPMN resource", testDeployBpmn).
		Step("Deploy a DMN resource", testDeployDmn).
		Step("Deploy a BPMN with new version", testDeployBpmnWithNewVersion).
		Run()
}
