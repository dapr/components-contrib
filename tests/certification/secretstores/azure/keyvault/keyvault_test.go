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

package keyvault_test

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	// SecretStores

	akv "github.com/dapr/components-contrib/secretstores/azure/keyvault"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/go-sdk/client"
)

const (
	sidecarName = "keyvault-sidecar"
)

func TestKeyVault(t *testing.T) {
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort := ports[0]
	currentHttpPort := ports[1]

	testGetKnownSecret := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		opt := map[string]string{
			"version": "2",
		}

		// This test reuses the Azure conformance test resources created using
		// .github/infrastructure/conformance/azure/setup-azure-conf-test.sh,
		// so it reuses the tests/conformance/secretstores/secretstores.go test secrets.
		res, err := client.GetSecret(ctx, "azurekeyvault", "secondsecret", opt)
		assert.NoError(t, err)
		assert.Equal(t, "efgh", res["secondsecret"])
		return nil
	}

	flow.New(t, "keyvault authentication using service principal").
		Step(sidecar.Run(sidecarName,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithComponentsPath("./components/serviceprincipal"),
				embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(currentHttpPort)),
			)...,
		)).
		Step("Getting known secret", testGetKnownSecret).
		Run()

	// Currently port reuse is still not quite working in the Dapr runtime.
	ports, err = dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)
	currentGrpcPort = ports[0]
	currentHttpPort = ports[1]

	flow.New(t, "keyvault authentication using certificate").
		Step(sidecar.Run(sidecarName,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithComponentsPath("./components/certificate"),
				embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(currentHttpPort)),
			)...,
		)).
		Step("Getting known secret", testGetKnownSecret, sidecar.Stop(sidecarName)).
		Run()

	// This test reuses the Azure conformance test resources created using
	// .github/infrastructure/conformance/azure/setup-azure-conf-test.sh
	managedIdentityTest := func(ctx flow.Context) error {
		_, err := exec.LookPath("az")
		if err != nil {
			t.Error("azure-cli not installed")
		}
		groupName, envVarFound := os.LookupEnv("AzureResourceGroupName")
		if envVarFound == false {
			t.Error("AzureResourceGroupName environment variable not set")
		}

		managedIdentityQuery := fmt.Sprintf("az identity show -g %s -n azure-managed-identity -otsv --query id", groupName)
		managedIdentityID, err := exec.Command("bash", "-c", managedIdentityQuery).Output()
		if err != nil {
			t.Error("azure-managed-identity not found")
		}

		registryName, envVarFound := os.LookupEnv("AzureContainerRegistryName")
		if envVarFound == false {
			t.Error("AzureContainerRegistryName environment variable not set")
		}
		keyvaultName, envVarFound := os.LookupEnv("AzureKeyVaultName")
		if envVarFound == false {
			t.Error("AzureKeyVaultName environment variable not set")
		}

		// build and publish the image
		buildContainerCommand := fmt.Sprintf("az acr build --image certification/keyvault:latest --registry %s --file managed-identity-app/Dockerfile ./managed-identity-app", registryName)
		fmt.Printf("Building Container ==== : %s\n", buildContainerCommand)
		err = exec.Command("bash", "-c", buildContainerCommand).Run()
		if err != nil {
			t.Error("failed to build and publish container")
		}

		// generate random string
		rand.Seed(time.Now().UnixNano())
		containerName := fmt.Sprintf("managedidentitytest%d", rand.Intn(10000)+1)

		// run the container using Azure Container Instances with injected managed identity
		registryPasswordSubcommand := fmt.Sprintf("az acr credential show -n %s --query passwords[0].value -otsv", registryName)
		registryUsernameSubcommand := fmt.Sprintf("az acr credential show -n %s --query username -otsv", registryName)
		containerCreateCommand := fmt.Sprintf("az container create -g %s -n %s --image %s.azurecr.io/certification/keyvault:latest --restart-policy never --environment-variables AzureKeyVaultName=%s --registry-password $(%s) --registry-username $(%s) --assign-identity %s", groupName, containerName, registryName, keyvaultName, registryPasswordSubcommand, registryUsernameSubcommand, managedIdentityID)
		fmt.Printf("Running Container in Azure Container Instances ==== : %s\n", containerCreateCommand)
		err = exec.Command("bash", "-c", containerCreateCommand).Run()
		if err != nil {
			t.Error(fmt.Sprintf("container %s not created: %s", containerName, err))
		}

		// read the container logs
		logsCommand := fmt.Sprintf("az container logs -g %s -n %s", groupName, containerName)
		fmt.Printf("Reading Container Logs ==== : %s\n", logsCommand)
		output, err := exec.Command("bash", "-c", logsCommand).Output()
		if err != nil {
			t.Error(fmt.Sprintf("failed to get logs for container %s: %s", containerName, err))
		}
		expectedOuput := `{"secondsecret":"efgh"}`
		outputFound := true
		// if string contains "efgh" then the secret was successfully read
		if !strings.Contains(string(output), expectedOuput) {
			// in the unlikely event the operation has not completed yet we will check the logs one more time in 60 seconds
			time.Sleep(time.Second * 60)
			output, err = exec.Command("bash", "-c", logsCommand).Output()
			if err != nil {
				t.Error(fmt.Sprintf("failed to get logs for container %s: %s", containerName, err))
			}
			if !strings.Contains(string(output), expectedOuput) {
				outputFound = false
			}
		}
		// shut down the container
		shutdownCommand := fmt.Sprintf("az container delete -g %s -n %s --yes", groupName, containerName)
		fmt.Printf("Shutting Down Container ==== : %s\n", shutdownCommand)
		err = exec.Command("bash", "-c", shutdownCommand).Run()
		if err != nil {
			t.Error(fmt.Sprintf("failed to shutdown container %s: %s", containerName, err))
		}

		if outputFound == false {
			assert.Fail(t, fmt.Sprintf("failed to read secret secondsecret from KeyVault %s using managed identity %s in container %s: %s", keyvaultName, managedIdentityID, containerName, err))
		}
		return nil
	}

	flow.New(t, "keyvault authentication using managed identity").
		Step("Test secret access using managed identity authentication", managedIdentityTest)
	// temporarily disable the managed identity test until we decide whether to remove this test or find a different way to spin up the required environment.
	// Run().
}

func componentRuntimeOptions() []embedded.Option {
	log := logger.NewLogger("dapr.components")

	secretstoreRegistry := secretstores_loader.NewRegistry()
	secretstoreRegistry.Logger = log
	secretstoreRegistry.RegisterComponent(akv.NewAzureKeyvaultSecretStore, "azure.keyvault")
	secretstoreRegistry.RegisterComponent(secretstore_env.NewEnvSecretStore, "local.env")

	return []embedded.Option{
		embedded.WithSecretStores(secretstoreRegistry),
	}
}
