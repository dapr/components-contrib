// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package keyvault_test

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	// SecretStores
	"github.com/dapr/components-contrib/secretstores"
	akv "github.com/dapr/components-contrib/secretstores/azure/keyvault"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"
	"github.com/dapr/dapr/pkg/runtime"
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

	log := logger.NewLogger("dapr.components")

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
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components/serviceprincipal"),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHttpPort),
			runtime.WithSecretStores(
				secretstores_loader.New("local.env", func() secretstores.SecretStore {
					return secretstore_env.NewEnvSecretStore(log)
				}),
				secretstores_loader.New("azure.keyvault", func() secretstores.SecretStore {
					return akv.NewAzureKeyvaultSecretStore(log)
				}),
			))).
		Step("Getting known secret", testGetKnownSecret)
		// Run()

	// Currently port reuse is still not quite working in the Dapr runtime.
	ports, err = dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)
	currentGrpcPort = ports[0]
	currentHttpPort = ports[1]

	flow.New(t, "keyvault authentication using certificate").
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components/certificate"),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHttpPort),
			runtime.WithSecretStores(
				secretstores_loader.New("local.env", func() secretstores.SecretStore {
					return secretstore_env.NewEnvSecretStore(log)
				}),
				secretstores_loader.New("azure.keyvault", func() secretstores.SecretStore {
					return akv.NewAzureKeyvaultSecretStore(log)
				}),
			))).
		Step("Getting known secret", testGetKnownSecret, sidecar.Stop(sidecarName))
		// Run()

	// az acr create -g dapr2-conf-test-rg -n dapr2conftestacr --sku Standard

	msiFunc := func(ctx flow.Context) error {
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

		// generate random string
		rand.Seed(time.Now().UnixNano())
		containerName := fmt.Sprintf("managedidentitytest%d", rand.Intn(10000)+1)

		containerCreateCommand := fmt.Sprintf("az container create -g %s -n %s --image ubuntu:latest --command-line 'tail -f /dev/null' --memory 4 --cpu 2 --assign-identity %s", groupName, containerName, managedIdentityID)
		fmt.Printf("Running Command ==== : %s", containerCreateCommand)
		// err = exec.Command("bash", "-c", containerCreateCommand).Run()
		// if err != nil {
		// 	t.Error(fmt.Sprintf("container %s not created: %s", containerName, err))
		// }

		return nil
	}

	flow.New(t, "keyvault authentication using managed identity").
		Step("Run the thing", msiFunc).
		Run()
}
