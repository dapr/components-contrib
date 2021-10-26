// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package keyvault_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	// SecretStores
	"github.com/dapr/components-contrib/secretstores"
	akv "github.com/dapr/components-contrib/secretstores/azure/keyvault"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"
	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/network"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/go-sdk/client"
)

const (
	sidecarName = "keyvault-sidecar"
)

func TestKeyVault(t *testing.T) {
	currentGrpcPort := runtime.DefaultDaprAPIGRPCPort
	currentHttpPort := runtime.DefaultDaprHTTPPort

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

		// See .github/infrastructure/conformance/azure/setup-azure-conf-test.sh
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
		Step("Interrupting network traffic for 10 seconds", network.InterruptNetwork(10*time.Second, nil, nil, "80", "443")).
		Step("Getting known secret", testGetKnownSecret).
		Run()

	// Currently port reuse is still not quite working in the Dapr runtime.
	currentGrpcPort++
	currentHttpPort++
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
		Step("Interrupting network traffic for 10 seconds", network.InterruptNetwork(10*time.Second, nil, nil, "80", "443")).
		Step("Getting known secret", testGetKnownSecret, sidecar.Stop(sidecarName)).
		Run()
}
