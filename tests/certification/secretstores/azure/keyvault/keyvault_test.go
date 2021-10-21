// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package keyvault_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	// SecretStores.
	"github.com/dapr/components-contrib/secretstores"
	akv "github.com/dapr/components-contrib/secretstores/azure/keyvault"
	secretstore_loader "github.com/dapr/dapr/pkg/components/secretstores"
	"github.com/dapr/dapr/pkg/runtime"
	"github.com/dapr/kit/logger"

	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/go-sdk/client"
)

const (
	sidecarName = "dapr-1"
)

func TestKeyvault(t *testing.T) {
	log := logger.NewLogger("dapr.components")

	testSecretsAPI := func(ctx flow.Context) error {
		client, err := client.NewClient()
		if err != nil {
			panic(err)
		}
		defer client.Close()

		opt := map[string]string{
			"version": "2",
		}

		_, err = client.GetSecret(ctx, "azurekeyvault-service-principal", "secondsecret", opt)
		assert.NoError(t, err)

		return nil
	}

	flow.New(t, "keyvault certification").
		// Step(app.Run(applicationID, fmt.Sprintf(":%d", appPort), application)).
		Step(sidecar.Run(sidecarName,
			// embedded.WithAppProtocol(runtime.HTTPProtocol, appPort),
			embedded.WithDaprGRPCPort(runtime.DefaultDaprAPIGRPCPort),
			embedded.WithDaprHTTPPort(runtime.DefaultDaprHTTPPort),
			runtime.WithSecretStores(
				secretstore_loader.New("azure.keyvault", func() secretstores.SecretStore {
					return akv.NewAzureKeyvaultSecretStore(log)
				}),
			))).
		Step("send and wait", testSecretsAPI).
		Run()
}
