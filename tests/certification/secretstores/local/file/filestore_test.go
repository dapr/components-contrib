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

package filestore_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	// SecretStores

	secretstore_file "github.com/dapr/components-contrib/secretstores/local/file"
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

func TestEnv(t *testing.T) {
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

		res, err := client.GetSecret(ctx, "file-secret-store", "certtestsecret", nil)
		assert.NoError(t, err)
		assert.Equal(t, "abcd", res["certtestsecret"])

		res, err = client.GetSecret(ctx, "file-secret-store", "nestedsecret:secret", nil)
		assert.NoError(t, err)
		assert.Equal(t, "efgh", res["nestedsecret:secret"])
		return nil
	}

	testGetKnownSecretWithCustomSeparator := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		res, err := client.GetSecret(ctx, "file-secret-store", "certtestsecret", nil)
		assert.NoError(t, err)
		assert.Equal(t, "abcd", res["certtestsecret"])

		res, err = client.GetSecret(ctx, "file-secret-store", "nestedsecret::secret", nil)
		assert.NoError(t, err)
		assert.Equal(t, "efgh", res["nestedsecret::secret"])
		return nil
	}

	flow.New(t, "file secret store reads expected value").
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components/defaultnestedseparator/"),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHttpPort),
			componentRuntimeOptions(),
		)).
		Step("Getting known secret", testGetKnownSecret).
		Run()

	flow.New(t, "file secret store reads expected value with custom nested separator").
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components/customnestedseparator/"),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHttpPort),
			componentRuntimeOptions(),
		)).
		Step("Getting known secret", testGetKnownSecretWithCustomSeparator).
		Run()
}

func componentRuntimeOptions() []runtime.Option {
	log := logger.NewLogger("dapr.components")

	secretstoreRegistry := secretstores_loader.NewRegistry()
	secretstoreRegistry.Logger = log
	secretstoreRegistry.RegisterComponent(secretstore_file.NewLocalSecretStore, "local.file")

	return []runtime.Option{
		runtime.WithSecretStores(secretstoreRegistry),
	}
}
