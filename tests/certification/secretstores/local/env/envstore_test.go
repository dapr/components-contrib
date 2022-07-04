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

package envstore_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	// SecretStores
	"github.com/dapr/components-contrib/secretstores"
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

func TestEnv(t *testing.T) {
	ports, err := dapr_testing.GetFreePorts(2)
	assert.NoError(t, err)

	currentGrpcPort := ports[0]
	currentHttpPort := ports[1]

	log := logger.NewLogger("dapr.components")

	os.Setenv("certtestsecret", "abcd")
	defer os.Unsetenv("certtestsecret")

	testGetKnownSecret := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			panic(err)
		}
		defer client.Close()

		res, err := client.GetSecret(ctx, "envvar-secret-store", "certtestsecret", nil)
		assert.NoError(t, err)
		assert.Equal(t, "abcd", res["certtestsecret"])
		return nil
	}

	flow.New(t, "environment secret store reads expected value").
		Step(sidecar.Run(sidecarName,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components/"),
			embedded.WithDaprGRPCPort(currentGrpcPort),
			embedded.WithDaprHTTPPort(currentHttpPort),
			runtime.WithSecretStores(
				secretstores_loader.New("local.env", func() secretstores.SecretStore {
					return secretstore_env.NewEnvSecretStore(log)
				}),
			))).
		Step("Getting known secret", testGetKnownSecret).
		Run()
}
