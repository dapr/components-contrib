/*
Copyright 2026 The Dapr Authors
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

package jobworker_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	zeebe_test "github.com/dapr/components-contrib/tests/certification/bindings/zeebe"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/app"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	"github.com/dapr/dapr/pkg/config/protocol"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/go-sdk/service/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobworkerWithOAuthMetadata(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(3)
	grpcPort := ports[0]
	httpPort := ports[1]
	appPort := ports[2]

	id := zeebe_test.TestID()
	ch := make(chan bool, 2)
	var count int32
	var oauthRequests int64

	oauthSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&oauthRequests, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"cert-test-token","token_type":"Bearer","expires_in":3600}`))
	}))
	defer oauthSrv.Close()

	resourcesPath := createOAuthJobworkerTestResources(t, oauthSrv.URL)

	workers := func(_ flow.Context, s common.Service) error {
		return s.AddBindingInvocationHandler(zeebe_test.JobworkerTestName, func(_ context.Context, _ *common.BindingEvent) ([]byte, error) {
			atomic.AddInt32(&count, 1)
			ch <- true
			return []byte("{}"), nil
		})
	}

	executeProcess := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		err := deployTestProcess(client, id, 1)
		require.NoError(t, err)

		for i := 0; i < 2; i++ {
			_, err = zeebe_test.CreateProcessInstance(client, ctx, map[string]interface{}{
				"bpmnProcessId": id,
			})
			require.NoError(t, err)
		}

		for i := 0; i < 2; i++ {
			select {
			case <-ch:
			case <-time.After(30 * time.Second):
				assert.FailNow(t, "read timeout")
			}
		}

		assert.Equal(t, int32(2), atomic.LoadInt32(&count))
		assert.Equal(t, int64(1), atomic.LoadInt64(&oauthRequests))

		return nil
	}

	flow.New(t, "Test jobworker with OAuth metadata").
		Step(dockercompose.Run("zeebe", zeebe_test.DockerComposeYaml)).
		Step("Waiting for Zeebe Readiness...", retry.Do(time.Second*3, 10, zeebe_test.CheckZeebeConnection)).
		Step(app.Run("workerApp", fmt.Sprintf(":%d", appPort), workers)).
		Step(sidecar.Run(zeebe_test.SidecarName,
			append(componentRuntimeOptions(),
				embedded.WithAppProtocol(protocol.HTTPProtocol, strconv.Itoa(appPort)),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
				embedded.WithResourcesPath(resourcesPath),
			)...,
		)).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Execute process", executeProcess).
		Step("Allow worker completion", flow.Sleep(2*time.Second)).
		Run()
}

func createOAuthJobworkerTestResources(t *testing.T, oauthServerURL string) string {
	t.Helper()

	resourcesPath := filepath.Join(t.TempDir(), "components")
	err := os.MkdirAll(resourcesPath, 0o755)
	require.NoError(t, err)

	cachePath := filepath.Join(resourcesPath, "zeebe-credentials.yaml")

	commandComponentPath := filepath.Join(resourcesPath, "zeebe-command.yaml")
	commandComponentYAML := fmt.Sprintf(`apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: zeebe-command
spec:
  type: bindings.zeebe.command
  version: v1
  metadata:
    - name: gatewayAddr
      value: localhost:26500
    - name: gatewayKeepAlive
      value: 45s
    - name: usePlainTextConnection
      value: true
`)
	err = os.WriteFile(commandComponentPath, []byte(commandComponentYAML), 0o600)
	require.NoError(t, err)

	jobworkerComponentPath := filepath.Join(resourcesPath, "zeebe-jobworker-test.yaml")
	jobworkerComponentYAML := fmt.Sprintf(`apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: zeebe-jobworker-test
spec:
  type: bindings.zeebe.jobworker
  version: v1
  metadata:
    - name: gatewayAddr
      value: localhost:26500
    - name: gatewayKeepAlive
      value: 45s
    - name: usePlainTextConnection
      value: true
    - name: jobType
      value: zeebe-jobworker-test
    - name: clientId
      value: cert-test-client
    - name: clientSecret
      value: cert-test-secret
    - name: authorizationServerUrl
      value: "%s"
    - name: tokenAudience
      value: localhost
    - name: tokenScope
      value: test.scope
    - name: clientConfigPath
      value: "%s"
`, oauthServerURL, cachePath)
	err = os.WriteFile(jobworkerComponentPath, []byte(jobworkerComponentYAML), 0o600)
	require.NoError(t, err)

	return resourcesPath
}
