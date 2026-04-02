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

package command_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/camunda/zeebe/clients/go/v8/pkg/pb"
	bindings_zeebe_command "github.com/dapr/components-contrib/bindings/zeebe/command"
	zeebe_test "github.com/dapr/components-contrib/tests/certification/bindings/zeebe"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopologyOperationWithOAuthMetadata(t *testing.T) {
	ports, _ := dapr_testing.GetFreePorts(2)
	grpcPort := ports[0]
	httpPort := ports[1]

	var oauthRequests int64
	oauthSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&oauthRequests, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"cert-test-token","token_type":"Bearer","expires_in":3600}`))
	}))
	defer oauthSrv.Close()

	resourcesPath := createOAuthTestResources(t, oauthSrv.URL)

	testInvokeTopology := func(ctx flow.Context) error {
		client := zeebe_test.GetDaprClient(grpcPort)
		defer client.Close()

		res, err := zeebe_test.ExecCommandOperation(
			ctx,
			client,
			bindings_zeebe_command.TopologyOperation,
			nil,
			map[string]string{},
		)
		require.NoError(t, err)

		topology := &pb.TopologyResponse{}
		err = json.Unmarshal(res.Data, topology)
		require.NoError(t, err)
		require.NotEmpty(t, topology.Brokers)

		res, err = zeebe_test.ExecCommandOperation(
			ctx,
			client,
			bindings_zeebe_command.TopologyOperation,
			nil,
			map[string]string{},
		)
		require.NoError(t, err)

		topology = &pb.TopologyResponse{}
		err = json.Unmarshal(res.Data, topology)
		require.NoError(t, err)
		require.NotEmpty(t, topology.Brokers)

		assert.Equal(t, int64(1), atomic.LoadInt64(&oauthRequests))
		return nil
	}

	flow.New(t, "Test topology operation with OAuth metadata").
		Step(dockercompose.Run("zeebe", zeebe_test.DockerComposeYaml)).
		Step("Waiting for Zeebe Readiness...", retry.Do(time.Second*3, 10, zeebe_test.CheckZeebeConnection)).
		Step(sidecar.Run(zeebe_test.SidecarName,
			append(componentRuntimeOptions(),
				embedded.WithoutApp(),
				embedded.WithResourcesPath(resourcesPath),
				embedded.WithDaprGRPCPort(strconv.Itoa(grpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(httpPort)),
			)...,
		)).
		Step("Waiting for the component to start", flow.Sleep(10*time.Second)).
		Step("Invoke topology operation", testInvokeTopology).
		Run()
}

func createOAuthTestResources(t *testing.T, oauthServerURL string) string {
	t.Helper()

	resourcesPath := filepath.Join(t.TempDir(), "components")
	err := os.MkdirAll(resourcesPath, 0o755)
	require.NoError(t, err)

	componentPath := filepath.Join(resourcesPath, "zeebe-command.yaml")
	componentYAML := fmt.Sprintf(`apiVersion: dapr.io/v1alpha1
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
`, oauthServerURL, filepath.Join(resourcesPath, "zeebe-credentials.yaml"))

	err = os.WriteFile(componentPath, []byte(componentYAML), 0o600)
	require.NoError(t, err)

	return resourcesPath
}
