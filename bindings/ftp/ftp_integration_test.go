//go:build integration_test

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

package ftp

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/kit/logger"
)

// Use the IP, not "localhost": Docker Desktop may resolve localhost to ::1 first.
const serverAddress = "127.0.0.1:2121"

func TestIntegrationCases(t *testing.T) {
	cleanUp := setupFtp(t)
	defer cleanUp()

	// The compose file runs pure-ftpd, which has no implicit FTPS mode; that mode is covered by unit tests only.
	for _, tlsMode := range []string{"none", "explicit"} {
		t.Run("File lifecycle tlsMode="+tlsMode, func(t *testing.T) {
			testFileLifecycle(t, tlsMode)
		})
	}
	t.Run("TLS verification is enforced", testTLSVerification)
	t.Run("Wrong password", testWrongPassword)
}

func setupFtp(t *testing.T) func() {
	dc := dockercompose.New("ftp", "docker-compose.yaml")
	ctx := flow.Context{
		T:       t,
		Context: t.Context(),
	}
	require.NoError(t, dc.Up(ctx))

	// pure-ftpd generates its self-signed certificate at startup, which can take a while on slow machines.
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", serverAddress, time.Second)
		if err != nil {
			return false
		}
		_ = c.Close()
		return true
	}, 120*time.Second, time.Second)

	return func() { _ = dc.Down(ctx) }
}

func baseProps(tlsMode string) map[string]string {
	return map[string]string{
		"rootPath":           "/upload",
		"address":            serverAddress,
		"username":           "foo",
		"password":           "pass",
		"tlsMode":            tlsMode,
		"insecureSkipVerify": "true",
	}
}

func initBinding(t *testing.T, props map[string]string) (*Ftp, error) {
	t.Helper()
	f := &Ftp{logger: logger.NewLogger("ftp")}
	m := bindings.Metadata{}
	m.Properties = props
	return f, f.Init(t.Context(), m)
}

func list(t *testing.T, f *Ftp, dir string) []listResponse {
	t.Helper()
	r, err := f.Invoke(t.Context(), &bindings.InvokeRequest{
		Operation: bindings.ListOperation,
		Metadata:  map[string]string{"fileName": dir},
	})
	require.NoError(t, err)

	var out []listResponse
	require.NoError(t, json.Unmarshal(r.Data, &out))
	return out
}

func testFileLifecycle(t *testing.T, tlsMode string) {
	f, err := initBinding(t, baseProps(tlsMode))
	require.NoError(t, err)

	ctx := t.Context()
	content := []byte("test data " + tlsMode)
	fileName := "sub/test.txt"

	r, err := f.Invoke(ctx, &bindings.InvokeRequest{
		Operation: bindings.CreateOperation,
		Data:      content,
		Metadata:  map[string]string{"fileName": fileName},
	})
	require.NoError(t, err)
	assert.JSONEq(t, `{"fileName":"test.txt"}`, string(r.Data))
	assert.Equal(t, "test.txt", r.Metadata["fileName"])

	assert.Equal(t, []listResponse{{FileName: "sub", IsDirectory: true}}, list(t, f, ""))
	assert.Equal(t, []listResponse{{FileName: "test.txt"}}, list(t, f, "sub"))

	r, err = f.Invoke(ctx, &bindings.InvokeRequest{
		Operation: bindings.GetOperation,
		Metadata:  map[string]string{"fileName": fileName},
	})
	require.NoError(t, err)
	assert.Equal(t, content, r.Data)

	_, err = f.Invoke(ctx, &bindings.InvokeRequest{
		Operation: renameOperation,
		Metadata:  map[string]string{"fileName": fileName, "newFileName": "sub/renamed.txt"},
	})
	require.NoError(t, err)
	assert.Equal(t, []listResponse{{FileName: "renamed.txt"}}, list(t, f, "sub"))

	_, err = f.Invoke(ctx, &bindings.InvokeRequest{
		Operation: bindings.DeleteOperation,
		Metadata:  map[string]string{"fileName": "sub/renamed.txt"},
	})
	require.NoError(t, err)
	assert.Empty(t, list(t, f, "sub"))

	_, err = f.Invoke(ctx, &bindings.InvokeRequest{
		Operation: bindings.GetOperation,
		Metadata:  map[string]string{"fileName": "sub/renamed.txt"},
	})
	require.Error(t, err)

	_, err = f.Invoke(ctx, &bindings.InvokeRequest{
		Operation: bindings.DeleteOperation,
		Metadata:  map[string]string{"fileName": "sub/renamed.txt"},
	})
	require.Error(t, err)

	require.NoError(t, f.Close())
}

func testTLSVerification(t *testing.T) {
	props := baseProps("explicit")
	props["insecureSkipVerify"] = "false"

	_, err := initBinding(t, props)

	require.ErrorContains(t, err, "certificate")
}

func testWrongPassword(t *testing.T) {
	props := baseProps("none")
	props["password"] = "wrong"

	_, err := initBinding(t, props)

	require.ErrorContains(t, err, "logging in")
}
