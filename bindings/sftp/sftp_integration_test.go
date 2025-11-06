/*
Copyright 2025 The Dapr Authors
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

package sftp

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	sftp "github.com/dapr/components-contrib/tests/utils/sftpproxy"
)

const ProxySftp = "0.0.0.0:2223"

var connectionStringEnvKey = "DAPR_TEST_SFTP_CONNSTRING"

// Run docker from the file location as the upload folder is relative to the test
// cd proxy
// docker run --name sftp -v ./upload:/home/foo/upload -p 2222:22 -d atmoz/sftp foo:pass:1001
// export DAPR_TEST_SFTP_CONNSTRING=localhost:2222
func TestIntegrationCases(t *testing.T) {
	connectionString := os.Getenv(connectionStringEnvKey)
	if connectionString == "" {
		t.Skipf("sftp binding integration skipped. To enable this test, define the connection string using environment variable '%[1]s' (example 'export %[1]s=\"localhost:2222\")'", connectionStringEnvKey)
	}

	t.Run("List operation", testListOperation)
	t.Run("Create operation", testCreateOperation)
	t.Run("Reconnections", testReconnect)
}

func testListOperation(t *testing.T) {
	proxy := &sftp.Proxy{
		ListenAddr:   ProxySftp,
		UpstreamAddr: os.Getenv(connectionStringEnvKey),
	}

	defer proxy.Close()
	go proxy.ListenAndServe()

	c := Sftp{}

	m := bindings.Metadata{}

	m.Properties = map[string]string{
		"rootPath":              "/upload",
		"address":               ProxySftp,
		"username":              "foo",
		"password":              "pass",
		"insecureIgnoreHostKey": "true",
	}

	err := c.Init(t.Context(), m)
	require.NoError(t, err)

	r, err := c.Invoke(t.Context(), &bindings.InvokeRequest{Operation: bindings.ListOperation})
	require.NoError(t, err)
	assert.NotNil(t, r.Data)

	var d []listResponse
	err = json.Unmarshal(r.Data, &d)
	require.NoError(t, err)

	assert.EqualValues(t, 1, proxy.ReconnectionCount.Load())
}

func testCreateOperation(t *testing.T) {
	proxy := &sftp.Proxy{
		ListenAddr:   ProxySftp,
		UpstreamAddr: os.Getenv(connectionStringEnvKey),
	}
	defer proxy.Close()
	go proxy.ListenAndServe()
	c := Sftp{}
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"rootPath":              "/upload",
		"address":               ProxySftp,
		"username":              "foo",
		"password":              "pass",
		"insecureIgnoreHostKey": "true",
	}

	err := os.Remove("./upload/test.txt")
	if err != nil && !os.IsNotExist(err) {
		require.NoError(t, err)
	}

	err = c.Init(t.Context(), m)
	require.NoError(t, err)

	r, err := c.Invoke(t.Context(), &bindings.InvokeRequest{
		Operation: bindings.CreateOperation,
		Data:      []byte("test data 1"),
		Metadata: map[string]string{
			"fileName": "test.txt",
		},
	})
	require.NoError(t, err)
	assert.NotNil(t, r.Data)

	file, err := os.Stat("./proxy/upload/test.txt")
	require.NoError(t, err)
	assert.Equal(t, "test.txt", file.Name())
	assert.EqualValues(t, 1, proxy.ReconnectionCount.Load())
}

func testReconnect(t *testing.T) {
	proxy := &sftp.Proxy{
		ListenAddr:   ProxySftp,
		UpstreamAddr: os.Getenv(connectionStringEnvKey),
	}
	defer proxy.Close()
	go proxy.ListenAndServe()

	c := Sftp{}

	m := bindings.Metadata{}

	m.Properties = map[string]string{
		"rootPath":              "/upload",
		"address":               ProxySftp,
		"username":              "foo",
		"password":              "pass",
		"insecureIgnoreHostKey": "true",
	}

	err := c.Init(t.Context(), m)
	require.NoError(t, err)

	t.Run("List operation", func(t *testing.T) {
		r, err := c.Invoke(t.Context(), &bindings.InvokeRequest{Operation: bindings.ListOperation})
		require.NoError(t, err)
		assert.NotNil(t, r.Data)

		_ = proxy.KillServerConn()

		r, err = c.Invoke(t.Context(), &bindings.InvokeRequest{Operation: bindings.ListOperation})
		require.NoError(t, err)
		assert.NotNil(t, r.Data)

		var d []listResponse
		err = json.Unmarshal(r.Data, &d)
		require.NoError(t, err)

		assert.EqualValues(t, 2, proxy.ReconnectionCount.Load())
	})

	t.Run("List delete - no reconnection", func(t *testing.T) {
		numReconnects := proxy.ReconnectionCount.Load()
		_, err := c.Invoke(t.Context(), &bindings.InvokeRequest{
			Operation: bindings.DeleteOperation,
			Metadata: map[string]string{
				"fileName": "file_does_not_exist.txt",
			},
		})

		require.Error(t, err)

		assert.EqualValues(t, numReconnects, proxy.ReconnectionCount.Load())
	})

	t.Run("List delete - reconnection", func(t *testing.T) {
		numReconnects := proxy.ReconnectionCount.Load()
		_ = proxy.KillServerConn()
		_, err := c.Invoke(t.Context(), &bindings.InvokeRequest{
			Operation: bindings.DeleteOperation,
			Metadata: map[string]string{
				"fileName": "file_does_not_exist.txt",
			},
		})

		require.Error(t, err)

		assert.EqualValues(t, numReconnects+1, proxy.ReconnectionCount.Load())
	})

	t.Run("List get - no reconnection", func(t *testing.T) {
		numReconnects := proxy.ReconnectionCount.Load()
		_, err := c.Invoke(t.Context(), &bindings.InvokeRequest{
			Operation: bindings.GetOperation,
			Metadata: map[string]string{
				"fileName": "file_does_not_exist.txt",
			},
		})

		require.Error(t, err)

		assert.EqualValues(t, numReconnects, proxy.ReconnectionCount.Load())
	})

	t.Run("List get - reconnection", func(t *testing.T) {
		numReconnects := proxy.ReconnectionCount.Load()
		_ = proxy.KillServerConn()
		_, err := c.Invoke(t.Context(), &bindings.InvokeRequest{
			Operation: bindings.GetOperation,
			Metadata: map[string]string{
				"fileName": "file_does_not_exist.txt",
			},
		})

		require.Error(t, err)

		assert.EqualValues(t, numReconnects+1, proxy.ReconnectionCount.Load())
	})
}
