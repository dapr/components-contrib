//go:build integration_test

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
	"context"
	"encoding/json"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	sftp "github.com/dapr/components-contrib/tests/utils/sftpproxy"
	"github.com/dapr/kit/logger"
)

const (
	ProxySftp        = "0.0.0.0:2223"
	ConnectionString = "0.0.0.0:2222"
)

func TestIntegrationCases(t *testing.T) {
	cleanUp := setupSftp(t)
	defer cleanUp()
	time.Sleep(1 * time.Second)
	t.Run("List operation", testListOperation)
	t.Run("Create operation", testCreateOperation)
	t.Run("Reconnections", testReconnect)
}

func setupSftp(t *testing.T) func() {
	dc := dockercompose.New("sftp", "docker-compose.yaml")
	ctx := flow.Context{
		T:       t,
		Context: t.Context(),
		Flow:    nil,
	}
	err := dc.Up(ctx)

	if err != nil {
		t.Fatal(err)
	}

	return func() { dc.Down(ctx) }
}

func testListOperation(t *testing.T) {
	proxy := &sftp.Proxy{
		ListenAddr:   ProxySftp,
		UpstreamAddr: ConnectionString,
	}

	defer proxy.Close()
	go proxy.ListenAndServe()

	c := Sftp{
		logger: logger.NewLogger("sftp"),
	}

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
		UpstreamAddr: ConnectionString,
	}
	defer proxy.Close()
	go proxy.ListenAndServe()
	c := Sftp{
		logger: logger.NewLogger("sftp"),
	}
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

	file, err := os.Stat("./upload/test.txt")
	require.NoError(t, err)
	assert.Equal(t, "test.txt", file.Name())
	assert.EqualValues(t, 1, proxy.ReconnectionCount.Load())
}

func testReconnect(t *testing.T) {
	proxy := &sftp.Proxy{
		ListenAddr:   ProxySftp,
		UpstreamAddr: ConnectionString,
	}
	defer proxy.Close()
	go proxy.ListenAndServe()

	c := Sftp{
		logger: logger.NewLogger("sftp"),
	}

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

	t.Run("Parallel ops - reconnection", func(t *testing.T) {
		numReconnects := proxy.ReconnectionCount.Load()
		ctx, cancelFn := context.WithCancel(t.Context())
		opCount := atomic.Int32{}
		opFailed := atomic.Int32{}
		for range 10 {
			go func(ctx context.Context) {
				for {
					select {
					case <-ctx.Done():
						return
					case <-time.After(time.Duration(100*rand.Float32()) * time.Millisecond):
						opCount.Add(1)
						r, err := c.Invoke(t.Context(), &bindings.InvokeRequest{Operation: bindings.ListOperation})
						if err != nil {
							opFailed.Add(1)
							break
						}

						assert.NotNil(t, r.Data)
					}
				}
			}(ctx)
		}

		go func(ctx context.Context) {
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(1 * time.Second):
					_ = proxy.KillServerConn()
				}
			}

		}(ctx)

		time.Sleep(time.Second * 5)
		cancelFn()

		totalOps := opCount.Load()
		failedOps := opFailed.Load()

		// Calculate 5% tolerance
		tolerance := float64(totalOps) * 0.05

		// Assert that failed operations are within 1% of total operations
		assert.InDelta(t, 0, failedOps, tolerance,
			"Expected less than 1%% of operations to fail. Total: %d, Failed: %d (%.2f%%)",
			totalOps, failedOps, (float64(failedOps)/float64(totalOps))*100)

		expectedReconnects := numReconnects + 5
		currentReconnects := proxy.ReconnectionCount.Load()
		assert.InDelta(t, expectedReconnects, currentReconnects, 2.0, "Expected %d reconnections, got %d", expectedReconnects, currentReconnects)
	})
}
