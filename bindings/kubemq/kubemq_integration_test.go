//go:build integration_test
// +build integration_test

package kubemq

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const (
	// Environment variable containing the host name for KubeMQ integration tests
	// To run using docker: docker run -d --hostname -kubemq --name test-kubemq -p 50000:50000 kubemq/kubemq-community:latest
	// In that case the address string will be: "localhost:50000"
	testKubeMQHostEnvKey = "DAPR_TEST_KUBEMQ_HOST"
)

func getTestKubeMQHost() string {
	host := os.Getenv(testKubeMQHostEnvKey)
	if host == "" {
		host = "localhost:50000"
	}
	return host
}

func getDefaultMetadata(channel string) bindings.Metadata {
	return bindings.Metadata{
		Base: metadata.Base{
			Name: "kubemq",
			Properties: map[string]string{
				"address":            getTestKubeMQHost(),
				"channel":            channel,
				"pollMaxItems":       "1",
				"autoAcknowledged":   "true",
				"pollTimeoutSeconds": "2",
			},
		},
	}
}

func Test_kubeMQ_Init(t *testing.T) {
	tests := []struct {
		name    string
		meta    bindings.Metadata
		wantErr bool
	}{
		{
			name: "init with valid options",
			meta: bindings.Metadata{
				Base: metadata.Base{
					Name: "kubemq",
					Properties: map[string]string{
						"address":            getTestKubeMQHost(),
						"channel":            "test",
						"pollMaxItems":       "1",
						"autoAcknowledged":   "true",
						"pollTimeoutSeconds": "2",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "init with invalid options",
			meta: bindings.Metadata{
				Base: metadata.Base{
					Name: "kubemq",
					Properties: map[string]string{
						"address":            "localhost-bad:50000",
						"channel":            "test",
						"pollMaxItems":       "1",
						"autoAcknowledged":   "true",
						"pollTimeoutSeconds": "2",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "init with invalid parsing options",
			meta: bindings.Metadata{
				Base: metadata.Base{
					Name: "kubemq",
					Properties: map[string]string{
						"address":            "bad",
						"channel":            "test",
						"pollMaxItems":       "1",
						"autoAcknowledged":   "true",
						"pollTimeoutSeconds": "2",
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kubemq := NewKubeMQ(logger.NewLogger("test"))
			err := kubemq.Init(tt.meta)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_kubeMQ_Invoke_Read_Single_Message(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	kubemq := NewKubeMQ(logger.NewLogger("test"))
	err := kubemq.Init(getDefaultMetadata("test.read.single"))
	require.NoError(t, err)
	dataReadCh := make(chan []byte)
	invokeRequest := &bindings.InvokeRequest{
		Data:     []byte("test"),
		Metadata: map[string]string{},
	}
	_, err = kubemq.Invoke(ctx, invokeRequest)
	require.NoError(t, err)
	_ = kubemq.Read(ctx, func(ctx context.Context, req *bindings.ReadResponse) ([]byte, error) {
		dataReadCh <- req.Data
		return req.Data, nil
	})
	select {
	case <-ctx.Done():
		require.Fail(t, "timeout waiting for read response")
	case data := <-dataReadCh:
		require.Equal(t, invokeRequest.Data, data)
	}
}

func Test_kubeMQ_Invoke_Read_Single_MessageWithHandlerError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	kubemq := NewKubeMQ(logger.NewLogger("test"))
	md := getDefaultMetadata("test.read.single.error")
	md.Properties["autoAcknowledged"] = "false"
	err := kubemq.Init(md)
	require.NoError(t, err)
	invokeRequest := &bindings.InvokeRequest{
		Data:     []byte("test"),
		Metadata: map[string]string{},
	}

	_, err = kubemq.Invoke(ctx, invokeRequest)
	require.NoError(t, err)
	firstReadCtx, firstReadCancel := context.WithTimeout(context.Background(), time.Second*3)
	defer firstReadCancel()
	_ = kubemq.Read(firstReadCtx, func(ctx context.Context, req *bindings.ReadResponse) ([]byte, error) {
		return nil, fmt.Errorf("handler error")
	})

	<-firstReadCtx.Done()
	dataReadCh := make(chan []byte)
	secondReadCtx, secondReadCancel := context.WithTimeout(context.Background(), time.Second*3)
	defer secondReadCancel()
	_ = kubemq.Read(secondReadCtx, func(ctx context.Context, req *bindings.ReadResponse) ([]byte, error) {
		dataReadCh <- req.Data
		return req.Data, nil
	})
	select {
	case <-secondReadCtx.Done():
		require.Fail(t, "timeout waiting for read response")
	case data := <-dataReadCh:
		require.Equal(t, invokeRequest.Data, data)
	}
}

func Test_kubeMQ_Invoke_Error(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	kubemq := NewKubeMQ(logger.NewLogger("test"))
	err := kubemq.Init(getDefaultMetadata("***test***"))
	require.NoError(t, err)

	invokeRequest := &bindings.InvokeRequest{
		Data:     []byte("test"),
		Metadata: map[string]string{},
	}
	_, err = kubemq.Invoke(ctx, invokeRequest)
	require.Error(t, err)
}
