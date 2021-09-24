// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package bindings

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/tests/conformance/utils"
	"github.com/dapr/kit/config"
	"github.com/dapr/kit/logger"
)

const (
	defaultTimeoutDuration = 60 * time.Second
	defaultWaitDuration    = time.Second

	// Use CloudEvent as default data because it is required by Azure's EventGrid.
	defaultOutputData = "[{\"eventType\":\"test\",\"eventTime\": \"2018-01-25T22:12:19.4556811Z\",\"subject\":\"dapr-conf-tests\",\"id\":\"A234-1234-1234\",\"data\":\"root/>\"}]"
)

// nolint:gochecknoglobals
var (
	testLogger = logger.NewLogger("bindingsTest")

	// Some bindings may require setup, this boolean/signal pair is used to signal that.
	waitForSetup bool
	ready        chan bool
)

type TestConfig struct {
	utils.CommonConfig
	URL                string            `mapstructure:"url"`
	InputMetadata      map[string]string `mapstructure:"input"`
	OutputMetadata     map[string]string `mapstructure:"output"`
	OutputData         string            `mapstructure:"outputData"`
	ReadBindingTimeout time.Duration     `mapstructure:"readBindingTimeout"`
	ReadBindingWait    time.Duration     `mapstructure:"readBindingWait"`
}

func NewTestConfig(name string, allOperations bool, operations []string, configMap map[string]interface{}) (TestConfig, error) {
	waitForSetup = false
	testConfig := TestConfig{
		CommonConfig: utils.CommonConfig{
			ComponentType: "bindings",
			ComponentName: name,
			AllOperations: allOperations,
			Operations:    utils.NewStringSet(operations...),
		},
		InputMetadata:      make(map[string]string),
		OutputMetadata:     make(map[string]string),
		OutputData:         defaultOutputData,
		ReadBindingTimeout: defaultTimeoutDuration,
		ReadBindingWait:    defaultWaitDuration,
	}

	err := config.Decode(configMap, &testConfig)
	if err != nil {
		return testConfig, err
	}

	// A url indicates that we need to test the http binding which requires a simple endpoint.
	if testConfig.URL != "" {
		waitForSetup = true
		startHTTPServer(testConfig.URL)
	}

	return testConfig, err
}

func startHTTPServer(url string) {
	if parts := strings.Split(url, ":"); len(parts) != 2 {
		testLogger.Errorf("URL must be in format {host}:{port}. Got: %s", url)
	} else {
		if port, err := strconv.Atoi(parts[1]); err != nil {
			testLogger.Errorf("Could not parse port number: %s", err.Error())
		} else {
			ready = make(chan bool)
			go utils.StartHTTPServer(port, ready)
		}
	}
}

func (tc *TestConfig) createInvokeRequest() bindings.InvokeRequest {
	// There is a possibility that the metadata map might be modified by the Invoke function(eg: azure blobstorage).
	// So we are making a copy of the config metadata map and setting the Metadata field before each request
	return bindings.InvokeRequest{
		Data:     []byte(tc.OutputData),
		Metadata: tc.CopyMap(tc.OutputMetadata),
	}
}

func ConformanceTests(t *testing.T, props map[string]string, inputBinding bindings.InputBinding, outputBinding bindings.OutputBinding, config TestConfig) {
	// TODO: Further investigate the parallelism issue below.
	// Some input bindings launch more goroutines than others. It was found that some were not able to run in parallel without increasing the GOMAXPROCS.
	// Example: runtime.GOMAXPROCS(20)

	if waitForSetup {
		testLogger.Info("Waiting on HTTP Server to start.")
		<-ready
	}

	// Init
	t.Run("init", func(t *testing.T) {
		// Check for an output binding specific operation before init
		if config.HasOperation("operations") {
			err := outputBinding.Init(bindings.Metadata{
				Properties: props,
			})
			assert.NoError(t, err, "expected no error setting up output binding")
		}
		// Check for an input binding specific operation before init
		if config.HasOperation("read") {
			err := inputBinding.Init(bindings.Metadata{
				Properties: props,
			})
			assert.NoError(t, err, "expected no error setting up input binding")
		}
	})

	// Operations
	if config.HasOperation("operations") {
		t.Run("operations", func(t *testing.T) {
			ops := outputBinding.Operations()
			for op := range config.Operations {
				match := false
				// remove all test related operation names
				if strings.EqualFold(op, "operations") || strings.EqualFold(op, "read") {
					continue
				}
				for _, o := range ops {
					if strings.EqualFold(string(o), op) {
						match = true
					}
				}
				assert.Truef(t, match, "expected operation %s to match list", op)
			}
		})
	}

	inputBindingCall := 0
	readChan := make(chan int)
	if config.HasOperation("read") {
		t.Run("read", func(t *testing.T) {
			go func() {
				err := inputBinding.Read(func(r *bindings.ReadResponse) ([]byte, error) {
					inputBindingCall++
					readChan <- inputBindingCall

					return nil, nil
				})
				assert.True(t, err == nil || errors.Is(err, context.Canceled), "expected Read canceled on Close")
			}()
		})
		// Special case for message brokers that are also bindings
		// Need a small wait here because with brokers like MQTT
		// if you publish before there is a consumer, the message is thrown out
		// Currently, there is no way to know when Read is successfully subscribed.
		time.Sleep(config.ReadBindingWait)
	}

	// CreateOperation
	createPerformed := false
	// Order matters here, we use the result of the create in other validations.
	if config.HasOperation(string(bindings.CreateOperation)) {
		t.Run("create", func(t *testing.T) {
			req := config.createInvokeRequest()
			req.Operation = bindings.CreateOperation
			_, err := outputBinding.Invoke(&req)
			assert.NoError(t, err, "expected no error invoking output binding")
			createPerformed = true
		})
	}

	// GetOperation
	if config.HasOperation(string(bindings.GetOperation)) {
		t.Run("get", func(t *testing.T) {
			req := config.createInvokeRequest()
			req.Operation = bindings.GetOperation
			resp, err := outputBinding.Invoke(&req)
			assert.Nil(t, err, "expected no error invoking output binding")
			if createPerformed {
				assert.Equal(t, req.Data, resp.Data)
			}
		})
	}

	// ListOperation
	if config.HasOperation(string(bindings.ListOperation)) {
		t.Run("list", func(t *testing.T) {
			req := config.createInvokeRequest()
			req.Operation = bindings.GetOperation
			_, err := outputBinding.Invoke(&req)
			assert.NoError(t, err, "expected no error invoking output binding")
		})
	}

	if config.CommonConfig.HasOperation("read") {
		t.Run("verify read", func(t *testing.T) {
			// To stop the test from hanging if there's no response, we can setup a simple timeout.
			select {
			case <-readChan:
				assert.Greater(t, inputBindingCall, 0)
			case <-time.After(config.ReadBindingTimeout):
				assert.Greater(t, inputBindingCall, 0)
			}
		})
	}

	// DeleteOperation
	if config.HasOperation(string(bindings.DeleteOperation)) {
		t.Run("delete", func(t *testing.T) {
			req := config.createInvokeRequest()
			req.Operation = bindings.DeleteOperation
			_, err := outputBinding.Invoke(&req)
			assert.Nil(t, err, "expected no error invoking output binding")

			if createPerformed && config.HasOperation(string(bindings.GetOperation)) {
				req.Operation = bindings.GetOperation
				resp, err := outputBinding.Invoke(&req)
				assert.NoError(t, err, "expected no error invoking output binding")
				assert.NotNil(t, resp)
				assert.Nil(t, resp.Data)
			}
		})
	}

	t.Run("close", func(t *testing.T) {
		// Check for an input-binding specific operation before close
		if config.HasOperation("read") {
			if closer, ok := inputBinding.(io.Closer); ok {
				err := closer.Close()
				assert.NoError(t, err, "expected no error closing input binding")
			}
		}
		// Check for an output-binding specific operation before close
		if config.HasOperation("operations") {
			if closer, ok := outputBinding.(io.Closer); ok {
				err := closer.Close()
				assert.NoError(t, err, "expected no error closing output binding")
			}
		}
	})
}
