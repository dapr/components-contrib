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

package apns

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

const (
	testKeyID  = "012345678"
	testTeamID = "876543210"

	// This is a valid PKCS #8 payload, but the key was generated for testing
	// use and is not being used in any production service.
	testPrivateKey = "-----BEGIN PRIVATE KEY-----\nMIGTAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBHkwdwIBAQQgHZdKErL0xQ3yalg+\nbUMpTpfo4bRVxYMnowSMkBIS3OSgCgYIKoZIzj0DAQehRANCAARjr0Ft+hWAeAfY\nkkOBk8GzMlV4Mo/APwcuXRlAHqkSUKi453YqgAPygkCNBmOhNWgynUp+XGxuj6in\nofsBN1Rw\n-----END PRIVATE KEY-----"
)

func TestInit(t *testing.T) {
	testLogger := logger.NewLogger("test")

	t.Run("uses the development service", func(t *testing.T) {
		metadata := bindings.Metadata{
			Properties: map[string]string{
				developmentKey: "true",
				keyIDKey:       testKeyID,
				teamIDKey:      testTeamID,
				privateKeyKey:  testPrivateKey,
			},
		}
		binding := NewAPNS(testLogger)
		err := binding.Init(metadata)
		assert.Nil(t, err)
		assert.Equal(t, developmentPrefix, binding.urlPrefix)
	})

	t.Run("uses the production service", func(t *testing.T) {
		metadata := bindings.Metadata{
			Properties: map[string]string{
				developmentKey: "false",
				keyIDKey:       testKeyID,
				teamIDKey:      testTeamID,
				privateKeyKey:  testPrivateKey,
			},
		}
		binding := NewAPNS(testLogger)
		err := binding.Init(metadata)
		assert.Nil(t, err)
		assert.Equal(t, productionPrefix, binding.urlPrefix)
	})

	t.Run("defaults to the production service", func(t *testing.T) {
		metadata := bindings.Metadata{
			Properties: map[string]string{
				keyIDKey:      testKeyID,
				teamIDKey:     testTeamID,
				privateKeyKey: testPrivateKey,
			},
		}
		binding := NewAPNS(testLogger)
		err := binding.Init(metadata)
		assert.Nil(t, err)
		assert.Equal(t, productionPrefix, binding.urlPrefix)
	})

	t.Run("invalid development value", func(t *testing.T) {
		metadata := bindings.Metadata{
			Properties: map[string]string{
				developmentKey: "True",
				keyIDKey:       testKeyID,
				teamIDKey:      testTeamID,
				privateKeyKey:  testPrivateKey,
			},
		}
		binding := NewAPNS(testLogger)
		err := binding.Init(metadata)
		assert.Error(t, err, "invalid value for development parameter: True")
	})

	t.Run("the key ID is required", func(t *testing.T) {
		metadata := bindings.Metadata{
			Properties: map[string]string{
				teamIDKey:     testTeamID,
				privateKeyKey: testPrivateKey,
			},
		}
		binding := NewAPNS(testLogger)
		err := binding.Init(metadata)
		assert.Error(t, err, "the key-id parameter is required")
	})

	t.Run("valid key ID", func(t *testing.T) {
		metadata := bindings.Metadata{
			Properties: map[string]string{
				keyIDKey:      testKeyID,
				teamIDKey:     testTeamID,
				privateKeyKey: testPrivateKey,
			},
		}
		binding := NewAPNS(testLogger)
		err := binding.Init(metadata)
		assert.Nil(t, err)
		assert.Equal(t, testKeyID, binding.authorizationBuilder.keyID)
	})

	t.Run("the team ID is required", func(t *testing.T) {
		metadata := bindings.Metadata{
			Properties: map[string]string{
				keyIDKey:      testKeyID,
				privateKeyKey: testPrivateKey,
			},
		}
		binding := NewAPNS(testLogger)
		err := binding.Init(metadata)
		assert.Error(t, err, "the team-id parameter is required")
	})

	t.Run("valid team ID", func(t *testing.T) {
		metadata := bindings.Metadata{
			Properties: map[string]string{
				keyIDKey:      testKeyID,
				teamIDKey:     testTeamID,
				privateKeyKey: testPrivateKey,
			},
		}
		binding := NewAPNS(testLogger)
		err := binding.Init(metadata)
		assert.Nil(t, err)
		assert.Equal(t, testTeamID, binding.authorizationBuilder.teamID)
	})

	t.Run("the private key is required", func(t *testing.T) {
		metadata := bindings.Metadata{
			Properties: map[string]string{
				keyIDKey:  testKeyID,
				teamIDKey: testTeamID,
			},
		}
		binding := NewAPNS(testLogger)
		err := binding.Init(metadata)
		assert.Error(t, err, "the private-key parameter is required")
	})

	t.Run("valid private key", func(t *testing.T) {
		metadata := bindings.Metadata{
			Properties: map[string]string{
				keyIDKey:      testKeyID,
				teamIDKey:     testTeamID,
				privateKeyKey: testPrivateKey,
			},
		}
		binding := NewAPNS(testLogger)
		err := binding.Init(metadata)
		assert.Nil(t, err)
		assert.NotNil(t, binding.authorizationBuilder.privateKey)
	})
}

func TestOperations(t *testing.T) {
	testLogger := logger.NewLogger("test")
	testBinding := NewAPNS(testLogger)
	operations := testBinding.Operations()
	assert.Equal(t, 1, len(operations))
	assert.Equal(t, bindings.CreateOperation, operations[0])
}

func TestInvoke(t *testing.T) {
	testLogger := logger.NewLogger("test")

	successRequest := &bindings.InvokeRequest{
		Operation: bindings.CreateOperation,
		Metadata: map[string]string{
			deviceTokenKey: "1234567890",
			pushTypeKey:    "alert",
			messageIDKey:   "123",
			expirationKey:  "1234567890",
			priorityKey:    "10",
			topicKey:       "test",
			collapseIDKey:  "1234567",
		},
	}

	t.Run("operation must be create", func(t *testing.T) {
		testBinding := makeTestBinding(t, testLogger)
		req := &bindings.InvokeRequest{Operation: bindings.DeleteOperation}
		_, err := testBinding.Invoke(context.TODO(), req)
		assert.Error(t, err, "operation not supported: delete")
	})

	t.Run("the device token is required", func(t *testing.T) {
		testBinding := makeTestBinding(t, testLogger)
		req := &bindings.InvokeRequest{
			Operation: bindings.CreateOperation,
			Metadata:  map[string]string{},
		}
		_, err := testBinding.Invoke(context.TODO(), req)
		assert.Error(t, err, "the device-token parameter is required")
	})

	t.Run("the authorization header is sent", func(t *testing.T) {
		testBinding := makeTestBinding(t, testLogger)
		testBinding.client = newTestClient(func(req *http.Request) *http.Response {
			assert.Contains(t, req.Header, "Authorization")

			return successResponse()
		})
		_, _ = testBinding.Invoke(context.TODO(), successRequest)
	})

	t.Run("the push type header is sent", func(t *testing.T) {
		testBinding := makeTestBinding(t, testLogger)
		testBinding.client = newTestClient(func(req *http.Request) *http.Response {
			assert.Contains(t, req.Header, "Apns-Push-Type")
			assert.Equal(t, "alert", req.Header.Get(pushTypeKey))

			return successResponse()
		})
		_, _ = testBinding.Invoke(context.TODO(), successRequest)
	})

	t.Run("the message ID is sent", func(t *testing.T) {
		testBinding := makeTestBinding(t, testLogger)
		testBinding.client = newTestClient(func(req *http.Request) *http.Response {
			assert.Contains(t, req.Header, "Apns-Id")
			assert.Equal(t, "123", req.Header.Get(messageIDKey))

			return successResponse()
		})
		_, _ = testBinding.Invoke(context.TODO(), successRequest)
	})

	t.Run("the expiration is sent", func(t *testing.T) {
		testBinding := makeTestBinding(t, testLogger)
		testBinding.client = newTestClient(func(req *http.Request) *http.Response {
			assert.Contains(t, req.Header, "Apns-Expiration")
			assert.Equal(t, "1234567890", req.Header.Get(expirationKey))

			return successResponse()
		})
		_, _ = testBinding.Invoke(context.TODO(), successRequest)
	})

	t.Run("the priority is sent", func(t *testing.T) {
		testBinding := makeTestBinding(t, testLogger)
		testBinding.client = newTestClient(func(req *http.Request) *http.Response {
			assert.Contains(t, req.Header, "Apns-Priority")
			assert.Equal(t, "10", req.Header.Get(priorityKey))

			return successResponse()
		})
		_, _ = testBinding.Invoke(context.TODO(), successRequest)
	})

	t.Run("the topic is sent", func(t *testing.T) {
		testBinding := makeTestBinding(t, testLogger)
		testBinding.client = newTestClient(func(req *http.Request) *http.Response {
			assert.Contains(t, req.Header, "Apns-Topic")
			assert.Equal(t, "test", req.Header.Get(topicKey))

			return successResponse()
		})
		_, _ = testBinding.Invoke(context.TODO(), successRequest)
	})

	t.Run("the collapse ID is sent", func(t *testing.T) {
		testBinding := makeTestBinding(t, testLogger)
		testBinding.client = newTestClient(func(req *http.Request) *http.Response {
			assert.Contains(t, req.Header, "Apns-Collapse-Id")
			assert.Equal(t, "1234567", req.Header.Get(collapseIDKey))

			return successResponse()
		})
		_, _ = testBinding.Invoke(context.TODO(), successRequest)
	})

	t.Run("the message ID is returned", func(t *testing.T) {
		testBinding := makeTestBinding(t, testLogger)
		testBinding.client = newTestClient(func(req *http.Request) *http.Response {
			return successResponse()
		})
		response, err := testBinding.Invoke(context.TODO(), successRequest)
		assert.Nil(t, err)
		assert.NotNil(t, response.Data)
		var body notificationResponse
		decoder := jsoniter.NewDecoder(bytes.NewReader(response.Data))
		err = decoder.Decode(&body)
		assert.Nil(t, err)
		assert.Equal(t, "12345", body.MessageID)
	})

	t.Run("returns the error code", func(t *testing.T) {
		testBinding := makeTestBinding(t, testLogger)
		testBinding.client = newTestClient(func(req *http.Request) *http.Response {
			body := "{\"reason\":\"BadDeviceToken\"}"

			return &http.Response{
				StatusCode: http.StatusBadRequest,
				Body:       io.NopCloser(strings.NewReader(body)),
			}
		})
		_, err := testBinding.Invoke(context.TODO(), successRequest)
		assert.Error(t, err, "BadDeviceToken")
	})
}

func makeTestBinding(t *testing.T, log logger.Logger) *APNS {
	testBinding := NewAPNS(log)
	bindingMetadata := bindings.Metadata{
		Properties: map[string]string{
			developmentKey: "true",
			keyIDKey:       testKeyID,
			teamIDKey:      testTeamID,
			privateKeyKey:  testPrivateKey,
		},
	}
	err := testBinding.Init(bindingMetadata)
	assert.Nil(t, err)

	return testBinding
}

func successResponse() *http.Response {
	response := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
	}
	response.Header.Add(messageIDKey, "12345")

	return response
}

// http://hassansin.github.io/Unit-Testing-http-client-in-Go

type roundTripFunc func(req *http.Request) *http.Response

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req), nil
}

func newTestClient(fn roundTripFunc) *http.Client {
	return &http.Client{Transport: fn}
}
