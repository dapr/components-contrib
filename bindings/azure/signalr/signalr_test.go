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

package signalr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestConfigurationValid(t *testing.T) {
	tests := []struct {
		name               string
		properties         map[string]string
		expectedEndpoint   string
		expectedAccessKey  string
		expectedHub        string
		additionalMetadata map[string]string
	}{
		{
			"With all properties",
			map[string]string{
				"connectionString": "Endpoint=https://fake.service.signalr.net;AccessKey=fakekey;Version=1.0;",
			},
			"https://fake.service.signalr.net",
			"fakekey",
			"",
			nil,
		},
		{
			"With missing version",
			map[string]string{
				"connectionString": "Endpoint=https://fake.service.signalr.net;AccessKey=fakekey;",
			},
			"https://fake.service.signalr.net",
			"fakekey",
			"",
			nil,
		},
		{
			"With semicolon after access key",
			map[string]string{
				"connectionString": "Endpoint=https://fake.service.signalr.net;AccessKey=fakekey",
			},
			"https://fake.service.signalr.net",
			"fakekey",
			"",
			nil,
		},
		{
			"With trailing slash in endpoint",
			map[string]string{
				"connectionString": "Endpoint=https://fake.service.signalr.net/;AccessKey=fakekey;Version=1.0",
			},
			"https://fake.service.signalr.net",
			"fakekey",
			"",
			nil,
		},
		{
			"With hub",
			map[string]string{
				"connectionString": "Endpoint=https://fake.service.signalr.net/;AccessKey=fakekey;Version=1.0",
				"hub":              "myhub",
			},
			"https://fake.service.signalr.net",
			"fakekey",
			"myhub",
			nil,
		},
		{
			"With AAD and no access key (system-assigned MSI)",
			map[string]string{
				"connectionString": "Endpoint=https://fake.service.signalr.net/;AuthType=aad;Version=1.0",
			},
			"https://fake.service.signalr.net",
			"",
			"",
			nil,
		},
		{
			"Add azureClientId to metadata map (user-assigned MSI)",
			map[string]string{
				"connectionString": "Endpoint=https://fake.service.signalr.net/;AuthType=aad;ClientId=b83aec5c-54a3-4e4a-8831-ba3f849b79a1;Version=1.0",
			},
			"https://fake.service.signalr.net",
			"",
			"",
			map[string]string{
				"azureClientId": "b83aec5c-54a3-4e4a-8831-ba3f849b79a1",
			},
		},
		{
			"Add Azure AD credentials to metadata map (Azure AD app)",
			map[string]string{
				"connectionString": "Endpoint=https://fake.service.signalr.net/;AuthType=aad;ClientId=b83aec5c-54a3-4e4a-8831-ba3f849b79a1;ClientSecret=fakesecret;TenantId=f0f4622e-e476-46b5-bd0c-1866d27117d4;Version=1.0",
			},
			"https://fake.service.signalr.net",
			"",
			"",
			map[string]string{
				"azureClientId":     "b83aec5c-54a3-4e4a-8831-ba3f849b79a1",
				"azureClientSecret": "fakesecret",
				"azureTenantId":     "f0f4622e-e476-46b5-bd0c-1866d27117d4",
			},
		},
		{
			"No connection string, access key",
			map[string]string{
				"endpoint":  "https://fake.service.signalr.net/",
				"accessKey": "fakekey",
			},
			"https://fake.service.signalr.net",
			"fakekey",
			"",
			nil,
		},
		{
			"No connection string, access key and hub",
			map[string]string{
				"endpoint":  "https://fake.service.signalr.net/",
				"accessKey": "fakekey",
				"hub":       "myhub",
			},
			"https://fake.service.signalr.net",
			"fakekey",
			"myhub",
			nil,
		},
		{
			"No connection string, Azure AD",
			map[string]string{
				"endpoint":          "https://fake.service.signalr.net/",
				"azureClientId":     "b83aec5c-54a3-4e4a-8831-ba3f849b79a1",
				"azureClientSecret": "fakesecret",
				"azureTenantId":     "f0f4622e-e476-46b5-bd0c-1866d27117d4",
			},
			"https://fake.service.signalr.net",
			"",
			"",
			map[string]string{
				"azureClientId":     "b83aec5c-54a3-4e4a-8831-ba3f849b79a1",
				"azureClientSecret": "fakesecret",
				"azureTenantId":     "f0f4622e-e476-46b5-bd0c-1866d27117d4",
			},
		},
		{
			"No connection string, Azure AD with aliased names",
			map[string]string{
				"endpoint":        "https://fake.service.signalr.net/",
				"spnClientId":     "b83aec5c-54a3-4e4a-8831-ba3f849b79a1",
				"spnClientSecret": "fakesecret",
				"spnTenantId":     "f0f4622e-e476-46b5-bd0c-1866d27117d4",
			},
			"https://fake.service.signalr.net",
			"",
			"",
			map[string]string{
				"spnClientId":     "b83aec5c-54a3-4e4a-8831-ba3f849b79a1",
				"spnClientSecret": "fakesecret",
				"spnTenantId":     "f0f4622e-e476-46b5-bd0c-1866d27117d4",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewSignalR(logger.NewLogger("test")).(*SignalR)
			err := s.parseMetadata(tt.properties)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedEndpoint, s.endpoint)
			assert.Equal(t, tt.expectedAccessKey, s.accessKey)
			assert.Equal(t, tt.expectedHub, s.hub)
			if len(tt.additionalMetadata) > 0 {
				for k := range tt.additionalMetadata {
					assert.Equal(t, tt.properties[k], tt.additionalMetadata[k])
				}
			}
		})
	}
}

func TestInvalidConfigurations(t *testing.T) {
	tests := []struct {
		name       string
		properties map[string]string
	}{
		{
			"Empty properties",
			map[string]string{},
		},
		{
			"Empty connection string",
			map[string]string{
				"connectionString": "",
			},
		},
		{
			"White spaces in connection string",
			map[string]string{
				"connectionString": "    ",
			},
		},
		{
			"Misspelled connection string",
			map[string]string{
				"connectionString1": "Endpoint=https://fake.service.signalr.net;AccessKey=fakekey;",
			},
		},
		{
			"Missing endpoint",
			map[string]string{
				"connectionString": "AccessKey=fakekey;",
			},
		},
		{
			"Missing access key (no AAD)",
			map[string]string{
				"connectionString1": "Endpoint=https://fake.service.signalr.net;",
			},
		},
		{
			"With empty endpoint value",
			map[string]string{
				"connectionString": "Endpoint=;AccessKey=fakekey;Version=1.0",
			},
		},
		{
			"With invalid version",
			map[string]string{
				"connectionString": "Endpoint=https://fake.service.signalr.net;AccessKey=fakekey;Version=2.0",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewSignalR(logger.NewLogger("test")).(*SignalR)
			err := s.parseMetadata(tt.properties)
			require.Error(t, err)
		})
	}
}

type MockTokenCredential struct {
	AccessToken string
}

func (m *MockTokenCredential) GetToken(ctx context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{
		Token:     m.AccessToken,
		ExpiresOn: time.Now().Add(time.Hour),
	}, nil
}

type mockTransport struct {
	response     *http.Response
	errToReturn  error
	request      *http.Request
	requestCount int32
}

func (t *mockTransport) reset() {
	atomic.StoreInt32(&t.requestCount, 0)
	t.request = nil
}

func (t *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	atomic.AddInt32(&t.requestCount, 1)
	t.request = req

	return t.response, t.errToReturn
}

func TestWriteShouldFail(t *testing.T) {
	httpTransport := &mockTransport{
		response: &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(""))},
	}

	s := NewSignalR(logger.NewLogger("test")).(*SignalR)
	s.endpoint = "https://fake.service.signalr.net"
	s.accessKey = "G7+nIt9n48+iYSltPRf1v8kE+MupFfEt/9NSNTKOdzA="
	s.httpClient = &http.Client{
		Transport: httpTransport,
	}

	t.Run("Missing hub should fail", func(t *testing.T) {
		httpTransport.reset()
		_, err := s.Invoke(context.Background(), &bindings.InvokeRequest{
			Data:     []byte("hello world"),
			Metadata: map[string]string{},
		})

		require.Error(t, err)
	})

	t.Run("SignalR call failed should be returned", func(t *testing.T) {
		httpTransport.reset()
		httpErr := errors.New("fake error")
		httpTransport.errToReturn = httpErr
		_, err := s.Invoke(context.Background(), &bindings.InvokeRequest{
			Data: []byte("hello world"),
			Metadata: map[string]string{
				hubKey: "testHub",
			},
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), httpErr.Error())
	})

	t.Run("SignalR call returns status != [200, 202]", func(t *testing.T) {
		httpTransport.reset()
		httpTransport.response.StatusCode = 401
		_, err := s.Invoke(context.Background(), &bindings.InvokeRequest{
			Data: []byte("hello world"),
			Metadata: map[string]string{
				hubKey: "testHub",
			},
		})

		require.Error(t, err)
	})
}

func TestWriteShouldSucceed(t *testing.T) {
	httpTransport := &mockTransport{
		response: &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(""))},
	}

	s := NewSignalR(logger.NewLogger("test")).(*SignalR)
	s.endpoint = "https://fake.service.signalr.net"
	s.accessKey = "AAbbcCsGEQKoLEH6oodDR0jK104Fu1c39Qgk+AA8D+M="
	s.httpClient = &http.Client{
		Transport: httpTransport,
	}

	t.Run("Has authorization", func(t *testing.T) {
		httpTransport.reset()
		_, err := s.Invoke(context.Background(), &bindings.InvokeRequest{
			Data: []byte("hello world"),
			Metadata: map[string]string{
				hubKey: "testHub",
			},
		})

		require.NoError(t, err)
		actualAuthorization := httpTransport.request.Header.Get("Authorization")
		assert.NotEmpty(t, actualAuthorization)
		assert.Truef(t, strings.HasPrefix(actualAuthorization, "Bearer "), "expecting to start with 'Bearer ', but was '%s'", actualAuthorization)
	})

	tests := []struct {
		name              string
		hubInWriteRequest string
		hubInMetadata     string
		groupID           string
		userID            string
		expectedURL       string
	}{
		{"Broadcast receiving hub should call SignalR service", "testHub", "", "", "", "https://fake.service.signalr.net/api/hubs/testhub/:send?api-version=2022-11-01"},
		{"Broadcast with hub metadata should call SignalR service", "", "testHub", "", "", "https://fake.service.signalr.net/api/hubs/testhub/:send?api-version=2022-11-01"},
		{"Group receiving hub should call SignalR service", "testHub", "", "mygroup", "", "https://fake.service.signalr.net/api/hubs/testhub/groups/mygroup/:send?api-version=2022-11-01"},
		{"Group with hub metadata should call SignalR service", "", "testHub", "mygroup", "", "https://fake.service.signalr.net/api/hubs/testhub/groups/mygroup/:send?api-version=2022-11-01"},
		{"User receiving hub should call SignalR service", "testHub", "", "", "myuser", "https://fake.service.signalr.net/api/hubs/testhub/users/myuser/:send?api-version=2022-11-01"},
		{"User with hub metadata should call SignalR service", "", "testHub", "", "myuser", "https://fake.service.signalr.net/api/hubs/testhub/users/myuser/:send?api-version=2022-11-01"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			httpTransport.reset()
			s.hub = tt.hubInMetadata
			_, err := s.Invoke(context.Background(), &bindings.InvokeRequest{
				Data: []byte("hello world"),
				Metadata: map[string]string{
					hubKey:   tt.hubInWriteRequest,
					userKey:  tt.userID,
					groupKey: tt.groupID,
				},
			})

			require.NoError(t, err)
			assert.Equal(t, int32(1), httpTransport.requestCount)
			assert.Equal(t, tt.expectedURL, httpTransport.request.URL.String())
			assert.NotNil(t, httpTransport.request)
			assert.Equal(t, "application/json; charset=utf-8", httpTransport.request.Header.Get("Content-Type"))
		})
	}
}

func TestGetShouldSucceed(t *testing.T) {
	payload := map[string]string{
		"token": "ABCDEFG.ABC.ABC",
	}
	payloadBytes, _ := json.Marshal(payload)
	httpTransport := &mockTransport{
		response: &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewBuffer(payloadBytes))},
	}

	s := NewSignalR(logger.NewLogger("test")).(*SignalR)
	s.endpoint = "https://fake.service.signalr.net"
	s.httpClient = &http.Client{
		Transport: httpTransport,
	}

	t.Run("Can get negotiate response with accessKey", func(t *testing.T) {
		s.aadToken = nil
		s.accessKey = "AAbbcCsGEQKoLEH6oodDR0jK104Fu1c39Qgk+AA8D+M="
		res, err := s.Invoke(context.Background(), &bindings.InvokeRequest{
			Metadata: map[string]string{
				hubKey: "testHub",
			},
			Operation: "clientNegotiate",
		})

		require.NoError(t, err)
		// when it is accessKey mode, there is no outbound call
		assert.Equal(t, int32(0), httpTransport.requestCount)

		assert.Equal(t, "application/json", *res.ContentType)

		assert.NotNil(t, res.Data)
		var data map[string]string
		err = json.Unmarshal(res.Data, &data)
		require.NoError(t, err)
		assert.Equal(t, "https://fake.service.signalr.net/client/?hub=testhub", data["url"])
		accessToken := data["accessToken"]
		assert.NotNil(t, accessToken)
		claims, err := jwt.ParseString(accessToken, jwt.WithVerify(false))

		require.NoError(t, err)
		audience := claims.Audience()
		assert.Equal(t, []string{"https://fake.service.signalr.net/client/?hub=testhub"}, audience)
	})

	t.Run("Can get negotiate response with accessKey and userId", func(t *testing.T) {
		s.aadToken = nil
		s.accessKey = "AAbbcCsGEQKoLEH6oodDR0jK104Fu1c39Qgk+AA8D+M="
		res, err := s.Invoke(context.Background(), &bindings.InvokeRequest{
			Metadata: map[string]string{
				hubKey:  "testHub",
				userKey: "user1",
			},
			Operation: "clientNegotiate",
		})

		require.NoError(t, err)
		// when it is accessKey mode, there is no outbound call
		assert.Equal(t, int32(0), httpTransport.requestCount)

		assert.Equal(t, "application/json", *res.ContentType)

		assert.NotNil(t, res.Data)
		var data map[string]string
		err = json.Unmarshal(res.Data, &data)
		require.NoError(t, err)
		assert.Equal(t, "https://fake.service.signalr.net/client/?hub=testhub", data["url"])
		accessToken := data["accessToken"]
		assert.NotNil(t, accessToken)
		claims, err := jwt.ParseString(accessToken, jwt.WithVerify(false))

		require.NoError(t, err)
		audience := claims.Audience()
		assert.Equal(t, []string{"https://fake.service.signalr.net/client/?hub=testhub"}, audience)
		user := claims.Subject()
		assert.Equal(t, "user1", user)
	})

	t.Run("Can get negotiate response with aad token and userId", func(t *testing.T) {
		s.aadToken = &MockTokenCredential{
			AccessToken: "mock-access-token",
		}

		httpTransport.reset()
		res, err := s.Invoke(context.Background(), &bindings.InvokeRequest{
			Metadata: map[string]string{
				hubKey:  "testHub",
				userKey: "user?1&2",
			},
			Operation: "clientNegotiate",
		})

		require.NoError(t, err)
		// when it is accessKey mode, there is no outbound call

		assert.Equal(t, int32(1), httpTransport.requestCount)
		assert.Equal(t, "https://fake.service.signalr.net/api/hubs/testhub/:generateToken?api-version=2022-11-01&userId=user%3F1%262", httpTransport.request.URL.String())
		assert.NotNil(t, httpTransport.request)

		assert.Equal(t, "application/json", *res.ContentType)

		assert.NotNil(t, res.Data)
		var data map[string]string
		err = json.Unmarshal(res.Data, &data)
		require.NoError(t, err)
		assert.Equal(t, "https://fake.service.signalr.net/client/?hub=testhub", data["url"])
		accessToken := data["accessToken"]
		assert.Equal(t, "ABCDEFG.ABC.ABC", accessToken)
	})
}
