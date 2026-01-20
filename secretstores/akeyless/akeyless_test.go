package akeyless

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/akeylesslabs/akeyless-go/v5"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testAccessIDIAM = "p-xt3sT2nah7gpwm"
	testAccessIDJwt = "p-xt3sT2nah7gpom"
	testAccessIDKey = "p-xt3sT2nah7gpam"
	testAccessKey   = "ABCD1233xxx="
	// {
	// "sub": "1234567890",
	//	 "name": "John Doe",
	//	 "iat": 1516239022
	// }
	testJWT         = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QeJkP5vWKT_yUZJgIeUAnYw2brk"
	testSecretValue = "r3vE4L3D"
)

var (
	mockStaticSecretItem                 = "/static-secret-test"
	mockStaticSecretJSONItemName         = "/static-secret-json-test"
	mockStaticSecretPasswordItemName     = "/static-secret-password-test"
	mockDynamicSecretItemName            = "/dynamic-secret-test"
	mockRotatedSecretItemName            = "/rotated-secret-test"
	mockDescribeStaticSecretName         = "/path/to/akeyless" + mockStaticSecretItem
	mockDescribeStaticSecretType         = StaticSecretResponse
	mockDescribeStaticSecretItemResponse = akeyless.Item{
		ItemName:  &mockDescribeStaticSecretName,
		ItemType:  &mockDescribeStaticSecretType,
		IsEnabled: func(b bool) *bool { return &b }(true),
	}
	mockStaticSecretJSONName             = "/path/to/akeyless" + mockStaticSecretJSONItemName
	mockGetSingleSecretJSONValueResponse = map[string]map[string]string{
		mockStaticSecretJSONName: {
			"some": "json",
		},
	}
	mockStaticSecretJSONItemResponse = akeyless.Item{
		ItemName:  &mockStaticSecretJSONName,
		ItemType:  &mockDescribeStaticSecretType,
		IsEnabled: func(b bool) *bool { return &b }(true),
	}
	mockStaticSecretPasswordName             = "/path/to/akeyless" + mockStaticSecretPasswordItemName
	mockGetSingleSecretPasswordValueResponse = map[string]map[string]string{
		mockStaticSecretPasswordName: {
			"password": testSecretValue,
			"username": "akeyless",
		},
	}
	mockDescribeDynamicSecretName         = "/path/to/akeyless" + mockDynamicSecretItemName
	mockDescribeDynamicSecretType         = DynamicSecretResponse
	mockDescribeDynamicSecretItemResponse = akeyless.Item{
		ItemName:  &mockDescribeDynamicSecretName,
		ItemType:  &mockDescribeDynamicSecretType,
		IsEnabled: func(b bool) *bool { return &b }(true),
		ItemGeneralInfo: &akeyless.ItemGeneralInfo{
			DynamicSecretProducerDetails: &akeyless.DynamicSecretProducerInfo{
				ProducerStatus: func(s string) *string { return &s }("ProducerConnected"),
			},
		},
	}
	mockGetSingleDynamicSecretValueResponse = map[string]interface{}{
		"value": "{\"user\":\"generated_username\",\"password\":\"generated_password\",\"ttl_in_minutes\":\"60\",\"id\":\"username\"}",
		"error": "",
	}
	mockDescribeRotatedSecretName         = "/path/to/akeyless" + mockRotatedSecretItemName
	mockDescribeRotatedSecretType         = RotatedSecretResponse
	mockDescribeRotatedSecretItemResponse = akeyless.Item{
		ItemName:  &mockDescribeRotatedSecretName,
		ItemType:  &mockDescribeRotatedSecretType,
		IsEnabled: func(b bool) *bool { return &b }(true),
		ItemGeneralInfo: &akeyless.ItemGeneralInfo{
			RotatedSecretDetails: &akeyless.RotatedSecretDetailsInfo{
				RotatorStatus: func(s string) *string { return &s }("RotationSucceeded"),
			},
		},
	}
	mockGetSingleRotatedSecretValueResponse = map[string]interface{}{
		"value": map[string]interface{}{
			"username":       "abcdefghijklmnopqrstuvwxyz",
			"password":       testSecretValue,
			"application_id": "1234567890",
		},
	}
)

var mockGetSingleSecretValueResponse = map[string]string{
	mockDescribeStaticSecretName: testSecretValue,
}

// Global mock server for all tests
var mockGateway *httptest.Server

// mockAuthenticate is a test version of the Authenticate function that uses a mock cloud ID
func mockAuthenticate(metadata *akeylessMetadata, akeylessSecretStore *akeylessSecretStore) error {
	// Initialize closeCh if not already set
	if akeylessSecretStore.closeCh == nil {
		akeylessSecretStore.closeCh = make(chan struct{})
	}

	authRequest := akeyless.NewAuth()
	authRequest.SetAccessId(metadata.AccessID)

	authRequest.SetAccessKey(metadata.AccessKey)

	config := akeyless.NewConfiguration()
	config.Servers = []akeyless.ServerConfiguration{
		{
			URL: metadata.GatewayURL,
		},
	}
	config.UserAgent = UserAgent
	config.AddDefaultHeader("akeylessclienttype", UserAgent)

	akeylessSecretStore.v2 = akeyless.NewAPIClient(config).V2Api

	out, _, err := akeylessSecretStore.v2.Auth(context.TODO()).Body(*authRequest).Execute()
	if err != nil {
		return fmt.Errorf("failed to authenticate with Akeyless: %w", err)
	}

	akeylessSecretStore.mu.Lock()
	akeylessSecretStore.token = out.GetToken()
	expirationStr := out.GetExpiration()
	akeylessSecretStore.mu.Unlock()

	// Parse and store expiration time (same as in authenticate)
	if expirationStr != "" {
		expiration, err := parseTokenExpirationDate(expirationStr)
		if err != nil {
			// Log warning but don't fail - expiration parsing is optional
			akeylessSecretStore.logger.Debugf("failed to parse token expiration '%s': %v", expirationStr, err)
		} else {
			akeylessSecretStore.mu.Lock()
			akeylessSecretStore.tokenExpiry = expiration
			akeylessSecretStore.mu.Unlock()
		}
	}

	return nil
}

// TestMain sets up and tears down the mock server for all tests
func TestMain(m *testing.M) {
	// Setup mock server that returns an *akeyless.AuthOutput
	mockGateway = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Handle different endpoints
		switch r.URL.Path {
		case "/auth":
			// Return a proper AuthOutput JSON response for authentication
			authOutput := akeyless.NewAuthOutput()
			authOutput.SetToken("t-1234567890")
			// Use a future expiration date (1 hour from now) to avoid token refresh during tests
			futureExpiration := time.Now().Add(1 * time.Hour).Format(time.RFC3339)
			authOutput.SetExpiration(futureExpiration)
			jsonResponse, _ := json.Marshal(authOutput)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)
		// Single static secret value
		case "/get-secret-value":
			jsonResponse, _ := json.Marshal(mockGetSingleSecretValueResponse)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)
		case "/get-rotated-secret-value":
			jsonResponse, _ := json.Marshal(&mockGetSingleRotatedSecretValueResponse)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)
		case "/list-items":
			listItemsResponse := akeyless.NewListItemsInPathOutput()
			listItemsResponse.SetItems(
				[]akeyless.Item{mockDescribeStaticSecretItemResponse},
			)
			jsonResponse, _ := json.Marshal(listItemsResponse)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)
		case "/describe-item":
			jsonResponse, _ := json.Marshal(mockDescribeStaticSecretItemResponse)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)
		default:
			// Default response for any other endpoint
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"message": "mock response"}`))
		}
	}))

	// Run tests
	code := m.Run()

	// Exit with the same code as the tests
	os.Exit(code)
}

func TestNewAkeylessSecretStore(t *testing.T) {
	log := logger.NewLogger("test")
	store := NewAkeylessSecretStore(log)
	assert.NotNil(t, store)
}

func TestInit(t *testing.T) {
	tests := []struct {
		name        string
		metadata    secretstores.Metadata
		expectError bool
	}{
		{
			name: "gw, access id and key",
			metadata: secretstores.Metadata{
				Base: metadata.Base{
					Properties: map[string]string{
						"accessId":   testAccessIDKey,
						"accessKey":  testAccessKey,
						"gatewayUrl": mockGateway.URL,
					},
				},
			},
			expectError: false,
		},
		{
			name: "gw, access id and jwt",
			metadata: secretstores.Metadata{
				Base: metadata.Base{
					Properties: map[string]string{
						"accessId":   testAccessIDJwt,
						"jwt":        testJWT,
						"gatewayUrl": mockGateway.URL,
					},
				},
			},
			expectError: false,
		},
		{
			name: "gw, access id (aws_iam)",
			metadata: secretstores.Metadata{
				Base: metadata.Base{
					Properties: map[string]string{
						"accessId":   testAccessIDIAM,
						"gatewayUrl": mockGateway.URL,
					},
				},
			},
			expectError: false,
		},
		{
			name: "missing access id",
			metadata: secretstores.Metadata{
				Base: metadata.Base{
					Properties: map[string]string{
						"gatewayUrl": mockGateway.URL,
					},
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := logger.NewLogger("test")
			store := NewAkeylessSecretStore(log).(*akeylessSecretStore)
			defer store.Close() // Clean up background goroutine

			tt.metadata.Properties["gatewayUrl"] = mockGateway.URL

			// For AWS IAM test, use mock authentication to avoid AWS dependency
			if tt.name == "gw, access id (aws_iam)" {
				// Parse metadata first
				m, err := store.parseMetadata(tt.metadata)
				require.NoError(t, err)

				// Use mock authentication instead of the real one
				err = mockAuthenticate(m, store)
				if tt.expectError {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.NotNil(t, store.v2)
					assert.NotNil(t, store.token)
				}
			} else {
				// Use normal Init for other test cases
				err := store.Init(t.Context(), tt.metadata)
				if tt.expectError {
					require.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.NotNil(t, store.v2)
					assert.NotNil(t, store.token)
				}
			}
		})
	}
}

func TestGetSecretWithoutInit(t *testing.T) {
	log := logger.NewLogger("test")
	store := NewAkeylessSecretStore(log).(*akeylessSecretStore)

	req := secretstores.GetSecretRequest{
		Name: "test-secret",
	}

	_, err := store.GetSecret(t.Context(), req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not initialized")
}

func TestBulkGetSecretWithoutInit(t *testing.T) {
	log := logger.NewLogger("test")
	store := NewAkeylessSecretStore(log).(*akeylessSecretStore)

	req := secretstores.BulkGetSecretRequest{}

	_, err := store.BulkGetSecret(t.Context(), req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not initialized")
}

func TestFeatures(t *testing.T) {
	log := logger.NewLogger("test")
	store := NewAkeylessSecretStore(log)

	features := store.Features()
	assert.Empty(t, features)
}

func TestClose(t *testing.T) {
	log := logger.NewLogger("test")
	store := NewAkeylessSecretStore(log)

	err := store.Close()
	assert.NoError(t, err)
}

func TestParseMetadata(t *testing.T) {
	tests := []struct {
		name        string
		properties  map[string]string
		expectError bool
		expected    *akeylessMetadata
	}{
		{
			name: "valid metadata with access id and key",
			properties: map[string]string{
				"accessId":  testAccessIDKey,
				"accessKey": testAccessKey,
			},
			expectError: false,
			expected: &akeylessMetadata{
				AccessID:   testAccessIDKey,
				AccessKey:  testAccessKey,
				GatewayURL: "https://api.akeyless.io", // Default gateway URL
			},
		},
		{
			name: "valid metadata with access id and jwt",
			properties: map[string]string{
				"accessId":   testAccessIDJwt,
				"jwt":        testJWT,
				"gatewayUrl": mockGateway.URL,
			},
			expectError: false,
			expected: &akeylessMetadata{
				AccessID:   testAccessIDJwt,
				JWT:        testJWT,
				GatewayURL: mockGateway.URL,
			},
		},
		{
			name: "valid metadata with access id aws_iam",
			properties: map[string]string{
				"accessId":   testAccessIDIAM,
				"gatewayUrl": mockGateway.URL,
			},
			expectError: false,
			expected: &akeylessMetadata{
				AccessID:   testAccessIDIAM,
				GatewayURL: mockGateway.URL,
			},
		},
		{
			name: "missing access id",
			properties: map[string]string{
				"gatewayUrl": mockGateway.URL,
			},
			expectError: true,
		},
		{
			name: "invalid gateway url",
			properties: map[string]string{
				"gatewayUrl": "http:/invalidaddress",
			},
			expectError: true,
		},
		{
			name: "invalid access id format",
			properties: map[string]string{
				"accessId": "invalid",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := logger.NewLogger("test")
			store := NewAkeylessSecretStore(log).(*akeylessSecretStore)

			meta := secretstores.Metadata{
				Base: metadata.Base{
					Properties: tt.properties,
				},
			}

			result, err := store.parseMetadata(meta)
			if tt.expectError {
				require.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestMockServerReturnsAuthOutput(t *testing.T) {
	// Test that the mock server properly returns an AuthOutput response
	store := NewAkeylessSecretStore(logger.NewLogger("test")).(*akeylessSecretStore)

	// Test with access key authentication
	meta := secretstores.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"accessId":   testAccessIDKey,
				"accessKey":  testAccessKey,
				"gatewayUrl": mockGateway.URL,
			},
		},
	}

	err := store.Init(t.Context(), meta)
	assert.NoError(t, err)
	assert.NotNil(t, store.v2)
	assert.NotNil(t, store.token)
	assert.Equal(t, "t-1234567890", store.token)
	defer store.Close() // Clean up background goroutine
}

func TestMockAWSCloudID(t *testing.T) {
	// Test that the mock AWS cloud ID works correctly
	store := NewAkeylessSecretStore(logger.NewLogger("test")).(*akeylessSecretStore)

	// Test with AWS IAM authentication using mock cloud ID
	meta := secretstores.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"accessId":   testAccessIDIAM,
				"gatewayUrl": mockGateway.URL,
			},
		},
	}

	// Parse metadata first
	m, err := store.parseMetadata(meta)
	require.NoError(t, err)

	// Use mock authentication with mock cloud ID
	err = mockAuthenticate(m, store)
	assert.NoError(t, err)
	assert.NotNil(t, store.v2)
	assert.NotNil(t, store.token)
	assert.Equal(t, "t-1234567890", store.token)
}

func TestGetSecret(t *testing.T) {
	// Setup a properly initialized store
	store := NewAkeylessSecretStore(logger.NewLogger("test")).(*akeylessSecretStore)
	meta := secretstores.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"accessId":   testAccessIDKey,
				"accessKey":  testAccessKey,
				"gatewayUrl": mockGateway.URL,
			},
		},
	}

	err := store.Init(t.Context(), meta)
	require.NoError(t, err)
	defer store.Close() // Clean up background goroutine

	tests := []struct {
		name           string
		request        secretstores.GetSecretRequest
		expectError    bool
		expectedSecret string
	}{
		{
			name: "test text single static secret",
			request: secretstores.GetSecretRequest{
				Name: mockDescribeStaticSecretName,
			},
			expectError:    false,
			expectedSecret: testSecretValue,
		},
		// {
		// 	name: "get non-existing secret",
		// 	request: secretstores.GetSecretRequest{
		// 		Name: mockDescribeStaticSecretName,
		// 	},
		// 	expectError:    true,
		// 	expectedSecret: "",
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response, err := store.GetSecret(t.Context(), tt.request)
			if tt.expectError {
				assert.Error(t, err)
				assert.Empty(t, response.Data)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, response.Data)
				assert.Contains(t, response.Data, tt.request.Name)
				assert.Equal(t, tt.expectedSecret, response.Data[tt.request.Name])
			}
		})
	}
}

func TestGetSingleSecretJSON(t *testing.T) {
	mockGateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Handle different endpoints
		switch r.URL.Path {
		case "/auth", "/v2/auth":
			// Return a proper AuthOutput JSON response for authentication
			authOutput := akeyless.NewAuthOutput()
			authOutput.SetToken("t-1234567890")
			// Use a future expiration date (1 hour from now) to avoid token refresh during tests
			futureExpiration := time.Now().Add(1 * time.Hour).Format(time.RFC3339)
			authOutput.SetExpiration(futureExpiration)
			jsonResponse, _ := json.Marshal(authOutput)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)
		// Single static secret value
		case "/get-secret-value":
			jsonResponse, _ := json.Marshal(&mockGetSingleSecretJSONValueResponse)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)
		case "/describe-item":
			mockDescribeItemResponse := akeyless.Item{
				ItemName: &mockStaticSecretJSONName,
				ItemType: &mockDescribeStaticSecretType,
			}
			jsonResponse, _ := json.Marshal(&mockDescribeItemResponse)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)
		default:
			// Default response for any other endpoint
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"message": "mock response"}`))
		}
	}))

	store := NewAkeylessSecretStore(logger.NewLogger("test")).(*akeylessSecretStore)
	meta := secretstores.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"accessId":   testAccessIDKey,
				"accessKey":  testAccessKey,
				"gatewayUrl": mockGateway.URL,
			},
		},
	}

	err := store.Init(t.Context(), meta)
	require.NoError(t, err)
	defer store.Close() // Clean up background goroutine

	response, err := store.GetSecret(t.Context(), secretstores.GetSecretRequest{
		Name: mockStaticSecretJSONName,
	})
	require.NoError(t, err)
	assert.NotNil(t, response.Data)
	assert.Contains(t, response.Data, mockStaticSecretJSONName)
	assert.JSONEq(t, "{\"some\":\"json\"}", response.Data[mockStaticSecretJSONName])

	mockGateway.Close()
}

func TestGetSingleSecretPassword(t *testing.T) {
	mockGateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Handle different endpoints
		switch r.URL.Path {
		case "/auth", "/v2/auth":
			// Return a proper AuthOutput JSON response for authentication
			authOutput := akeyless.NewAuthOutput()
			authOutput.SetToken("t-1234567890")
			// Use a future expiration date (1 hour from now) to avoid token refresh during tests
			futureExpiration := time.Now().Add(1 * time.Hour).Format(time.RFC3339)
			authOutput.SetExpiration(futureExpiration)
			jsonResponse, _ := json.Marshal(authOutput)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)
		// Single static secret value
		case "/get-secret-value":
			jsonResponse, _ := json.Marshal(&mockGetSingleSecretPasswordValueResponse)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)
		case "/describe-item":
			mockDescribeItemResponse := akeyless.Item{
				ItemName: &mockStaticSecretPasswordName,
				ItemType: &mockDescribeStaticSecretType,
			}
			jsonResponse, _ := json.Marshal(&mockDescribeItemResponse)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)
		default:
			// Default response for any other endpoint
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"message": "mock response"}`))
		}
	}))

	store := NewAkeylessSecretStore(logger.NewLogger("test")).(*akeylessSecretStore)
	meta := secretstores.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"accessId":   testAccessIDKey,
				"accessKey":  testAccessKey,
				"gatewayUrl": mockGateway.URL,
			},
		},
	}

	err := store.Init(t.Context(), meta)
	require.NoError(t, err)
	defer store.Close() // Clean up background goroutine

	response, err := store.GetSecret(t.Context(), secretstores.GetSecretRequest{
		Name: mockStaticSecretPasswordName,
	})
	require.NoError(t, err)
	assert.NotNil(t, response.Data)
	assert.Contains(t, response.Data, mockStaticSecretPasswordName)
	assert.JSONEq(t, "{\"password\":\"r3vE4L3D\",\"username\":\"akeyless\"}", response.Data[mockStaticSecretPasswordName])

	mockGateway.Close()
}

// Test GetSecretType functions
func TestGetSecretType(t *testing.T) {
	// Test GetSecretType
	store := NewAkeylessSecretStore(logger.NewLogger("test")).(*akeylessSecretStore)
	meta := secretstores.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"accessId":   testAccessIDKey,
				"accessKey":  testAccessKey,
				"gatewayUrl": mockGateway.URL,
			},
		},
	}

	ctx := t.Context()
	err := store.Init(ctx, meta)
	require.NoError(t, err)
	defer store.Close() // Clean up background goroutine

	secretType, err := store.getSecretType(ctx, mockDescribeStaticSecretName)
	assert.NoError(t, err)
	assert.Equal(t, StaticSecretResponse, secretType)
}

func TestGetSingleDynamicSecret(t *testing.T) {

	var mockGateway = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Handle different endpoints
		switch r.URL.Path {
		case "/auth":
			// Return a proper AuthOutput JSON response for authentication
			authOutput := akeyless.NewAuthOutput()
			authOutput.SetToken("t-1234567890")
			// Use a future expiration date (1 hour from now) to avoid token refresh during tests
			futureExpiration := time.Now().Add(1 * time.Hour).Format(time.RFC3339)
			authOutput.SetExpiration(futureExpiration)
			jsonResponse, _ := json.Marshal(authOutput)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)
		// Single dynamic secret value
		case "/get-dynamic-secret-value":
			jsonResponse, _ := json.Marshal(&mockGetSingleDynamicSecretValueResponse)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)
		case "/describe-item":
			jsonResponse, _ := json.Marshal(&mockDescribeDynamicSecretItemResponse)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)
		default:
			// Default response for any other endpoint
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"message": "mock response"}`))
		}
	}))
	// Test GetSingleDynamicSecret
	store := NewAkeylessSecretStore(logger.NewLogger("test")).(*akeylessSecretStore)
	meta := secretstores.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"accessId":   testAccessIDKey,
				"accessKey":  testAccessKey,
				"gatewayUrl": mockGateway.URL,
			},
		},
	}

	ctx := t.Context()
	err := store.Init(ctx, meta)
	require.NoError(t, err)
	defer store.Close() // Clean up background goroutine

	secretValue, err := store.getSingleSecretValue(ctx, mockDescribeDynamicSecretName, DynamicSecretResponse)
	require.NoError(t, err)
	assert.Equal(t, "{\"user\":\"generated_username\",\"password\":\"generated_password\",\"ttl_in_minutes\":\"60\",\"id\":\"username\"}", secretValue)
	mockGateway.Close()
}
func TestGetSingleRotatedSecret(t *testing.T) {
	var mockGateway = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Handle different endpoints
		switch r.URL.Path {
		case "/auth":
			// Return a proper AuthOutput JSON response for authentication
			authOutput := akeyless.NewAuthOutput()
			authOutput.SetToken("t-1234567890")
			// Use a future expiration date (1 hour from now) to avoid token refresh during tests
			futureExpiration := time.Now().Add(1 * time.Hour).Format(time.RFC3339)
			authOutput.SetExpiration(futureExpiration)
			jsonResponse, _ := json.Marshal(authOutput)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)
		// Single dynamic secret value
		case "/get-rotated-secret-value":
			jsonResponse, _ := json.Marshal(&mockGetSingleRotatedSecretValueResponse)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)
		case "/describe-item":
			jsonResponse, _ := json.Marshal(&mockDescribeRotatedSecretItemResponse)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)
		default:
			// Default response for any other endpoint
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"message": "mock response"}`))
		}
	}))
	// Test GetSingleRotatedSecret
	store := NewAkeylessSecretStore(logger.NewLogger("test")).(*akeylessSecretStore)
	meta := secretstores.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"accessId":   testAccessIDKey,
				"accessKey":  testAccessKey,
				"gatewayUrl": mockGateway.URL,
			},
		},
	}

	ctx := t.Context()
	err := store.Init(ctx, meta)
	require.NoError(t, err)
	defer store.Close() // Clean up background goroutine

	secretValue, err := store.getSingleSecretValue(ctx, mockDescribeRotatedSecretName, RotatedSecretResponse)
	assert.NoError(t, err)
	assert.Equal(t, "{\"value\":{\"application_id\":\"1234567890\",\"password\":\"r3vE4L3D\",\"username\":\"abcdefghijklmnopqrstuvwxyz\"}}", secretValue)

	mockGateway.Close()
}

func TestGetBulkSecretValues(t *testing.T) {

	var mockGateway = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Handle different endpoints
		switch r.URL.Path {
		case "/auth":
			// Return a proper AuthOutput JSON response for authentication
			authOutput := akeyless.NewAuthOutput()
			authOutput.SetToken("t-1234567890")
			// Use a future expiration date (1 hour from now) to avoid token refresh during tests
			futureExpiration := time.Now().Add(1 * time.Hour).Format(time.RFC3339)
			authOutput.SetExpiration(futureExpiration)
			jsonResponse, _ := json.Marshal(authOutput)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)

		case "/get-secret-value":
			secretValue := map[string]string{
				mockStaticSecretItem:         testSecretValue,
				mockStaticSecretJSONItemName: "{\"some\":\"json\"}",
			}
			jsonResponse, _ := json.Marshal(&secretValue)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)

		case "/list-items":
			items := akeyless.NewListItemsInPathOutput()
			items.SetItems(
				[]akeyless.Item{
					mockDescribeStaticSecretItemResponse,
					mockStaticSecretJSONItemResponse,
					mockDescribeDynamicSecretItemResponse,
					mockDescribeRotatedSecretItemResponse,
				},
			)
			jsonResponse, _ := json.Marshal(&items)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)
		// Single dynamic secret value
		case "/get-dynamic-secret-value":
			jsonResponse, _ := json.Marshal(&mockGetSingleDynamicSecretValueResponse)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)

		case "/get-rotated-secret-value":
			jsonResponse, _ := json.Marshal(&mockGetSingleRotatedSecretValueResponse)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)

		default:
			// Default response for any other endpoint
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"message": "mock response"}`))
		}
	}))

	store := NewAkeylessSecretStore(logger.NewLogger("test")).(*akeylessSecretStore)
	meta := secretstores.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"accessId":   testAccessIDKey,
				"accessKey":  testAccessKey,
				"gatewayUrl": mockGateway.URL,
			},
		},
	}

	err := store.Init(t.Context(), meta)
	require.NoError(t, err)
	defer store.Close() // Clean up background goroutine

	response, err := store.BulkGetSecret(t.Context(), secretstores.BulkGetSecretRequest{})
	require.NoError(t, err)
	assert.NotNil(t, response.Data)

	// Check that we got all 4 secrets (excluding any empty keys)
	nonEmptySecrets := 0
	for key, value := range response.Data {
		if key != "" && len(value) > 0 {
			nonEmptySecrets++
		}
	}
	assert.Equal(t, 4, nonEmptySecrets)

	// Check static secret (text) - using the actual key from the response
	staticSecretKey := "/static-secret-test"
	assert.Contains(t, response.Data, staticSecretKey)
	assert.Equal(t, testSecretValue, response.Data[staticSecretKey][staticSecretKey])

	// Check static secret (JSON)
	jsonSecretKey := "/static-secret-json-test"
	assert.Contains(t, response.Data, jsonSecretKey)
	assert.JSONEq(t, "{\"some\":\"json\"}", response.Data[jsonSecretKey][jsonSecretKey])

	// Check dynamic secret
	dynamicSecretKey := "/path/to/akeyless/dynamic-secret-test" //nolint:gosec // G101: test data only
	assert.Contains(t, response.Data, dynamicSecretKey)
	expectedDynamicValue := "{\"user\":\"generated_username\",\"password\":\"generated_password\",\"ttl_in_minutes\":\"60\",\"id\":\"username\"}"
	assert.Equal(t, expectedDynamicValue, response.Data[dynamicSecretKey][dynamicSecretKey])

	// Check rotated secret
	rotatedSecretKey := "/path/to/akeyless/rotated-secret-test" //nolint:gosec // G101: test data only
	assert.Contains(t, response.Data, rotatedSecretKey)
	assert.Equal(t, "{\"value\":{\"application_id\":\"1234567890\",\"password\":\"r3vE4L3D\",\"username\":\"abcdefghijklmnopqrstuvwxyz\"}}", response.Data[rotatedSecretKey][rotatedSecretKey])

	mockGateway.Close()
}

func TestGetBulkSecretValuesFromDifferentPaths(t *testing.T) {
	// Test recursive secret retrieval from different hierarchical paths
	// This test simulates a folder structure where:
	// - Root "/" contains 4 subfolders
	// - Each subfolder contains different types of secrets
	// - The listItemsRecursively method should traverse all folders

	// Define mock secrets for different paths
	staticSecret1 := "/path/to/static/secrets/secret1"
	staticSecret2 := "/path/to/static/secrets/secret2"
	staticSecret3 := "/path/to/static/secrets/secret3"
	dynamicSecret1 := "/path/to/dynamic/secrets/dynamic1"
	dynamicSecret2 := "/path/to/dynamic/secrets/dynamic2"
	rotatedSecret1 := "/path/to/rotated/secrets/rotated1"
	mixedStaticSecret := "/path/to/mixed/secrets/mixed-static" //nolint:gosec // G101: test data only
	mixedDynamicSecret := "/path/to/mixed/secrets/mixed-dynamic"
	mixedRotatedSecret := "/path/to/mixed/secrets/mixed-rotated"

	// Create mock items for different paths
	staticItem1 := akeyless.Item{
		ItemName:  &staticSecret1,
		ItemType:  &mockDescribeStaticSecretType,
		IsEnabled: func(b bool) *bool { return &b }(true),
	}
	staticItem2 := akeyless.Item{
		ItemName:  &staticSecret2,
		ItemType:  &mockDescribeStaticSecretType,
		IsEnabled: func(b bool) *bool { return &b }(true),
	}
	staticItem3 := akeyless.Item{
		ItemName:  &staticSecret3,
		ItemType:  &mockDescribeStaticSecretType,
		IsEnabled: func(b bool) *bool { return &b }(true),
	}
	dynamicItem1 := akeyless.Item{
		ItemName:  &dynamicSecret1,
		ItemType:  &mockDescribeDynamicSecretType,
		IsEnabled: func(b bool) *bool { return &b }(true),
	}
	dynamicItem2 := akeyless.Item{
		ItemName:  &dynamicSecret2,
		ItemType:  &mockDescribeDynamicSecretType,
		IsEnabled: func(b bool) *bool { return &b }(true),
	}
	rotatedItem1 := akeyless.Item{
		ItemName:  &rotatedSecret1,
		ItemType:  &mockDescribeRotatedSecretType,
		IsEnabled: func(b bool) *bool { return &b }(true),
	}
	mixedStaticItem := akeyless.Item{
		ItemName:  &mixedStaticSecret,
		ItemType:  &mockDescribeStaticSecretType,
		IsEnabled: func(b bool) *bool { return &b }(true),
	}
	mixedDynamicItem := akeyless.Item{
		ItemName:  &mixedDynamicSecret,
		ItemType:  &mockDescribeDynamicSecretType,
		IsEnabled: func(b bool) *bool { return &b }(true),
	}
	mixedRotatedItem := akeyless.Item{
		ItemName:  &mixedRotatedSecret,
		ItemType:  &mockDescribeRotatedSecretType,
		IsEnabled: func(b bool) *bool { return &b }(true),
	}

	var mockGateway = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Handle different endpoints
		switch r.URL.Path {
		case "/auth":
			// Return a proper AuthOutput JSON response for authentication
			authOutput := akeyless.NewAuthOutput()
			authOutput.SetToken("t-1234567890")
			// Use a future expiration date (1 hour from now) to avoid token refresh during tests
			futureExpiration := time.Now().Add(1 * time.Hour).Format(time.RFC3339)
			authOutput.SetExpiration(futureExpiration)
			jsonResponse, _ := json.Marshal(authOutput)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)

		case "/get-secret-value":
			secretValue := map[string]string{
				staticSecret1:     testSecretValue,
				staticSecret2:     "static-secret-2-value",
				staticSecret3:     "static-secret-3-value",
				mixedStaticSecret: "mixed-static-secret-value",
			}
			jsonResponse, _ := json.Marshal(&secretValue)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)

		case "/list-items":
			// Parse the path from request body to determine what to return
			body, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`{"message": "failed to read request body"}`))
				return
			}

			var listItemsRequest akeyless.ListItems
			if err := json.Unmarshal(body, &listItemsRequest); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`{"message": "failed to parse request body"}`))
				return
			}

			path := ""
			if listItemsRequest.Path != nil {
				path = *listItemsRequest.Path
			}
			// Debug: Uncomment to see recursive calls
			// fmt.Printf("DEBUG: list-items called for path: '%s'\n", path)

			var items akeyless.ListItemsInPathOutput

			switch path {
			case "/":
				// Root path returns only folders, no items
				folders := []string{
					"/path/to/static/secrets",
					"/path/to/dynamic/secrets",
					"/path/to/rotated/secrets",
					"/path/to/mixed/secrets",
				}
				items.SetFolders(folders)
				items.SetItems([]akeyless.Item{})

			case "/path/to/static/secrets":
				// Static secrets folder
				items.SetItems([]akeyless.Item{staticItem1, staticItem2, staticItem3})
				items.SetFolders([]string{})

			case "/path/to/dynamic/secrets":
				// Dynamic secrets folder
				items.SetItems([]akeyless.Item{dynamicItem1, dynamicItem2})
				items.SetFolders([]string{})

			case "/path/to/rotated/secrets":
				// Rotated secrets folder
				items.SetItems([]akeyless.Item{rotatedItem1})
				items.SetFolders([]string{})

			case "/path/to/mixed/secrets":
				// Mixed secrets folder
				items.SetItems([]akeyless.Item{mixedStaticItem, mixedDynamicItem, mixedRotatedItem})
				items.SetFolders([]string{})

			default:
				// Unknown path
				items.SetItems([]akeyless.Item{})
				items.SetFolders([]string{})
			}

			jsonResponse, _ := json.Marshal(&items)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)

		case "/get-dynamic-secret-value":
			// Create dynamic secret responses for each secret
			dynamicSecretResponse := map[string]interface{}{
				"value": "{\"user\":\"dynamic-secret-1\",\"password\":\"dynamic-secret-1-value\",\"ttl_in_minutes\":\"60\",\"id\":\"dynamic-secret-1\"}",
				"error": "",
			}
			jsonResponse, _ := json.Marshal(&dynamicSecretResponse)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)

		case "/get-rotated-secret-value":
			// Create rotated secret response
			rotatedSecretResponse := map[string]interface{}{
				"value": map[string]interface{}{
					"username":       "rotated-user",
					"password":       "rotated-secret-1-value",
					"application_id": "1234567890",
				},
			}
			jsonResponse, _ := json.Marshal(&rotatedSecretResponse)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)

		case "/describe-item":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`{"message": "failed to read request body"}`))
				return
			}

			var describeItemRequest akeyless.DescribeItem
			if err := json.Unmarshal(body, &describeItemRequest); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`{"message": "failed to parse request body"}`))
				return
			}

			var itemResponse akeyless.Item
			switch describeItemRequest.Name {
			case staticSecret1, staticSecret2, staticSecret3, mixedStaticSecret:
				itemResponse = akeyless.Item{
					ItemName:  &describeItemRequest.Name,
					ItemType:  &mockDescribeStaticSecretType,
					IsEnabled: func(b bool) *bool { return &b }(true),
				}
			case dynamicSecret1, dynamicSecret2, mixedDynamicSecret:
				itemResponse = akeyless.Item{
					ItemName:  &describeItemRequest.Name,
					ItemType:  &mockDescribeDynamicSecretType,
					IsEnabled: func(b bool) *bool { return &b }(true),
					ItemGeneralInfo: &akeyless.ItemGeneralInfo{
						DynamicSecretProducerDetails: &akeyless.DynamicSecretProducerInfo{
							ProducerStatus: func(s string) *string { return &s }("ProducerConnected"),
						},
					},
				}
			case rotatedSecret1, mixedRotatedSecret:
				itemResponse = akeyless.Item{
					ItemName:  &describeItemRequest.Name,
					ItemType:  &mockDescribeRotatedSecretType,
					IsEnabled: func(b bool) *bool { return &b }(true),
					ItemGeneralInfo: &akeyless.ItemGeneralInfo{
						RotatedSecretDetails: &akeyless.RotatedSecretDetailsInfo{
							RotatorStatus: func(s string) *string { return &s }("RotationSucceeded"),
						},
					},
				}
			default:
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`{"message": "invalid item name"}`))
				return
			}

			jsonResponse, _ := json.Marshal(&itemResponse)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)

		default:
			// Default response for any other endpoint
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"message": "mock response"}`))
		}
	}))

	store := NewAkeylessSecretStore(logger.NewLogger("test")).(*akeylessSecretStore)
	meta := secretstores.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"accessId":   testAccessIDKey,
				"accessKey":  testAccessKey,
				"gatewayUrl": mockGateway.URL,
			},
		},
	}

	err := store.Init(t.Context(), meta)
	require.NoError(t, err)
	defer store.Close() // Clean up background goroutine

	response, err := store.BulkGetSecret(t.Context(), secretstores.BulkGetSecretRequest{})
	require.NoError(t, err)
	assert.NotNil(t, response.Data)

	// Check that we got all 9 secrets (4 static, 3 dynamic, 2 rotated)
	nonEmptySecrets := 0
	for key, value := range response.Data {
		if key != "" && len(value) > 0 {
			nonEmptySecrets++
		}
	}
	assert.Equal(t, 9, nonEmptySecrets)

	// Check static secrets from /path/to/static/secrets
	assert.Contains(t, response.Data, staticSecret1)
	assert.Equal(t, testSecretValue, response.Data[staticSecret1][staticSecret1])
	assert.Contains(t, response.Data, staticSecret2)
	assert.Equal(t, "static-secret-2-value", response.Data[staticSecret2][staticSecret2])
	assert.Contains(t, response.Data, staticSecret3)
	assert.Equal(t, "static-secret-3-value", response.Data[staticSecret3][staticSecret3])

	// Check dynamic secrets from /path/to/dynamic/secrets
	assert.Contains(t, response.Data, dynamicSecret1)
	expectedDynamicValue1 := "{\"user\":\"dynamic-secret-1\",\"password\":\"dynamic-secret-1-value\",\"ttl_in_minutes\":\"60\",\"id\":\"dynamic-secret-1\"}"
	assert.Equal(t, expectedDynamicValue1, response.Data[dynamicSecret1][dynamicSecret1])
	assert.Contains(t, response.Data, dynamicSecret2)
	expectedDynamicValue2 := "{\"user\":\"dynamic-secret-1\",\"password\":\"dynamic-secret-1-value\",\"ttl_in_minutes\":\"60\",\"id\":\"dynamic-secret-1\"}"
	assert.Equal(t, expectedDynamicValue2, response.Data[dynamicSecret2][dynamicSecret2])

	// Check rotated secret from /path/to/rotated/secrets
	assert.Contains(t, response.Data, rotatedSecret1)
	expectedRotatedValue1 := "{\"value\":{\"application_id\":\"1234567890\",\"password\":\"rotated-secret-1-value\",\"username\":\"rotated-user\"}}"
	assert.Equal(t, expectedRotatedValue1, response.Data[rotatedSecret1][rotatedSecret1])

	// Check mixed secrets from /path/to/mixed/secrets
	assert.Contains(t, response.Data, mixedStaticSecret)
	assert.Equal(t, "mixed-static-secret-value", response.Data[mixedStaticSecret][mixedStaticSecret])
	assert.Contains(t, response.Data, mixedDynamicSecret)
	expectedMixedDynamicValue := "{\"user\":\"dynamic-secret-1\",\"password\":\"dynamic-secret-1-value\",\"ttl_in_minutes\":\"60\",\"id\":\"dynamic-secret-1\"}"
	assert.Equal(t, expectedMixedDynamicValue, response.Data[mixedDynamicSecret][mixedDynamicSecret])
	assert.Contains(t, response.Data, mixedRotatedSecret)
	expectedMixedRotatedValue := "{\"value\":{\"application_id\":\"1234567890\",\"password\":\"rotated-secret-1-value\",\"username\":\"rotated-user\"}}"
	assert.Equal(t, expectedMixedRotatedValue, response.Data[mixedRotatedSecret][mixedRotatedSecret])

	mockGateway.Close()
}

func TestParseSecretTypes(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    []string
		expectError bool
	}{
		{
			name:     "all",
			input:    "all",
			expected: []string{StaticSecretType, DynamicSecretType, RotatedSecretType},
		},
		{
			name:     "static",
			input:    "static",
			expected: []string{StaticSecretType},
		},
		{
			name:     "dynamic",
			input:    "dynamic",
			expected: []string{DynamicSecretType},
		},
		{
			name:     "rotated",
			input:    "rotated",
			expected: []string{RotatedSecretType},
		},
		{
			name:     "static,dynamic",
			input:    "static,dynamic",
			expected: []string{StaticSecretType, DynamicSecretType},
		},
		{
			name:     "static,dynamic,rotated",
			input:    "static,dynamic,rotated",
			expected: []string{StaticSecretType, DynamicSecretType, RotatedSecretType},
		},
		{
			name:        "invalid",
			input:       "invalid",
			expectError: true,
		},
		{
			name:        "empty",
			input:       "",
			expectError: false,
			expected:    supportedSecretTypes,
		},
		{
			name:        "mixed case",
			input:       "Static,Dynamic,ROTATED",
			expectError: false,
			expected:    []string{StaticSecretType, DynamicSecretType, RotatedSecretType},
		},
		{
			name:        "duplicates",
			input:       "static-secret,dynamic-secret,static-secret",
			expectError: false,
			expected:    []string{StaticSecretType, DynamicSecretType},
		},
		{
			name:        "mixed sdk format and direct format",
			input:       "static-secret,dynamic-secret,rotated-secret,static",
			expectError: false,
			expected:    []string{StaticSecretType, DynamicSecretType, RotatedSecretType},
		},
		{
			name:        "invalid type",
			input:       "invalid",
			expectError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsedTypes, err := parseSecretTypes(tt.input)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, parsedTypes)
			}
		})
	}
}
