package akeyless

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/akeylesslabs/akeyless-go/v5"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/secretstores"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testAccessIdIAM = "p-xt3sT2nah7gpwm"
	testAccessIdJwt = "p-xt3sT2nah7gpom"
	testAccessIdKey = "p-xt3sT2nah7gpam"
	testAccessKey   = "ABCD1233xxx="
	// {
	// "sub": "1234567890",
	//	 "name": "John Doe",
	//	 "iat": 1516239022
	// }
	testJWT = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QeJkP5vWKT_yUZJgIeUAnYw2brk"
	// testDynamicSecretName = "/path/to/dynamic-secret-test"
	// testRotatedSecretName = "/path/to/rotated-secret-test"
	testSecretValue = "r3vE4L3D"
)

var (
	mockDescribeStaticSecretName         = "/path/to/akeyless/static-secret-test"
	mockDescribeStaticSecretType         = AKEYLESS_SECRET_TYPE_STATIC_SECRET_RESPONSE
	mockDescribeStaticSecretItemResponse = akeyless.Item{
		ItemName: &mockDescribeStaticSecretName,
		ItemType: &mockDescribeStaticSecretType,
	}
	mockStaticSecretJSONName             = "/path/to/akeyless/static-secret-json-test"
	mockGetSingleSecretJSONValueResponse = map[string]map[string]string{
		mockStaticSecretJSONName: {
			"some": "json",
		},
	}
	mockStaticSecretPasswordName             = "/path/to/akeyless/static-secret-password-test"
	mockGetSingleSecretPasswordValueResponse = map[string]map[string]string{
		mockStaticSecretPasswordName: {
			"password": testSecretValue,
			"username": "akeyless",
		},
	}
	mockDescribeDynamicSecretName         = "/path/to/akeyless/dynamic-secret-test"
	mockDescribeDynamicSecretType         = AKEYLESS_SECRET_TYPE_DYNAMIC_SECRET_RESPONSE
	mockDescribeDynamicSecretItemResponse = akeyless.Item{
		ItemName: &mockDescribeDynamicSecretName,
		ItemType: &mockDescribeDynamicSecretType,
	}
	mockGetSingleDynamicSecretValueResponse = DynamicSecretResponse{
		ID:  "{\"secret_name\": \"tmp.p-1234567890.GV7LR\",\"secret_key_id\": \"1234567890\"}",
		Msg: "User  has been added successfully to the following Group(s): [] Role(s): [] Expires on Thu Sep 25 15:54:06 UTC 2025",
		Secret: DynamicSecretSecret{
			AppID:       "1234567890",
			DisplayName: "tmp.p-1234567890.GV7LR",
			EndDateTime: "2025-09-26T14:54:05.1643791Z",
			KeyID:       "1234567890",
			SecretText:  testSecretValue,
			TenantID:    "1234567890",
		},
		TTLInMinutes: "60",
	}
)

var mockGetSingleSecretValueResponse = map[string]string{
	mockDescribeStaticSecretName: testSecretValue,
}

// Global mock server for all tests
var mockGateway *httptest.Server

// Mock AWS cloud ID for testing
const mockCloudID = "123456789012"

// mockAuthenticate is a test version of the Authenticate function that uses a mock cloud ID
func mockAuthenticate(metadata *akeylessMetadata, akeylessSecretStore *akeylessSecretStore) error {
	authRequest := akeyless.NewAuth()
	authRequest.SetAccessId(metadata.AccessID)
	authRequest.SetAccessType(metadata.AccessType)

	// Depending on the access type we set the appropriate authentication method
	switch metadata.AccessType {
	// If access type is AWS IAM we use the mock cloud ID
	case AKEYLESS_AUTH_ACCESS_IAM:
		akeylessSecretStore.logger.Debug("Using mock cloud ID for AWS IAM...")
		authRequest.SetCloudId(mockCloudID)
	case AKEYLESS_AUTH_ACCESS_JWT:
		akeylessSecretStore.logger.Debug("Setting JWT for authentication...")
		authRequest.SetJwt(metadata.JWT)
	case AKEYLESS_AUTH_DEFAULT_ACCESS_TYPE:
		akeylessSecretStore.logger.Debug("Setting access key for authentication...")
		authRequest.SetAccessKey(metadata.AccessKey)
	}

	config := akeyless.NewConfiguration()
	config.Servers = []akeyless.ServerConfiguration{
		{
			URL: metadata.GatewayURL,
		},
	}
	config.UserAgent = AKEYLESS_USER_AGENT
	config.AddDefaultHeader("akeylessclienttype", AKEYLESS_USER_AGENT)

	akeylessSecretStore.v2 = akeyless.NewAPIClient(config).V2Api

	out, _, err := akeylessSecretStore.v2.Auth(context.Background()).Body(*authRequest).Execute()
	if err != nil {
		return fmt.Errorf("failed to authenticate with Akeyless: %w", err)
	}

	akeylessSecretStore.token = out.GetToken()

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
			authOutput.SetExpiration("2025-01-01T00:00:00Z")
			jsonResponse, _ := json.Marshal(authOutput)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)
		// Single static secret value
		case "/get-secret-value":
			jsonResponse, _ := json.Marshal(mockGetSingleSecretValueResponse)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)
		case "/get-dynamic-secret-value":
			var dynamicResponse = DynamicSecretResponse{
				ID:  "{\"secret_name\": \"tmp.p-1234567890.GV7LR\",\"secret_key_id\": \"1234567890\"}",
				Msg: "User  has been added successfully to the following Group(s): [] Role(s): [] Expires on Thu Sep 25 15:54:06 UTC 2025",
				Secret: DynamicSecretSecret{
					AppID:       "1234567890",
					DisplayName: "tmp.p-1234567890.GV7LR",
					EndDateTime: "2025-09-26T14:54:05.1643791Z",
					KeyID:       "1234567890",
					SecretText:  testSecretValue,
					TenantID:    "1234567890",
				},
				TTLInMinutes: "60",
			}
			jsonResponse, _ := json.Marshal(dynamicResponse)
			w.WriteHeader(http.StatusOK)
			w.Write(jsonResponse)
		// case "/get-rotated-secret-value", "/v2/get-rotated-secret-value":
		// 	var rotatedResponse = RotatedSecretResponse{
		// 		Value: RotatedSecretValue{
		// 			Username:      "abcdefghijklmnopqrstuvwxyz",
		// 			Password:      testSecretValue,
		// 			ApplicationID: "1234567890",
		// 		},
		// 	}
		// 	jsonResponse, _ := json.Marshal(rotatedResponse)
		// 	w.WriteHeader(http.StatusOK)
		// 	w.Write(jsonResponse)
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
						"accessId":   testAccessIdKey,
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
						"accessId":   testAccessIdJwt,
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
						"accessId":   testAccessIdIAM,
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
				err := store.Init(context.Background(), tt.metadata)
				if tt.expectError {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					assert.NotNil(t, store.v2)
					assert.NotNil(t, store.token)
				}
			}
		})
	}
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
				"accessId":  testAccessIdKey,
				"accessKey": testAccessKey,
			},
			expectError: false,
			expected: &akeylessMetadata{
				AccessID:   testAccessIdKey,
				AccessKey:  testAccessKey,
				AccessType: AKEYLESS_AUTH_DEFAULT_ACCESS_TYPE,
				GatewayURL: "https://api.akeyless.io", // Default gateway URL
			},
		},
		{
			name: "valid metadata with access id and jwt",
			properties: map[string]string{
				"accessId":   testAccessIdJwt,
				"jwt":        testJWT,
				"gatewayUrl": mockGateway.URL,
			},
			expectError: false,
			expected: &akeylessMetadata{
				AccessID:   testAccessIdJwt,
				JWT:        testJWT,
				AccessType: AKEYLESS_AUTH_ACCESS_JWT,
				GatewayURL: mockGateway.URL,
			},
		},
		{
			name: "valid metadata with access id (aws_iam)",
			properties: map[string]string{
				"accessId":   testAccessIdIAM,
				"gatewayUrl": mockGateway.URL,
			},
			expectError: false,
			expected: &akeylessMetadata{
				AccessID:   testAccessIdIAM,
				AccessType: AKEYLESS_AUTH_ACCESS_IAM,
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
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestGetComponentMetadata(t *testing.T) {
	log := logger.NewLogger("test")
	store := NewAkeylessSecretStore(log).(*akeylessSecretStore)

	metadata := store.GetComponentMetadata()
	require.NotNil(t, metadata)

	// Check that the metadata contains the expected fields
	assert.Contains(t, metadata, "gatewayUrl")
	assert.Contains(t, metadata, "accessId")
	assert.Contains(t, metadata, "jwt")
	assert.Contains(t, metadata, "accessKey")

	// Check that the metadata fields exist
	accessIdField := metadata["accessId"]
	require.NotNil(t, accessIdField)

	gatewayField := metadata["gatewayUrl"]
	require.NotNil(t, gatewayField)
}

func TestMockServerReturnsAuthOutput(t *testing.T) {
	// Test that the mock server properly returns an AuthOutput response
	store := NewAkeylessSecretStore(logger.NewLogger("test")).(*akeylessSecretStore)

	// Test with access key authentication
	meta := secretstores.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"accessId":   testAccessIdKey,
				"accessKey":  testAccessKey,
				"gatewayUrl": mockGateway.URL,
			},
		},
	}

	err := store.Init(context.Background(), meta)
	assert.NoError(t, err)
	assert.NotNil(t, store.v2)
	assert.NotNil(t, store.token)
	assert.Equal(t, "t-1234567890", store.token)
}

func TestMockAWSCloudID(t *testing.T) {
	// Test that the mock AWS cloud ID works correctly
	store := NewAkeylessSecretStore(logger.NewLogger("test")).(*akeylessSecretStore)

	// Test with AWS IAM authentication using mock cloud ID
	meta := secretstores.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"accessId":   testAccessIdIAM,
				"gatewayUrl": mockGateway.URL,
			},
		},
	}

	// Parse metadata first
	m, err := store.parseMetadata(meta)
	require.NoError(t, err)
	assert.Equal(t, AKEYLESS_AUTH_ACCESS_IAM, m.AccessType)

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
				"accessId":   testAccessIdKey,
				"accessKey":  testAccessKey,
				"gatewayUrl": mockGateway.URL,
			},
		},
	}

	err := store.Init(context.Background(), meta)
	require.NoError(t, err)

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
		// TODO: add non-existing secret test
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
			response, err := store.GetSecret(context.Background(), tt.request)
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

	var mockGateway *httptest.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Handle different endpoints
		switch r.URL.Path {
		case "/auth", "/v2/auth":
			// Return a proper AuthOutput JSON response for authentication
			authOutput := akeyless.NewAuthOutput()
			authOutput.SetToken("t-1234567890")
			authOutput.SetExpiration("2025-01-01T00:00:00Z")
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
				"accessId":   testAccessIdKey,
				"accessKey":  testAccessKey,
				"gatewayUrl": mockGateway.URL,
			},
		},
	}

	err := store.Init(context.Background(), meta)
	require.NoError(t, err)

	response, err := store.GetSecret(context.Background(), secretstores.GetSecretRequest{
		Name: mockStaticSecretJSONName,
	})
	require.NoError(t, err)
	assert.NotNil(t, response.Data)
	assert.Contains(t, response.Data, mockStaticSecretJSONName)
	assert.Equal(t, "{\"some\":\"json\"}", response.Data[mockStaticSecretJSONName])

	mockGateway.Close()
}

func TestGetSingleSecretPassword(t *testing.T) {

	var mockGateway *httptest.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Handle different endpoints
		switch r.URL.Path {
		case "/auth", "/v2/auth":
			// Return a proper AuthOutput JSON response for authentication
			authOutput := akeyless.NewAuthOutput()
			authOutput.SetToken("t-1234567890")
			authOutput.SetExpiration("2025-01-01T00:00:00Z")
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
				"accessId":   testAccessIdKey,
				"accessKey":  testAccessKey,
				"gatewayUrl": mockGateway.URL,
			},
		},
	}

	err := store.Init(context.Background(), meta)
	require.NoError(t, err)

	response, err := store.GetSecret(context.Background(), secretstores.GetSecretRequest{
		Name: mockStaticSecretPasswordName,
	})
	require.NoError(t, err)
	assert.NotNil(t, response.Data)
	assert.Contains(t, response.Data, mockStaticSecretPasswordName)
	assert.Equal(t, "{\"password\":\"r3vE4L3D\",\"username\":\"akeyless\"}", response.Data[mockStaticSecretPasswordName])

	mockGateway.Close()
}

// Test GetSecretType functions
func TestGetSecretType(t *testing.T) {
	// Test GetSecretType
	store := NewAkeylessSecretStore(logger.NewLogger("test")).(*akeylessSecretStore)
	meta := secretstores.Metadata{
		Base: metadata.Base{
			Properties: map[string]string{
				"accessId":   testAccessIdKey,
				"accessKey":  testAccessKey,
				"gatewayUrl": mockGateway.URL,
			},
		},
	}

	err := store.Init(context.Background(), meta)
	require.NoError(t, err)

	secretType, err := GetSecretType(mockDescribeStaticSecretName, store)
	assert.NoError(t, err)
	assert.Equal(t, AKEYLESS_SECRET_TYPE_STATIC_SECRET_RESPONSE, secretType)
}

func TestGetSingleDynamicSecret(t *testing.T) {

	var mockGateway *httptest.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Handle different endpoints
		switch r.URL.Path {
		case "/auth":
			// Return a proper AuthOutput JSON response for authentication
			authOutput := akeyless.NewAuthOutput()
			authOutput.SetToken("t-1234567890")
			authOutput.SetExpiration("2025-01-01T00:00:00Z")
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
				"accessId":   testAccessIdKey,
				"accessKey":  testAccessKey,
				"gatewayUrl": mockGateway.URL,
			},
		},
	}

	err := store.Init(context.Background(), meta)
	require.NoError(t, err)

	secretValue, err := GetSingleSecretValue(mockDescribeDynamicSecretName, AKEYLESS_SECRET_TYPE_DYNAMIC_SECRET_RESPONSE, store)
	assert.NoError(t, err)
	assert.Equal(t, "{\"displayName\":\"tmp.p-1234567890.GV7LR\",\"secretText\":\"r3vE4L3D\"}", secretValue)

	mockGateway.Close()
}
