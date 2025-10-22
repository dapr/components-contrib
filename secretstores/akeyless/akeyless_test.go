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
	testJWT         = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QeJkP5vWKT_yUZJgIeUAnYw2brk"
	testSecretValue = "r3vE4L3D"
)

var (
	mockStaticSecretItem                 = "/static-secret-test"
	mockStaticSecretJSONItemName         = "/static-secret-json-test"
	mockStaticSecretPasswordItemName     = "/static-secret-password-test"
	mockDynamicSecretItemName            = "/dynamic-secret-test"
	mockRotatedSecretItemName            = "/rotated-secret-test"
	mockDescribeStaticSecretName         = fmt.Sprintf("/path/to/akeyless%s", mockStaticSecretItem)
	mockDescribeStaticSecretType         = AKEYLESS_SECRET_TYPE_STATIC_SECRET_RESPONSE
	mockDescribeStaticSecretItemResponse = akeyless.Item{
		ItemName:  &mockDescribeStaticSecretName,
		ItemType:  &mockDescribeStaticSecretType,
		IsEnabled: func(b bool) *bool { return &b }(true),
	}
	mockStaticSecretJSONName             = fmt.Sprintf("/path/to/akeyless%s", mockStaticSecretJSONItemName)
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
	mockStaticSecretPasswordName             = fmt.Sprintf("/path/to/akeyless%s", mockStaticSecretPasswordItemName)
	mockGetSingleSecretPasswordValueResponse = map[string]map[string]string{
		mockStaticSecretPasswordName: {
			"password": testSecretValue,
			"username": "akeyless",
		},
	}
	mockDescribeDynamicSecretName         = fmt.Sprintf("/path/to/akeyless%s", mockDynamicSecretItemName)
	mockDescribeDynamicSecretType         = AKEYLESS_SECRET_TYPE_DYNAMIC_SECRET_RESPONSE
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
	mockDescribeRotatedSecretName         = fmt.Sprintf("/path/to/akeyless%s", mockRotatedSecretItemName)
	mockDescribeRotatedSecretType         = AKEYLESS_SECRET_TYPE_ROTATED_SECRET_RESPONSE
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

	secretType, err := store.GetSecretType(mockDescribeStaticSecretName)
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

	secretValue, err := store.GetSingleSecretValue(mockDescribeDynamicSecretName, AKEYLESS_SECRET_TYPE_DYNAMIC_SECRET_RESPONSE)
	assert.NoError(t, err)
	assert.Equal(t, "{\"user\":\"generated_username\",\"password\":\"generated_password\",\"ttl_in_minutes\":\"60\",\"id\":\"username\"}", secretValue)

	mockGateway.Close()
}

func TestGetSingleRotatedSecret(t *testing.T) {

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
				"accessId":   testAccessIdKey,
				"accessKey":  testAccessKey,
				"gatewayUrl": mockGateway.URL,
			},
		},
	}

	err := store.Init(context.Background(), meta)
	require.NoError(t, err)

	secretValue, err := store.GetSingleSecretValue(mockDescribeRotatedSecretName, AKEYLESS_SECRET_TYPE_ROTATED_SECRET_RESPONSE)
	assert.NoError(t, err)
	assert.Equal(t, "{\"value\":{\"application_id\":\"1234567890\",\"password\":\"r3vE4L3D\",\"username\":\"abcdefghijklmnopqrstuvwxyz\"}}", secretValue)

	mockGateway.Close()
}

func TestGetBulkSecretValues(t *testing.T) {

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
				"accessId":   testAccessIdKey,
				"accessKey":  testAccessKey,
				"gatewayUrl": mockGateway.URL,
			},
		},
	}

	err := store.Init(context.Background(), meta)
	require.NoError(t, err)

	response, err := store.BulkGetSecret(context.Background(), secretstores.BulkGetSecretRequest{})
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
	assert.Equal(t, "{\"some\":\"json\"}", response.Data[jsonSecretKey][jsonSecretKey])

	// Check dynamic secret
	dynamicSecretKey := "/path/to/akeyless/dynamic-secret-test"
	assert.Contains(t, response.Data, dynamicSecretKey)
	expectedDynamicValue := "{\"user\":\"generated_username\",\"password\":\"generated_password\",\"ttl_in_minutes\":\"60\",\"id\":\"username\"}"
	assert.Equal(t, expectedDynamicValue, response.Data[dynamicSecretKey][dynamicSecretKey])

	// Check rotated secret
	rotatedSecretKey := "/path/to/akeyless/rotated-secret-test"
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
	mixedStaticSecret := "/path/to/mixed/secrets/mixed-static"
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
				"accessId":   testAccessIdKey,
				"accessKey":  testAccessKey,
				"gatewayUrl": mockGateway.URL,
			},
		},
	}

	err := store.Init(context.Background(), meta)
	require.NoError(t, err)

	response, err := store.BulkGetSecret(context.Background(), secretstores.BulkGetSecretRequest{})
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
