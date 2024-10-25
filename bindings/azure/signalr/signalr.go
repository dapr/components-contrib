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
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwt"

	"github.com/dapr/components-contrib/bindings"
	azauth "github.com/dapr/components-contrib/common/authentication/azure"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	"github.com/dapr/kit/ptr"
)

type TokenResponse struct {
	Token string `json:"token"`
}

const (
	connectionStringKey = "connectionString"
	accessKeyKey        = "accessKey"
	endpointKey         = "endpoint"
	hubKey              = "hub"

	// REST API version
	apiVersion = "2022-11-01"

	// Invoke metadata keys.
	groupKey = "group"
	userKey  = "user"

	// OperationKind
	ClientNegotiateOperation bindings.OperationKind = "clientNegotiate"
)

// Metadata keys.
// Azure AD credentials are parsed separately and not listed here.
type SignalRMetadata struct {
	Endpoint         string `mapstructure:"endpoint"`
	AccessKey        string `mapstructure:"accessKey"`
	Hub              string `mapstructure:"hub"`
	ConnectionString string `mapstructure:"connectionString"`
}

// Global HTTP client
var httpClient *http.Client

func init() {
	httpClient = &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				MinVersion: tls.VersionTLS12,
			},
		},
	}
}

// NewSignalR creates a new output binding for Azure SignalR.
func NewSignalR(logger logger.Logger) bindings.OutputBinding {
	return &SignalR{
		logger:     logger,
		httpClient: httpClient,
	}
}

// SignalR is an output binding for Azure SignalR.
type SignalR struct {
	endpoint  string
	accessKey string
	hub       string
	userAgent string
	aadToken  azcore.TokenCredential

	httpClient *http.Client
	logger     logger.Logger
}

// Init is responsible for initializing the SignalR output based on the metadata.
func (s *SignalR) Init(_ context.Context, metadata bindings.Metadata) (err error) {
	s.userAgent = "dapr-" + logger.DaprVersion

	err = s.parseMetadata(metadata.Properties)
	if err != nil {
		return err
	}

	// If using AAD for authentication, init the token provider
	if s.accessKey == "" {
		var settings azauth.EnvironmentSettings
		settings, err = azauth.NewEnvironmentSettings(metadata.Properties)
		if err != nil {
			return err
		}
		s.aadToken, err = settings.GetTokenCredential()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *SignalR) parseMetadata(md map[string]string) (err error) {
	m := SignalRMetadata{}
	err = kitmd.DecodeMetadata(md, &m)
	if err != nil {
		return err
	}

	// Start by parsing the connection string if present
	if m.ConnectionString != "" {
		// Expected options:
		// Access key: "Endpoint=https://<servicename>.service.signalr.net;AccessKey=<access key>;Version=1.0;"
		// System-assigned managed identity: "Endpoint=https://<servicename>.service.signalr.net;AuthType=aad;Version=1.0;"
		// User-assigned managed identity: "Endpoint=https://<servicename>.service.signalr.net;AuthType=aad;ClientId=<clientid>;Version=1.0;"
		// Azure AD application: "Endpoint=https://<servicename>.service.signalr.net;AuthType=aad;ClientId=<clientid>;ClientSecret=<clientsecret>;TenantId=<tenantid>;Version=1.0;"
		// Note: connection string can't be used if the client secret contains the ; key
		connectionValues := strings.Split(strings.TrimSpace(m.ConnectionString), ";")
		useAAD := false
		for _, connectionValue := range connectionValues {
			if i := strings.Index(connectionValue, "="); i != -1 && len(connectionValue) > (i+1) {
				k := connectionValue[0:i]
				switch k {
				case "Endpoint":
					s.endpoint = connectionValue[i+1:]
				case "AccessKey":
					s.accessKey = connectionValue[i+1:]
				case "AuthType":
					if connectionValue[i+1:] != "aad" {
						return errors.New("invalid value for AuthType in the connection string; only 'aad' is supported")
					}
					useAAD = true
				case "ClientId", "ClientSecret", "TenantId":
					v := connectionValue[i+1:]
					// Set the values in the metadata map so they can be picked up by the azauth module
					md["azure"+k] = v
				case "Version":
					v := connectionValue[i+1:]
					// We only support version "1.0"
					if v != "1.0" {
						return fmt.Errorf("invalid value for Version in the connection string: '%s'; only version '1.0' is supported", v)
					}
				}
			} else if len(connectionValue) != 0 {
				return errors.New("the connection string is invalid or malformed")
			}
		}

		// Check here because if we use a connection string, we'd have an explicit "AuthType=aad" option
		// We would otherwise catch this issue later, but here we can be more explicit with the error
		if s.accessKey == "" && !useAAD {
			return errors.New("missing AccessKey in the connection string")
		}
	}

	// Parse the other metadata keys, which could also override the values from the connection string
	if m.Hub != "" {
		s.hub = m.Hub
	}
	if m.Endpoint != "" {
		s.endpoint = m.Endpoint
	}
	if m.AccessKey != "" {
		s.accessKey = m.AccessKey
	}

	// Trim ending "/" from endpoint
	s.endpoint = strings.TrimSuffix(s.endpoint, "/")

	// Check for required values
	if s.endpoint == "" {
		return errors.New("missing endpoint in the metadata or connection string")
	}

	return nil
}

func (s *SignalR) getHub(req *bindings.InvokeRequest) (string, error) {
	hub, ok := req.Metadata[hubKey]
	if !ok || hub == "" {
		hub = s.hub
	}
	if hub == "" {
		return "", errors.New("missing hub")
	}

	// Hub name is lower-cased in the official SDKs (e.g. .NET)
	return strings.ToLower(hub), nil
}

func (s *SignalR) resolveAPIURL(req *bindings.InvokeRequest) (string, error) {
	hub, err := s.getHub(req)
	if err != nil {
		return "", err
	}

	var url string
	if group, ok := req.Metadata[groupKey]; ok && group != "" {
		url = fmt.Sprintf("%s/api/hubs/%s/groups/%s/:send?api-version=%s", s.endpoint, hub, group, apiVersion)
	} else if user, ok := req.Metadata[userKey]; ok && user != "" {
		url = fmt.Sprintf("%s/api/hubs/%s/users/%s/:send?api-version=%s", s.endpoint, hub, user, apiVersion)
	} else {
		url = fmt.Sprintf("%s/api/hubs/%s/:send?api-version=%s", s.endpoint, hub, apiVersion)
	}

	return url, nil
}

func (s *SignalR) sendRequestToSignalR(ctx context.Context, url string, token string, data []byte) ([]byte, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}

	httpReq.Header.Set("Authorization", "Bearer "+token)
	httpReq.Header.Set("Content-Type", "application/json; charset=utf-8")
	httpReq.Header.Set("User-Agent", s.userAgent)

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request to azure signalr api failed: %w", err)
	}
	defer resp.Body.Close()

	// Read the body regardless to drain it and ensure the connection can be reused
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return nil, fmt.Errorf("azure signalr failed with code %d, content is '%s'", resp.StatusCode, string(body))
	}

	s.logger.Debugf("azure signalr call to '%s' completed with code %d", url, resp.StatusCode)

	return body, nil
}

func (s *SignalR) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation, ClientNegotiateOperation}
}

func (s *SignalR) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	switch req.Operation {
	case ClientNegotiateOperation:
		return s.GenerateClientNegotiateResponse(ctx, req)
	case bindings.CreateOperation:
		return s.SendMessages(ctx, req)
	default:
		// return nil, fmt.Errorf("invalid operation '%s'; supported operations: '%s', '%s'", req.Operation, ClientNegotiateOperation, bindings.CreateOperation)
		// We invoke SendMessage for backwards-compatibility if no operation is defined
		return s.SendMessages(ctx, req)
	}
}

func (s *SignalR) GenerateClientNegotiateResponse(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	// Generate token
	hub, err := s.getHub(req)
	if err != nil {
		return nil, err
	}

	user := req.Metadata[userKey]
	clientURL := fmt.Sprintf("%s/client/?hub=%s", s.endpoint, hub)

	// If we have an Azure AD token provider, invoke REST API to generate token
	// Otherwise, generate token locally
	var token string
	if s.aadToken != nil {
		token, err = s.GetAadClientAccessToken(ctx, hub, user)
	} else {
		// Default to 60 minutes
		token, err = s.getToken(ctx, clientURL, user, 60)
	}

	if err != nil {
		return nil, fmt.Errorf("error generating negotiate payload: %w", err)
	}

	// Create the negotiate JSON payload
	payload := map[string]string{
		"url":         clientURL,
		"accessToken": token,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("error generating negotiate payload: %w", err)
	}

	response := bindings.InvokeResponse{
		Data:        data,
		Metadata:    map[string]string{},
		ContentType: ptr.Of("application/json"),
	}
	return &response, nil
}

func (s *SignalR) GetAadClientAccessToken(ctx context.Context, hub string, user string) (string, error) {
	aadToken, err := s.getAadToken(ctx)
	if err != nil {
		return "", err
	}

	u := fmt.Sprintf("%s/api/hubs/%s/:generateToken?api-version=%s", s.endpoint, hub, apiVersion)
	if user != "" {
		u += "&userId=" + url.QueryEscape(user)
	}

	body, err := s.sendRequestToSignalR(ctx, u, aadToken, nil)
	if err != nil {
		return "", err
	}

	var tokenResponse TokenResponse
	err = json.Unmarshal(body, &tokenResponse)
	if err != nil {
		return "", err
	}
	if tokenResponse.Token == "" {
		return "", errors.New("token is empty in response from Azure SignalR")
	}

	return tokenResponse.Token, err
}

func (s *SignalR) SendMessages(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	url, err := s.resolveAPIURL(req)
	if err != nil {
		return nil, err
	}

	token, err := s.getToken(ctx, url, "", 15)
	if err != nil {
		return nil, err
	}

	_, err = s.sendRequestToSignalR(ctx, url, token, req.Data)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// Returns an access token for a request to the given URL
func (s *SignalR) getAadToken(ctx context.Context) (string, error) {
	at, err := s.aadToken.GetToken(ctx, policy.TokenRequestOptions{
		Scopes: []string{"https://signalr.azure.com/.default"},
	})
	if err != nil {
		return "", err
	}
	return at.Token, nil
}

func (s *SignalR) signJwtToken(audience string, expireAt time.Time, user string) (string, error) {
	builder := jwt.NewBuilder().
		Audience([]string{audience}).
		Expiration(expireAt)

	// Add the subject if the user ID is not empty
	if user != "" {
		builder = builder.Subject(user)
	}
	token, err := builder.Build()
	if err != nil {
		return "", fmt.Errorf("failed to build token: %w", err)
	}
	signed, err := jwt.Sign(token, jwt.WithKey(jwa.HS256, []byte(s.accessKey)))
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %w", err)
	}

	return string(signed), nil
}

// Returns an access token for a request to the given URL
func (s *SignalR) getToken(ctx context.Context, url string, user string, expireMinutes int) (string, error) {
	// If we have an Azure AD token provider, use that first
	if s.aadToken != nil {
		return s.getAadToken(ctx)
	}
	return s.signJwtToken(url, time.Now().Add(time.Duration(expireMinutes)*time.Minute), user)
}

// GetComponentMetadata returns the metadata of the component.
func (s *SignalR) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := SignalRMetadata{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}

func (s *SignalR) Close() error {
	return nil
}
