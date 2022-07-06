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
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	jwt "github.com/golang-jwt/jwt/v4"
	"github.com/pkg/errors"

	azauth "github.com/dapr/components-contrib/authentication/azure"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

const (
	errorPrefix = "azure signalr error:"
	logPrefix   = "azure signalr:"

	// Metadata keys.
	// Azure AD credentials are parsed separately and not listed here.
	connectionStringKey = "connectionString"
	accessKeyKey        = "accessKey"
	endpointKey         = "endpoint"
	hubKey              = "hub"

	// Invoke metadata keys.
	groupKey = "group"
	userKey  = "user"
)

// Global HTTP client
var httpClient *http.Client

func init() {
	httpClient = &http.Client{
		Timeout: 30 * time.Second,
	}
}

// NewSignalR creates a new output binding for Azure SignalR.
func NewSignalR(logger logger.Logger) *SignalR {
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
func (s *SignalR) Init(metadata bindings.Metadata) (err error) {
	s.userAgent = "dapr-" + logger.DaprVersion

	err = s.parseMetadata(metadata.Properties)
	if err != nil {
		return err
	}

	// If using AAD for authentication, init the token provider
	if s.accessKey == "" {
		var settings azauth.EnvironmentSettings
		settings, err = azauth.NewEnvironmentSettings("signalr", metadata.Properties)
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
	// Start by parsing the connection string if present
	connectionString, ok := md[connectionStringKey]
	if ok && connectionString != "" {
		// Expected options:
		// Access key: "Endpoint=https://<servicename>.service.signalr.net;AccessKey=<access key>;Version=1.0;"
		// System-assigned managed identity: "Endpoint=https://<servicename>.service.signalr.net;AuthType=aad;Version=1.0;"
		// User-assigned managed identity: "Endpoint=https://<servicename>.service.signalr.net;AuthType=aad;ClientId=<clientid>;Version=1.0;"
		// Azure AD application: "Endpoint=https://<servicename>.service.signalr.net;AuthType=aad;ClientId=<clientid>;ClientSecret=<clientsecret>;TenantId=<tenantid>;Version=1.0;"
		// Note: connection string can't be used if the client secret contains the ; key
		connectionValues := strings.Split(strings.TrimSpace(connectionString), ";")
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
						return fmt.Errorf("invalid value for AuthType in the connection string; only 'aad' is supported")
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
				return fmt.Errorf("the connection string is invalid or malformed")
			}
		}

		// Check here because if we use a connection string, we'd have an explicit "AuthType=aad" option
		// We would otherwise catch this issue later, but here we can be more explicit with the error
		if s.accessKey == "" && !useAAD {
			return fmt.Errorf("missing AccessKey in the connection string")
		}
	}

	// Parse the other metadata keys, which could also override the values from the connection string
	if v, ok := md[hubKey]; ok && v != "" {
		s.hub = v
	}
	if v, ok := md[endpointKey]; ok && v != "" {
		s.endpoint = v
	}
	if v, ok := md[accessKeyKey]; ok && v != "" {
		s.accessKey = v
	}

	// Trim ending "/" from endpoint
	s.endpoint = strings.TrimSuffix(s.endpoint, "/")

	// Check for required values
	if s.endpoint == "" {
		return fmt.Errorf("missing endpoint in the metadata or connection string")
	}

	return nil
}

func (s *SignalR) resolveAPIURL(req *bindings.InvokeRequest) (string, error) {
	hub, ok := req.Metadata[hubKey]
	if !ok || hub == "" {
		hub = s.hub
	}
	if hub == "" {
		return "", fmt.Errorf("%s missing hub", errorPrefix)
	}

	// Hub name is lower-cased in the official SDKs (e.g. .NET)
	hub = strings.ToLower(hub)

	var url string
	if group, ok := req.Metadata[groupKey]; ok && group != "" {
		url = fmt.Sprintf("%s/api/v1/hubs/%s/groups/%s", s.endpoint, hub, group)
	} else if user, ok := req.Metadata[userKey]; ok && user != "" {
		url = fmt.Sprintf("%s/api/v1/hubs/%s/users/%s", s.endpoint, hub, user)
	} else {
		url = fmt.Sprintf("%s/api/v1/hubs/%s", s.endpoint, hub)
	}

	return url, nil
}

func (s *SignalR) sendMessageToSignalR(ctx context.Context, url string, token string, data []byte) error {
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	httpReq.Header.Set("Authorization", "Bearer "+token)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("User-Agent", s.userAgent)

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return errors.Wrap(err, "request to azure signalr api failed")
	}
	defer resp.Body.Close()

	// Read the body regardless to drain it and ensure the connection can be reused
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("%s azure signalr failed with  code %d, content is '%s'", errorPrefix, resp.StatusCode, string(body))
	}

	s.logger.Debugf("%s azure signalr call to '%s' completed with code %d", logPrefix, url, resp.StatusCode)

	return nil
}

func (s *SignalR) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (s *SignalR) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	url, err := s.resolveAPIURL(req)
	if err != nil {
		return nil, err
	}

	token, err := s.getToken(ctx, url)
	if err != nil {
		return nil, err
	}

	err = s.sendMessageToSignalR(ctx, url, token, req.Data)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// Returns an access token for a request to the given URL
func (s *SignalR) getToken(ctx context.Context, url string) (token string, err error) {
	// If we have an Azure AD token provider, use that first
	if s.aadToken != nil {
		var at azcore.AccessToken
		at, err = s.aadToken.GetToken(ctx, policy.TokenRequestOptions{
			Scopes: []string{"https://signalr.azure.com/.default"},
		})
		if err != nil {
			return "", err
		}
		token = at.Token
	} else {
		claims := &jwt.StandardClaims{
			ExpiresAt: time.Now().Add(15 * time.Minute).Unix(),
			Audience:  url,
		}
		err = claims.Valid()
		if err != nil {
			return "", err
		}

		jwtToken := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		token, err = jwtToken.SignedString([]byte(s.accessKey))
		if err != nil {
			return "", err
		}
	}

	return token, nil
}
