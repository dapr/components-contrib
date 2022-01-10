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

	"github.com/golang-jwt/jwt"
	"github.com/pkg/errors"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

const (
	errorPrefix         = "azure signalr error:"
	logPrefix           = "azure signalr:"
	connectionStringKey = "connectionString"
	hubKey              = "hub"
	groupKey            = "group"
	userKey             = "user"
)

// NewSignalR creates a new pub/sub based on Azure SignalR.
func NewSignalR(logger logger.Logger) *SignalR {
	return &SignalR{
		tokens:     make(map[string]signalrCachedToken),
		httpClient: &http.Client{Timeout: 30 * time.Second},
		logger:     logger,
	}
}

type signalrCachedToken struct {
	token      string
	expiration time.Time
}

// SignalR is an output binding for Azure SignalR.
type SignalR struct {
	endpoint   string
	accessKey  string
	version    string
	hub        string
	userAgent  string
	tokens     map[string]signalrCachedToken
	httpClient *http.Client

	logger logger.Logger
}

// Init is responsible for initializing the SignalR output based on the metadata.
func (s *SignalR) Init(metadata bindings.Metadata) error {
	s.userAgent = "dapr-" + logger.DaprVersion

	connectionString, ok := metadata.Properties[connectionStringKey]
	if !ok || connectionString == "" {
		return fmt.Errorf("missing connection string")
	}

	if hub, ok := metadata.Properties[hubKey]; ok && hub != "" {
		s.hub = hub
	}

	// Expected: Endpoint=https://<servicename>.service.signalr.net;AccessKey=<access key>;Version=1.0;
	connectionValues := strings.Split(strings.TrimSpace(connectionString), ";")
	for _, connectionValue := range connectionValues {
		if i := strings.Index(connectionValue, "="); i != -1 && len(connectionValue) > (i+1) {
			k := connectionValue[0:i]
			switch k {
			case "Endpoint":
				s.endpoint = connectionValue[i+1:]
				if s.endpoint[len(s.endpoint)-1] == '/' {
					s.endpoint = s.endpoint[:len(s.endpoint)-1]
				}
			case "AccessKey":
				s.accessKey = connectionValue[i+1:]
			case "Version":
				s.version = connectionValue[i+1:]
			}
		}
	}

	if s.endpoint == "" {
		return fmt.Errorf("missing endpoint in connection string")
	}

	if s.accessKey == "" {
		return fmt.Errorf("missing access key in connection string")
	}

	return nil
}

func (s *SignalR) resolveAPIURL(req *bindings.InvokeRequest) (string, error) {
	hub := s.hub
	if hub == "" {
		hubFromRequest, ok := req.Metadata[hubKey]
		if !ok || hubFromRequest == "" {
			return "", fmt.Errorf("%s missing hub", errorPrefix)
		}

		hub = hubFromRequest
	}

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

func (s *SignalR) sendMessageToSignalR(url string, token string, data []byte) error {
	httpReq, err := http.NewRequestWithContext(context.Background(), "POST", url, bytes.NewBuffer(data))
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

	if resp.StatusCode != 200 && resp.StatusCode != 202 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		return fmt.Errorf("%s azure signalr returned code %d, content is '%s'", errorPrefix, resp.StatusCode, string(body))
	}

	s.logger.Debugf("%s azure signalr call to '%s' returned with status code %d", logPrefix, url, resp.StatusCode)

	return nil
}

func (s *SignalR) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}

func (s *SignalR) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	url, err := s.resolveAPIURL(req)
	if err != nil {
		return nil, err
	}

	token, err := s.ensureValidToken(url)
	if err != nil {
		return nil, err
	}

	err = s.sendMessageToSignalR(url, token, req.Data)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (s *SignalR) ensureValidToken(url string) (string, error) {
	now := time.Now()

	if existing, ok := s.tokens[url]; ok {
		if existing.token != "" && now.Before(existing.expiration) {
			return existing.token, nil
		}
	}

	expiration := now.Add(1 * time.Hour)

	claims := &jwt.StandardClaims{
		ExpiresAt: expiration.Unix(),
		Audience:  url,
	}

	if err := claims.Valid(); err != nil {
		return "", err
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	newToken, err := token.SignedString([]byte(s.accessKey))
	if err != nil {
		return "", err
	}

	s.tokens[url] = signalrCachedToken{token: newToken, expiration: expiration.Add(time.Minute * -5)}

	return newToken, nil
}
