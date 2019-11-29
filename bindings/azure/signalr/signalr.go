// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package signalr

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/dapr/components-contrib/bindings"
	"github.com/gbrlsnchs/jwt/v3"
	"github.com/pkg/errors"

	log "github.com/Sirupsen/logrus"
)

const (
	errorPrefix         = "azure signalr error:"
	logPrefix           = "azure signalr:"
	connectionStringKey = "connectionString"
	hubKey              = "hub"
	groupKey            = "group"
	userKey             = "user"
)

// NewSignalR creates a new pub/sub based on Azure SignalR
func NewSignalR() *SignalR {
	return &SignalR{
		tokens:     make(map[string]signalrCachedToken),
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

type signalrCachedToken struct {
	token      string
	expiration time.Time
}

// SignalR is an output binding for Azure SignalR
type SignalR struct {
	endpoint   string
	accessKey  string
	version    string
	hub        string
	tokens     map[string]signalrCachedToken
	httpClient *http.Client
}

// Init is responsible for initializing the SignalR output based on the metadata
func (s *SignalR) Init(metadata bindings.Metadata) error {

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
		if i := strings.Index(connectionValue, "="); i != -1 {
			k := connectionValue[0:i]
			switch k {
			case "Endpoint":
				s.endpoint = connectionValue[i+1:]
				if s.endpoint[len(s.endpoint)-1] == '/' {
					s.endpoint = s.endpoint[:len(s.endpoint)-1]
				}
				break
			case "AccessKey":
				s.accessKey = connectionValue[i+1:]
				break
			case "Version":
				s.version = connectionValue[i+1:]
				break
			}
		}
	}

	if len(s.endpoint) == 0 {
		return fmt.Errorf("missing endpoint in connection string")
	}

	if len(s.accessKey) == 0 {
		return fmt.Errorf("missing access key in connection string")
	}

	return nil
}

func (s *SignalR) resolveAPIURL(req *bindings.WriteRequest) (string, error) {

	hub := s.hub
	if hub == "" {
		hubFromRequest, ok := req.Metadata[hubKey]
		if !ok || hubFromRequest == "" {
			return "", fmt.Errorf("%s missing hub", errorPrefix)
		}

		hub = hubFromRequest
	}

	url := ""
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
	httpReq, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	httpReq.Header.Add("Authorization", "Bearer "+token)
	httpReq.Header.Set("Content-Type", "application/json")

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

	log.Debugf("%s azure signalr call to '%s' returned with status code %d", logPrefix, url, resp.StatusCode)

	return nil
}

func (s *SignalR) Write(req *bindings.WriteRequest) error {

	url, err := s.resolveAPIURL(req)
	if err != nil {
		return err
	}

	token, err := s.ensureValidToken(url)
	if err != nil {
		return err
	}

	err = s.sendMessageToSignalR(url, token, req.Data)
	if err != nil {
		return err
	}

	return nil
}

func (s *SignalR) ensureValidToken(url string) (string, error) {
	now := time.Now()

	if existing, ok := s.tokens[url]; ok {
		if existing.token != "" && now.Before(existing.expiration) {
			return existing.token, nil
		}
	}

	expiration := now.Add(1 * time.Hour)
	pl := jwt.Payload{
		Audience:       jwt.Audience{url},
		ExpirationTime: jwt.NumericDate(expiration),
	}

	var hs = jwt.NewHS256([]byte(s.accessKey))

	token, err := jwt.Sign(pl, hs)

	if err != nil {
		return "", err
	}

	newToken := string(token)
	s.tokens[url] = signalrCachedToken{token: newToken, expiration: expiration.Add(time.Minute * -5)}

	return newToken, nil
}
