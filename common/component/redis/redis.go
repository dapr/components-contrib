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

package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/cenkalti/backoff/v4"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"golang.org/x/mod/semver"
	"golang.org/x/oauth2"

	"github.com/dapr/components-contrib/common/authentication/azure"
	"github.com/dapr/components-contrib/configuration"
	"github.com/dapr/components-contrib/metadata"
	kitlogger "github.com/dapr/kit/logger"
	kitretry "github.com/dapr/kit/retry"
)

const (
	ClusterType = "cluster"
	NodeType    = "node"

	processingTimeoutKey     = "processingTimeout"
	redeliverIntervalKey     = "redeliverInterval"
	redisMinRetryIntervalKey = "redisMinRetryInterval"
	maxRetryBackoffKey       = "maxRetryBackoff"
	redisMaxRetriesKey       = "redisMaxRetries"
	maxRetriesKey            = "maxRetries"
)

type RedisXMessage struct {
	ID     string
	Values map[string]interface{}
}

type RedisXStream struct {
	Stream   string
	Messages []RedisXMessage
}

type RedisXPendingExt struct {
	ID         string
	Consumer   string
	Idle       time.Duration
	RetryCount int64
}

type RedisPipeliner interface {
	Exec(ctx context.Context) error
	Do(ctx context.Context, args ...interface{})
}

//nolint:interfacebloat
type RedisClient interface {
	IsNilValueError(error) bool
	Context() context.Context
	DoRead(ctx context.Context, args ...interface{}) (interface{}, error)
	DoWrite(ctx context.Context, args ...interface{}) error
	Del(ctx context.Context, keys ...string) error
	Get(ctx context.Context, key string) (string, error)
	GetDel(ctx context.Context, key string) (string, error)
	Close() error
	PingResult(ctx context.Context) (string, error)
	ConfigurationSubscribe(ctx context.Context, args *ConfigurationSubscribeArgs)
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) (*bool, error)
	EvalInt(ctx context.Context, script string, keys []string, args ...interface{}) (*int, error, error)
	XAdd(ctx context.Context, stream string, maxLenApprox int64, streamTTL string, values map[string]interface{}) (string, error)
	XGroupCreateMkStream(ctx context.Context, stream string, group string, start string) error
	XAck(ctx context.Context, stream string, group string, messageID string) error
	XReadGroupResult(ctx context.Context, group string, consumer string, streams []string, count int64, block time.Duration) ([]RedisXStream, error)
	XPendingExtResult(ctx context.Context, stream string, group string, start string, end string, count int64) ([]RedisXPendingExt, error)
	XClaimResult(ctx context.Context, stream string, group string, consumer string, minIdleTime time.Duration, messageIDs []string) ([]RedisXMessage, error)
	TxPipeline() RedisPipeliner
	TTLResult(ctx context.Context, key string) (time.Duration, error)
	AuthACL(ctx context.Context, username, password string) error
}

type ConfigurationSubscribeArgs struct {
	HandleSubscribedChange func(ctx context.Context, req *configuration.SubscribeRequest, handler configuration.UpdateHandler, channel string, id string)
	Req                    *configuration.SubscribeRequest
	Handler                configuration.UpdateHandler
	RedisChannel           string
	IsAllKeysChannel       bool
	ID                     string
}

func ParseClientFromProperties(properties map[string]string, componentType metadata.ComponentType, ctx context.Context, logger *kitlogger.Logger) (RedisClient, *Settings, error) {
	settings := Settings{}

	// upgrade legacy metadata properties and set defaults
	switch componentType {
	case metadata.ConfigurationStoreType:
		// Apply legacy defaults
		settings.RedisMaxRetries = 3
		settings.RedisMaxRetryInterval = Duration(2 * time.Second)
		settings.RedisMinRetryInterval = Duration(8 * time.Millisecond)
	case metadata.StateStoreType, metadata.LockStoreType:
		// Apply legacy defaults
		settings.RedisMaxRetries = 3
		settings.RedisMinRetryInterval = Duration(2 * time.Second)
		// Parse legacy keys
		if properties[redisMinRetryIntervalKey] == "" {
			if properties[maxRetryBackoffKey] != "" {
				// due to different duration formats, do not simply change the key name
				parsedVal, err := strconv.ParseInt(properties[maxRetryBackoffKey], 10, 0)
				if err != nil {
					return nil, nil, fmt.Errorf("redis store error: can't parse maxRetryBackoff field: %s", err)
				}
				settings.RedisMinRetryInterval = Duration(time.Duration(parsedVal))
			}
		}
		if properties[redisMaxRetriesKey] == "" {
			if properties[maxRetriesKey] != "" {
				properties[redisMaxRetriesKey] = properties[maxRetriesKey]
			}
		}

	case metadata.PubSubType:
		settings.ProcessingTimeout = 60 * time.Second
		settings.RedeliverInterval = 15 * time.Second
		settings.QueueDepth = 100
		settings.Concurrency = 10
	}

	err := settings.Decode(properties)
	if err != nil {
		return nil, nil, fmt.Errorf("redis client configuration error: %w", err)
	}

	switch componentType {
	case metadata.PubSubType:
		if val, ok := properties[processingTimeoutKey]; ok && val != "" {
			if processingTimeoutMs, parseErr := strconv.ParseUint(val, 10, 64); parseErr == nil {
				// because of legacy reasons, we need to interpret a number as milliseconds
				// the library would default to seconds otherwise
				settings.ProcessingTimeout = time.Duration(processingTimeoutMs) * time.Millisecond //nolint:gosec
			}
			// if there was an error we would try to interpret it as a duration string, which was already done in Decode()
		}

		if val, ok := properties[redeliverIntervalKey]; ok && val != "" {
			if redeliverIntervalMs, parseErr := strconv.ParseUint(val, 10, 64); parseErr == nil {
				// because of legacy reasons, we need to interpret a number as milliseconds
				// the library would default to seconds otherwise
				settings.RedeliverInterval = time.Duration(redeliverIntervalMs) * time.Millisecond //nolint:gosec
			}
			// if there was an error we would try to interpret it as a duration string, which was already done in Decode()
		}
	}
	var oidcTokenSource *OAuthTokenSourcePrivateKeyJWT
	var oidcTokenExpiry time.Time
	if settings.UseOIDC {
		oidcTokenSource, oidcTokenExpiry, err = settings.GetOIDCTokenSourceAndSetInitialTokenAsPassword(ctx, logger)
		if err != nil {
			return nil, nil, err
		}
	}

	var tokenExpires *time.Time
	var tokenCredential *azcore.TokenCredential
	if settings.UseEntraID {
		tokenExpires, tokenCredential, err = settings.GetEntraIDCredentialAndSetInitialTokenAsPassword(ctx, &properties)
		if err != nil {
			return nil, nil, err
		}
	}

	var c RedisClient
	newClientFunc := newV8Client
	if settings.Failover {
		newClientFunc = newV8FailoverClient
	}

	c, err = newClientFunc(&settings)
	if err != nil {
		return nil, nil, fmt.Errorf("redis client configuration error: %w", err)
	}

	version, err := GetServerVersion(c)
	closeErr := c.Close() // close the client to avoid leaking connections
	if closeErr != nil {
		return nil, nil, closeErr
	}

	useNewClient := false
	if err != nil {
		// we couldn't query the server version, so we will assume the v8 client is not supported
		useNewClient = true
	} else if semver.Compare("v"+version, "v7.0.0") > -1 {
		// if the server version is >= 7, we will use the v9 client
		useNewClient = true
	}

	if useNewClient {
		newClientFunc = newV9Client
		if settings.Failover {
			newClientFunc = newV9FailoverClient
		}
	}
	c, err = newClientFunc(&settings)
	if err != nil {
		return nil, nil, fmt.Errorf("redis client configuration error: %w", err)
	}

	// start the token refresh goroutine

	if settings.UseEntraID {
		StartEntraIDTokenRefreshBackgroundRoutine(c, settings.Username, *tokenExpires, tokenCredential, logger)
	}
	if settings.UseOIDC {
		StartOIDCTokenRefreshBackgroundRoutine(c, settings.Username, oidcTokenExpiry, oidcTokenSource, logger)
	}
	return c, &settings, nil
}

func StartEntraIDTokenRefreshBackgroundRoutine(client RedisClient, username string, nextExpiration time.Time, cred *azcore.TokenCredential, logger *kitlogger.Logger) {
	go func(cred *azcore.TokenCredential, username string, logger *kitlogger.Logger) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		backoffConfig := kitretry.DefaultConfig()
		backoffConfig.MaxRetries = 3
		backoffConfig.Policy = kitretry.PolicyExponential

		var backoffManager backoff.BackOff
		const refreshGracePeriod = 5 * time.Minute
		tokenRefreshDuration := time.Until(nextExpiration.Add(-refreshGracePeriod))

		(*logger).Debugf("redis client: starting entraID token refresh loop")

		for {
			(*logger).Debugf("redis client: next entraID token refresh: %v", tokenRefreshDuration)
			select {
			case <-ctx.Done():
				(*logger).Infof("redis client: entraID token refresh stopped due to context cancellation")
				return
			case <-time.After(tokenRefreshDuration):
				(*logger).Debug("redis client: refreshing entraID token")
				// Get a new access token
				backoffManager = backoffConfig.NewBackOffWithContext(ctx)
				var token azcore.AccessToken
				tokenErr := kitretry.NotifyRecover(
					func() error {
						var innerTokenErr error
						token, innerTokenErr = (*cred).GetToken(ctx, policy.TokenRequestOptions{
							Scopes: []string{"https://redis.azure.com/.default"},
						})
						return innerTokenErr
					},
					backoffManager,
					func(err error, _ time.Duration) {
						(*logger).Debugf("redis client: entraID token acquisition failed with error: %v. Retrying...", err)
					},
					func() {
						(*logger).Debug("redis client: entraID token acquisition succeeded after error")
					},
				)
				if tokenErr != nil {
					_ = client.Close()
					(*logger).Fatalf("redis client: entraID token acquisition failed: %v", tokenErr)
					return
				}

				// Use the new access token via the Redis AUTH command
				backoffManager = backoffConfig.NewBackOffWithContext(ctx)
				authErr := kitretry.NotifyRecover(
					func() error {
						innerAuthErr := client.AuthACL(ctx, username, token.Token)
						return innerAuthErr
					},
					backoffManager,
					func(err error, _ time.Duration) {
						(*logger).Debugf("redis client: entraID auth failed with error: %v. Retrying...", err)
					},
					func() {
						(*logger).Debug("redis client: entraID auth succeeded after error")
					},
				)
				if authErr != nil {
					_ = client.Close()
					(*logger).Fatalf("redis client: entraID auth failed: %v", authErr)
					return
				}
				// Since the entraID auth succeeded we are setting the duration to wait for the next iteration of the refresh loop

				(*logger).Debugf("redis client: entraID auth token successfully refreshed with the server")

				tokenRefreshDuration = time.Until(token.ExpiresOn.Add(-refreshGracePeriod))
			}
		}
	}(cred, username, logger)
}

func (s *Settings) GetEntraIDCredentialAndSetInitialTokenAsPassword(ctx context.Context, properties *map[string]string) (*time.Time, *azcore.TokenCredential, error) {
	if len(s.Password) > 0 || len(s.Username) > 0 {
		return nil, nil, errors.New(
			"redis client configuration error: username or password must not be specified when using Entra ID authentication",
		)
	}
	envSettings, err := azure.NewEnvironmentSettings(*properties)
	if err != nil {
		return nil, nil, fmt.Errorf("redis client configuration error: %w", err)
	}
	cred, err := envSettings.GetTokenCredential()
	if err != nil {
		return nil, nil, fmt.Errorf("redis client configuration error: %w", err)
	}

	token, err := cred.GetToken(ctx, policy.TokenRequestOptions{
		Scopes: []string{"https://redis.azure.com/.default"},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("redis client configuration error: %w", err)
	}

	s.Password = token.Token

	// This token has already been validated by EntraID. We use insecure parsing to get the object ID.
	parsedToken, err := jwt.ParseString(token.Token, jwt.WithVerify(false), jwt.WithValidate(false))
	if err != nil {
		return nil, nil, fmt.Errorf("redis client configuration error: %w", err)
	}
	objectID, found := parsedToken.Get("oid")

	if found {
		s.Username = objectID.(string)
	} else {
		return nil, nil, errors.New("redis client configuration error: could not parse object ID from Auth token")
	}
	return &token.ExpiresOn, &cred, nil
}

// GetOIDCTokenSourceAndSetInitialTokenAsPassword validates the OIDC private_key_jwt
// settings, builds the token source, fetches the initial access token and sets it as
// the password for the Redis connection. It returns the token source and the expiry
// of the initial token so a refresh routine can be scheduled.
func (s *Settings) GetOIDCTokenSourceAndSetInitialTokenAsPassword(ctx context.Context, logger *kitlogger.Logger) (*OAuthTokenSourcePrivateKeyJWT, time.Time, error) {
	if s.UseEntraID {
		return nil, time.Time{}, errors.New("redis client configuration error: useEntraID and useOIDC must not be enabled at the same time")
	}
	if len(s.Password) > 0 {
		return nil, time.Time{}, errors.New("redis client configuration error: password must not be specified when using OIDC authentication")
	}
	if s.OidcTokenEndpoint == "" {
		return nil, time.Time{}, errors.New("redis client configuration error: missing oidcTokenEndpoint for OIDC authentication")
	}
	if s.OidcClientID == "" {
		return nil, time.Time{}, errors.New("redis client configuration error: missing oidcClientID for OIDC authentication")
	}
	if s.OidcClientAssertionCert == "" {
		return nil, time.Time{}, errors.New("redis client configuration error: missing oidcClientAssertionCert for OIDC authentication")
	}
	if s.OidcClientAssertionKey == "" {
		return nil, time.Time{}, errors.New("redis client configuration error: missing oidcClientAssertionKey for OIDC authentication")
	}

	// Leave Username empty unless the operator set one explicitly. An empty
	// username makes the client send the 1-argument AUTH <token> form, which the
	// widest range of RESP-compatible auth layers accept; a configured username
	// switches to the 2-argument AUTH <username> <token> (ACL) form.

	// Parse the comma-delimited scopes, trimming whitespace and dropping empty entries.
	s.internalOidcScopes = nil
	for _, scope := range strings.Split(s.OidcScopes, ",") {
		if scope = strings.TrimSpace(scope); scope != "" {
			s.internalOidcScopes = append(s.internalOidcScopes, scope)
		}
	}
	if len(s.internalOidcScopes) == 0 {
		(*logger).Warn("redis client: no OIDC scopes specified, using default 'openid' scope only. This is a security risk for token reuse.")
		s.internalOidcScopes = []string{"openid"}
	}

	tokenSource := &OAuthTokenSourcePrivateKeyJWT{
		TokenEndpoint:       oauth2.Endpoint{TokenURL: s.OidcTokenEndpoint},
		ClientID:            s.OidcClientID,
		Scopes:              s.internalOidcScopes,
		ClientAssertionCert: s.OidcClientAssertionCert,
		ClientAssertionKey:  s.OidcClientAssertionKey,
		Resource:            s.OidcResource,
		Audience:            s.OidcAudience,
		Kid:                 s.OidcKid,
	}
	if s.OidcCACert != "" {
		if err := tokenSource.addCa(s.OidcCACert); err != nil {
			return nil, time.Time{}, fmt.Errorf("redis client configuration error: %w", err)
		}
	}

	token, err := tokenSource.Token(ctx)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("redis client configuration error: %w", err)
	}
	s.Password = token.AccessToken

	return tokenSource, token.Expiry, nil
}

// StartOIDCTokenRefreshBackgroundRoutine refreshes the OIDC access token before it
// expires and re-authenticates the live Redis connection pool with it.
func StartOIDCTokenRefreshBackgroundRoutine(client RedisClient, username string, nextExpiration time.Time, tokenSource *OAuthTokenSourcePrivateKeyJWT, logger *kitlogger.Logger) {
	go runTokenRefreshLoop(client, username, nextExpiration, logger, "OIDC", func(ctx context.Context) (string, time.Time, error) {
		token, err := tokenSource.ForceToken(ctx)
		if err != nil {
			return "", time.Time{}, err
		}
		return token.AccessToken, token.Expiry, nil
	})
}

// runTokenRefreshLoop periodically refreshes an authentication token before it
// expires and re-authenticates the existing Redis connection pool via the AUTH
// command. fetch must return a fresh token and its expiry. On definitive failure
// (after retries) the client is closed and the process is terminated, mirroring
// the behavior of the EntraID token refresh routine.
func runTokenRefreshLoop(client RedisClient, username string, nextExpiration time.Time, logger *kitlogger.Logger, label string, fetch func(ctx context.Context) (string, time.Time, error)) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	backoffConfig := kitretry.DefaultConfig()
	backoffConfig.MaxRetries = 3
	backoffConfig.Policy = kitretry.PolicyExponential

	var backoffManager backoff.BackOff
	const refreshGracePeriod = 5 * time.Minute
	// minTokenRefreshInterval floors the wait between refreshes so a short-lived
	// or near-expiry token (remaining lifetime <= refreshGracePeriod) cannot
	// drive the loop into a tight refresh+AUTH spin against the IdP and Redis.
	const minTokenRefreshInterval = 5 * time.Second
	nextRefresh := func(expiry time.Time) time.Duration {
		if d := time.Until(expiry.Add(-refreshGracePeriod)); d > minTokenRefreshInterval {
			return d
		}
		return minTokenRefreshInterval
	}
	tokenRefreshDuration := nextRefresh(nextExpiration)

	(*logger).Debugf("redis client: starting %s token refresh loop", label)

	for {
		(*logger).Debugf("redis client: next %s token refresh: %v", label, tokenRefreshDuration)
		select {
		case <-ctx.Done():
			(*logger).Infof("redis client: %s token refresh stopped due to context cancellation", label)
			return
		case <-time.After(tokenRefreshDuration):
			(*logger).Debugf("redis client: refreshing %s token", label)
			// Get a new access token
			var newToken string
			var newExpiry time.Time
			backoffManager = backoffConfig.NewBackOffWithContext(ctx)
			tokenErr := kitretry.NotifyRecover(
				func() error {
					var innerTokenErr error
					newToken, newExpiry, innerTokenErr = fetch(ctx)
					return innerTokenErr
				},
				backoffManager,
				func(err error, _ time.Duration) {
					(*logger).Debugf("redis client: %s token acquisition failed with error: %v. Retrying...", label, err)
				},
				func() {
					(*logger).Debugf("redis client: %s token acquisition succeeded after error", label)
				},
			)
			if tokenErr != nil {
				_ = client.Close()
				(*logger).Fatalf("redis client: %s token acquisition failed: %v", label, tokenErr)
				return
			}

			// Re-authenticate the connection with the new token via the Redis AUTH
			// command. DoWrite executes the command immediately (unlike AuthACL,
			// which enqueues on a pipeline). With no username we send the
			// 1-argument AUTH <token> form; with one we send AUTH <username> <token>.
			backoffManager = backoffConfig.NewBackOffWithContext(ctx)
			authErr := kitretry.NotifyRecover(
				func() error {
					if username == "" {
						return client.DoWrite(ctx, "AUTH", newToken)
					}
					return client.DoWrite(ctx, "AUTH", username, newToken)
				},
				backoffManager,
				func(err error, _ time.Duration) {
					(*logger).Debugf("redis client: %s auth failed with error: %v. Retrying...", label, err)
				},
				func() {
					(*logger).Debugf("redis client: %s auth succeeded after error", label)
				},
			)
			if authErr != nil {
				_ = client.Close()
				(*logger).Fatalf("redis client: %s auth failed: %v", label, authErr)
				return
			}

			(*logger).Debugf("redis client: %s auth token successfully refreshed with the server", label)

			// Schedule the next refresh based on the new token's expiry
			tokenRefreshDuration = nextRefresh(newExpiry)
		}
	}
}

func ClientHasJSONSupport(c RedisClient) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := c.DoWrite(ctx, "JSON.GET")
	if err == nil {
		return true
	}

	if strings.HasPrefix(err.Error(), "ERR unknown command") {
		return false
	}
	return true
}

func GetServerVersion(c RedisClient) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := c.DoRead(ctx, "INFO", "server")
	if err != nil {
		return "", err
	}
	// get row in string res beginning with "redis_version"
	rows := strings.Split(res.(string), "\n")
	for _, row := range rows {
		if strings.HasPrefix(row, "redis_version:") {
			return strings.TrimSpace(strings.Split(row, ":")[1]), nil
		}
	}
	return "", errors.New("could not find redis_version in redis info response")
}

// GetConnectedSlaves returns the number of slaves connected to the Redis master.
func GetConnectedSlaves(ctx context.Context, c RedisClient) (int, error) {
	const connectedSlavesReplicas = "connected_slaves:"

	res, err := c.DoRead(ctx, "INFO", "replication")
	if err != nil {
		return 0, err
	}

	// Response example: https://redis.io/commands/info#return-value
	// # Replication\r\nrole:master\r\nconnected_slaves:1\r\n
	s, _ := strconv.Unquote(fmt.Sprintf("%q", res))
	if len(s) == 0 {
		return 0, nil
	}

	infos := strings.Split(s, "\r\n")
	for _, info := range infos {
		if strings.HasPrefix(info, connectedSlavesReplicas) {
			parsedReplicas, _ := strconv.ParseInt(info[len(connectedSlavesReplicas):], 10, 32)
			return int(parsedReplicas), nil
		}
	}

	return 0, nil
}

type RedisError string

func (e RedisError) Error() string { return string(e) }

func (RedisError) RedisError() {}
