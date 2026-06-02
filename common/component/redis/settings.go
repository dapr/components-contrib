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
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"

	"github.com/dapr/kit/config"
)

const defaultRedisPort = "6379"

// entraIDRedisScope is the OAuth scope used to acquire Entra ID access tokens for
// Azure Cache/Managed Redis. Kept as a single constant so the initial-token fetch,
// the per-connection OnConnect fetch, and the background refresh loop never drift.
const entraIDRedisScope = "https://redis.azure.com/.default"

type Settings struct {
	// The Redis host
	Host string `mapstructure:"redisHost"`
	// The Redis port (optional, appended to Host if Host does not already contain a port)
	Port uint16 `mapstructure:"redisPort"`
	// The Redis password
	Password string `mapstructure:"redisPassword"`
	// The Redis username
	Username string `mapstructure:"redisUsername"`
	// The Redis Sentinel password
	SentinelPassword string `mapstructure:"sentinelPassword"`
	// The Redis Sentinel username
	SentinelUsername string `mapstructure:"sentinelUsername"`
	// Database to be selected after connecting to the server.
	DB int `mapstructure:"redisDB"`
	// The redis type node or cluster
	RedisType string `mapstructure:"redisType"`
	// Maximum number of retries before giving up.
	// A value of -1 (not 0) disables retries
	// Default is 3 retries
	RedisMaxRetries int `mapstructure:"redisMaxRetries"`
	// Minimum backoff between each retry.
	// Default is 8 milliseconds; -1 disables backoff.
	RedisMinRetryInterval Duration `mapstructure:"redisMinRetryInterval"`
	// Maximum backoff between each retry.
	// Default is 512 milliseconds; -1 disables backoff.
	RedisMaxRetryInterval Duration `mapstructure:"redisMaxRetryInterval"`
	// Dial timeout for establishing new connections.
	DialTimeout Duration `mapstructure:"dialTimeout"`
	// Timeout for socket reads. If reached, commands will fail
	// with a timeout instead of blocking. Use value -1 for no timeout and 0 for default.
	ReadTimeout Duration `mapstructure:"readTimeout"`
	// Timeout for socket writes. If reached, commands will fail
	WriteTimeout Duration `mapstructure:"writeTimeout"`
	// Maximum number of socket connections.
	PoolSize int `mapstructure:"poolSize"`
	// Minimum number of idle connections which is useful when establishing
	// new connection is slow.
	MinIdleConns int `mapstructure:"minIdleConns"`
	// Connection age at which client retires (closes) the connection.
	// Default is to not close aged connections.
	MaxConnAge Duration `mapstructure:"maxConnAge"`
	// Amount of time client waits for connection if all connections
	// are busy before returning an error.
	// Default is ReadTimeout + 1 second.
	PoolTimeout Duration `mapstructure:"poolTimeout"`
	// Amount of time after which client closes idle connections.
	// Should be less than server's timeout.
	// Default is 5 minutes. -1 disables idle timeout check.
	IdleTimeout Duration `mapstructure:"idleTimeout"`
	// Frequency of idle checks made by idle connections reaper.
	// Default is 1 minute. -1 disables idle connections reaper,
	// but idle connections are still discarded by the client
	// if IdleTimeout is set.
	IdleCheckFrequency Duration `mapstructure:"idleCheckFrequency"`
	// The master name
	SentinelMasterName string `mapstructure:"sentinelMasterName"`
	// Use Redis Sentinel for automatic failover.
	Failover bool `mapstructure:"failover"`

	// A flag to enable TLS for the Redis connection
	EnableTLS bool `mapstructure:"enableTLS"`

	// A flag to skip TLS certificate verification (insecure, use only for testing).
	// Defaults to false. When EnableTLS is true and this is false, proper certificate
	// verification is performed.
	InsecureSkipTLSVerify bool `mapstructure:"insecureSkipTLSVerify"`

	// Client certificate and key
	ClientCert string `mapstructure:"clientCert"`
	ClientKey  string `mapstructure:"clientKey"`

	// == state only properties ==
	TTLInSeconds *int   `mapstructure:"ttlInSeconds" mdonly:"state"`
	QueryIndexes string `mapstructure:"queryIndexes" mdonly:"state"`

	// == pubsub only properties ==
	// The consumer identifier
	ConsumerID string `mapstructure:"consumerID" mdonly:"pubsub"`
	// The interval between checking for pending messages to redelivery (0 disables redelivery)
	RedeliverInterval time.Duration `mapstructure:"-" mdonly:"pubsub"`
	// The amount time a message must be pending before attempting to redeliver it (0 disables redelivery)
	ProcessingTimeout time.Duration `mapstructure:"processingTimeout" mdonly:"pubsub"`
	// The size of the message queue for processing
	QueueDepth uint `mapstructure:"queueDepth" mdonly:"pubsub"`
	// The number of concurrent workers that are processing messages
	Concurrency uint `mapstructure:"concurrency" mdonly:"pubsub"`

	// The max len of stream
	MaxLenApprox int64 `mapstructure:"maxLenApprox" mdonly:"pubsub"`

	// The TTL of stream entries
	StreamTTL time.Duration `mapstructure:"streamTTL" mdonly:"pubsub"`

	// EntraID / AzureAD Authentication based on the shared code which essentially uses the DefaultAzureCredential
	// from the official Azure Identity SDK for Go
	UseEntraID bool `mapstructure:"useEntraID" mapstructurealiases:"useAzureAD"`

	// == Entra ID runtime state (populated by InitEntraIDCredential when UseEntraID is true; not configurable via metadata) ==

	// entraIDUsername is the OID parsed from the initial Entra access token's "oid" claim.
	// Used as the Redis ACL username by both the per-new-connection AUTH (OnConnect) and the
	// periodic AUTH ACL refresh goroutine.
	entraIDUsername string

	// entraIDTokenCredential is the Azure SDK credential used to acquire fresh Entra access
	// tokens on demand. The credential implementation caches tokens until close to expiry,
	// so calling GetToken on every new pool connection is inexpensive in steady state.
	entraIDTokenCredential azcore.TokenCredential
}

// entraIDToken acquires a fresh Entra access token for the Redis scope using the cached
// credential. The credential caches tokens internally until shortly before expiry, so in
// steady state this is a cheap in-memory lookup rather than a network round-trip.
func (s *Settings) entraIDToken(ctx context.Context) (azcore.AccessToken, error) {
	if s.entraIDTokenCredential == nil {
		return azcore.AccessToken{}, errors.New("redis client: EntraID credential not initialized")
	}
	return s.entraIDTokenCredential.GetToken(ctx, policy.TokenRequestOptions{
		Scopes: []string{entraIDRedisScope},
	})
}

// EntraIDFetchAuthArgs returns the Redis ACL username and a freshly-acquired Entra access
// token suitable for use as the AUTH password. It must only be called when UseEntraID is
// true and after InitEntraIDCredential has succeeded; otherwise it returns an error.
//
// This is invoked from the OnConnect callback installed on the underlying go-redis client
// so that every new pool connection authenticates with a current token, rather than the
// stale snapshot Password that would otherwise be sent during initial AUTH.
func (s *Settings) EntraIDFetchAuthArgs(ctx context.Context) (username, password string, err error) {
	if s.entraIDUsername == "" {
		return "", "", errors.New("redis client: EntraID username (OID) not initialized; AUTH ACL requires a non-empty username")
	}
	tok, err := s.entraIDToken(ctx)
	if err != nil {
		return "", "", fmt.Errorf("failed to acquire EntraID token for redis AUTH: %w", err)
	}
	return s.entraIDUsername, tok.Token, nil
}

func (s *Settings) Decode(in interface{}) error {
	if err := config.Decode(in, s); err != nil {
		return fmt.Errorf("decode failed. %w", err)
	}

	resolved, err := resolveHost(s.Host, s.Port)
	if err != nil {
		return err
	}
	s.Host = resolved

	return nil
}

// resolveHost ensures Host contains a port. If Host already includes a port
// (contains ":"), it is returned unchanged. Otherwise, the separate Port value
// is appended; when Port is empty the Redis default (6379) is used.
// For comma-separated host lists (cluster/sentinel), each entry is resolved
// individually.
// Returns an error if any address already contains a port that conflicts with
// a separately configured port value.
func resolveHost(host string, port uint16) (string, error) {
	if host == "" {
		return host, nil
	}

	// Comma-separated addresses (cluster or sentinel mode).
	if strings.Contains(host, ",") {
		parts := strings.Split(host, ",")
		addrs := make([]string, len(parts))
		for i, addr := range parts {
			resolved, err := resolveAddr(strings.TrimSpace(addr), port)
			if err != nil {
				return "", err
			}
			addrs[i] = resolved
		}
		return strings.Join(addrs, ","), nil
	}

	return resolveAddr(host, port)
}

// resolveAddr appends port to a single address when it does not already
// contain one. Returns an error if the address already contains a port that
// conflicts with the separately configured port value.
func resolveAddr(addr string, port uint16) (string, error) {
	if addr == "" {
		return addr, nil
	}

	portStr := strconv.FormatUint(uint64(port), 10)

	// Already has a port.
	if _, existingPort, err := net.SplitHostPort(addr); err == nil {
		if port != 0 && portStr != existingPort {
			return "", fmt.Errorf(
				"redis host %q already contains port %s, but redisPort is set to %s; "+
					"either remove the port from redisHost or remove the redisPort setting",
				addr, existingPort, portStr)
		}
		return addr, nil
	}

	if port == 0 {
		portStr = defaultRedisPort
	}
	return net.JoinHostPort(addr, portStr), nil
}

func (s *Settings) SetCertificate(fn func(cert *tls.Certificate)) error {
	if s.ClientCert == "" || s.ClientKey == "" {
		return nil
	}
	cert, err := tls.X509KeyPair([]byte(s.ClientCert), []byte(s.ClientKey))
	if err != nil {
		return err
	}
	fn(&cert)
	return nil
}

func (s *Settings) GetMinID(now time.Time) string {
	// If StreamTTL is not set, return empty string (no trimming)
	if s.StreamTTL == 0 {
		return ""
	}

	return fmt.Sprintf("%d-1", now.Add(-s.StreamTTL).UnixMilli())
}

type Duration time.Duration

func (r *Duration) DecodeString(value string) error {
	if val, err := strconv.Atoi(value); err == nil {
		if val < 0 {
			*r = Duration(val)

			return nil
		}
		*r = Duration(time.Duration(val) * time.Millisecond)

		return nil
	}

	// Convert it by parsing
	d, err := time.ParseDuration(value)
	if err != nil {
		return err
	}

	*r = Duration(d)

	return nil
}
