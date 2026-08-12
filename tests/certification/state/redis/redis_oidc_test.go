/*
Copyright 2026 The Dapr Authors
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

package redis_test

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	redis "github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/secretstores"
	secretstore_env "github.com/dapr/components-contrib/secretstores/local/env"
	"github.com/dapr/components-contrib/state"
	state_redis "github.com/dapr/components-contrib/state/redis"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"
	state_loader "github.com/dapr/dapr/pkg/components/state"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
)

const (
	oidcClientID        = "dapr-redis-client-jwt"
	oidcKid             = "redis-cert-test-kid"
	oidcTokenOne        = "dapr-redis-oidc-token-1"
	oidcTokenTwo        = "dapr-redis-oidc-token-2"
	oidcClientAssertion = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
)

type tokenRequestRecord struct {
	form        url.Values
	assertion   string
	issuedToken string
}

// mockIdentityProvider is an OAuth2 token endpoint that verifies the
// private_key_jwt client assertion (RS256 signature against the client
// certificate's public key) before issuing the configured access token.
// Redis cannot validate JWTs itself, so the certification of the wire format
// happens here, and Redis is configured (via ACL) to accept the issued tokens
// as AUTH passwords.
type mockIdentityProvider struct {
	publicKey *rsa.PublicKey

	lock      sync.Mutex
	token     string
	expiresIn int
	requests  []tokenRequestRecord
}

func (m *mockIdentityProvider) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	assertion := r.FormValue("client_assertion")
	if err := verifyRS256(assertion, m.publicKey); err != nil {
		http.Error(w, "invalid client assertion: "+err.Error(), http.StatusUnauthorized)
		return
	}

	m.lock.Lock()
	token, expiresIn := m.token, m.expiresIn
	m.requests = append(m.requests, tokenRequestRecord{form: r.Form, assertion: assertion, issuedToken: token})
	m.lock.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"access_token": token,
		"expires_in":   expiresIn,
	})
}

func (m *mockIdentityProvider) setToken(token string, expiresIn int) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.token = token
	m.expiresIn = expiresIn
}

func (m *mockIdentityProvider) requestCount() int {
	m.lock.Lock()
	defer m.lock.Unlock()
	return len(m.requests)
}

func (m *mockIdentityProvider) request(i int) tokenRequestRecord {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.requests[i]
}

func (m *mockIdentityProvider) lastIssuedToken() string {
	m.lock.Lock()
	defer m.lock.Unlock()
	if len(m.requests) == 0 {
		return ""
	}
	return m.requests[len(m.requests)-1].issuedToken
}

// verifyRS256 checks the JWT signature against the given RSA public key.
func verifyRS256(token string, pub *rsa.PublicKey) error {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return errors.New("JWT must have 3 parts")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return fmt.Errorf("invalid signature encoding: %w", err)
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	return rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest[:], sig)
}

func decodeJWTSegment(t *testing.T, segment string) map[string]interface{} {
	t.Helper()
	decoded, err := base64.RawURLEncoding.DecodeString(segment)
	require.NoError(t, err)
	var out map[string]interface{}
	require.NoError(t, json.Unmarshal(decoded, &out))
	return out
}

func TestRedisOIDC(t *testing.T) {
	t.Setenv("DAPR_CLIENT_TIMEOUT_SECONDS", "10")

	log := logger.NewLogger("dapr.components")

	// Generate a private key and certificate for the OIDC client assertion.
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Dapr"},
		},
	}
	cert, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})

	// The initial token expires in 310s: the background refresh runs 5 minutes
	// before expiry, so the first refresh fires ~10s after init.
	idp := &mockIdentityProvider{publicKey: &key.PublicKey, token: oidcTokenOne, expiresIn: 310}
	idpServer := httptest.NewServer(idp)
	defer idpServer.Close()

	t.Setenv("OIDC_TOKEN_ENDPOINT", idpServer.URL)
	t.Setenv("OIDC_CLIENT_ASSERTION_CERT", string(certPEM))
	t.Setenv("OIDC_CLIENT_ASSERTION_KEY", string(keyPEM))
	t.Setenv("OIDC_CLIENT_KID", oidcKid)

	ports, err := dapr_testing.GetFreePorts(2)
	require.NoError(t, err)
	currentGrpcPort := ports[0]
	currentHTTPPort := ports[1]

	newRegistries := func() (*state_loader.Registry, *secretstores_loader.Registry) {
		stateRegistry := state_loader.NewRegistry()
		stateRegistry.Logger = log
		stateRegistry.RegisterComponent(func(l logger.Logger) state.Store {
			return state_redis.NewRedisStateStore(l)
		}, "redis")

		secretstoreRegistry := secretstores_loader.NewRegistry()
		secretstoreRegistry.Logger = log
		secretstoreRegistry.RegisterComponent(func(l logger.Logger) secretstores.SecretStore {
			return secretstore_env.NewEnvSecretStore(l)
		}, "local.env")

		return stateRegistry, secretstoreRegistry
	}

	checkRedisConnection := func(ctx flow.Context) error {
		rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
		defer rdb.Close()
		return rdb.Ping(ctx).Err()
	}

	// Require a password for the default user and accept both tokens, so the
	// rotation from token-1 to token-2 is race-free: connections authenticated
	// with either token remain valid.
	setupRedisACL := func(ctx flow.Context) error {
		rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
		defer rdb.Close()
		err := rdb.Do(ctx, "ACL", "SETUSER", "default", "on", "resetpass",
			">"+oidcTokenOne, ">"+oidcTokenTwo, "allkeys", "allchannels", "+@all").Err()
		if err != nil {
			// Redis < 6.2 has no channel permissions
			err = rdb.Do(ctx, "ACL", "SETUSER", "default", "on", "resetpass",
				">"+oidcTokenOne, ">"+oidcTokenTwo, "allkeys", "+@all").Err()
		}
		if err != nil {
			return fmt.Errorf("failed to configure Redis ACL: %w", err)
		}
		return nil
	}

	verifyNoAuthFails := func(ctx flow.Context) error {
		rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
		defer rdb.Close()
		if err := rdb.Ping(ctx).Err(); err == nil {
			return errors.New("expected unauthenticated PING to fail after ACL setup")
		}
		return nil
	}

	basicTest := func(keySuffix string) flow.Runnable {
		return func(ctx flow.Context) error {
			client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
			if err != nil {
				return err
			}
			defer client.Close()

			stateKey := certificationTestPrefix + "oidc-" + keySuffix
			err = client.SaveState(ctx, stateStoreName, stateKey, []byte("redisOIDCCert"), nil)
			require.NoError(t, err)

			item, err := client.GetState(ctx, stateStoreName, stateKey, nil)
			require.NoError(t, err)
			require.Equal(t, "redisOIDCCert", string(item.Value))

			return client.DeleteState(ctx, stateStoreName, stateKey, nil)
		}
	}

	// Switch the identity provider to the second token. The long expiry pushes
	// the refresh after the next one ~2h out, so the refresh loop is dormant by
	// the time the sidecar stops (the loop terminates the process if it cannot
	// re-authenticate, e.g. against a closed client).
	rotateToken := func(ctx flow.Context) error {
		idp.setToken(oidcTokenTwo, 7200)
		return nil
	}

	waitForTokenRefresh := func(ctx flow.Context) error {
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) {
			if idp.lastIssuedToken() == oidcTokenTwo {
				// Give the AuthACL re-authentication a moment to complete
				time.Sleep(2 * time.Second)
				return nil
			}
			time.Sleep(time.Second)
		}
		return errors.New("timed out waiting for the background token refresh")
	}

	assertWireFormat := func(ctx flow.Context) error {
		require.GreaterOrEqual(t, idp.requestCount(), 2, "expected the initial token request and at least one refresh")
		req := idp.request(0)

		require.Equal(t, "client_credentials", req.form.Get("grant_type"))
		require.Equal(t, oidcClientID, req.form.Get("client_id"))
		require.Equal(t, oidcClientAssertion, req.form.Get("client_assertion_type"))
		require.Equal(t, "openid redis-cert", req.form.Get("scope"))
		require.Equal(t, "URI:RS-redis-cert-test", req.form.Get("resource"))

		parts := strings.Split(req.assertion, ".")
		require.Len(t, parts, 3, "client assertion must be a JWT")

		header := decodeJWTSegment(t, parts[0])
		require.Equal(t, "RS256", header["alg"])
		require.Equal(t, oidcKid, header["kid"])

		claims := decodeJWTSegment(t, parts[1])
		require.Equal(t, oidcClientID, claims["iss"])
		require.Equal(t, oidcClientID, claims["sub"])
		// Some IdPs require the audience to be a single JSON string, not an array
		require.IsType(t, "", claims["aud"])
		require.Equal(t, idpServer.URL, claims["aud"])
		require.NotEmpty(t, claims["jti"])

		return nil
	}

	stateRegistry, secretstoreRegistry := newRegistries()

	flow.New(t, "redis state store with OIDC private_key_jwt authentication").
		Step(dockercompose.Run("redisoidc", dockerComposeYAML)).
		Step("Waiting for Redis readiness", retry.Do(time.Second*3, 10, checkRedisConnection)).
		Step("Configure Redis ACL to accept the OIDC tokens as passwords", setupRedisACL).
		Step("Verify unauthenticated access is rejected", verifyNoAuthFails).
		Step(sidecar.Run(
			sidecarNamePrefix+"oidc",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
			embedded.WithComponentsPath("components/docker/oidc"),
			embedded.WithStates(stateRegistry),
			embedded.WithSecretStores(secretstoreRegistry),
		)).
		Step("Run basic test authenticated with the initial token", basicTest("initial")).
		Step("Rotate the identity provider token", rotateToken).
		Step("Wait for the background token refresh", waitForTokenRefresh).
		Step("Run basic test after the token refresh", basicTest("refreshed")).
		Step("Assert the private_key_jwt wire format", assertWireFormat).
		Step("stop dapr", sidecar.Stop(sidecarNamePrefix+"oidc")).
		Run()

	conflictStateRegistry, conflictSecretstoreRegistry := newRegistries()

	testForStateStoreNotConfigured := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(fmt.Sprint(currentGrpcPort))
		if err != nil {
			return err
		}
		defer client.Close()

		err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"oidc-conflict", []byte("redisOIDCCert"), nil)
		require.EqualError(t, err, stateStoreNoConfigError)

		return nil
	}

	flow.New(t, "redis state store with conflicting useOIDC and redisPassword fails to initialize").
		Step(dockercompose.Run("redisoidc", dockerComposeYAML)).
		Step("Waiting for Redis readiness", retry.Do(time.Second*3, 10, checkRedisConnection)).
		Step(sidecar.Run(
			sidecarNamePrefix+"oidc-conflict",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
			embedded.WithComponentsPath("components/docker/oidcConflict"),
			embedded.WithStates(conflictStateRegistry),
			embedded.WithSecretStores(conflictSecretstoreRegistry),
		)).
		Step("Verify the state store is not configured", testForStateStoreNotConfigured).
		Step("stop dapr", sidecar.Stop(sidecarNamePrefix+"oidc-conflict")).
		Run()
}
