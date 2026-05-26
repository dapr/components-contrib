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

// Certification test for the state/redis component's useOIDC flow.
// Brings up Keycloak + JWKS + Valkey + a JWT-validating RESP proxy via
// docker-compose, then exercises state.save / state.get / state.delete
// through a Dapr sidecar configured with OAuth2 private_key_jwt auth.

package redis_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/secretstores"
	secretstores_env "github.com/dapr/components-contrib/secretstores/local/env"
	"github.com/dapr/components-contrib/state"
	state_redis "github.com/dapr/components-contrib/state/redis"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	secretstores_loader "github.com/dapr/dapr/pkg/components/secretstores"
	state_loader "github.com/dapr/dapr/pkg/components/state"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
)

const (
	dockerComposeYAMLAuth = "docker-compose.auth.yml"
	clusterNameOIDC       = "redis_oidc"
	sidecarNameOIDC       = "redis-oidc-sidecar"
	appIDOIDC             = "app-redis-oidc"
)

// TestRedisOIDCWithCertificates exercises the useOIDC private_key_jwt auth
// path end-to-end against Keycloak + a JWT-validating RESP proxy in front
// of Valkey. The test sets the cert/key/JWK on env vars; Keycloak picks
// them up via JWKS (served by an nginx sidecar in compose), and Dapr picks
// them up via secretstores.local.env referenced from the statestore
// component's metadata.
func TestRedisOIDCWithCertificates(t *testing.T) {
	log := logger.NewLogger("dapr.components")

	stateStore := state_redis.NewRedisStateStore(log).(*state_redis.StateStore)
	stateRegistry := state_loader.NewRegistry()
	stateRegistry.Logger = log
	stateRegistry.RegisterComponent(func(l logger.Logger) state.Store {
		return stateStore
	}, "redis")

	secretRegistry := secretstores_loader.NewRegistry()
	secretRegistry.Logger = log
	secretRegistry.RegisterComponent(func(l logger.Logger) secretstores.SecretStore {
		return secretstores_env.NewEnvSecretStore(l)
	}, "local.env")

	ports, err := dapr_testing.GetFreePorts(2)
	require.NoError(t, err)
	grpcPort := ports[0]
	httpPort := ports[1]

	// --- Generate an X.509 cert + RSA key on the fly. ---
	// The PRIVATE key is consumed by Dapr to sign the client assertion.
	// The PUBLIC key (as a JWK) is published via the jwks container and
	// fetched by Keycloak to verify the assertion.
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{Organization: []string{"Dapr Test"}},
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})

	kid := uuid.New().String()
	modulus := key.PublicKey.N.Bytes()
	exponent := big.NewInt(int64(key.PublicKey.E)).Bytes()

	// secretstores.local.env reads from os env, so the same set of vars
	// is consumed by docker-compose (jwks container) and by the Dapr
	// component metadata (via secretKeyRef).
	t.Setenv("OIDC_CLIENT_ASSERTION_CERT", string(certPEM))
	t.Setenv("OIDC_CLIENT_ASSERTION_KEY", string(keyPEM))
	t.Setenv("OIDC_CLIENT_KID", kid)
	t.Setenv("OIDC_CLIENT_JWK_N", base64.RawURLEncoding.EncodeToString(modulus))
	t.Setenv("OIDC_CLIENT_JWK_E", base64.RawURLEncoding.EncodeToString(exponent))

	exercise := func(ctx flow.Context) error {
		c, err := client.NewClientWithPort(fmt.Sprint(grpcPort))
		require.NoError(t, err)
		defer c.Close()

		k := "oidc-cert-key-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:8]
		require.NoError(t, c.SaveState(ctx, "statestore", k, []byte("oidc-was-here"), nil))

		item, err := c.GetState(ctx, "statestore", k, nil)
		require.NoError(t, err)
		assert.Equal(t, "oidc-was-here", string(item.Value))

		require.NoError(t, c.DeleteState(ctx, "statestore", k, nil))
		return nil
	}

	// Wait for the Keycloak realm to be fully imported (the broad
	// /health/ready healthcheck used by the compose file passes before
	// realm import completes) AND for the proxy to have finished its
	// lazy JWKS init.
	waitForReady := func(ctx flow.Context) error {
		deadline := time.Now().Add(2 * time.Minute)
		for time.Now().Before(deadline) {
			realm, err := http.Get("http://localhost:8080/realms/local")
			if err == nil {
				realm.Body.Close()
				if realm.StatusCode == http.StatusOK {
					// Realm is up — exercise the token endpoint once to
					// ensure the JWKS the proxy needs is also reachable.
					certs, err := http.Get("http://localhost:8080/realms/local/protocol/openid-connect/certs")
					if err == nil {
						certs.Body.Close()
						if certs.StatusCode == http.StatusOK {
							return nil
						}
					}
				}
			}
			time.Sleep(2 * time.Second)
		}
		return fmt.Errorf("timed out waiting for Keycloak realm 'local' to be ready")
	}

	flow.New(t, "state/redis useOIDC private_key_jwt").
		Step(dockercompose.Run(clusterNameOIDC, dockerComposeYAMLAuth)).
		Step("wait for Keycloak realm + JWKS", waitForReady).
		Step(sidecar.Run(sidecarNameOIDC,
			embedded.WithoutApp(),
			embedded.WithComponentsPath("./components/auth_oidc_certs"),
			embedded.WithDaprGRPCPort(fmt.Sprint(grpcPort)),
			embedded.WithDaprHTTPPort(fmt.Sprint(httpPort)),
			embedded.WithStates(stateRegistry),
			embedded.WithSecretStores(secretRegistry),
		)).
		Step("save / get / delete via OIDC-authenticated statestore", exercise).
		Step("stop sidecar", sidecar.Stop(sidecarNameOIDC)).
		Step("stop docker compose", dockercompose.Stop(clusterNameOIDC, dockerComposeYAMLAuth)).
		Run()

	// Belt-and-braces: scrub env so other tests don't pick these up.
	for _, k := range []string{
		"OIDC_CLIENT_ASSERTION_CERT", "OIDC_CLIENT_ASSERTION_KEY",
		"OIDC_CLIENT_KID", "OIDC_CLIENT_JWK_N", "OIDC_CLIENT_JWK_E",
	} {
		_ = os.Unsetenv(k)
	}
}
