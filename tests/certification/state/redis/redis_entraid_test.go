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
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	redis "github.com/go-redis/redis/v8"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/state"
	state_redis "github.com/dapr/components-contrib/state/redis"
	"github.com/dapr/components-contrib/tests/certification/embedded"
	"github.com/dapr/components-contrib/tests/certification/flow"
	"github.com/dapr/components-contrib/tests/certification/flow/dockercompose"
	"github.com/dapr/components-contrib/tests/certification/flow/retry"
	"github.com/dapr/components-contrib/tests/certification/flow/sidecar"
	state_loader "github.com/dapr/dapr/pkg/components/state"
	dapr_testing "github.com/dapr/dapr/pkg/testing"
	"github.com/dapr/go-sdk/client"
	"github.com/dapr/kit/logger"
)

const (
	entraOID    = "dapr-redis-entra-oid"
	entraHeader = "dapr-redis-entra-header"
)

// fakeManagedIdentity is an Azure App Service managed identity endpoint. The
// Azure SDK uses it when IDENTITY_ENDPOINT and IDENTITY_HEADER are set, so the
// component's Entra ID path runs end to end without Azure.
type fakeManagedIdentity struct {
	lock     sync.Mutex
	token    string
	lifetime time.Duration
	issued   []string
	resource string
}

func (m *fakeManagedIdentity) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-IDENTITY-HEADER") != entraHeader {
		http.Error(w, "missing identity header", http.StatusUnauthorized)
		return
	}

	m.lock.Lock()
	token, expiresOn := m.token, time.Now().Add(m.lifetime).Unix()
	m.issued = append(m.issued, token)
	m.resource = r.URL.Query().Get("resource")
	m.lock.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"access_token": token,
		"expires_on":   strconv.FormatInt(expiresOn, 10),
		"token_type":   "Bearer",
	})
}

func (m *fakeManagedIdentity) setToken(token string, lifetime time.Duration) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.token = token
	m.lifetime = lifetime
}

func (m *fakeManagedIdentity) lastIssued() string {
	m.lock.Lock()
	defer m.lock.Unlock()
	if len(m.issued) == 0 {
		return ""
	}
	return m.issued[len(m.issued)-1]
}

// entraToken returns a JWT carrying the oid claim, which the component uses as
// the Redis ACL username. The component does not verify the signature, so a
// throwaway key works.
func entraToken(t *testing.T, id string, lifetime time.Duration) string {
	tok, err := jwt.NewBuilder().JwtID(id).Claim("oid", entraOID).Expiration(time.Now().Add(lifetime)).Build()
	require.NoError(t, err)
	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.HS256, []byte("certification-test-key")))
	require.NoError(t, err)
	return string(signed)
}

// TestRedisEntraID reproduces dapr/components-contrib#3554: once the Entra ID
// token the component started with is revoked, every new connection must
// authenticate with the refreshed token rather than the one from init.
func TestRedisEntraID(t *testing.T) {
	t.Setenv("DAPR_CLIENT_TIMEOUT_SECONDS", "10")

	log := logger.NewLogger("dapr.components")

	// The first token lives under the Azure SDK's 5 minute cache margin, so every
	// fetch reaches the endpoint, and the refresh routine fires at its midpoint.
	// The second lives long enough to be cached and to leave the refresh routine
	// dormant when the sidecar stops (it terminates the process if it cannot AUTH).
	tokenOne := entraToken(t, "1", 30*time.Second)
	tokenTwo := entraToken(t, "2", 2*time.Hour)

	mi := &fakeManagedIdentity{token: tokenOne, lifetime: 30 * time.Second}
	miServer := httptest.NewServer(mi)
	defer miServer.Close()
	t.Setenv("IDENTITY_ENDPOINT", miServer.URL)
	t.Setenv("IDENTITY_HEADER", entraHeader)

	ports, err := dapr_testing.GetFreePorts(2)
	require.NoError(t, err)
	currentGrpcPort := ports[0]
	currentHTTPPort := ports[1]

	stateRegistry := state_loader.NewRegistry()
	stateRegistry.Logger = log
	stateRegistry.RegisterComponent(func(l logger.Logger) state.Store {
		return state_redis.NewRedisStateStore(l)
	}, "redis")

	// The default user stays passwordless, so this client keeps admin access
	// while the component authenticates as the Entra object ID.
	adminDo := func(ctx flow.Context, args ...interface{}) *redis.Cmd {
		rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
		defer rdb.Close()
		return rdb.Do(ctx, args...)
	}

	checkRedisConnection := func(ctx flow.Context) error {
		return adminDo(ctx, "PING").Err()
	}

	setUserACL := func(rules ...string) flow.Runnable {
		return func(ctx flow.Context) error {
			args := []interface{}{"ACL", "SETUSER", entraOID, "on", "allkeys", "allchannels", "+@all"}
			for _, r := range rules {
				args = append(args, r)
			}
			err := adminDo(ctx, args...).Err()
			if err != nil {
				// Redis < 6.2 has no channel permissions
				args = append(args[:5], args[6:]...)
				err = adminDo(ctx, args...).Err()
			}
			return err
		}
	}

	basicTest := func(keySuffix string) flow.Runnable {
		return func(ctx flow.Context) error {
			client, err := client.NewClientWithPort(strconv.Itoa(currentGrpcPort))
			if err != nil {
				return err
			}
			defer client.Close()

			stateKey := certificationTestPrefix + "entraid-" + keySuffix
			err = client.SaveState(ctx, stateStoreName, stateKey, []byte("redisEntraIDCert"), nil)
			require.NoError(t, err)

			item, err := client.GetState(ctx, stateStoreName, stateKey, nil)
			require.NoError(t, err)
			require.Equal(t, "redisEntraIDCert", string(item.Value))

			return client.DeleteState(ctx, stateStoreName, stateKey, nil)
		}
	}

	rotateToken := func(ctx flow.Context) error {
		mi.setToken(tokenTwo, 2*time.Hour)
		return nil
	}

	// Nothing else dials between the rotation and the refresh, so the first
	// fetch of the second token is the refresh routine's.
	waitForTokenRefresh := func(ctx flow.Context) error {
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) {
			if mi.lastIssued() == tokenTwo {
				// Give the refresh routine's AUTH a moment to complete.
				time.Sleep(2 * time.Second)
				return nil
			}
			time.Sleep(time.Second)
		}
		return errors.New("timed out waiting for the background token refresh")
	}

	// Drop every connection the component holds, so the next request dials.
	killComponentConnections := func(ctx flow.Context) error {
		killed, err := adminDo(ctx, "CLIENT", "KILL", "USER", entraOID).Int()
		if err != nil {
			return err
		}
		if killed == 0 {
			return errors.New("expected the component to hold at least one connection")
		}
		return nil
	}

	assertAuthenticatedAsObjectID := func(ctx flow.Context) error {
		list, err := adminDo(ctx, "CLIENT", "LIST").Text()
		require.NoError(t, err)
		require.Contains(t, list, "user="+entraOID)

		mi.lock.Lock()
		defer mi.lock.Unlock()
		require.Equal(t, "https://redis.azure.com", mi.resource)
		return nil
	}

	flow.New(t, "redis state store with Entra ID authentication re-authenticates new connections after token rotation").
		Step(dockercompose.Run("redisentraid", dockerComposeYAML)).
		Step("Waiting for Redis readiness", retry.Do(time.Second*3, 10, checkRedisConnection)).
		Step("Accept the initial token for the Entra object ID", setUserACL(">"+tokenOne)).
		Step(sidecar.Run(
			sidecarNamePrefix+"entraid",
			embedded.WithoutApp(),
			embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
			embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
			embedded.WithComponentsPath("components/docker/entraid"),
			embedded.WithStates(stateRegistry),
		)).
		Step("Run basic test authenticated with the initial token", basicTest("initial")).
		Step("Accept both tokens", setUserACL(">"+tokenOne, ">"+tokenTwo)).
		Step("Rotate the managed identity token", rotateToken).
		Step("Wait for the background token refresh", waitForTokenRefresh).
		Step("Revoke the initial token", setUserACL("resetpass", ">"+tokenTwo)).
		Step("Drop the component's connections", killComponentConnections).
		Step("Run basic test on new connections after the initial token was revoked", basicTest("rotated")).
		Step("Assert the component authenticated as the Entra object ID", assertAuthenticatedAsObjectID).
		Step("stop dapr", sidecar.Stop(sidecarNamePrefix+"entraid")).
		Run()
}
