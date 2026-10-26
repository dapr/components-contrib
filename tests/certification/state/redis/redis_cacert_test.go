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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
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
	dockerComposeTLSYAML = "docker-compose-tls.yml"
	redisTLSAddr         = "localhost:6380"
)

// writeTestPKI writes a private CA and a server certificate it issued for localhost into dir, as the TLS Redis
// expects them (ca.crt, server.crt, server.key). It returns the CA certificate's PEM.
func writeTestPKI(t *testing.T, dir string) []byte {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "dapr redis certification CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	ca, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	serverTpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTpl, ca, &serverKey.PublicKey, caKey)
	require.NoError(t, err)
	serverKeyDER, err := x509.MarshalECPrivateKey(serverKey)
	require.NoError(t, err)

	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	// The Redis container does not run as the test's user, so the directory and files must be world-readable.
	require.NoError(t, os.Chmod(dir, 0o755))
	for name, content := range map[string][]byte{
		"ca.crt":     caPEM,
		"server.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}),
		"server.key": pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: serverKeyDER}),
	} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), content, 0o644)) //nolint:gosec
	}
	return caPEM
}

// TestRedisCACert runs Redis with TLS only, serving a certificate from a private CA, and checks that caCert is what
// lets the component verify it: with caCert it works; without it, or with caCert but without enableTLS, the state store
// is not configured.
func TestRedisCACert(t *testing.T) {
	t.Setenv("DAPR_CLIENT_TIMEOUT_SECONDS", "10")

	log := logger.NewLogger("dapr.components")

	tlsDir := t.TempDir()
	caPEM := writeTestPKI(t, tlsDir)
	t.Setenv("REDIS_TLS_DIR", tlsDir)
	t.Setenv("REDIS_CA_CERT", string(caPEM))

	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(caPEM))

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

	pingTLS := func(ctx flow.Context, roots *x509.CertPool) error {
		rdb := redis.NewClient(&redis.Options{
			Addr:      redisTLSAddr,
			TLSConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
		})
		defer rdb.Close()
		return rdb.Ping(ctx).Err()
	}

	checkRedisConnection := func(ctx flow.Context) error {
		return pingTLS(ctx, roots)
	}

	// A client that trusts only the system roots must be refused, so the cases below test the CA and nothing else.
	verifySystemRootsRefused := func(ctx flow.Context) error {
		system, err := x509.SystemCertPool()
		if err != nil {
			system = x509.NewCertPool()
		}
		err = pingTLS(ctx, system)
		var unknownAuthority x509.UnknownAuthorityError
		if !errors.As(err, &unknownAuthority) {
			return fmt.Errorf("expected the server certificate to be unknown to the system roots, got: %w", err)
		}
		return nil
	}

	basicTest := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(strconv.Itoa(currentGrpcPort))
		if err != nil {
			return err
		}
		defer client.Close()

		stateKey := certificationTestPrefix + "cacert"
		err = client.SaveState(ctx, stateStoreName, stateKey, []byte("redisCACertCert"), nil)
		require.NoError(t, err)

		item, err := client.GetState(ctx, stateStoreName, stateKey, nil)
		require.NoError(t, err)
		require.Equal(t, "redisCACertCert", string(item.Value))

		return client.DeleteState(ctx, stateStoreName, stateKey, nil)
	}

	testForStateStoreNotConfigured := func(ctx flow.Context) error {
		client, err := client.NewClientWithPort(strconv.Itoa(currentGrpcPort))
		if err != nil {
			return err
		}
		defer client.Close()

		err = client.SaveState(ctx, stateStoreName, certificationTestPrefix+"cacert-refused", []byte("redisCACertCert"), nil)
		require.EqualError(t, err, stateStoreNoConfigError)
		return nil
	}

	run := func(desc, name, componentsPath string, test flow.Runnable) {
		stateRegistry, secretstoreRegistry := newRegistries()
		flow.New(t, desc).
			Step(dockercompose.Run("rediscacert", dockerComposeTLSYAML)).
			Step("Waiting for Redis readiness over TLS", retry.Do(time.Second*3, 10, checkRedisConnection)).
			Step("Verify the system roots do not trust the server", verifySystemRootsRefused).
			Step(sidecar.Run(
				sidecarNamePrefix+name,
				embedded.WithoutApp(),
				embedded.WithDaprGRPCPort(strconv.Itoa(currentGrpcPort)),
				embedded.WithDaprHTTPPort(strconv.Itoa(currentHTTPPort)),
				embedded.WithComponentsPath(componentsPath),
				embedded.WithStates(stateRegistry),
				embedded.WithSecretStores(secretstoreRegistry),
			)).
			Step("Run the test", test).
			Step("stop dapr", sidecar.Stop(sidecarNamePrefix+name)).
			Run()
	}

	run("redis state store verifies a private-CA server certificate with caCert",
		"cacert", "components/docker/caCert", basicTest)
	run("redis state store without caCert fails to verify a private-CA server certificate",
		"cacert-missing", "components/docker/caCertMissing", testForStateStoreNotConfigured)
	run("redis state store with caCert but without enableTLS fails to initialize",
		"cacert-without-tls", "components/docker/caCertWithoutTLS", testForStateStoreNotConfigured)
}
