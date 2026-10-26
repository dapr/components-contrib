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

package redis

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	v8 "github.com/go-redis/redis/v8"
	v9 "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testCA is a private CA and a server certificate it issued for "redis.example".
type testCA struct {
	pem  string
	leaf *x509.Certificate
}

func newTestCA(t *testing.T, name string) testCA {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	ca, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	leafTpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "redis.example"}, DNSNames: []string{"redis.example"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTpl, ca, &leafKey.PublicKey, caKey)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(leafDER)
	require.NoError(t, err)
	return testCA{pem: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})), leaf: leaf}
}

// verifies reports whether a TLS config's roots verify the CA's server certificate.
func verifies(cfg *tls.Config, ca testCA) bool {
	if cfg == nil || cfg.RootCAs == nil {
		return false
	}
	_, err := ca.leaf.Verify(x509.VerifyOptions{Roots: cfg.RootCAs, DNSName: "redis.example"})
	return err == nil
}

func TestSetRootCAs(t *testing.T) {
	t.Run("unset calls nothing", func(t *testing.T) {
		called := false
		require.NoError(t, (&Settings{}).SetRootCAs(func(*x509.CertPool) { called = true }))
		assert.False(t, called)
	})

	t.Run("invalid PEM is an error", func(t *testing.T) {
		err := (&Settings{CACert: "not a certificate"}).SetRootCAs(func(*x509.CertPool) {})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "caCert")
	})

	t.Run("the CA verifies the server it issued", func(t *testing.T) {
		ca := newTestCA(t, "private CA")
		var pool *x509.CertPool
		require.NoError(t, (&Settings{CACert: ca.pem}).SetRootCAs(func(p *x509.CertPool) { pool = p }))
		assert.True(t, verifies(&tls.Config{RootCAs: pool}, ca)) //nolint:gosec
	})

	t.Run("a bundle of two CAs trusts both, as during a rotation", func(t *testing.T) {
		old, next := newTestCA(t, "old CA"), newTestCA(t, "new CA")
		var pool *x509.CertPool
		require.NoError(t, (&Settings{CACert: old.pem + next.pem}).SetRootCAs(func(p *x509.CertPool) { pool = p }))
		cfg := &tls.Config{RootCAs: pool} //nolint:gosec
		assert.True(t, verifies(cfg, old))
		assert.True(t, verifies(cfg, next))
	})
}

func TestDecodeCACert(t *testing.T) {
	ca := newTestCA(t, "private CA")
	props := getFakeProperties()
	props["caCert"] = ca.pem
	m := &Settings{}
	require.NoError(t, m.Decode(props))
	assert.Equal(t, ca.pem, m.CACert)
}

// Every client the component can build applies caCert to the TLS config it connects with: v8 and v9, node, cluster and
// failover. None of them dials on construction.
func TestClientsApplyCACert(t *testing.T) {
	ca := newTestCA(t, "private CA")
	settings := func(mut func(*Settings)) *Settings {
		s := &Settings{Host: "redis.example:6379", EnableTLS: true, CACert: ca.pem}
		mut(s)
		return s
	}
	node := func(*Settings) {}
	cluster := func(s *Settings) { s.RedisType = ClusterType }
	failover := func(s *Settings) { s.Failover = true; s.SentinelMasterName = "mymaster" }

	tlsOf := func(t *testing.T, c RedisClient) *tls.Config {
		t.Helper()
		switch cl := c.(type) {
		case v9Client:
			switch u := cl.client.(type) {
			case *v9.Client:
				return u.Options().TLSConfig
			case *v9.ClusterClient:
				return u.Options().TLSConfig
			}
		case v8Client:
			switch u := cl.client.(type) {
			case *v8.Client:
				return u.Options().TLSConfig
			case *v8.ClusterClient:
				return u.Options().TLSConfig
			}
		}
		t.Fatalf("unexpected client %T", c)
		return nil
	}

	for name, build := range map[string]func() (RedisClient, error){
		"v9 node":     func() (RedisClient, error) { return newV9Client(settings(node)) },
		"v9 cluster":  func() (RedisClient, error) { return newV9Client(settings(cluster)) },
		"v9 failover": func() (RedisClient, error) { return newV9FailoverClient(settings(failover)) },
		"v8 node":     func() (RedisClient, error) { return newV8Client(settings(node)) },
		"v8 cluster":  func() (RedisClient, error) { return newV8Client(settings(cluster)) },
		"v8 failover": func() (RedisClient, error) { return newV8FailoverClient(settings(failover)) },
	} {
		t.Run(name, func(t *testing.T) {
			c, err := build()
			require.NoError(t, err)
			assert.True(t, verifies(tlsOf(t, c), ca), "the client's TLS config must trust caCert")
		})
	}
}
