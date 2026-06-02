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
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v8 "github.com/go-redis/redis/v8"
	v9 "github.com/redis/go-redis/v9"
)

// fakeTokenCredential is a stub azcore.TokenCredential for exercising the
// Entra ID auth path without contacting Azure.
type fakeTokenCredential struct {
	token string
	err   error
}

func (f fakeTokenCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	if f.err != nil {
		return azcore.AccessToken{}, f.err
	}
	return azcore.AccessToken{Token: f.token, ExpiresOn: time.Now().Add(time.Hour)}, nil
}

// entraIDSettings returns Settings wired as InitEntraIDCredential would leave
// them after a successful init, but with a stubbed credential.
func entraIDSettings(redisType string, failover bool) *Settings {
	var cred azcore.TokenCredential = fakeTokenCredential{token: "fresh-token"}
	return &Settings{
		Host:                   "localhost:6379",
		RedisType:              redisType,
		Failover:               failover,
		UseEntraID:             true,
		entraIDUsername:        "00000000-0000-0000-0000-000000000000",
		entraIDTokenCredential: &cred,
	}
}

// v8OnConnectAndPassword extracts the OnConnect callback and Password configured
// on the underlying go-redis v8 client, across the node/cluster variants.
func v8OnConnectAndPassword(t *testing.T, c RedisClient) (onConnect any, password string) {
	t.Helper()
	switch client := c.(v8Client).client.(type) {
	case *v8.Client:
		return client.Options().OnConnect, client.Options().Password
	case *v8.ClusterClient:
		return client.Options().OnConnect, client.Options().Password
	default:
		t.Fatalf("unexpected v8 client type %T", client)
		return nil, ""
	}
}

// v9OnConnectAndPassword extracts the OnConnect callback and Password configured
// on the underlying go-redis v9 client, across the node/cluster variants.
func v9OnConnectAndPassword(t *testing.T, c RedisClient) (onConnect any, password string) {
	t.Helper()
	switch client := c.(v9Client).client.(type) {
	case *v9.Client:
		return client.Options().OnConnect, client.Options().Password
	case *v9.ClusterClient:
		return client.Options().OnConnect, client.Options().Password
	default:
		t.Fatalf("unexpected v9 client type %T", client)
		return nil, ""
	}
}

func TestEntraIDOnConnectWiring(t *testing.T) {
	cases := []struct {
		name      string
		redisType string
		failover  bool
		newV8     func(*Settings) (RedisClient, error)
		newV9     func(*Settings) (RedisClient, error)
	}{
		{name: "node", redisType: NodeType, newV8: newV8Client, newV9: newV9Client},
		{name: "cluster", redisType: ClusterType, newV8: newV8Client, newV9: newV9Client},
		{name: "failover", redisType: NodeType, failover: true, newV8: newV8FailoverClient, newV9: newV9FailoverClient},
		{name: "failover-cluster", redisType: ClusterType, failover: true, newV8: newV8FailoverClient, newV9: newV9FailoverClient},
	}

	for _, tc := range cases {
		t.Run("v8/"+tc.name+"/entraID-enabled", func(t *testing.T) {
			c, err := tc.newV8(entraIDSettings(tc.redisType, tc.failover))
			require.NoError(t, err)
			require.NotNil(t, c)
			defer c.Close()

			onConnect, password := v8OnConnectAndPassword(t, c)
			assert.NotNil(t, onConnect, "OnConnect must be wired when UseEntraID is true")
			assert.Empty(t, password, "static Password must not be set when UseEntraID is true")
		})

		t.Run("v8/"+tc.name+"/entraID-disabled", func(t *testing.T) {
			s := entraIDSettings(tc.redisType, tc.failover)
			s.UseEntraID = false
			c, err := tc.newV8(s)
			require.NoError(t, err)
			require.NotNil(t, c)
			defer c.Close()

			onConnect, _ := v8OnConnectAndPassword(t, c)
			assert.Nil(t, onConnect, "OnConnect must not be wired when UseEntraID is false")
		})

		t.Run("v9/"+tc.name+"/entraID-enabled", func(t *testing.T) {
			c, err := tc.newV9(entraIDSettings(tc.redisType, tc.failover))
			require.NoError(t, err)
			require.NotNil(t, c)
			defer c.Close()

			onConnect, password := v9OnConnectAndPassword(t, c)
			assert.NotNil(t, onConnect, "OnConnect must be wired when UseEntraID is true")
			assert.Empty(t, password, "static Password must not be set when UseEntraID is true")
		})

		t.Run("v9/"+tc.name+"/entraID-disabled", func(t *testing.T) {
			s := entraIDSettings(tc.redisType, tc.failover)
			s.UseEntraID = false
			c, err := tc.newV9(s)
			require.NoError(t, err)
			require.NotNil(t, c)
			defer c.Close()

			onConnect, _ := v9OnConnectAndPassword(t, c)
			assert.Nil(t, onConnect, "OnConnect must not be wired when UseEntraID is false")
		})
	}
}

func TestEntraIDFetchAuthArgs(t *testing.T) {
	t.Run("returns username and fresh token", func(t *testing.T) {
		s := entraIDSettings(NodeType, false)
		user, pass, err := s.EntraIDFetchAuthArgs(context.Background())
		require.NoError(t, err)
		assert.Equal(t, "00000000-0000-0000-0000-000000000000", user)
		assert.Equal(t, "fresh-token", pass)
	})

	t.Run("errors when credential not initialized", func(t *testing.T) {
		s := &Settings{UseEntraID: true, entraIDUsername: "oid"}
		_, _, err := s.EntraIDFetchAuthArgs(context.Background())
		require.Error(t, err)
	})

	t.Run("errors when username (OID) not initialized", func(t *testing.T) {
		var cred azcore.TokenCredential = fakeTokenCredential{token: "fresh-token"}
		s := &Settings{UseEntraID: true, entraIDTokenCredential: &cred}
		_, _, err := s.EntraIDFetchAuthArgs(context.Background())
		require.Error(t, err)
	})

	t.Run("propagates token acquisition error", func(t *testing.T) {
		var cred azcore.TokenCredential = fakeTokenCredential{err: errors.New("boom")}
		s := &Settings{UseEntraID: true, entraIDUsername: "oid", entraIDTokenCredential: &cred}
		_, _, err := s.EntraIDFetchAuthArgs(context.Background())
		require.Error(t, err)
	})
}
