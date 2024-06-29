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
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	redisComponent "github.com/dapr/components-contrib/common/component/redis"
	contribMetadata "github.com/dapr/components-contrib/metadata"

	"github.com/dapr/components-contrib/configuration"
	"github.com/dapr/kit/logger"
)

func TestConfigurationStore_Get(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()
	require.NoError(t, s.Set("testKey", "testValue"))
	require.NoError(t, s.Set("testKey2", "testValue2"))
	type fields struct {
		client redisComponent.RedisClient

		json     jsoniter.API
		replicas int
		logger   logger.Logger
	}
	type args struct {
		ctx context.Context
		req *configuration.GetRequest
	}
	tests := []struct {
		name    string
		prepare func(redisComponent.RedisClient)
		restore func(redisComponent.RedisClient)
		fields  fields
		args    args
		want    *configuration.GetResponse
		wantErr bool
	}{
		{
			name: "normal get redis value",
			fields: fields{
				client: c,
				json:   jsoniter.ConfigFastest,
				logger: logger.NewLogger("test"),
			},
			args: args{
				req: &configuration.GetRequest{
					Keys: []string{"testKey"},
				},
				ctx: context.Background(),
			},
			want: &configuration.GetResponse{
				Items: map[string]*configuration.Item{
					"testKey": {
						Value:    "testValue",
						Metadata: make(map[string]string),
					},
				},
			},
		},
		{
			name: "get with no request key",
			fields: fields{
				client: c,
				json:   jsoniter.ConfigFastest,
				logger: logger.NewLogger("test"),
			},
			args: args{
				req: &configuration.GetRequest{},
				ctx: context.Background(),
			},
			want: &configuration.GetResponse{
				Items: map[string]*configuration.Item{
					"testKey": {
						Value:    "testValue",
						Metadata: make(map[string]string),
					},
					"testKey2": {
						Value:    "testValue2",
						Metadata: make(map[string]string),
					},
				},
			},
		},
		{
			name: "get with not exists key",
			fields: fields{
				client: c,
				json:   jsoniter.ConfigFastest,
				logger: logger.NewLogger("test"),
			},
			args: args{
				req: &configuration.GetRequest{
					Keys: []string{"notExistKey"},
				},
				ctx: context.Background(),
			},
			want: &configuration.GetResponse{
				Items: map[string]*configuration.Item{},
			},
		},
		{
			name: "test does not throw error for wrong type during get all",
			prepare: func(client redisComponent.RedisClient) {
				client.DoWrite(context.Background(), "HSET", "notSupportedType", []string{"key1", "value1", "key2", "value2"})
			},
			fields: fields{
				client: c,
				json:   jsoniter.ConfigFastest,
				logger: logger.NewLogger("test"),
			},
			args: args{
				req: &configuration.GetRequest{},
				ctx: context.Background(),
			},
			want: &configuration.GetResponse{
				Items: map[string]*configuration.Item{
					"testKey": {
						Value:    "testValue",
						Metadata: make(map[string]string),
					},
					"testKey2": {
						Value:    "testValue2",
						Metadata: make(map[string]string),
					},
				},
			},
			restore: func(client redisComponent.RedisClient) {
				client.DoWrite(context.Background(), "HDEL", "notSupportedType")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.prepare != nil {
				tt.prepare(tt.fields.client)
			}
			r := &ConfigurationStore{
				client:   tt.fields.client,
				json:     tt.fields.json,
				replicas: tt.fields.replicas,
				logger:   tt.fields.logger,
			}
			got, err := r.Get(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if got == nil {
				t.Errorf("Get() got configuration response is nil")
				return
			}

			if len(got.Items) != len(tt.want.Items) {
				t.Errorf("Get() got len = %v, want len = %v", len(got.Items), len(tt.want.Items))
				return
			}

			if len(got.Items) == 0 {
				return
			}

			for k := range got.Items {
				assert.Equal(t, tt.want.Items[k], got.Items[k])
			}
			if tt.restore != nil {
				tt.restore(tt.fields.client)
			}
		})
	}
}

func TestParseConnectedSlaves(t *testing.T) {
	store := &ConfigurationStore{logger: logger.NewLogger("test")}

	t.Run("Empty info", func(t *testing.T) {
		slaves := store.parseConnectedSlaves("")
		assert.Equal(t, 0, slaves, "connected slaves must be 0")
	})

	t.Run("connectedSlaves property is not included", func(t *testing.T) {
		slaves := store.parseConnectedSlaves("# Replication\r\nrole:master\r\n")
		assert.Equal(t, 0, slaves, "connected slaves must be 0")
	})

	t.Run("connectedSlaves is 2", func(t *testing.T) {
		slaves := store.parseConnectedSlaves("# Replication\r\nrole:master\r\nconnected_slaves:2\r\n")
		assert.Equal(t, 2, slaves, "connected slaves must be 2")
	})

	t.Run("connectedSlaves is 1", func(t *testing.T) {
		slaves := store.parseConnectedSlaves("# Replication\r\nrole:master\r\nconnected_slaves:1")
		assert.Equal(t, 1, slaves, "connected slaves must be 1")
	})
}

func TestNewRedisConfigurationStore(t *testing.T) {
	type args struct {
		logger logger.Logger
	}
	tests := []struct {
		name string
		args args
		want configuration.Store
	}{
		{
			args: args{
				logger: logger.NewLogger("test"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewRedisConfigurationStore(tt.args.logger)
			assert.NotNil(t, got)
		})
	}
}

func Test_parseRedisMetadata(t *testing.T) {
	type args struct {
		meta configuration.Metadata
	}
	testProperties := make(map[string]string)
	testProperties["redisHost"] = "testHost"
	testProperties["redisPassword"] = "testPassword"
	testProperties["enableTLS"] = "true"
	testProperties["redisMaxRetries"] = "10"
	testProperties["redisMaxRetryInterval"] = "100ms"
	testProperties["redisMinRetryInterval"] = "10ms"
	testProperties["failover"] = "true"
	testProperties["sentinelMasterName"] = "tesSentinelMasterName"
	testProperties["redisDB"] = "1"
	testSettings := redisComponent.Settings{
		Host:                  "testHost",
		Password:              "testPassword",
		EnableTLS:             true,
		RedisMaxRetries:       10,
		RedisMaxRetryInterval: redisComponent.Duration(100 * time.Millisecond),
		RedisMinRetryInterval: redisComponent.Duration(10 * time.Millisecond),
		Failover:              true,
		SentinelMasterName:    "tesSentinelMasterName",
		DB:                    1,
	}

	testDefaultProperties := make(map[string]string)
	testDefaultProperties["redisHost"] = "testHost"
	defaultSettings := redisComponent.Settings{
		Host:                  "testHost",
		Password:              "",
		EnableTLS:             false,
		RedisMaxRetries:       3,
		RedisMaxRetryInterval: redisComponent.Duration(time.Second * 2),
		RedisMinRetryInterval: redisComponent.Duration(time.Millisecond * 8),
		Failover:              false,
		SentinelMasterName:    "",
		DB:                    0,
	}

	tests := []struct {
		name    string
		args    args
		want    redisComponent.Settings
		wantErr bool
	}{
		{
			args: args{
				meta: configuration.Metadata{Base: contribMetadata.Base{
					Properties: testProperties,
				}},
			},
			want: testSettings,
		},
		{
			args: args{
				meta: configuration.Metadata{Base: contribMetadata.Base{
					Properties: testDefaultProperties,
				}},
			},
			want: defaultSettings,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			log := logger.NewLogger("dapr.components")
			_, got, err := redisComponent.ParseClientFromProperties(tt.args.meta.Properties, contribMetadata.ConfigurationStoreType, ctx, &log)
			if (err != nil) != tt.wantErr {
				t.Errorf("edisComponent.ParseClientFromProperties error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, tt.want.Host, got.Host)
			assert.Equal(t, tt.want.Password, got.Password)
			assert.Equal(t, tt.want.EnableTLS, got.EnableTLS)
			assert.Equal(t, tt.want.RedisMaxRetries, got.RedisMaxRetries)
			assert.Equal(t, tt.want.RedisMaxRetryInterval, got.RedisMaxRetryInterval)
			assert.Equal(t, tt.want.RedisMinRetryInterval, got.RedisMinRetryInterval)
			assert.Equal(t, tt.want.Failover, got.Failover)
			assert.Equal(t, tt.want.SentinelMasterName, got.SentinelMasterName)
			assert.Equal(t, tt.want.DB, got.DB)
		})
	}
}

func setupMiniredis() (*miniredis.Miniredis, redisComponent.RedisClient) {
	ctx := context.Background()
	log := logger.NewLogger("dapr.components")
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	props := map[string]string{
		"redisHost": s.Addr(),
		"redisDB":   "0",
	}
	redisClient, _, _ := redisComponent.ParseClientFromProperties(props, contribMetadata.ConfigurationStoreType, ctx, &log)

	return s, redisClient
}
