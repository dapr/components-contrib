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

package appconfig

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azappconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/configuration"
	mdata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

type MockConfigurationStore struct{}

const (
	testMaxRetryDelay                      = "120s"
	testSubscribePollIntervalAndReqTimeout = "30s"
)

func (m *MockConfigurationStore) GetSetting(ctx context.Context, key string, options *azappconfig.GetSettingOptions) (azappconfig.GetSettingResponse, error) {
	if key == "testKey" || key == "test_sentinel_key" {
		settings := azappconfig.Setting{}

		settings.Key = ptr.Of("testKey")
		settings.Value = ptr.Of("testValue")

		resp := azappconfig.GetSettingResponse{}
		resp.Setting = settings
		return resp, nil
	}
	resp := azappconfig.GetSettingResponse{}
	return resp, nil
}

func (m *MockConfigurationStore) NewListSettingsPager(selector azappconfig.SettingSelector, options *azappconfig.ListSettingsOptions) *runtime.Pager[azappconfig.ListSettingsPageResponse] {
	settings := make([]azappconfig.Setting, 2)

	setting1 := azappconfig.Setting{}
	setting1.Key = ptr.Of("testKey-1")
	setting1.Value = ptr.Of("testValue-1")

	setting2 := azappconfig.Setting{}
	setting2.Key = ptr.Of("testKey-2")
	setting2.Value = ptr.Of("testValue-2")
	settings[0] = setting1
	settings[1] = setting2

	return runtime.NewPager(runtime.PagingHandler[azappconfig.ListSettingsPageResponse]{
		More: func(azappconfig.ListSettingsPageResponse) bool {
			return false
		},
		Fetcher: func(ctx context.Context, cur *azappconfig.ListSettingsPageResponse) (azappconfig.ListSettingsPageResponse, error) {
			listSettingPage := azappconfig.ListSettingsPageResponse{}
			listSettingPage.Settings = settings
			return listSettingPage, nil
		},
	})
}

func TestNewAzureAppConfigurationStore(t *testing.T) {
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
			got := NewAzureAppConfigurationStore(tt.args.logger)
			assert.NotNil(t, got)
		})
	}
}

func Test_getConfigurationWithProvidedKeys(t *testing.T) {
	s := NewAzureAppConfigurationStore(logger.NewLogger("test")).(*ConfigurationStore)

	s.client = &MockConfigurationStore{}

	t.Run("call getConfiguration for provided keys", func(t *testing.T) {
		req := configuration.GetRequest{
			Keys:     []string{"testKey"},
			Metadata: map[string]string{},
		}
		res, err := s.Get(context.Background(), &req)
		require.NoError(t, err)
		assert.Len(t, res.Items, 1)
	})
}

func Test_subscribeConfigurationWithProvidedKeys(t *testing.T) {
	s := NewAzureAppConfigurationStore(logger.NewLogger("test")).(*ConfigurationStore)

	s.client = &MockConfigurationStore{}

	metadata := make(map[string]string)
	metadata["sentinelkey"] = "test_sentinel_key"

	t.Run("call subscribe with sentinel key", func(t *testing.T) {
		req := configuration.SubscribeRequest{
			Keys:     []string{"testKey"},
			Metadata: metadata,
		}
		subID, err := s.Subscribe(context.Background(), &req, updateEventHandler)
		assert.NotEmpty(t, subID)
		require.NoError(t, err)
		unReq := &configuration.UnsubscribeRequest{
			ID: subID,
		}
		s.Unsubscribe(context.Background(), unReq)
	})

	t.Run("call subscribe w/o sentinel key", func(t *testing.T) {
		req := configuration.SubscribeRequest{
			Keys:     []string{"testKey"},
			Metadata: make(map[string]string),
		}
		_, err := s.Subscribe(context.Background(), &req, updateEventHandler)
		require.Error(t, err)
	})
}

func Test_unsubscribeConfigurationWithProvidedKeys(t *testing.T) {
	s := NewAzureAppConfigurationStore(logger.NewLogger("test")).(*ConfigurationStore)

	s.client = &MockConfigurationStore{}
	cancelContext, cancel := context.WithCancel(context.Background())
	s.cancelMap.Store("id1", cancel)

	t.Run("call unsubscribe with incorrect subId", func(t *testing.T) {
		req := configuration.UnsubscribeRequest{
			ID: "id_not_exist",
		}
		err := s.Unsubscribe(cancelContext, &req)
		require.Error(t, err)
		_, ok := s.cancelMap.Load("id1")
		assert.True(t, ok)
	})

	t.Run("call unsubscribe with correct subId", func(t *testing.T) {
		req := configuration.UnsubscribeRequest{
			ID: "id1",
		}
		err := s.Unsubscribe(cancelContext, &req)
		require.NoError(t, err)
		_, ok := s.cancelMap.Load("id1")
		assert.False(t, ok)
	})
}

func Test_getConfigurationWithNoProvidedKeys(t *testing.T) {
	s := NewAzureAppConfigurationStore(logger.NewLogger("test")).(*ConfigurationStore)

	s.client = &MockConfigurationStore{}

	t.Run("call getConfiguration for provided keys", func(t *testing.T) {
		req := configuration.GetRequest{
			Keys:     []string{},
			Metadata: map[string]string{},
		}
		res, err := s.Get(context.Background(), &req)
		require.NoError(t, err)
		assert.Len(t, res.Items, 2)
	})
}

func TestInit(t *testing.T) {
	s := NewAzureAppConfigurationStore(logger.NewLogger("test"))
	t.Run("Init with valid appConfigHost metadata", func(t *testing.T) {
		testProperties := make(map[string]string)
		testProperties[host] = "testHost"
		testProperties[maxRetries] = "3"
		testProperties[retryDelay] = "4s"
		testProperties[maxRetryDelay] = testMaxRetryDelay
		testProperties[subscribePollInterval] = testSubscribePollIntervalAndReqTimeout
		testProperties[requestTimeout] = testSubscribePollIntervalAndReqTimeout

		m := configuration.Metadata{Base: mdata.Base{
			Properties: testProperties,
		}}

		err := s.Init(context.Background(), m)
		require.NoError(t, err)
		cs, ok := s.(*ConfigurationStore)
		assert.True(t, ok)
		assert.Equal(t, testProperties[host], cs.metadata.Host)
		assert.Equal(t, 3, cs.metadata.MaxRetries)
		assert.Equal(t, time.Second*4, cs.metadata.RetryDelay)
		assert.Equal(t, time.Second*120, cs.metadata.MaxRetryDelay)
		assert.Equal(t, time.Second*30, cs.metadata.SubscribePollInterval)
		assert.Equal(t, time.Second*30, cs.metadata.RequestTimeout)
	})

	t.Run("Init with valid appConfigConnectionString metadata", func(t *testing.T) {
		testProperties := make(map[string]string)
		testProperties[connectionString] = "Endpoint=https://foo.azconfig.io;Id=osOX-l9-s0:sig;Secret=00000000000000000000000000000000000000000000"
		testProperties[maxRetries] = "3"
		testProperties[retryDelay] = "4s"
		testProperties[maxRetryDelay] = testMaxRetryDelay
		testProperties[subscribePollInterval] = testSubscribePollIntervalAndReqTimeout
		testProperties[requestTimeout] = testSubscribePollIntervalAndReqTimeout

		m := configuration.Metadata{Base: mdata.Base{
			Properties: testProperties,
		}}

		err := s.Init(context.Background(), m)
		require.NoError(t, err)
		cs, ok := s.(*ConfigurationStore)
		assert.True(t, ok)
		assert.Equal(t, testProperties[connectionString], cs.metadata.ConnectionString)
		assert.Equal(t, 3, cs.metadata.MaxRetries)
		assert.Equal(t, time.Second*4, cs.metadata.RetryDelay)
		assert.Equal(t, time.Second*120, cs.metadata.MaxRetryDelay)
		assert.Equal(t, time.Second*30, cs.metadata.SubscribePollInterval)
		assert.Equal(t, time.Second*30, cs.metadata.RequestTimeout)
	})
}

func TestParseMetadata(t *testing.T) {
	t.Run("parse metadata with "+host, func(t *testing.T) {
		testProperties := make(map[string]string)
		testProperties[host] = "testHost"
		testProperties[maxRetries] = "3"
		testProperties[retryDelay] = "4s"
		testProperties[maxRetryDelay] = testMaxRetryDelay
		testProperties[subscribePollInterval] = testSubscribePollIntervalAndReqTimeout
		testProperties[requestTimeout] = testSubscribePollIntervalAndReqTimeout

		meta := configuration.Metadata{Base: mdata.Base{
			Properties: testProperties,
		}}

		want := metadata{
			Host:                  "testHost",
			MaxRetries:            3,
			RetryDelay:            time.Second * 4,
			MaxRetryDelay:         time.Second * 120,
			SubscribePollInterval: time.Second * 30,
			RequestTimeout:        time.Second * 30,
		}

		m := metadata{}
		err := m.Parse(logger.NewLogger("test"), meta)
		require.NoError(t, err)
		assert.Equal(t, want.Host, m.Host)
		assert.Equal(t, want.MaxRetries, m.MaxRetries)
		assert.Equal(t, want.RetryDelay, m.RetryDelay)
		assert.Equal(t, want.MaxRetryDelay, m.MaxRetryDelay)
		assert.Equal(t, want.SubscribePollInterval, m.SubscribePollInterval)
		assert.Equal(t, want.RequestTimeout, m.RequestTimeout)
	})

	t.Run("parse metadata with "+connectionString, func(t *testing.T) {
		testProperties := make(map[string]string)
		testProperties[connectionString] = "testConnectionString"
		testProperties[maxRetries] = "3"
		testProperties[retryDelay] = "4s"
		testProperties[maxRetryDelay] = testMaxRetryDelay
		testProperties[subscribePollInterval] = testSubscribePollIntervalAndReqTimeout
		testProperties[requestTimeout] = testSubscribePollIntervalAndReqTimeout

		meta := configuration.Metadata{Base: mdata.Base{
			Properties: testProperties,
		}}

		want := metadata{
			ConnectionString:      "testConnectionString",
			MaxRetries:            3,
			RetryDelay:            time.Second * 4,
			MaxRetryDelay:         time.Second * 120,
			SubscribePollInterval: time.Second * 30,
			RequestTimeout:        time.Second * 30,
		}

		m := metadata{}
		err := m.Parse(logger.NewLogger("test"), meta)
		require.NoError(t, err)
		assert.Equal(t, want.ConnectionString, m.ConnectionString)
		assert.Equal(t, want.MaxRetries, m.MaxRetries)
		assert.Equal(t, want.RetryDelay, m.RetryDelay)
		assert.Equal(t, want.MaxRetryDelay, m.MaxRetryDelay)
		assert.Equal(t, want.SubscribePollInterval, m.SubscribePollInterval)
		assert.Equal(t, want.RequestTimeout, m.RequestTimeout)
	})

	t.Run(fmt.Sprintf("both %s and %s fields set in metadata", host, connectionString), func(t *testing.T) {
		testProperties := make(map[string]string)
		testProperties[host] = "testHost"
		testProperties[connectionString] = "testConnectionString"
		testProperties[maxRetries] = "3"
		testProperties[retryDelay] = "4s"
		testProperties[maxRetryDelay] = testMaxRetryDelay
		testProperties[subscribePollInterval] = testSubscribePollIntervalAndReqTimeout
		testProperties[requestTimeout] = testSubscribePollIntervalAndReqTimeout

		meta := configuration.Metadata{Base: mdata.Base{
			Properties: testProperties,
		}}

		m := metadata{}
		err := m.Parse(logger.NewLogger("test"), meta)
		require.Error(t, err)
	})

	t.Run(fmt.Sprintf("both %s and %s fields not set in metadata", host, connectionString), func(t *testing.T) {
		testProperties := make(map[string]string)
		testProperties[host] = ""
		testProperties[connectionString] = ""
		testProperties[maxRetries] = "3"
		testProperties[retryDelay] = "4s"
		testProperties[maxRetryDelay] = testMaxRetryDelay
		testProperties[subscribePollInterval] = testSubscribePollIntervalAndReqTimeout
		testProperties[requestTimeout] = testSubscribePollIntervalAndReqTimeout

		meta := configuration.Metadata{Base: mdata.Base{
			Properties: testProperties,
		}}

		m := metadata{}
		err := m.Parse(logger.NewLogger("test"), meta)
		require.Error(t, err)
	})
}

func updateEventHandler(ctx context.Context, e *configuration.UpdateEvent) error {
	return nil
}
