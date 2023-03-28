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

	"github.com/dapr/components-contrib/configuration"
	mdata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

type MockConfigurationStore struct{}

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

func (m *MockConfigurationStore) NewListSettingsPager(selector azappconfig.SettingSelector, options *azappconfig.ListSettingsOptions) *runtime.Pager[azappconfig.ListSettingsPage] {
	settings := make([]azappconfig.Setting, 2)

	setting1 := azappconfig.Setting{}
	setting1.Key = ptr.Of("testKey-1")
	setting1.Value = ptr.Of("testValue-1")

	setting2 := azappconfig.Setting{}
	setting2.Key = ptr.Of("testKey-2")
	setting2.Value = ptr.Of("testValue-2")
	settings[0] = setting1
	settings[1] = setting2

	return runtime.NewPager(runtime.PagingHandler[azappconfig.ListSettingsPage]{
		More: func(azappconfig.ListSettingsPage) bool {
			return false
		},
		Fetcher: func(ctx context.Context, cur *azappconfig.ListSettingsPage) (azappconfig.ListSettingsPage, error) {
			listSettingPage := azappconfig.ListSettingsPage{}
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
		assert.Nil(t, err)
		assert.True(t, len(res.Items) == 1)
	})
}

func Test_subscribeConfigurationWithProvidedKeys(t *testing.T) {
	s := NewAzureAppConfigurationStore(logger.NewLogger("test")).(*ConfigurationStore)

	s.client = &MockConfigurationStore{}

	metadata := make(map[string]string)
	metadata["sentinelKey"] = "test_sentinel_key"

	t.Run("call subscribe with sentinel key", func(t *testing.T) {
		req := configuration.SubscribeRequest{
			Keys:     []string{"testKey"},
			Metadata: metadata,
		}
		subID, err := s.Subscribe(context.Background(), &req, updateEventHandler)
		assert.True(t, len(subID) > 0)
		assert.Nil(t, err)
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
		assert.NotNil(t, err)
	})
}

func Test_unsubscribeConfigurationWithProvidedKeys(t *testing.T) {
	s := NewAzureAppConfigurationStore(logger.NewLogger("test")).(*ConfigurationStore)

	s.client = &MockConfigurationStore{}
	cancelContext, cancel := context.WithCancel(context.Background())
	s.subscribeCancelCtxMap.Store("id1", cancel)

	t.Run("call unsubscribe with incorrect subId", func(t *testing.T) {
		req := configuration.UnsubscribeRequest{
			ID: "id_not_exist",
		}
		err := s.Unsubscribe(cancelContext, &req)
		assert.NotNil(t, err)
		_, ok := s.subscribeCancelCtxMap.Load("id1")
		assert.True(t, ok)
	})

	t.Run("call unsubscribe with correct subId", func(t *testing.T) {
		req := configuration.UnsubscribeRequest{
			ID: "id1",
		}
		err := s.Unsubscribe(cancelContext, &req)
		assert.Nil(t, err)
		_, ok := s.subscribeCancelCtxMap.Load("id1")
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
		assert.Nil(t, err)
		assert.True(t, len(res.Items) == 2)
	})
}

func TestInit(t *testing.T) {
	s := NewAzureAppConfigurationStore(logger.NewLogger("test"))
	t.Run("Init with valid appConfigHost metadata", func(t *testing.T) {
		testProperties := make(map[string]string)
		testProperties[host] = "testHost"
		testProperties[maxRetries] = "3"
		testProperties[retryDelay] = "4000000000"
		testProperties[maxRetryDelay] = "120000000000"
		testProperties[subscribePollInterval] = "30000000000"
		testProperties[requestTimeout] = "30000000000"

		m := configuration.Metadata{Base: mdata.Base{
			Properties: testProperties,
		}}

		err := s.Init(context.Background(), m)
		assert.Nil(t, err)
		cs, ok := s.(*ConfigurationStore)
		assert.True(t, ok)
		assert.Equal(t, testProperties[host], cs.metadata.Host)
		assert.Equal(t, 3, cs.metadata.MaxRetries)
		assert.Equal(t, time.Second*4, cs.metadata.internalRetryDelay)
		assert.Equal(t, time.Second*120, cs.metadata.internalMaxRetryDelay)
		assert.Equal(t, time.Second*30, cs.metadata.internalSubscribePollInterval)
		assert.Equal(t, time.Second*30, cs.metadata.internalRequestTimeout)
	})

	t.Run("Init with valid appConfigConnectionString metadata", func(t *testing.T) {
		testProperties := make(map[string]string)
		testProperties[connectionString] = "Endpoint=https://foo.azconfig.io;Id=osOX-l9-s0:sig;Secret=00000000000000000000000000000000000000000000"
		testProperties[maxRetries] = "3"
		testProperties[retryDelay] = "4000000000"
		testProperties[maxRetryDelay] = "120000000000"
		testProperties[subscribePollInterval] = "30000000000"
		testProperties[requestTimeout] = "30000000000"

		m := configuration.Metadata{Base: mdata.Base{
			Properties: testProperties,
		}}

		err := s.Init(context.Background(), m)
		assert.Nil(t, err)
		cs, ok := s.(*ConfigurationStore)
		assert.True(t, ok)
		assert.Equal(t, testProperties[connectionString], cs.metadata.ConnectionString)
		assert.Equal(t, 3, cs.metadata.MaxRetries)
		assert.Equal(t, time.Second*4, cs.metadata.internalRetryDelay)
		assert.Equal(t, time.Second*120, cs.metadata.internalMaxRetryDelay)
		assert.Equal(t, time.Second*30, cs.metadata.internalSubscribePollInterval)
		assert.Equal(t, time.Second*30, cs.metadata.internalRequestTimeout)
	})
}

func Test_parseMetadata(t *testing.T) {
	t.Run(fmt.Sprintf("parse metadata with %s", host), func(t *testing.T) {
		testProperties := make(map[string]string)
		testProperties[host] = "testHost"
		testProperties[maxRetries] = "3"
		testProperties[retryDelay] = "4000000000"
		testProperties[maxRetryDelay] = "120000000000"
		testProperties[subscribePollInterval] = "30000000000"
		testProperties[requestTimeout] = "30000000000"

		meta := configuration.Metadata{Base: mdata.Base{
			Properties: testProperties,
		}}

		want := metadata{
			Host:                          "testHost",
			MaxRetries:                    3,
			internalRetryDelay:            time.Second * 4,
			internalMaxRetryDelay:         time.Second * 120,
			internalSubscribePollInterval: time.Second * 30,
			internalRequestTimeout:        time.Second * 30,
		}

		m, _ := parseMetadata(meta)
		assert.NotNil(t, m)
		assert.Equal(t, want.Host, m.Host)
		assert.Equal(t, want.MaxRetries, m.MaxRetries)
		assert.Equal(t, want.internalRetryDelay, m.internalRetryDelay)
		assert.Equal(t, want.internalMaxRetryDelay, m.internalMaxRetryDelay)
		assert.Equal(t, want.internalSubscribePollInterval, m.internalSubscribePollInterval)
		assert.Equal(t, want.internalRequestTimeout, m.internalRequestTimeout)
	})

	t.Run(fmt.Sprintf("parse metadata with %s", connectionString), func(t *testing.T) {
		testProperties := make(map[string]string)
		testProperties[connectionString] = "testConnectionString"
		testProperties[maxRetries] = "3"
		testProperties[retryDelay] = "4000000000"
		testProperties[maxRetryDelay] = "120000000000"
		testProperties[subscribePollInterval] = "30000000000"
		testProperties[requestTimeout] = "30000000000"

		meta := configuration.Metadata{Base: mdata.Base{
			Properties: testProperties,
		}}

		want := metadata{
			ConnectionString:              "testConnectionString",
			MaxRetries:                    3,
			internalRetryDelay:            time.Second * 4,
			internalMaxRetryDelay:         time.Second * 120,
			internalSubscribePollInterval: time.Second * 30,
			internalRequestTimeout:        time.Second * 30,
		}

		m, _ := parseMetadata(meta)
		assert.NotNil(t, m)
		assert.Equal(t, want.ConnectionString, m.ConnectionString)
		assert.Equal(t, want.MaxRetries, m.MaxRetries)
		assert.Equal(t, want.internalRetryDelay, m.internalRetryDelay)
		assert.Equal(t, want.internalMaxRetryDelay, m.internalMaxRetryDelay)
		assert.Equal(t, want.internalSubscribePollInterval, m.internalSubscribePollInterval)
		assert.Equal(t, want.internalRequestTimeout, m.internalRequestTimeout)
	})

	t.Run(fmt.Sprintf("both %s and %s fields set in metadata", host, connectionString), func(t *testing.T) {
		testProperties := make(map[string]string)
		testProperties[host] = "testHost"
		testProperties[connectionString] = "testConnectionString"
		testProperties[maxRetries] = "3"
		testProperties[retryDelay] = "4000000000"
		testProperties[maxRetryDelay] = "120000000000"
		testProperties[subscribePollInterval] = "30000000000"
		testProperties[requestTimeout] = "30000000000"

		meta := configuration.Metadata{Base: mdata.Base{
			Properties: testProperties,
		}}

		_, err := parseMetadata(meta)
		assert.Error(t, err)
	})

	t.Run(fmt.Sprintf("both %s and %s fields not set in metadata", host, connectionString), func(t *testing.T) {
		testProperties := make(map[string]string)
		testProperties[host] = ""
		testProperties[connectionString] = ""
		testProperties[maxRetries] = "3"
		testProperties[retryDelay] = "4000000000"
		testProperties[maxRetryDelay] = "120000000000"
		testProperties[subscribePollInterval] = "30000000000"
		testProperties[requestTimeout] = "30000000000"

		meta := configuration.Metadata{Base: mdata.Base{
			Properties: testProperties,
		}}

		_, err := parseMetadata(meta)
		assert.Error(t, err)
	})
}

func updateEventHandler(ctx context.Context, e *configuration.UpdateEvent) error {
	return nil
}
