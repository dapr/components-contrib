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
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/configuration"
	"github.com/dapr/kit/logger"
)

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

func TestInit(t *testing.T) {
	s := NewAzureAppConfigurationStore(logger.NewLogger("test"))
	t.Run("Init with valid appConfigHost metadata", func(t *testing.T) {
		testProperties := make(map[string]string)
		testProperties[host] = "testHost"
		testProperties[maxRetries] = "3"
		testProperties[retryDelay] = "4000000000"
		testProperties[maxRetryDelay] = "120000000000"

		m := configuration.Metadata{
			Properties: testProperties,
		}

		err := s.Init(m)
		assert.Nil(t, err)
		cs, ok := s.(*ConfigurationStore)
		assert.True(t, ok)
		assert.Equal(t, testProperties[host], cs.metadata.host)
		assert.Equal(t, 3, cs.metadata.maxRetries)
		assert.Equal(t, time.Second*4, cs.metadata.retryDelay)
		assert.Equal(t, time.Second*120, cs.metadata.maxRetryDelay)
	})

	t.Run("Init with valid appConfigConnectionString metadata", func(t *testing.T) {
		testProperties := make(map[string]string)
		testProperties[connectionString] = "Endpoint=https://foo.azconfig.io;Id=osOX-l9-s0:sig;Secret=00000000000000000000000000000000000000000000"
		testProperties[maxRetries] = "3"
		testProperties[retryDelay] = "4000000000"
		testProperties[maxRetryDelay] = "120000000000"

		m := configuration.Metadata{
			Properties: testProperties,
		}

		err := s.Init(m)
		assert.Nil(t, err)
		cs, ok := s.(*ConfigurationStore)
		assert.True(t, ok)
		assert.Equal(t, testProperties[connectionString], cs.metadata.connectionString)
		assert.Equal(t, 3, cs.metadata.maxRetries)
		assert.Equal(t, time.Second*4, cs.metadata.retryDelay)
		assert.Equal(t, time.Second*120, cs.metadata.maxRetryDelay)
	})
}

func Test_parseMetadata(t *testing.T) {
	t.Run(fmt.Sprintf("parse metadata with %s", host), func(t *testing.T) {
		testProperties := make(map[string]string)
		testProperties[host] = "testHost"
		testProperties[maxRetries] = "3"
		testProperties[retryDelay] = "4000000000"
		testProperties[maxRetryDelay] = "120000000000"

		meta := configuration.Metadata{
			Properties: testProperties,
		}

		want := metadata{
			host:          "testHost",
			maxRetries:    3,
			retryDelay:    time.Second * 4,
			maxRetryDelay: time.Second * 120,
		}

		m, _ := parseMetadata(meta)
		assert.NotNil(t, m)
		if !reflect.DeepEqual(m, want) {
			t.Errorf("parseMetadata() got = %v, want %v", m, want)
		}
	})

	t.Run(fmt.Sprintf("parse metadata with %s", connectionString), func(t *testing.T) {
		testProperties := make(map[string]string)
		testProperties[connectionString] = "testConnectionString"
		testProperties[maxRetries] = "3"
		testProperties[retryDelay] = "4000000000"
		testProperties[maxRetryDelay] = "120000000000"

		meta := configuration.Metadata{
			Properties: testProperties,
		}

		want := metadata{
			connectionString: "testConnectionString",
			maxRetries:       3,
			retryDelay:       time.Second * 4,
			maxRetryDelay:    time.Second * 120,
		}

		m, _ := parseMetadata(meta)
		assert.NotNil(t, m)
		if !reflect.DeepEqual(m, want) {
			t.Errorf("parseMetadata() got = %v, want %v", m, want)
		}
	})

	t.Run(fmt.Sprintf("both %s and %s fields set in metadata", host, connectionString), func(t *testing.T) {
		testProperties := make(map[string]string)
		testProperties[host] = "testHost"
		testProperties[connectionString] = "testConnectionString"
		testProperties[maxRetries] = "3"
		testProperties[retryDelay] = "4000000000"
		testProperties[maxRetryDelay] = "120000000000"

		meta := configuration.Metadata{
			Properties: testProperties,
		}

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

		meta := configuration.Metadata{
			Properties: testProperties,
		}

		_, err := parseMetadata(meta)
		assert.Error(t, err)
	})
}
