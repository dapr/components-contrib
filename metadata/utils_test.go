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

package metadata

import (
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/kit/metadata"
)

func TestTryGetTTL(t *testing.T) {
	tests := []struct {
		name    string
		md      map[string]string
		result  any
		wantOK  bool
		wantErr bool
		errStr  string
	}{
		{
			name: "ttl valid duration 20s",
			md: map[string]string{
				TTLMetadataKey: "20s",
			},
			result: time.Duration(20) * time.Second,
			wantOK: true,
		},
		{
			name: "ttl valid integer 20",
			md: map[string]string{
				TTLMetadataKey: "20",
			},
			result: time.Duration(20) * time.Second,
			wantOK: true,
		},
		{
			name: "ttlInSeconds valid duration 20s",
			md: map[string]string{
				TTLInSecondsMetadataKey: "20s",
			},
			result: time.Duration(20) * time.Second,
			wantOK: true,
		},
		{
			name: "ttlInSeconds valid integer 20",
			md: map[string]string{
				TTLInSecondsMetadataKey: "20",
			},
			result: time.Duration(20) * time.Second,
			wantOK: true,
		},
		{
			name: "invalid integer 20b",
			md: map[string]string{
				TTLMetadataKey: "20b",
			},
			wantOK:  false,
			wantErr: true,
			errStr:  "value must be a valid integer: actual is '20b'",
			result:  time.Duration(0) * time.Second,
		},
		{
			name: "negative ttl -1",
			md: map[string]string{
				TTLMetadataKey: "-1",
			},
			wantOK:  false,
			wantErr: true,
			errStr:  "value must be higher than zero: actual is '-1'",
			result:  time.Duration(0) * time.Second,
		},
		{
			name: "negative ttl -1s",
			md: map[string]string{
				TTLMetadataKey: "-1s",
			},
			wantOK:  true,
			wantErr: false,
			result:  time.Duration(0) * time.Second,
		},
		{
			name:    "no ttl",
			md:      map[string]string{},
			wantOK:  false,
			wantErr: false,
			result:  time.Duration(0) * time.Second,
		},
		{
			name: "out of range",
			md: map[string]string{
				TTLMetadataKey: fmt.Sprintf("%d1", math.MaxInt64),
			},
			wantOK:  false,
			wantErr: true,
			result:  time.Duration(0) * time.Second,
			errStr:  "value must be a valid integer: actual is '92233720368547758071'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, ok, err := TryGetTTL(tt.md)

			if tt.wantErr {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.errStr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantOK, ok, "wanted ok, but instead got not ok")
			assert.Equal(t, tt.result, d, "expected result %v, but instead got = %v", tt.result, d)
		})
	}
}

func TestIsRawPayload(t *testing.T) {
	t.Run("Metadata not found", func(t *testing.T) {
		val, err := IsRawPayload(map[string]string{
			"notfound": "1",
		})

		assert.False(t, val)
		require.NoError(t, err)
	})

	t.Run("Metadata map is nil", func(t *testing.T) {
		val, err := IsRawPayload(nil)

		assert.False(t, val)
		require.NoError(t, err)
	})

	t.Run("Metadata with bad value", func(t *testing.T) {
		val, err := IsRawPayload(map[string]string{
			"rawPayload": "Not a boolean",
		})

		assert.False(t, val)
		require.Error(t, err)
	})

	t.Run("Metadata with correct value as false", func(t *testing.T) {
		val, err := IsRawPayload(map[string]string{
			"rawPayload": "false",
		})

		assert.False(t, val)
		require.NoError(t, err)
	})

	t.Run("Metadata with correct value as true", func(t *testing.T) {
		val, err := IsRawPayload(map[string]string{
			"rawPayload": "true",
		})

		assert.True(t, val)
		require.NoError(t, err)
	})
}

func TestMetadataStructToStringMap(t *testing.T) {
	t.Run("Test metadata struct to metadata info conversion", func(t *testing.T) {
		type NestedStruct struct {
			NestedStringCustom string `mapstructure:"nested_string_custom"`
			NestedString       string
		}

		type testMetadata struct {
			NestedStruct              `mapstructure:",squash"`
			Mystring                  string
			Myduration                metadata.Duration
			Myinteger                 int
			Myfloat64                 float64
			Mybool                    *bool
			MyRegularDuration         time.Duration
			SomethingWithCustomName   string `mapstructure:"something_with_custom_name"`
			PubSubOnlyProperty        string `mapstructure:"pubsub_only_property" mdonly:"pubsub"`
			BindingOnlyProperty       string `mapstructure:"binding_only_property" mdonly:"bindings"`
			PubSubAndBindingProperty  string `mapstructure:"pubsub_and_binding_property" mdonly:"pubsub,bindings"`
			MyDurationArray           []time.Duration
			NotExportedByMapStructure string `mapstructure:"-"`
			notExported               string //nolint:structcheck,unused
			DeprecatedProperty        string `mapstructure:"something_deprecated" mddeprecated:"true"`
			Aliased                   string `mapstructure:"aliased" mdaliases:"another,name"`
			Ignored                   string `mapstructure:"ignored" mdignore:"true"`
		}
		m := testMetadata{}
		metadatainfo := MetadataMap{}
		GetMetadataInfoFromStructType(reflect.TypeOf(m), &metadatainfo, BindingType)

		_ = assert.NotEmpty(t, metadatainfo["Mystring"]) &&
			assert.Equal(t, "string", metadatainfo["Mystring"].Type)
		_ = assert.NotEmpty(t, metadatainfo["Myduration"]) &&
			assert.Equal(t, "metadata.Duration", metadatainfo["Myduration"].Type)
		_ = assert.NotEmpty(t, metadatainfo["Myinteger"]) &&
			assert.Equal(t, "int", metadatainfo["Myinteger"].Type)
		_ = assert.NotEmpty(t, metadatainfo["Myfloat64"]) &&
			assert.Equal(t, "float64", metadatainfo["Myfloat64"].Type)
		_ = assert.NotEmpty(t, metadatainfo["Mybool"]) &&
			assert.Equal(t, "*bool", metadatainfo["Mybool"].Type)
		_ = assert.NotEmpty(t, metadatainfo["MyRegularDuration"]) &&
			assert.Equal(t, "time.Duration", metadatainfo["MyRegularDuration"].Type)
		_ = assert.NotEmpty(t, metadatainfo["something_with_custom_name"]) &&
			assert.Equal(t, "string", metadatainfo["something_with_custom_name"].Type)
		assert.NotContains(t, metadatainfo, "NestedStruct")
		assert.NotContains(t, metadatainfo, "SomethingWithCustomName")
		_ = assert.NotEmpty(t, metadatainfo["nested_string_custom"]) &&
			assert.Equal(t, "string", metadatainfo["nested_string_custom"].Type)
		_ = assert.NotEmpty(t, metadatainfo["NestedString"]) &&
			assert.Equal(t, "string", metadatainfo["NestedString"].Type)
		assert.NotContains(t, metadatainfo, "pubsub_only_property")
		_ = assert.NotEmpty(t, metadatainfo["binding_only_property"]) &&
			assert.Equal(t, "string", metadatainfo["binding_only_property"].Type)
		_ = assert.NotEmpty(t, metadatainfo["pubsub_and_binding_property"]) &&
			assert.Equal(t, "string", metadatainfo["pubsub_and_binding_property"].Type)
		_ = assert.NotEmpty(t, metadatainfo["MyDurationArray"]) &&
			assert.Equal(t, "[]time.Duration", metadatainfo["MyDurationArray"].Type)
		assert.NotContains(t, metadatainfo, "NotExportedByMapStructure")
		assert.NotContains(t, metadatainfo, "notExported")
		_ = assert.NotEmpty(t, metadatainfo["something_deprecated"]) &&
			assert.Equal(t, "string", metadatainfo["something_deprecated"].Type) &&
			assert.True(t, metadatainfo["something_deprecated"].Deprecated)
		_ = assert.NotEmpty(t, metadatainfo["aliased"]) &&
			assert.Equal(t, "string", metadatainfo["aliased"].Type) &&
			assert.False(t, metadatainfo["aliased"].Deprecated) &&
			assert.False(t, metadatainfo["aliased"].Ignored) &&
			assert.Equal(t, []string{"another", "name"}, metadatainfo["aliased"].Aliases)
		_ = assert.NotEmpty(t, metadatainfo["ignored"]) &&
			assert.Equal(t, "string", metadatainfo["ignored"].Type) &&
			assert.False(t, metadatainfo["ignored"].Deprecated) &&
			assert.True(t, metadatainfo["ignored"].Ignored) &&
			assert.Empty(t, metadatainfo["ignored"].Aliases)
	})
}
