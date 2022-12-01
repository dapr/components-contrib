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
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIsRawPayload(t *testing.T) {
	t.Run("Metadata not found", func(t *testing.T) {
		val, err := IsRawPayload(map[string]string{
			"notfound": "1",
		})

		assert.Equal(t, false, val)
		assert.Nil(t, err)
	})

	t.Run("Metadata map is nil", func(t *testing.T) {
		val, err := IsRawPayload(nil)

		assert.Equal(t, false, val)
		assert.Nil(t, err)
	})

	t.Run("Metadata with bad value", func(t *testing.T) {
		val, err := IsRawPayload(map[string]string{
			"rawPayload": "Not a boolean",
		})

		assert.Equal(t, false, val)
		assert.NotNil(t, err)
	})

	t.Run("Metadata with correct value as false", func(t *testing.T) {
		val, err := IsRawPayload(map[string]string{
			"rawPayload": "false",
		})

		assert.Equal(t, false, val)
		assert.Nil(t, err)
	})

	t.Run("Metadata with correct value as true", func(t *testing.T) {
		val, err := IsRawPayload(map[string]string{
			"rawPayload": "true",
		})

		assert.Equal(t, true, val)
		assert.Nil(t, err)
	})
}

func TestTryGetContentType(t *testing.T) {
	t.Run("Metadata without content type", func(t *testing.T) {
		val, ok := TryGetContentType(map[string]string{})

		assert.Equal(t, "", val)
		assert.Equal(t, false, ok)
	})

	t.Run("Metadata with empty content type", func(t *testing.T) {
		val, ok := TryGetContentType(map[string]string{
			"contentType": "",
		})

		assert.Equal(t, "", val)
		assert.Equal(t, false, ok)
	})

	t.Run("Metadata with corrent content type", func(t *testing.T) {
		const contentType = "application/cloudevent+json"
		val, ok := TryGetContentType(map[string]string{
			"contentType": contentType,
		})

		assert.Equal(t, contentType, val)
		assert.Equal(t, true, ok)
	})
}

func TestMetadataDecode(t *testing.T) {
	t.Run("Test metadata decoding", func(t *testing.T) {
		type testMetadata struct {
			Mystring          string        `json:"mystring"`
			Myduration        Duration      `json:"myduration"`
			Myinteger         int           `json:"myinteger,string"`
			Myfloat64         float64       `json:"myfloat64,string"`
			Mybool            *bool         `json:"mybool,omitempty"`
			MyRegularDuration time.Duration `json:"myregularduration"`
		}

		var m testMetadata

		testData := make(map[string]string)
		testData["mystring"] = "test"
		testData["myduration"] = "3s"
		testData["myinteger"] = "1"
		testData["myfloat64"] = "1.1"
		testData["mybool"] = "true"
		testData["myregularduration"] = "6m"

		err := DecodeMetadata(testData, &m)

		assert.Nil(t, err)
		assert.Equal(t, true, *m.Mybool)
		assert.Equal(t, "test", m.Mystring)
		assert.Equal(t, 1, m.Myinteger)
		assert.Equal(t, 1.1, m.Myfloat64)
		assert.Equal(t, 6*time.Minute, m.MyRegularDuration)
		assert.Equal(t, Duration{Duration: 3 * time.Second}, m.Myduration)
	})

	t.Run("Test metadata decode hook for truthy values", func(t *testing.T) {
		type testMetadata struct {
			BoolPointer            *bool
			BoolPointerNotProvided *bool
			BoolValueOn            bool
			BoolValue1             bool
			BoolValueTrue          bool
			BoolValue0             bool
			BoolValueFalse         bool
			BoolValueNonsense      bool
		}

		var m testMetadata

		testData := make(map[string]string)
		testData["boolpointer"] = "on"
		testData["boolvalueon"] = "on"
		testData["boolvalue1"] = "1"
		testData["boolvaluetrue"] = "true"
		testData["boolvalue0"] = "0"
		testData["boolvaluefalse"] = "false"
		testData["boolvaluenonsense"] = "nonsense"

		err := DecodeMetadata(testData, &m)
		assert.NoError(t, err)
		assert.True(t, *m.BoolPointer)
		assert.True(t, m.BoolValueOn)
		assert.True(t, m.BoolValue1)
		assert.True(t, m.BoolValueTrue)
		assert.False(t, m.BoolValue0)
		assert.False(t, m.BoolValueFalse)
		assert.False(t, m.BoolValueNonsense)
		assert.Nil(t, m.BoolPointerNotProvided)
	})

	t.Run("Test metadata decode for string arrays", func(t *testing.T) {
		type testMetadata struct {
			StringArray                           []string
			StringArrayPointer                    *[]string
			EmptyStringArray                      []string
			EmptyStringArrayPointer               *[]string
			EmptyStringArrayWithComma             []string
			EmptyStringArrayPointerWithComma      *[]string
			StringArrayOneElement                 []string
			StringArrayOneElementPointer          *[]string
			StringArrayOneElementWithComma        []string
			StringArrayOneElementPointerWithComma *[]string
		}

		var m testMetadata

		testData := make(map[string]string)
		testData["stringarray"] = "one,two,three"
		testData["stringarraypointer"] = "one,two,three"
		testData["emptystringarray"] = ""
		testData["emptystringarraypointer"] = ""
		testData["stringarrayoneelement"] = "test"
		testData["stringarrayoneelementpointer"] = "test"
		testData["stringarrayoneelementwithcomma"] = "test,"
		testData["stringarrayoneelementpointerwithcomma"] = "test,"
		testData["emptystringarraywithcomma"] = ","
		testData["emptystringarraypointerwithcomma"] = ","

		err := DecodeMetadata(testData, &m)
		assert.NoError(t, err)
		assert.Equal(t, []string{"one", "two", "three"}, m.StringArray)
		assert.Equal(t, []string{"one", "two", "three"}, *m.StringArrayPointer)
		assert.Equal(t, []string{""}, m.EmptyStringArray)
		assert.Equal(t, []string{""}, *m.EmptyStringArrayPointer)
		assert.Equal(t, []string{"test"}, m.StringArrayOneElement)
		assert.Equal(t, []string{"test"}, *m.StringArrayOneElementPointer)
		assert.Equal(t, []string{"test", ""}, m.StringArrayOneElementWithComma)
		assert.Equal(t, []string{"test", ""}, *m.StringArrayOneElementPointerWithComma)
		assert.Equal(t, []string{"", ""}, m.EmptyStringArrayWithComma)
		assert.Equal(t, []string{"", ""}, *m.EmptyStringArrayPointerWithComma)
	})
}

func TestMetadataStructToStringMap(t *testing.T) {
	t.Run("Test metadata struct to metadata info conversion", func(t *testing.T) {
		type NestedStruct struct {
			NestedStringCustom string `mapstructure:"nested_string_custom"`
			NestedString       string
		}

		type testMetadata struct {
			NestedStruct            `mapstructure:",squash"`
			Mystring                string
			Myduration              Duration
			Myinteger               int
			Myfloat64               float64
			Mybool                  *bool `json:",omitempty"`
			MyRegularDuration       time.Duration
			SomethingWithCustomName string `mapstructure:"something_with_custom_name"`
		}
		m := testMetadata{}
		metadatainfo := map[string]string{}
		GetMetadataInfoFromStructType(reflect.TypeOf(m), &metadatainfo)

		assert.Equal(t, "string", metadatainfo["Mystring"])
		assert.Equal(t, "metadata.Duration", metadatainfo["Myduration"])
		assert.Equal(t, "int", metadatainfo["Myinteger"])
		assert.Equal(t, "float64", metadatainfo["Myfloat64"])
		assert.Equal(t, "*bool", metadatainfo["Mybool"])
		assert.Equal(t, "time.Duration", metadatainfo["MyRegularDuration"])
		assert.Equal(t, "string", metadatainfo["something_with_custom_name"])
		assert.NotContains(t, metadatainfo, "NestedStruct")
		assert.NotContains(t, metadatainfo, "SomethingWithCustomName")
		assert.Equal(t, "string", metadatainfo["nested_string_custom"])
		assert.Equal(t, "string", metadatainfo["NestedString"])
	})
}
