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
			Mystring   string   `json:"mystring"`
			Myduration Duration `json:"myduration"`
			Myinteger  int      `json:"myinteger,string"`
			Myfloat64  float64  `json:"myfloat64,string"`
			Mybool     *bool    `json:"mybool,omitempty"`
		}

		var m testMetadata

		testData := make(map[string]string)
		testData["mystring"] = "test"
		testData["myduration"] = "3s"
		testData["myinteger"] = "1"
		testData["myfloat64"] = "1.1"
		testData["mybool"] = "true"

		err := DecodeMetadata(testData, &m)
		fmt.Println(testData)

		assert.Nil(t, err)
		assert.Equal(t, true, *m.Mybool)
		assert.Equal(t, "test", m.Mystring)
		assert.Equal(t, 1, m.Myinteger)
		assert.Equal(t, 1.1, m.Myfloat64)
		assert.Equal(t, Duration{Duration: 3 * time.Second}, m.Myduration)
	})
}
