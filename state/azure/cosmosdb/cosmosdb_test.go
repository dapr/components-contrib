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

package cosmosdb

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/state"
)

type widget struct {
	Color string `json:"color"`
}

func TestCreateCosmosItem(t *testing.T) {
	value := widget{Color: "red"}
	partitionKey := "/partitionKey"
	t.Run("create item for golang struct", func(t *testing.T) {
		req := state.SetRequest{
			Key:   "testKey",
			Value: value,
		}

		item, err := createUpsertItem("application/json", req, partitionKey)
		assert.NoError(t, err)
		assert.Equal(t, partitionKey, item.PartitionKey)
		assert.Equal(t, "testKey", item.ID)
		assert.Equal(t, value, item.Value)
		assert.Nil(t, item.TTL)

		// items need to be marshallable to JSON with encoding/json
		b, err := json.Marshal(item)
		assert.NoError(t, err)

		j := map[string]interface{}{}
		err = json.Unmarshal(b, &j)
		assert.NoError(t, err)

		m, ok := j["value"].(map[string]interface{})
		assert.Truef(t, ok, "value should be a map")
		assert.NotContains(t, j, "ttl")

		assert.Equal(t, "red", m["color"])
	})

	t.Run("create item for JSON bytes", func(t *testing.T) {
		// Bytes are handled the same way, does not matter if is JSON or JPEG.
		bytes, err := json.Marshal(value)
		assert.NoError(t, err)

		req := state.SetRequest{
			Key:   "testKey",
			Value: bytes,
		}

		item, err := createUpsertItem("application/json", req, partitionKey)
		assert.NoError(t, err)
		assert.Equal(t, partitionKey, item.PartitionKey)
		assert.Equal(t, "testKey", item.ID)
		assert.Nil(t, item.TTL)

		// items need to be marshallable to JSON with encoding/json
		b, err := json.Marshal(item)
		assert.NoError(t, err)

		j := map[string]interface{}{}
		err = json.Unmarshal(b, &j)
		assert.NoError(t, err)

		m, ok := j["value"].(map[string]interface{})
		assert.Truef(t, ok, "value should be a map")
		assert.NotContains(t, j, "ttl")

		assert.Equal(t, "red", m["color"])
	})

	t.Run("create item for String bytes", func(t *testing.T) {
		// Bytes are handled the same way, does not matter if is JSON or JPEG.
		bytes, err := json.Marshal(value)
		assert.NoError(t, err)

		req := state.SetRequest{
			Key:   "testKey",
			Value: bytes,
		}

		item, err := createUpsertItem("text/plain", req, partitionKey)
		assert.NoError(t, err)
		assert.Equal(t, partitionKey, item.PartitionKey)
		assert.Equal(t, "testKey", item.ID)
		assert.Nil(t, item.TTL)

		// items need to be marshallable to JSON with encoding/json
		b, err := json.Marshal(item)
		assert.NoError(t, err)

		j := map[string]interface{}{}
		err = json.Unmarshal(b, &j)
		assert.NoError(t, err)

		value := j["value"]
		m, ok := value.(string)
		assert.Truef(t, ok, "value should be a string")
		assert.NotContains(t, j, "ttl")

		assert.Equal(t, "{\"color\":\"red\"}", m)
	})

	t.Run("create item for random bytes", func(t *testing.T) {
		// Bytes are handled as per content-type
		bytes := []byte{0x1}

		req := state.SetRequest{
			Key:   "testKey",
			Value: bytes,
		}

		item, err := createUpsertItem("application/json", req, partitionKey)
		assert.NoError(t, err)
		assert.Equal(t, partitionKey, item.PartitionKey)
		assert.Equal(t, "testKey", item.ID)
		assert.Nil(t, item.TTL)

		// items need to be marshallable to JSON with encoding/json
		b, err := json.Marshal(item)
		assert.NoError(t, err)

		j := map[string]interface{}{}
		err = json.Unmarshal(b, &j)
		assert.NoError(t, err)

		value := j["value"]
		m, ok := value.(string)
		assert.Truef(t, ok, "value should be a string")
		assert.NotContains(t, j, "ttl")

		assert.Equal(t, "AQ==", m)
	})

	t.Run("create item for random bytes", func(t *testing.T) {
		// Bytes are handled as per content-type
		bytes := []byte{0x1}

		req := state.SetRequest{
			Key:   "testKey",
			Value: bytes,
		}

		item, err := createUpsertItem("application/octet-stream", req, partitionKey)
		assert.NoError(t, err)
		assert.Equal(t, partitionKey, item.PartitionKey)
		assert.Equal(t, "testKey", item.ID)
		assert.Nil(t, item.TTL)

		// items need to be marshallable to JSON with encoding/json
		b, err := json.Marshal(item)
		assert.NoError(t, err)

		j := map[string]interface{}{}
		err = json.Unmarshal(b, &j)
		assert.NoError(t, err)

		value := j["value"]
		m, ok := value.(string)
		assert.Truef(t, ok, "value should be a string")
		assert.NotContains(t, j, "ttl")

		assert.Equal(t, "AQ==", m)
	})
}

func TestCreateCosmosItemWithTTL(t *testing.T) {
	value := widget{Color: "red"}
	partitionKey := "/partitionKey"
	t.Run("Create Item with TTL", func(t *testing.T) {
		ttl := 100
		req := state.SetRequest{
			Key:   "testKey",
			Value: value,
			Metadata: map[string]string{
				metadataTTLKey: strconv.Itoa(ttl),
			},
		}

		item, err := createUpsertItem("application/json", req, partitionKey)
		assert.NoError(t, err)
		assert.Equal(t, partitionKey, item.PartitionKey)
		assert.Equal(t, "testKey", item.ID)
		assert.Equal(t, value, item.Value)
		assert.Equal(t, ttl, *item.TTL)

		// items need to be marshallable to JSON with encoding/json
		b, err := json.Marshal(item)
		assert.NoError(t, err)

		j := map[string]interface{}{}
		err = json.Unmarshal(b, &j)
		assert.NoError(t, err)

		m, ok := j["value"].(map[string]interface{})
		assert.Truef(t, ok, "value should be a map")
		assert.Equal(t, float64(ttl), j["ttl"])

		assert.Equal(t, "red", m["color"])
	})

	t.Run("Create Item with TTL set to Persist items", func(t *testing.T) {
		ttl := -1
		req := state.SetRequest{
			Key:   "testKey",
			Value: value,
			Metadata: map[string]string{
				metadataTTLKey: strconv.Itoa(ttl),
			},
		}

		item, err := createUpsertItem("application/json", req, partitionKey)
		assert.NoError(t, err)
		assert.Equal(t, partitionKey, item.PartitionKey)
		assert.Equal(t, "testKey", item.ID)
		assert.Equal(t, value, item.Value)
		assert.Equal(t, ttl, *item.TTL)

		// items need to be marshallable to JSON with encoding/json
		b, err := json.Marshal(item)
		assert.NoError(t, err)

		j := map[string]interface{}{}
		err = json.Unmarshal(b, &j)
		assert.NoError(t, err)

		m, ok := j["value"].(map[string]interface{})
		assert.Truef(t, ok, "value should be a map")
		assert.Equal(t, float64(ttl), j["ttl"])

		assert.Equal(t, "red", m["color"])
	})

	t.Run("Create Item with Invalid TTL", func(t *testing.T) {
		req := state.SetRequest{
			Key:   "testKey",
			Value: value,
			Metadata: map[string]string{
				metadataTTLKey: "notattl",
			},
		}

		_, err := createUpsertItem("application/json", req, partitionKey)
		assert.Error(t, err)
	})
}
