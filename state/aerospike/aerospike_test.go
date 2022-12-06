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

package aerospike

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
)

const (
	hosts     = "hosts"
	namespace = "namespace"
	set       = "set"
)

func TestValidateMetadataForValidInputs(t *testing.T) {
	type testCase struct {
		name       string
		properties map[string]string
	}
	tests := []testCase{
		{"with mandatory fields", map[string]string{
			hosts:     "host1:1234",
			namespace: "foobarnamespace",
		}},
		{"with multiple hosts", map[string]string{
			hosts:     "host1:7777,host2:8888,host3:9999",
			namespace: "foobarnamespace",
		}},
		{"with optional fields", map[string]string{
			hosts:     "host1:1234",
			namespace: "foobarnamespace",
			set:       "fooset",
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metadata := state.Metadata{Base: metadata.Base{Properties: test.properties}}
			_, err := parseAndValidateMetadata(metadata)
			assert.Nil(t, err)
		})
	}
}

func TestValidateMetadataForInvalidInputs(t *testing.T) {
	type testCase struct {
		name       string
		properties map[string]string
	}
	tests := []testCase{
		{"With missing hosts", map[string]string{
			namespace: "foobarnamespace",
			set:       "fooset",
		}},
		{"With invalid hosts 1", map[string]string{
			hosts:     "host1",
			namespace: "foobarnamespace",
			set:       "fooset",
		}},
		{"With invalid hosts 2", map[string]string{
			hosts:     "host1:8080,host2",
			namespace: "foobarnamespace",
			set:       "fooset",
		}},
		{"With missing namspace", map[string]string{
			hosts: "host1:1234",
			set:   "fooset",
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metadata := state.Metadata{Base: metadata.Base{Properties: test.properties}}
			_, err := parseAndValidateMetadata(metadata)
			assert.NotNil(t, err)
		})
	}
}

func TestParseHostsForValidInputs(t *testing.T) {
	type testCase struct {
		name      string
		hostPorts string
	}
	tests := []testCase{
		{"valid host ports", "host1:1234"},
		{"valid multiple host ports", "host1:7777,host2:8888,host3:9999"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := parseHosts(test.hostPorts)
			assert.Nil(t, err)
			assert.NotNil(t, result)
			assert.True(t, len(result) >= 1)
		})
	}
}

func TestParseHostsForInvalidInputs(t *testing.T) {
	type testCase struct {
		name      string
		hostPorts string
	}
	tests := []testCase{
		{"missing port", "host1"},
		{"multiple entries missing port", "host1:1234,host2"},
		{"invalid port", "host1:foo"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseHosts(test.hostPorts)
			assert.NotNil(t, err)
		})
	}
}

func TestConvertETag(t *testing.T) {
	t.Run("valid conversion", func(t *testing.T) {
		result, err := convertETag("42")
		assert.Nil(t, err)
		assert.Equal(t, uint32(42), result)
	})

	t.Run("invalid conversion", func(t *testing.T) {
		_, err := convertETag("junk")
		assert.NotNil(t, err)
	})
}
