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

package conformance

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStandaloneIsYaml(t *testing.T) {
	request := NewStandaloneComponents("test_component_path")

	assert.True(t, request.isYaml("test.yaml"))
	assert.True(t, request.isYaml("test.YAML"))
	assert.True(t, request.isYaml("test.yml"))
	assert.True(t, request.isYaml("test.YML"))
	assert.False(t, request.isYaml("test.md"))
	assert.False(t, request.isYaml("test.txt"))
	assert.False(t, request.isYaml("test.sh"))
}

func TestStandaloneDecodeValidYaml(t *testing.T) {
	request := StandaloneComponents{
		componentsPath: "test_component_path",
	}
	yaml := `
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
   name: statestore
spec:
   type: state.couchbase
   metadata:
   - name: prop1
     value: value1
   - name: prop2
     value: value2
`
	components := request.decodeYaml("components/messagebus.yaml", []byte(yaml))
	require.Len(t, components, 1)
	assert.Equal(t, "statestore", components[0].Name)
	assert.Equal(t, "state.couchbase", components[0].Spec.Type)
	require.Len(t, components[0].Spec.Metadata, 2)
	assert.Equal(t, "prop1", components[0].Spec.Metadata[0].Name)
	assert.Equal(t, "value1", components[0].Spec.Metadata[0].Value.String())
}

func TestStandaloneDecodeInvalidComponent(t *testing.T) {
	request := NewStandaloneComponents("test_component_path")
	yaml := `
apiVersion: dapr.io/v1alpha1
kind: Subscription
metadata:
   name: testsub
spec:
   metadata:
   - name: prop1
     value: value1
   - name: prop2
     value: value2
`
	components := request.decodeYaml("components/messagebus.yaml", []byte(yaml))
	assert.Len(t, components, 0)
}

func TestStandaloneDecodeUnsuspectingFile(t *testing.T) {
	request := NewStandaloneComponents("test_component_path")

	components := request.decodeYaml("components/messagebus.yaml", []byte("hey there"))
	assert.Len(t, components, 0)
}

func TestStandaloneDecodeInvalidYaml(t *testing.T) {
	request := NewStandaloneComponents("test_component_path")
	yaml := `
INVALID_YAML_HERE
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
name: statestore`
	components := request.decodeYaml("components/messagebus.yaml", []byte(yaml))
	assert.Len(t, components, 0)
}

func TestStandaloneDecodeValidMultiYaml(t *testing.T) {
	request := NewStandaloneComponents("test_component_path")
	yaml := `
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
    name: statestore1
spec:
  type: state.couchbase
  metadata:
    - name: prop1
      value: value1
    - name: prop2
      value: value2
---
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
    name: statestore2
spec:
  type: state.redis
  metadata:
    - name: prop3
      value: value3
`
	components := request.decodeYaml("components/messagebus.yaml", []byte(yaml))
	assert.Len(t, components, 2)
	assert.Equal(t, "statestore1", components[0].Name)
	assert.Equal(t, "state.couchbase", components[0].Spec.Type)
	assert.Len(t, components[0].Spec.Metadata, 2)
	assert.Equal(t, "prop1", components[0].Spec.Metadata[0].Name)
	assert.Equal(t, "value1", components[0].Spec.Metadata[0].Value.String())
	assert.Equal(t, "prop2", components[0].Spec.Metadata[1].Name)
	assert.Equal(t, "value2", components[0].Spec.Metadata[1].Value.String())

	assert.Equal(t, "statestore2", components[1].Name)
	assert.Equal(t, "state.redis", components[1].Spec.Type)
	assert.Len(t, components[1].Spec.Metadata, 1)
	assert.Equal(t, "prop3", components[1].Spec.Metadata[0].Name)
	assert.Equal(t, "value3", components[1].Spec.Metadata[0].Value.String())
}

func TestStandaloneDecodeInValidDocInMultiYaml(t *testing.T) {
	request := NewStandaloneComponents("test_component_path")
	yaml := `
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
    name: statestore1
spec:
  type: state.couchbase
  metadata:
    - name: prop1
      value: value1
    - name: prop2
      value: value2
---
INVALID_YAML_HERE
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
name: invalidyaml
---
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
    name: statestore2
spec:
  type: state.redis
  metadata:
    - name: prop3
      value: value3
`
	components := request.decodeYaml("components/messagebus.yaml", []byte(yaml))
	assert.Len(t, components, 2)

	assert.Equal(t, "statestore1", components[0].Name)
	assert.Equal(t, "state.couchbase", components[0].Spec.Type)
	assert.Len(t, components[0].Spec.Metadata, 2)
	assert.Equal(t, "prop1", components[0].Spec.Metadata[0].Name)
	assert.Equal(t, "value1", components[0].Spec.Metadata[0].Value.String())
	assert.Equal(t, "prop2", components[0].Spec.Metadata[1].Name)
	assert.Equal(t, "value2", components[0].Spec.Metadata[1].Value.String())

	assert.Equal(t, "statestore2", components[1].Name)
	assert.Equal(t, "state.redis", components[1].Spec.Type)
	assert.Len(t, components[1].Spec.Metadata, 1)
	assert.Equal(t, "prop3", components[1].Spec.Metadata[0].Name)
	assert.Equal(t, "value3", components[1].Spec.Metadata[0].Value.String())
}
