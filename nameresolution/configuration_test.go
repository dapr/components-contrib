// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package nameresolution_test

import (
	"testing"

	"github.com/dapr/components-contrib/nameresolution"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfiguration(t *testing.T) {
	tests := map[string]struct {
		input    interface{}
		expected interface{}
		err      string
	}{
		"simple": {input: "test", expected: "test"},
		"map of string to interface{}": {
			input: map[string]interface{}{
				"test": "1234",
				"nested": map[string]interface{}{
					"value": "5678",
				},
			}, expected: map[string]interface{}{
				"test": "1234",
				"nested": map[string]interface{}{
					"value": "5678",
				},
			},
		},
		"map of string to interface{} with error": {
			input: map[string]interface{}{
				"test": "1234",
				"nested": map[interface{}]interface{}{
					5678: "5678",
				},
			}, err: "error parsing config field: 5678",
		},
		"map of interface{} to interface{}": {
			input: map[string]interface{}{
				"test": "1234",
				"nested": map[interface{}]interface{}{
					"value": "5678",
				},
			}, expected: map[string]interface{}{
				"test": "1234",
				"nested": map[string]interface{}{
					"value": "5678",
				},
			},
		},
		"map of interface{} to interface{} with error": {
			input: map[interface{}]interface{}{
				"test": "1234",
				"nested": map[interface{}]interface{}{
					5678: "5678",
				},
			}, err: "error parsing config field: 5678",
		},
		"slice of interface{}": {
			input: []interface{}{
				map[interface{}]interface{}{
					"value": "5678",
				},
			}, expected: []interface{}{
				map[string]interface{}{
					"value": "5678",
				},
			},
		},
		"slice of interface{} with error": {
			input: []interface{}{
				map[interface{}]interface{}{
					1234: "1234",
				},
			}, err: "error parsing config field: 1234",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			actual, err := nameresolution.ConvertConfig(tc.input)
			if tc.err != "" {
				require.Error(t, err)
				assert.EqualError(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.expected, actual)
		})
	}
}
