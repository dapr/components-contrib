// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package etcd

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// toConfigProperties
func TestToConfigProperties(t *testing.T) {
	t.Run("With all required fields", func(t *testing.T) {
		properties := map[string]string{
			"endpoints":   "localhost:6379",
			"dialTimeout": "5s",
		}
		cp, err := toConfigProperties(properties)
		assert.Equal(t, err, nil, fmt.Sprintf("Unexpected error: %v", err))
		assert.NotNil(t, cp, "failed to respond to missing data field")
		assert.Equal(t, "localhost:6379", cp.Endpoints, "failed to get endpoints")
		assert.Equal(t, "5s", cp.DialTimeout, "failed to get DialTimeout")
	})
}

// toEtcdConfig
func TestToEtcdConfig(t *testing.T) {
	t.Run("With valid fields", func(t *testing.T) {
		cp := &configProperties{
			Endpoints:   "localhost:6379",
			DialTimeout: "5s",
		}
		_, err := toEtcdConfig(cp)
		assert.Equal(t, err, nil, fmt.Sprintf("Unexpected error: %v", err))
	})
	t.Run("With invalid fields", func(t *testing.T) {
		cp := &configProperties{}
		_, err := toEtcdConfig(cp)
		assert.NotNil(t, err, "Expected error due to invalid fields")
	})
}

// validateRequired
func TestValidateRequired(t *testing.T) {
	t.Run("With all required fields", func(t *testing.T) {
		configProps := &configProperties{
			Endpoints:   "localhost:6379",
			DialTimeout: "5s",
		}
		err := validateRequired(configProps)
		assert.Equal(t, nil, err, "failed to read all fields")
	})
	t.Run("With missing endpoints", func(t *testing.T) {
		configProps := &configProperties{
			DialTimeout: "5s",
		}
		err := validateRequired(configProps)
		assert.NotNil(t, err, "failed to get missing endpoints error")
	})
	t.Run("With missing dialTimeout", func(t *testing.T) {
		configProps := &configProperties{
			DialTimeout: "5s",
		}
		err := validateRequired(configProps)
		assert.NotNil(t, err, "failed to get invalid dialTimeout error")
	})
}
