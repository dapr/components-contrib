package utils

import (
	"testing"

	"github.com/dapr/components-contrib/metadata"
	"github.com/stretchr/testify/assert"
)

func TestValidate(t *testing.T) {
	validationMap := map[string]metadata.ValidationType{
		"key_required": metadata.Required,
		"key_optional": metadata.Optional,
		"key_denied":   metadata.Denied,
	}
	items := []struct {
		meta         map[string]string
		validatePass bool
		desc         string
	}{
		{
			meta: map[string]string{
				"key_required": "hah",
			},
			validatePass: true,
			desc:         "key `key_required` exists",
		},
		{
			meta: map[string]string{
				"key_required": "hah",
				"key_denied":   "hah",
			},
			validatePass: false,
			desc:         "key `key_denied` should not exist",
		},
		{
			meta: map[string]string{
				"key_required": "hah",
				"key_optional": "hah",
			},
			validatePass: true,
			desc:         "key `key_optional` is optional.",
		},
		{
			meta: map[string]string{
				"key_required": "hah",
				"key_miss":     "hah",
			},
			validatePass: true,
			desc:         "key `key_miss` won't be validated.",
		},
	}
	for _, item := range items {
		err := Validate(item.meta, validationMap)
		assert.Equal(t, err == nil, item.validatePass, item.desc)
	}
}
