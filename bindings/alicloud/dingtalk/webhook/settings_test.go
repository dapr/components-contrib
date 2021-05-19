// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSettingsDecode(t *testing.T) { //nolint:paralleltest
	props := map[string]string{
		"url":    "a",
		"secret": "b",
		"id":     "c",
	}

	var settings Settings
	err := settings.Decode(props)
	require.NoError(t, err)
	assert.Equal(t, "a", settings.URL)
	assert.Equal(t, "b", settings.Secret)
	assert.Equal(t, "c", settings.ID)
}
