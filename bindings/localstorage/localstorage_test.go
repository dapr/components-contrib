// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package localstorage

import (
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"rootPath": "/files"}
	localStorage := NewLocalStorage(logger.NewLogger("test"))
	meta, err := localStorage.parseMetadata(m)
	assert.Nil(t, err)
	assert.Equal(t, "/files", meta.RootPath)
}
