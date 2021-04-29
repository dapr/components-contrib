// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package zeebe

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVariableStringToArrayRemovesSpaces(t *testing.T) {
	vars := VariableStringToArray("  a,   b,  c  ")
	require.Equal(t, 3, len(vars))
	assert.Equal(t, "a", vars[0])
	assert.Equal(t, "b", vars[1])
	assert.Equal(t, "c", vars[2])
}
