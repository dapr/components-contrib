// +build conftests

// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package conformance

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBindingsConformance(t *testing.T) {
	tc, err := NewTestConfiguration("../config/bindings/tests.yml")
	assert.NoError(t, err)
	assert.NotNil(t, tc)
	tc.Run(t)
}
