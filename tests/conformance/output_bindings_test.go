// +build conftests

package conformance

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOutputBindingConformance(t *testing.T) {
	tc, err := NewTestConfiguration("../config/bindings/output_tests.yml")
	assert.NoError(t, err)
	assert.NotNil(t, tc)
	tc.Run(t)
}
