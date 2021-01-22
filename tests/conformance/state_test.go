// +build conftests

package conformance

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStateConformance(t *testing.T) {
	tc, err := NewTestConfiguration("../config/state/tests.yml")
	assert.NoError(t, err)
	assert.NotNil(t, tc)
	tc.Run(t)
}
