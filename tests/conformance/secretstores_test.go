// +build conftests

package conformance

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSecretStoreConformance(t *testing.T) {
	tc, err := NewTestConfiguration("../config/secretstores/tests.yml")
	assert.NoError(t, err)
	assert.NotNil(t, tc)
	tc.Run(t)
}
