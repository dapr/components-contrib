package aws

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamARN_NilClient(t *testing.T) {
	arn, err := StreamARN(t.Context(), nil, "foo")
	assert.Nil(t, arn)

	require.ErrorContains(t, err, "kinesis client is nil")
}
