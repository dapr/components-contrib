package aws

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStreamARN_NilClient(t *testing.T) {
	arn, err := StreamARN(t.Context(), nil, "foo")
	assert.Nil(t, arn)
	assert.True(t, errors.Is(err, errors.New("kinesis client is nil")) || err != nil)
}
