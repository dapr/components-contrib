package client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// go test -timeout 30s ./client -count 1 -run ^TestInvokeBinding$

func TestInvokeBinding(t *testing.T) {
	ctx := context.Background()
	in := &InvokeBindingRequest{
		Name:      "test",
		Operation: "fn",
	}

	t.Run("output binding without data", func(t *testing.T) {
		err := testClient.InvokeOutputBinding(ctx, in)
		assert.Nil(t, err)
	})

	t.Run("output binding", func(t *testing.T) {
		in.Data = []byte("test")
		err := testClient.InvokeOutputBinding(ctx, in)
		assert.Nil(t, err)
	})

	t.Run("binding without data", func(t *testing.T) {
		in.Data = nil
		out, err := testClient.InvokeBinding(ctx, in)
		assert.Nil(t, err)
		assert.NotNil(t, out)
	})

	t.Run("binding with data and meta", func(t *testing.T) {
		in.Data = []byte("test")
		in.Metadata = map[string]string{"k1": "v1", "k2": "v2"}
		out, err := testClient.InvokeBinding(ctx, in)
		assert.Nil(t, err)
		assert.NotNil(t, out)
		assert.Equal(t, "test", string(out.Data))
	})

}
