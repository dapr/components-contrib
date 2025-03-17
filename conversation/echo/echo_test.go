package echo

import (
	"testing"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/kit/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConverse(t *testing.T) {
	e := NewEcho(logger.NewLogger("echo test"))
	e.Init(t.Context(), conversation.Metadata{})

	r, err := e.Converse(t.Context(), &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			{
				Message: "hello",
			},
		},
	})
	require.NoError(t, err)
	assert.Len(t, r.Outputs, 1)
	assert.Equal(t, "hello", r.Outputs[0].Result)
}
