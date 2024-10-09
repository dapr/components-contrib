package echo

import (
	"context"
	"testing"

	"github.com/dapr/components-contrib/conversation"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
)

func TestConverse(t *testing.T) {
	e := NewEcho(logger.NewLogger("echo test"))
	e.Init(context.Background(), conversation.Metadata{})

	r, err := e.Converse(context.Background(), &conversation.ConversationRequest{
		Inputs: []conversation.ConversationInput{
			{
				Message: "hello",
			},
		},
	})
	assert.NoError(t, err)
	assert.Len(t, r.Outputs, 1)
	assert.Equal(t, "hello", r.Outputs[0].Result)
}
