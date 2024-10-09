package bedrock

import (
	"testing"

	"github.com/dapr/components-contrib/conversation"
	"github.com/stretchr/testify/assert"
	"github.com/tmc/langchaingo/llms"
)

func TestConvertRole(t *testing.T) {
	roles := map[string]string{
		conversation.RoleSystem:    string(llms.ChatMessageTypeSystem),
		conversation.RoleAssistant: string(llms.ChatMessageTypeAI),
		conversation.RoleFunction:  string(llms.ChatMessageTypeFunction),
		conversation.RoleUser:      string(llms.ChatMessageTypeHuman),
		conversation.RoleTool:      string(llms.ChatMessageTypeTool),
	}

	for k, v := range roles {
		r := convertRole(conversation.Role(k))
		assert.Equal(t, v, string(r))
	}
}
