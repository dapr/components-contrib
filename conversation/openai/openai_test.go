package openai

import (
	"testing"

	"github.com/dapr/components-contrib/conversation"
	"github.com/stretchr/testify/assert"

	openai "github.com/sashabaranov/go-openai"
)

func TestConvertRole(t *testing.T) {
	roles := map[string]string{
		conversation.RoleSystem:    string(openai.ChatMessageRoleSystem),
		conversation.RoleAssistant: string(openai.ChatMessageRoleAssistant),
		conversation.RoleFunction:  string(openai.ChatMessageRoleFunction),
		conversation.RoleUser:      string(openai.ChatMessageRoleUser),
		conversation.RoleTool:      string(openai.ChatMessageRoleTool),
	}

	for k, v := range roles {
		r := convertRole(conversation.Role(k))
		assert.Equal(t, v, string(r))
	}
}
