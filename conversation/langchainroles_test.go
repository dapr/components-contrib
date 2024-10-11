/*
Copyright 2024 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package conversation

import (
	"testing"

	"github.com/tmc/langchaingo/llms"

	"github.com/stretchr/testify/assert"
)

func TestConvertLangchainRole(t *testing.T) {
	roles := map[string]string{
		RoleSystem:    string(llms.ChatMessageTypeSystem),
		RoleAssistant: string(llms.ChatMessageTypeAI),
		RoleFunction:  string(llms.ChatMessageTypeFunction),
		RoleUser:      string(llms.ChatMessageTypeHuman),
		RoleTool:      string(llms.ChatMessageTypeTool),
	}

	for k, v := range roles {
		r := ConvertLangchainRole(Role(k))
		assert.Equal(t, v, string(r))
	}
}
