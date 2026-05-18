/*
Copyright 2026 The Dapr Authors
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

package git

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/kit/logger"
)

const testVersion = "abc1234"

func TestSelectMapper(t *testing.T) {
	tests := []struct {
		name    string
		mode    string
		want    string
		wantErr bool
	}{
		{"empty defaults to file", "", mappingModeFile, false},
		{"lowercase file", "file", mappingModeFile, false},
		{"uppercase FILE", "FILE", mappingModeFile, false},
		{"agentYaml mixed case", "agentYaml", mappingModeAgentYAML, false},
		{"prompty", "prompty", mappingModePrompty, false},
		{"unsupported mode rejected", "bogus", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := selectMapper(tt.mode)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, m.Name())
		})
	}
}

func TestFileMapper(t *testing.T) {
	mapper := fileMapper{}
	entries := []fileEntry{
		{RelPath: "agent_role.txt", Bytes: []byte("Weather expert")},
		{RelPath: "nested/agent_goal.txt", Bytes: []byte("Help users")},
		{RelPath: "instructions.json", Bytes: []byte(`["be concise"]`)},
	}
	out, err := mapper.Map(entries, testVersion, logger.NewLogger("test"))
	require.NoError(t, err)
	require.Len(t, out, 3)

	assert.Equal(t, "Weather expert", out["agent_role.txt"].Value)
	assert.Equal(t, testVersion, out["agent_role.txt"].Version)
	assert.Equal(t, "Help users", out["nested/agent_goal.txt"].Value)
	assert.Equal(t, `["be concise"]`, out["instructions.json"].Value)
}

func TestAgentYAMLMapper(t *testing.T) {
	mapper := agentYAMLMapper{}
	yamlInput := []byte(`agent_role: Weather expert
agent_goal: Help users plan trips
agent_instructions:
  - be concise
  - cite sources
max_iterations: 5
`)
	jsonInput := []byte(`{"agent_role": "Travel agent", "tool_choice": "auto"}`)

	entries := []fileEntry{
		{RelPath: "weather.yaml", Bytes: yamlInput},
		{RelPath: "travel.json", Bytes: jsonInput},
	}
	out, err := mapper.Map(entries, testVersion, logger.NewLogger("test"))
	require.NoError(t, err)

	assert.Equal(t, "Weather expert", out["weather/agent_role"].Value)
	assert.Equal(t, "Help users plan trips", out["weather/agent_goal"].Value)
	assert.Equal(t, "5", out["weather/max_iterations"].Value)

	// Non-scalar lossless round-trip.
	require.Contains(t, out, "weather/agent_instructions")
	roundtrip := out["weather/agent_instructions"].Value
	assert.Contains(t, roundtrip, "be concise")
	assert.Contains(t, roundtrip, "cite sources")

	assert.Equal(t, "Travel agent", out["travel/agent_role"].Value)
	assert.Equal(t, "auto", out["travel/tool_choice"].Value)
}

func TestAgentYAMLMapper_NonYAMLFileRejected(t *testing.T) {
	// mappingMode=agentYaml must hard-error on non-yaml/json files so the
	// operator knows their `path` scope and mappingMode are mismatched.
	mapper := agentYAMLMapper{}
	entries := []fileEntry{
		{RelPath: "weather.yaml", Bytes: []byte("agent_role: ok\n")},
		{RelPath: "README.md", Bytes: []byte("not yaml")},
	}
	_, err := mapper.Map(entries, testVersion, logger.NewLogger("test"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mappingMode=agentYaml does not accept \"README.md\"")
}

func TestAgentYAMLMapper_SubdirectoryStemUniqueness(t *testing.T) {
	// Two files with the same basename in different subdirectories must not
	// collide on emitted keys. This guards against the historical stemOf
	// behaviour that discarded directory components.
	mapper := agentYAMLMapper{}
	entries := []fileEntry{
		{RelPath: "team-a/weather.yaml", Bytes: []byte("agent_role: Weather A\n")},
		{RelPath: "team-b/weather.yaml", Bytes: []byte("agent_role: Weather B\n")},
	}
	out, err := mapper.Map(entries, testVersion, logger.NewLogger("test"))
	require.NoError(t, err)
	assert.Equal(t, "Weather A", out["team-a_weather/agent_role"].Value)
	assert.Equal(t, "Weather B", out["team-b_weather/agent_role"].Value)
}

func TestAgentYAMLMapper_BadFileRejected(t *testing.T) {
	// A YAML file that doesn't decode into a top-level map is a structural
	// mismatch: the operator told the component to expect agent-shaped YAML.
	// Hard-error so they notice rather than silently dropping their config.
	mapper := agentYAMLMapper{}
	entries := []fileEntry{
		{RelPath: "bad.yaml", Bytes: []byte("- not a map\n- but a sequence\n")},
	}
	_, err := mapper.Map(entries, testVersion, logger.NewLogger("test"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse \"bad.yaml\"")
}

func TestPromptyMapper(t *testing.T) {
	full := []byte(`---
name: Weather
agent_role: Weather expert
agent_goal: Help users plan trips
agent_instructions:
  - be concise
---
You are a friendly weather assistant.
`)
	bodyOnly := []byte(`Just a system prompt with no frontmatter.
`)
	frontmatterOnly := []byte(`---
agent_role: Travel agent
---
`)

	entries := []fileEntry{
		{RelPath: "weather.prompty", Bytes: full},
		{RelPath: "raw.prompty", Bytes: bodyOnly},
		{RelPath: "travel.prompty", Bytes: frontmatterOnly},
	}
	out, err := promptyMapper{}.Map(entries, testVersion, logger.NewLogger("test"))
	require.NoError(t, err)

	assert.Equal(t, "Weather expert", out["weather/agent_role"].Value)
	assert.Equal(t, "Help users plan trips", out["weather/agent_goal"].Value)
	assert.Equal(t, "You are a friendly weather assistant.",
		strings.TrimSpace(out["weather/agent_system_prompt"].Value))

	require.Contains(t, out, "raw/agent_system_prompt")
	assert.Contains(t, out["raw/agent_system_prompt"].Value, "Just a system prompt")
	_, hasRawFrontmatter := out["raw/agent_role"]
	assert.False(t, hasRawFrontmatter)

	assert.Equal(t, "Travel agent", out["travel/agent_role"].Value)
	_, hasTravelBody := out["travel/agent_system_prompt"]
	assert.False(t, hasTravelBody)
}

func TestPromptyMapper_NonPromptyFileRejected(t *testing.T) {
	// mappingMode=prompty must hard-error on non-.prompty files.
	entries := []fileEntry{
		{RelPath: "weather.prompty", Bytes: []byte("---\nagent_role: ok\n---\n")},
		{RelPath: "ignored.txt", Bytes: []byte("nope")},
	}
	_, err := promptyMapper{}.Map(entries, testVersion, logger.NewLogger("test"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mappingMode=prompty does not accept \"ignored.txt\"")
}

func TestPromptyMapper_NoClosingDelim(t *testing.T) {
	// No closing `---` — everything after the opener is frontmatter, no body.
	in := []byte(`---
agent_role: Recovered
agent_goal: From malformed input
`)
	out, err := promptyMapper{}.Map(
		[]fileEntry{{RelPath: "fallback.prompty", Bytes: in}},
		testVersion, logger.NewLogger("test"),
	)
	require.NoError(t, err)
	assert.Equal(t, "Recovered", out["fallback/agent_role"].Value)
	_, hasBody := out["fallback/agent_system_prompt"]
	assert.False(t, hasBody)
}
