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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/configuration"
	contribMetadata "github.com/dapr/components-contrib/metadata"
)

func TestMetadata_Parse(t *testing.T) {
	tests := []struct {
		name    string
		props   map[string]string
		wantErr string
		check   func(t *testing.T, m *metadata)
	}{
		{
			name:    "missing remoteUrl",
			props:   map[string]string{},
			wantErr: "remoteUrl is required",
		},
		{
			name: "defaults applied",
			props: map[string]string{
				"remoteUrl": "https://example.com/repo.git",
			},
			check: func(t *testing.T, m *metadata) {
				assert.Equal(t, "main", m.branch())
				assert.Equal(t, ".", m.path())
				assert.Equal(t, 0, m.depth())
				assert.Equal(t, 5*time.Minute, m.pollInterval())
				assert.Equal(t, 30*time.Second, m.fetchTimeout())
				assert.False(t, m.includeHidden())
				assert.Equal(t, mappingModeFile, m.mappingMode())
				assert.Equal(t, authModeNone, m.resolveAuthMode())
				assert.Equal(t, "git", m.user())
				assert.Equal(t, "https://api.github.com", m.apiBase())
				assert.Equal(t, 5*time.Minute, m.refreshSkew())
				assert.Equal(t, 5*time.Minute, m.rateLimitRetryAfter())
			},
		},
		{
			name: "explicit overrides",
			props: map[string]string{
				"remoteUrl":     "https://example.com/repo.git",
				"branch":        "release",
				"path":          "agents",
				"depth":         "5",
				"pollInterval":  "15s",
				"fetchTimeout":  "10s",
				"includeHidden": "true",
				"mappingMode":   "agentYaml",
			},
			check: func(t *testing.T, m *metadata) {
				assert.Equal(t, "release", m.branch())
				assert.Equal(t, "agents", m.path())
				assert.Equal(t, 5, m.depth())
				assert.Equal(t, 15*time.Second, m.pollInterval())
				assert.Equal(t, 10*time.Second, m.fetchTimeout())
				assert.True(t, m.includeHidden())
				//nolint:testifylint // not a YAML payload comparison; mode constant happens to contain "yaml"
				assert.Equal(t, mappingModeAgentYAML, m.mappingMode())
			},
		},
		{
			name: "unsupported mappingMode",
			props: map[string]string{
				"remoteUrl":   "https://example.com/repo.git",
				"mappingMode": "bogus",
			},
			wantErr: `unsupported mappingMode "bogus"`,
		},
		{
			name: "auto-detect ssh from URL",
			props: map[string]string{
				"remoteUrl":      "git@github.com:org/repo.git",
				"privateKeyPath": "/etc/ssh/key",
			},
			check: func(t *testing.T, m *metadata) {
				assert.Equal(t, authModeSSH, m.resolveAuthMode())
			},
		},
		{
			name: "auto-detect ssh missing key",
			props: map[string]string{
				"remoteUrl": "git@github.com:org/repo.git",
			},
			wantErr: "SSH auth requires privateKey or privateKeyPath",
		},
		{
			name: "auto-detect pat from token",
			props: map[string]string{
				"remoteUrl": "https://example.com/repo.git",
				"token":     "ghp_xxx",
			},
			check: func(t *testing.T, m *metadata) {
				assert.Equal(t, authModePAT, m.resolveAuthMode())
			},
		},
		{
			name: "githubApp missing installationId",
			props: map[string]string{
				"remoteUrl":  "https://example.com/repo.git",
				"appId":      "1",
				"privateKey": "PEM",
			},
			wantErr: "installationId",
		},
		{
			name: "githubApp full config",
			props: map[string]string{
				"remoteUrl":      "https://example.com/repo.git",
				"appId":          "100",
				"installationId": "200",
				"privateKey":     "PEM",
			},
			check: func(t *testing.T, m *metadata) {
				assert.Equal(t, authModeGithubApp, m.resolveAuthMode())
				require.NotNil(t, m.AppID)
				assert.Equal(t, int64(100), *m.AppID)
				require.NotNil(t, m.InstallationID)
				assert.Equal(t, int64(200), *m.InstallationID)
			},
		},
		{
			name: "pollInterval below remote minimum",
			props: map[string]string{
				"remoteUrl":    "https://example.com/repo.git",
				"pollInterval": "500ms",
			},
			wantErr: "below minimum",
		},
		{
			name: "pollInterval allowed for file:// below remote minimum",
			props: map[string]string{
				"remoteUrl":    "file:///tmp/repo.git",
				"pollInterval": "500ms",
			},
			check: func(t *testing.T, m *metadata) {
				assert.Equal(t, 500*time.Millisecond, m.pollInterval())
			},
		},
		{
			name: "pollInterval below file minimum",
			props: map[string]string{
				"remoteUrl":    "file:///tmp/repo.git",
				"pollInterval": "10ms",
			},
			wantErr: "below minimum",
		},
		{
			name: "url with embedded credentials rejected",
			props: map[string]string{
				"remoteUrl": "https://user:tok@example.com/repo.git",
			},
			wantErr: "credentials must not be embedded",
		},
		{
			name: "http url rejected with authenticated profile",
			props: map[string]string{
				"remoteUrl": "http://example.com/repo.git",
				"token":     "abc",
			},
			wantErr: "http:// is not allowed",
		},
		{
			name: "apiBase requires https",
			props: map[string]string{
				"remoteUrl":      "https://example.com/repo.git",
				"appId":          "1",
				"installationId": "2",
				"privateKey":     "PEM",
				"apiBase":        "http://api.example/",
			},
			wantErr: "apiBase must use https",
		},
		{
			name: "path with leading slash",
			props: map[string]string{
				"remoteUrl": "https://example.com/repo.git",
				"path":      "/etc",
			},
			wantErr: "must be repo-relative",
		},
		{
			name: "path escaping repo root",
			props: map[string]string{
				"remoteUrl": "https://example.com/repo.git",
				"path":      "../outside",
			},
			wantErr: "must not escape",
		},
		{
			name: "path equal to .git rejected",
			props: map[string]string{
				"remoteUrl": "https://example.com/repo.git",
				"path":      ".git",
			},
			wantErr: "must not reference the .git directory",
		},
		{
			name: "path with .git subdirectory segment rejected",
			props: map[string]string{
				"remoteUrl": "https://example.com/repo.git",
				"path":      "agents/.git",
			},
			wantErr: "must not reference the .git directory",
		},
		{
			name: "path reaching into .git rejected",
			props: map[string]string{
				"remoteUrl": "https://example.com/repo.git",
				"path":      ".git/config",
			},
			wantErr: "must not reference the .git directory",
		},
		{
			name: "path with .git in middle rejected",
			props: map[string]string{
				"remoteUrl": "https://example.com/repo.git",
				"path":      "a/.git/b",
			},
			wantErr: "must not reference the .git directory",
		},
		{
			name: "rateLimitRetryAfter custom",
			props: map[string]string{
				"remoteUrl":           "https://example.com/repo.git",
				"rateLimitRetryAfter": "15m",
			},
			check: func(t *testing.T, m *metadata) {
				assert.Equal(t, 15*time.Minute, m.rateLimitRetryAfter())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &metadata{}
			err := m.parse(configuration.Metadata{Base: contribMetadata.Base{Properties: tt.props}})
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			if tt.check != nil {
				tt.check(t, m)
			}
		})
	}
}

func TestMetadata_NormalizeMode(t *testing.T) {
	cases := []struct {
		name string
		in   *string
		want string
	}{
		{"nil", nil, ""},
		{"empty string", stringPtr(""), ""},
		{"mixed case", stringPtr("File"), "file"},
		{"with spaces", stringPtr("  prompty  "), "prompty"},
		{"all caps", stringPtr("AGENTYAML"), "agentyaml"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, normalizeMode(tt.in))
		})
	}
}

func stringPtr(s string) *string { return &s }
