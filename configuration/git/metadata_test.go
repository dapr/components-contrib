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
			name:    "missing url",
			props:   map[string]string{},
			wantErr: "url is required",
		},
		{
			name: "defaults applied",
			props: map[string]string{
				"url": "https://example.com/repo.git",
			},
			check: func(t *testing.T, m *metadata) {
				assert.Equal(t, "main", m.branch())
				assert.Equal(t, ".", m.path())
				assert.Equal(t, 0, m.depth())
				assert.Equal(t, 30*time.Second, m.pollInterval())
				assert.Equal(t, 30*time.Second, m.fetchTimeout())
				assert.False(t, m.includeHidden())
				assert.Equal(t, mappingModeFile, m.mappingMode())
				assert.Equal(t, authModeNone, m.resolveAuthMode())
				assert.Equal(t, "git", m.sshUser())
				assert.Equal(t, "https://api.github.com", m.githubAppAPIBase())
				assert.Equal(t, 5*time.Minute, m.githubAppRefreshSkew())
			},
		},
		{
			name: "explicit overrides",
			props: map[string]string{
				"url":           "https://example.com/repo.git",
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
				"url":         "https://example.com/repo.git",
				"mappingMode": "bogus",
			},
			wantErr: `unsupported mappingMode "bogus"`,
		},
		{
			name: "unsupported authMode",
			props: map[string]string{
				"url":      "https://example.com/repo.git",
				"authMode": "kerberos",
			},
			wantErr: `unsupported authMode "kerberos"`,
		},
		{
			name: "auto-detect ssh from URL",
			props: map[string]string{
				"url":               "git@github.com:org/repo.git",
				"sshPrivateKeyPath": "/etc/ssh/key",
			},
			check: func(t *testing.T, m *metadata) {
				assert.Equal(t, authModeSSH, m.resolveAuthMode())
			},
		},
		{
			name: "auto-detect ssh missing key",
			props: map[string]string{
				"url": "git@github.com:org/repo.git",
			},
			wantErr: "authMode=ssh requires sshPrivateKey or sshPrivateKeyPath",
		},
		{
			name: "pat requires token",
			props: map[string]string{
				"url":      "https://example.com/repo.git",
				"authMode": "pat",
			},
			wantErr: "authMode=pat requires token",
		},
		{
			name: "auto-detect pat from token",
			props: map[string]string{
				"url":   "https://example.com/repo.git",
				"token": "ghp_xxx",
			},
			check: func(t *testing.T, m *metadata) {
				assert.Equal(t, authModePAT, m.resolveAuthMode())
			},
		},
		{
			name: "githubApp missing installation id",
			props: map[string]string{
				"url":                 "https://example.com/repo.git",
				"authMode":            "githubApp",
				"githubAppId":         "1",
				"githubAppPrivateKey": "PEM",
			},
			wantErr: "githubAppInstallationId",
		},
		{
			name: "githubApp full config",
			props: map[string]string{
				"url":                     "https://example.com/repo.git",
				"authMode":                "githubApp",
				"githubAppId":             "100",
				"githubAppInstallationId": "200",
				"githubAppPrivateKey":     "PEM",
			},
			check: func(t *testing.T, m *metadata) {
				assert.Equal(t, authModeGithubApp, m.resolveAuthMode())
				require.NotNil(t, m.GithubAppID)
				assert.Equal(t, int64(100), *m.GithubAppID)
				require.NotNil(t, m.GithubAppInstallationID)
				assert.Equal(t, int64(200), *m.GithubAppInstallationID)
			},
		},
		{
			name: "pollInterval below remote minimum",
			props: map[string]string{
				"url":          "https://example.com/repo.git",
				"pollInterval": "500ms",
			},
			wantErr: "below minimum",
		},
		{
			name: "pollInterval allowed for file:// below remote minimum",
			props: map[string]string{
				"url":          "file:///tmp/repo.git",
				"pollInterval": "500ms",
			},
			check: func(t *testing.T, m *metadata) {
				assert.Equal(t, 500*time.Millisecond, m.pollInterval())
			},
		},
		{
			name: "pollInterval below file minimum",
			props: map[string]string{
				"url":          "file:///tmp/repo.git",
				"pollInterval": "10ms",
			},
			wantErr: "below minimum",
		},
		{
			name: "url with embedded credentials rejected",
			props: map[string]string{
				"url": "https://user:tok@example.com/repo.git",
			},
			wantErr: "credentials must not be embedded",
		},
		{
			name: "http url rejected with authenticated mode",
			props: map[string]string{
				"url":      "http://example.com/repo.git",
				"authMode": "pat",
				"token":    "abc",
			},
			wantErr: "http:// is not allowed",
		},
		{
			name: "githubAppApiBase requires https",
			props: map[string]string{
				"url":                     "https://example.com/repo.git",
				"authMode":                "githubApp",
				"githubAppId":             "1",
				"githubAppInstallationId": "2",
				"githubAppPrivateKey":     "PEM",
				"githubAppApiBase":        "http://api.example/",
			},
			wantErr: "githubAppApiBase must use https",
		},
		{
			name: "path with leading slash",
			props: map[string]string{
				"url":  "https://example.com/repo.git",
				"path": "/etc",
			},
			wantErr: "must be repo-relative",
		},
		{
			name: "path escaping repo root",
			props: map[string]string{
				"url":  "https://example.com/repo.git",
				"path": "../outside",
			},
			wantErr: "must not escape",
		},
		{
			name: "path equal to .git rejected",
			props: map[string]string{
				"url":  "https://example.com/repo.git",
				"path": ".git",
			},
			wantErr: "must not reference the .git directory",
		},
		{
			name: "path with .git subdirectory segment rejected",
			props: map[string]string{
				"url":  "https://example.com/repo.git",
				"path": "agents/.git",
			},
			wantErr: "must not reference the .git directory",
		},
		{
			name: "path reaching into .git rejected",
			props: map[string]string{
				"url":  "https://example.com/repo.git",
				"path": ".git/config",
			},
			wantErr: "must not reference the .git directory",
		},
		{
			name: "path with .git in middle rejected",
			props: map[string]string{
				"url":  "https://example.com/repo.git",
				"path": "a/.git/b",
			},
			wantErr: "must not reference the .git directory",
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
