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
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/dapr/components-contrib/configuration"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	mappingModeFile      = "file"
	mappingModeAgentYAML = "agentyaml"
	mappingModePrompty   = "prompty"

	authModeNone      = "none"
	authModePAT       = "pat"
	authModeSSH       = "ssh"
	authModeGithubApp = "githubapp"

	defaultBranch               = "main"
	defaultPath                 = "."
	defaultDepth                = 0
	defaultPollInterval         = 30 * time.Second
	defaultFetchTimeout         = 30 * time.Second
	defaultMaxFileSizeBytes     = int64(1 * 1024 * 1024) // 1 MiB per file
	defaultSnapshotCacheSize    = 4
	defaultMappingMode          = mappingModeFile
	defaultSSHUser              = "git"
	defaultGithubAppAPIBase     = "https://api.github.com"
	defaultGithubAppRefreshSkew = 5 * time.Minute
	// minPollInterval is the absolute floor for remote (https/ssh) URLs to
	// guard the operator's git provider rate-limit budget. file:// URLs
	// bypass this floor since they have no rate-limit concern.
	minPollInterval      = time.Second
	minLocalPollInterval = 100 * time.Millisecond
)

type metadata struct {
	URL string `mapstructure:"url"` // required

	Branch            *string        `mapstructure:"branch"`
	Path              *string        `mapstructure:"path"`
	Depth             *int           `mapstructure:"depth"`
	PollInterval      *time.Duration `mapstructure:"pollInterval"`
	FetchTimeout      *time.Duration `mapstructure:"fetchTimeout"`
	IncludeHidden     *bool          `mapstructure:"includeHidden"`
	EmitInitialState  *bool          `mapstructure:"emitInitialState"`
	MaxFileSize       *int64         `mapstructure:"maxFileSize"`
	SnapshotCacheSize *int           `mapstructure:"snapshotCacheSize"`

	MappingMode *string `mapstructure:"mappingMode"`
	AuthMode    *string `mapstructure:"authMode"`

	// PAT
	Username *string `mapstructure:"username"`
	Token    string  `mapstructure:"token"`

	// SSH
	SSHUser                  *string `mapstructure:"sshUser"`
	SSHPrivateKey            string  `mapstructure:"sshPrivateKey"`
	SSHPrivateKeyPath        *string `mapstructure:"sshPrivateKeyPath"`
	SSHPassphrase            string  `mapstructure:"sshPassphrase"`
	SSHKnownHosts            *string `mapstructure:"sshKnownHosts"`
	SSHKnownHostsPath        *string `mapstructure:"sshKnownHostsPath"`
	SSHInsecureIgnoreHostKey *bool   `mapstructure:"sshInsecureIgnoreHostKey"`

	// GitHub App
	GithubAppID             *int64         `mapstructure:"githubAppId"`
	GithubAppInstallationID *int64         `mapstructure:"githubAppInstallationId"`
	GithubAppPrivateKey     string         `mapstructure:"githubAppPrivateKey"`
	GithubAppPrivateKeyPath *string        `mapstructure:"githubAppPrivateKeyPath"`
	GithubAppAPIBase        *string        `mapstructure:"githubAppApiBase"`
	GithubAppRefreshSkew    *time.Duration `mapstructure:"githubAppRefreshSkew"`
}

func (m *metadata) parse(meta configuration.Metadata) error {
	if err := kitmd.DecodeMetadata(meta.Properties, m); err != nil {
		return fmt.Errorf("failed to decode metadata: %w", err)
	}

	if strings.TrimSpace(m.URL) == "" {
		return errors.New("url is required")
	}

	mode := normalizeMode(m.MappingMode)
	switch mode {
	case "", mappingModeFile, mappingModeAgentYAML, mappingModePrompty:
	default:
		return fmt.Errorf("unsupported mappingMode %q (supported: file, agentYaml, prompty)", *m.MappingMode)
	}

	auth := normalizeMode(m.AuthMode)
	switch auth {
	case "", authModeNone, authModePAT, authModeSSH, authModeGithubApp:
	default:
		return fmt.Errorf("unsupported authMode %q (supported: none, pat, ssh, githubApp)", *m.AuthMode)
	}

	resolved := m.resolveAuthMode()
	if err := m.validateAuthFields(resolved); err != nil {
		return err
	}
	if err := m.validateURL(resolved); err != nil {
		return err
	}

	if m.PollInterval != nil {
		floor := minPollInterval
		if strings.HasPrefix(m.URL, "file://") {
			floor = minLocalPollInterval
		}
		if *m.PollInterval < floor {
			return fmt.Errorf("pollInterval %s is below minimum of %s", *m.PollInterval, floor)
		}
	}

	if m.Path != nil {
		clean := filepath.ToSlash(filepath.Clean(*m.Path))
		if strings.HasPrefix(clean, "/") {
			return fmt.Errorf("path %q must be repo-relative (no leading slash)", *m.Path)
		}
		if clean == ".." || strings.HasPrefix(clean, "../") {
			return fmt.Errorf("path %q must not escape the repository root", *m.Path)
		}
	}

	return nil
}

func (m *metadata) branch() string {
	if m.Branch != nil && *m.Branch != "" {
		return *m.Branch
	}
	return defaultBranch
}

func (m *metadata) path() string {
	if m.Path != nil && *m.Path != "" {
		return *m.Path
	}
	return defaultPath
}

func (m *metadata) depth() int {
	if m.Depth != nil {
		return *m.Depth
	}
	return defaultDepth
}

func (m *metadata) pollInterval() time.Duration {
	if m.PollInterval != nil {
		return *m.PollInterval
	}
	return defaultPollInterval
}

func (m *metadata) fetchTimeout() time.Duration {
	if m.FetchTimeout != nil {
		return *m.FetchTimeout
	}
	return defaultFetchTimeout
}

func (m *metadata) includeHidden() bool {
	if m.IncludeHidden != nil {
		return *m.IncludeHidden
	}
	return false
}

func (m *metadata) emitInitialState() bool {
	if m.EmitInitialState != nil {
		return *m.EmitInitialState
	}
	return true
}

func (m *metadata) maxFileSize() int64 {
	if m.MaxFileSize != nil && *m.MaxFileSize > 0 {
		return *m.MaxFileSize
	}
	return defaultMaxFileSizeBytes
}

func (m *metadata) snapshotCacheSize() int {
	if m.SnapshotCacheSize != nil && *m.SnapshotCacheSize > 0 {
		return *m.SnapshotCacheSize
	}
	return defaultSnapshotCacheSize
}

func (m *metadata) mappingMode() string {
	if mode := normalizeMode(m.MappingMode); mode != "" {
		return mode
	}
	return defaultMappingMode
}

func (m *metadata) sshUser() string {
	if m.SSHUser != nil && *m.SSHUser != "" {
		return *m.SSHUser
	}
	return defaultSSHUser
}

func (m *metadata) sshInsecureIgnoreHostKey() bool {
	if m.SSHInsecureIgnoreHostKey != nil {
		return *m.SSHInsecureIgnoreHostKey
	}
	return false
}

func (m *metadata) githubAppAPIBase() string {
	if m.GithubAppAPIBase != nil && *m.GithubAppAPIBase != "" {
		return *m.GithubAppAPIBase
	}
	return defaultGithubAppAPIBase
}

func (m *metadata) githubAppRefreshSkew() time.Duration {
	if m.GithubAppRefreshSkew != nil {
		return *m.GithubAppRefreshSkew
	}
	return defaultGithubAppRefreshSkew
}

// resolveAuthMode returns the auth mode to use, applying auto-detection when
// authMode is empty.
func (m *metadata) resolveAuthMode() string {
	if mode := normalizeMode(m.AuthMode); mode != "" {
		return mode
	}
	if strings.HasPrefix(m.URL, "git@") || strings.HasPrefix(m.URL, "ssh://") {
		return authModeSSH
	}
	if m.GithubAppID != nil && *m.GithubAppID != 0 {
		return authModeGithubApp
	}
	if m.Token != "" {
		return authModePAT
	}
	return authModeNone
}

func (m *metadata) validateAuthFields(mode string) error {
	switch mode {
	case authModeNone:
		return nil
	case authModePAT:
		if m.Token == "" {
			return errors.New("authMode=pat requires token")
		}
		return nil
	case authModeSSH:
		if m.SSHPrivateKey == "" && (m.SSHPrivateKeyPath == nil || *m.SSHPrivateKeyPath == "") {
			return errors.New("authMode=ssh requires sshPrivateKey or sshPrivateKeyPath")
		}
		return nil
	case authModeGithubApp:
		if m.GithubAppID == nil || *m.GithubAppID == 0 {
			return errors.New("authMode=githubApp requires githubAppId")
		}
		if m.GithubAppInstallationID == nil || *m.GithubAppInstallationID == 0 {
			return errors.New("authMode=githubApp requires githubAppInstallationId")
		}
		if m.GithubAppPrivateKey == "" && (m.GithubAppPrivateKeyPath == nil || *m.GithubAppPrivateKeyPath == "") {
			return errors.New("authMode=githubApp requires githubAppPrivateKey or githubAppPrivateKeyPath")
		}
		if m.GithubAppAPIBase != nil && *m.GithubAppAPIBase != "" {
			u, err := url.Parse(*m.GithubAppAPIBase)
			if err != nil {
				return fmt.Errorf("githubAppApiBase: %w", err)
			}
			if u.Scheme != "https" {
				return errors.New("githubAppApiBase must use https://")
			}
		}
		return nil
	}
	return fmt.Errorf("unsupported authMode %q", mode)
}

// validateURL rejects insecure schemes (http://) for authenticated modes and
// rejects URLs that embed credentials inline so operators are forced to use
// the secret-reference mechanism. SCP-style SSH URLs (git@host:path) are
// recognised explicitly because they don't parse cleanly via url.Parse.
func (m *metadata) validateURL(resolvedAuth string) error {
	if strings.HasPrefix(m.URL, "git@") {
		// SCP-style: no scheme to validate, no userinfo to extract beyond the
		// "git@" sentinel.
		return nil
	}
	parsed, err := url.Parse(m.URL)
	if err != nil {
		return fmt.Errorf("url: %w", err)
	}
	if parsed.User != nil && parsed.User.Username() != "" {
		if _, hasPass := parsed.User.Password(); hasPass {
			return errors.New("url: credentials must not be embedded in the URL; use authMode with the appropriate metadata field and a Dapr secret reference")
		}
	}
	if resolvedAuth != authModeNone && parsed.Scheme == "http" {
		return errors.New("url: http:// is not allowed with authenticated auth modes; use https://, ssh://, or file://")
	}
	return nil
}

func normalizeMode(s *string) string {
	if s == nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(*s))
}
