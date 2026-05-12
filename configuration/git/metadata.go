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

	// authMode* constants are internal sentinels used by the auto-detector
	// and downstream auth strategy selection. They are not user-facing —
	// operators do not select a mode explicitly; it is derived from which
	// metadata fields are populated (see resolveAuthMode).
	authModeNone      = "none"
	authModePAT       = "pat"
	authModeSSH       = "ssh"
	authModeGithubApp = "githubapp"

	defaultBranch              = "main"
	defaultPath                = "."
	defaultDepth               = 0
	defaultPollInterval        = 5 * time.Minute
	defaultFetchTimeout        = 30 * time.Second
	defaultMaxFileSizeBytes    = int64(1 * 1024 * 1024) // 1 MiB per file
	defaultSnapshotCacheSize   = 4
	defaultMappingMode         = mappingModeFile
	defaultUser                = "git"
	defaultAPIBase             = "https://api.github.com"
	defaultRefreshSkew         = 5 * time.Minute
	defaultRateLimitRetryAfter = 5 * time.Minute
	// minPollInterval is the absolute floor for remote (https/ssh) URLs to
	// guard the operator's git provider rate-limit budget. file:// URLs
	// bypass this floor since they have no rate-limit concern.
	minPollInterval      = time.Second
	minLocalPollInterval = 100 * time.Millisecond
)

// metadata is the decoded form of the Dapr component spec for
// configuration.git. Field names use the mapstructure tag to match the
// user-facing YAML keys. There is no `authMode` selector — the active auth
// profile is inferred from which fields are set (see resolveAuthMode).
type metadata struct {
	RemoteURL string `mapstructure:"remoteUrl"` // required

	Branch              *string        `mapstructure:"branch"`
	Path                *string        `mapstructure:"path"`
	Depth               *int           `mapstructure:"depth"`
	PollInterval        *time.Duration `mapstructure:"pollInterval"`
	FetchTimeout        *time.Duration `mapstructure:"fetchTimeout"`
	IncludeHidden       *bool          `mapstructure:"includeHidden"`
	EmitInitialState    *bool          `mapstructure:"emitInitialState"`
	MaxFileSize         *int64         `mapstructure:"maxFileSize"`
	SnapshotCacheSize   *int           `mapstructure:"snapshotCacheSize"`
	RateLimitRetryAfter *time.Duration `mapstructure:"rateLimitRetryAfter"`

	MappingMode *string `mapstructure:"mappingMode"`

	// Personal Access Token profile
	Username *string `mapstructure:"username"`
	Token    string  `mapstructure:"token"`

	// SSH profile. PrivateKey/PrivateKeyPath are also reused by the GitHub
	// App profile — auto-detection picks one based on AppID presence.
	User                  *string `mapstructure:"user"`
	Passphrase            string  `mapstructure:"passphrase"`
	KnownHosts            *string `mapstructure:"knownHosts"`
	KnownHostsPath        *string `mapstructure:"knownHostsPath"`
	InsecureIgnoreHostKey *bool   `mapstructure:"insecureIgnoreHostKey"`

	// GitHub App profile
	AppID          *int64         `mapstructure:"appId"`
	InstallationID *int64         `mapstructure:"installationId"`
	APIBase        *string        `mapstructure:"apiBase"`
	RefreshSkew    *time.Duration `mapstructure:"refreshSkew"`

	// Shared between SSH and GitHub App profiles.
	PrivateKey     string  `mapstructure:"privateKey"`
	PrivateKeyPath *string `mapstructure:"privateKeyPath"`
}

func (m *metadata) parse(meta configuration.Metadata) error {
	if err := kitmd.DecodeMetadata(meta.Properties, m); err != nil {
		return fmt.Errorf("failed to decode metadata: %w", err)
	}

	if strings.TrimSpace(m.RemoteURL) == "" {
		return errors.New("remoteUrl is required")
	}

	mode := normalizeMode(m.MappingMode)
	switch mode {
	case "", mappingModeFile, mappingModeAgentYAML, mappingModePrompty:
	default:
		return fmt.Errorf("unsupported mappingMode %q (supported: file, agentYaml, prompty)", *m.MappingMode)
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
		if strings.HasPrefix(m.RemoteURL, "file://") {
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
		// Reject any path segment equal to ".git" so the walker never sees
		// a scope that descends into the git metadata directory. The walker
		// also refuses to enter `.git` as a backstop (see walk.go), but
		// catching it at validation gives a clear error message instead of
		// a silently empty snapshot.
		for _, seg := range strings.Split(clean, "/") {
			if seg == ".git" {
				return fmt.Errorf("path %q must not reference the .git directory", *m.Path)
			}
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

func (m *metadata) rateLimitRetryAfter() time.Duration {
	if m.RateLimitRetryAfter != nil && *m.RateLimitRetryAfter > 0 {
		return *m.RateLimitRetryAfter
	}
	return defaultRateLimitRetryAfter
}

func (m *metadata) mappingMode() string {
	if mode := normalizeMode(m.MappingMode); mode != "" {
		return mode
	}
	return defaultMappingMode
}

func (m *metadata) user() string {
	if m.User != nil && *m.User != "" {
		return *m.User
	}
	return defaultUser
}

func (m *metadata) insecureIgnoreHostKey() bool {
	if m.InsecureIgnoreHostKey != nil {
		return *m.InsecureIgnoreHostKey
	}
	return false
}

func (m *metadata) apiBase() string {
	if m.APIBase != nil && *m.APIBase != "" {
		return *m.APIBase
	}
	return defaultAPIBase
}

func (m *metadata) refreshSkew() time.Duration {
	if m.RefreshSkew != nil {
		return *m.RefreshSkew
	}
	return defaultRefreshSkew
}

// resolveAuthMode derives which auth profile is active from the metadata
// the operator provided. There is no explicit selector — presence of
// fields determines the profile. Priority:
//
//  1. appId set → GitHub App
//  2. URL is SSH-scheme (git@ or ssh://) → SSH
//  3. token set → PAT
//  4. otherwise → no auth (public HTTPS or file://)
func (m *metadata) resolveAuthMode() string {
	if m.AppID != nil && *m.AppID != 0 {
		return authModeGithubApp
	}
	if strings.HasPrefix(m.RemoteURL, "git@") || strings.HasPrefix(m.RemoteURL, "ssh://") {
		return authModeSSH
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
			return errors.New("PAT auth requires token")
		}
		return nil
	case authModeSSH:
		if m.PrivateKey == "" && (m.PrivateKeyPath == nil || *m.PrivateKeyPath == "") {
			return errors.New("SSH auth requires privateKey or privateKeyPath")
		}
		return nil
	case authModeGithubApp:
		if m.AppID == nil || *m.AppID == 0 {
			return errors.New("GitHub App auth requires appId")
		}
		if m.InstallationID == nil || *m.InstallationID == 0 {
			return errors.New("GitHub App auth requires installationId")
		}
		if m.PrivateKey == "" && (m.PrivateKeyPath == nil || *m.PrivateKeyPath == "") {
			return errors.New("GitHub App auth requires privateKey or privateKeyPath")
		}
		if m.APIBase != nil && *m.APIBase != "" {
			u, err := url.Parse(*m.APIBase)
			if err != nil {
				return fmt.Errorf("apiBase: %w", err)
			}
			if u.Scheme != "https" {
				return errors.New("apiBase must use https://")
			}
		}
		return nil
	}
	return fmt.Errorf("unsupported auth mode %q", mode)
}

// validateURL rejects insecure schemes (http://) for authenticated profiles
// and rejects URLs that embed credentials inline so operators are forced to
// store them in a secret store. SCP-style SSH URLs (git@host:path) and local
// file:// URLs are recognised explicitly: the former doesn't parse cleanly
// via url.Parse, and the latter may legitimately contain Windows drive
// letters (e.g. file:///C:/Users/...) that net/url interprets as host:port.
func (m *metadata) validateURL(resolvedAuth string) error {
	if strings.HasPrefix(m.RemoteURL, "git@") {
		return nil
	}
	if strings.HasPrefix(m.RemoteURL, "file://") {
		return nil
	}
	parsed, err := url.Parse(m.RemoteURL)
	if err != nil {
		return fmt.Errorf("remoteUrl: %w", err)
	}
	if parsed.User != nil && parsed.User.Username() != "" {
		if _, hasPass := parsed.User.Password(); hasPass {
			return errors.New("remoteUrl: credentials must not be embedded in the URL; supply them via the appropriate auth profile field (typically backed by a configured secret store)")
		}
	}
	if resolvedAuth != authModeNone && parsed.Scheme == "http" {
		return errors.New("remoteUrl: http:// is not allowed with an authenticated profile; use https://, ssh://, or file://")
	}
	return nil
}

func normalizeMode(s *string) string {
	if s == nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(*s))
}
