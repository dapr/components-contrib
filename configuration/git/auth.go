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
	"fmt"

	"github.com/dapr/components-contrib/configuration/git/auth"
	"github.com/dapr/kit/logger"
)

// selectAuth resolves the active auth strategy from metadata and builds it
// via the auth sub-package. log is guaranteed non-nil by callers (it
// threads through from the Store's constructor which always supplies a
// logger).
func selectAuth(m *metadata, log logger.Logger) (auth.Strategy, error) {
	switch m.resolveAuthMode() {
	case authModeNone:
		return auth.NewNone(), nil
	case authModePAT:
		username := ""
		if m.Username != nil {
			username = *m.Username
		}
		return auth.NewPAT(username, m.Token), nil
	case authModeSSH:
		return auth.NewSSH(auth.SSHConfig{
			User:                  m.user(),
			PrivateKey:            m.PrivateKey,
			PrivateKeyPath:        ptrDeref(m.PrivateKeyPath),
			Passphrase:            m.Passphrase,
			KnownHosts:            ptrDeref(m.KnownHosts),
			KnownHostsPath:        ptrDeref(m.KnownHostsPath),
			InsecureIgnoreHostKey: m.insecureIgnoreHostKey(),
		}, log)
	case authModeGithubApp:
		return auth.NewGitHubApp(auth.GitHubAppConfig{
			AppID:          derefInt64(m.AppID),
			InstallationID: derefInt64(m.InstallationID),
			PrivateKey:     m.PrivateKey,
			PrivateKeyPath: ptrDeref(m.PrivateKeyPath),
			APIBase:        m.apiBase(),
			RefreshSkew:    m.refreshSkew(),
		}, log, auth.DefaultInstallationTokenFetcher)
	}
	return nil, fmt.Errorf("unsupported auth mode %q", m.resolveAuthMode())
}

func ptrDeref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func derefInt64(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}
