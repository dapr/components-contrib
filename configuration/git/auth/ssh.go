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

package auth

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/go-git/go-git/v5/plumbing/transport"
	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/dapr/kit/logger"
)

// SSHConfig collects the parameters required to construct an SSH auth
// strategy. Exactly one of PrivateKey or PrivateKeyPath must be non-empty.
// Exactly one of KnownHosts or KnownHostsPath must be non-empty unless
// InsecureIgnoreHostKey is true.
type SSHConfig struct {
	User                  string // SSH user; defaults to "git" if empty.
	PrivateKey            string // Inline PEM-encoded SSH private key.
	PrivateKeyPath        string // Path to a PEM-encoded SSH private key on disk.
	Passphrase            string // Passphrase for the SSH private key.
	KnownHosts            string // Inline OpenSSH known_hosts entries.
	KnownHostsPath        string // Path to an OpenSSH known_hosts file.
	InsecureIgnoreHostKey bool   // When true, disables host-key verification.
}

type sshStrategy struct {
	user       string
	keys       *gitssh.PublicKeys
	knownHosts ssh.HostKeyCallback
	insecure   bool
}

// NewSSH returns an SSH auth Strategy. When cfg.InsecureIgnoreHostKey is
// true a warning is logged and host-key verification is disabled — never
// safe in production. log must be non-nil.
func NewSSH(cfg SSHConfig, log logger.Logger) (Strategy, error) {
	user := cfg.User
	if user == "" {
		user = "git"
	}
	keyBytes, err := loadKeyMaterial("ssh", cfg.PrivateKey, cfg.PrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("ssh: %w", err)
	}
	pk, err := gitssh.NewPublicKeys(user, keyBytes, cfg.Passphrase)
	if err != nil {
		return nil, fmt.Errorf("ssh: parse private key: %w", err)
	}
	a := &sshStrategy{user: user, keys: pk}

	if cfg.InsecureIgnoreHostKey {
		log.Warnf("git ssh: insecureIgnoreHostKey=true — host key verification is DISABLED. Do not use in production.")
		a.insecure = true
		// Explicitly opting in to disabled host-key verification because the
		// operator set insecureIgnoreHostKey=true. Loud-logged above.
		pk.HostKeyCallback = ssh.InsecureIgnoreHostKey() //nolint:gosec // G106: documented opt-in via metadata
		return a, nil
	}

	cb, err := buildKnownHostsCallback(cfg.KnownHosts, cfg.KnownHostsPath)
	if err != nil {
		return nil, fmt.Errorf("ssh: known_hosts: %w", err)
	}
	pk.HostKeyCallback = cb
	a.knownHosts = cb
	return a, nil
}

func (a *sshStrategy) AuthMethod(context.Context) (transport.AuthMethod, error) {
	return a.keys, nil
}

func (a *sshStrategy) Close() error { return nil }

func buildKnownHostsCallback(inline, path string) (ssh.HostKeyCallback, error) {
	if path != "" {
		return knownhosts.New(path)
	}
	if inline != "" {
		// `knownhosts.New` requires a file path. Persist the inline data to a
		// temp file just long enough to hand it off; the file is removed
		// before we return regardless of outcome.
		f, err := os.CreateTemp("", "dapr-knownhosts-*")
		if err != nil {
			return nil, fmt.Errorf("create knownHosts temp file: %w", err)
		}
		tmp := f.Name()
		defer func() {
			_ = f.Close()
			_ = os.Remove(tmp)
		}()
		if _, err := f.WriteString(inline); err != nil {
			return nil, fmt.Errorf("write knownHosts: %w", err)
		}
		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("close knownHosts: %w", err)
		}
		return knownhosts.New(tmp)
	}
	return nil, errors.New("no knownHosts/knownHostsPath configured and insecureIgnoreHostKey is false")
}
