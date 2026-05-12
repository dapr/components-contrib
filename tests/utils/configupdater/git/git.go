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
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/dapr/components-contrib/configuration"
	"github.com/dapr/components-contrib/tests/utils/configupdater"
	"github.com/dapr/kit/logger"
)

// ConfigUpdater backs the conformance test for the git configuration store.
// Each Add/Update/Delete operation writes files to a private working clone,
// commits, and pushes back to the upstream repo so that the configuration
// store under test sees the change on its next poll tick.
//
// The updater pairs with `mappingMode: file` so emitted store keys are equal
// to the filenames written here. After every push, the updater waits long
// enough for the store's polling loop to observe and absorb the new HEAD —
// this lets the conformance harness call Get/verifySubscribers immediately
// after a write without racing the poller.
type ConfigUpdater struct {
	logger logger.Logger

	upstreamURL string
	branch      string
	workdir     string
	repo        *gogit.Repository
	wt          *gogit.Worktree
	settleWait  time.Duration

	mu  sync.Mutex
	seq atomic.Int64
}

// NewGitConfigUpdater constructs a fresh updater. props["remoteUrl"] is the
// upstream URL (typically file://). props["branch"] defaults to "main".
func NewGitConfigUpdater(log logger.Logger) configupdater.Updater {
	return &ConfigUpdater{logger: log}
}

func (u *ConfigUpdater) Init(props map[string]string) error {
	u.upstreamURL = props["remoteUrl"]
	if u.upstreamURL == "" {
		return errors.New("remoteUrl is required")
	}
	u.branch = props["branch"]
	if u.branch == "" {
		u.branch = "main"
	}

	pollInterval, _ := time.ParseDuration(props["pollInterval"])
	if pollInterval <= 0 {
		pollInterval = 250 * time.Millisecond
	}
	// settleWait gives the store-under-test enough time to observe the
	// pushed commit on its next poll tick. The 500ms margin absorbs
	// goroutine scheduling jitter and ticker alignment. If conformance
	// tests start failing intermittently on slow CI, this is the knob.
	u.settleWait = pollInterval + 500*time.Millisecond

	dir, err := os.MkdirTemp("", "dapr-config-git-updater-")
	if err != nil {
		return fmt.Errorf("create workdir: %w", err)
	}
	u.workdir = dir

	repo, cloneErr := gogit.PlainClone(dir, false, &gogit.CloneOptions{URL: u.upstreamURL})
	if cloneErr != nil {
		// Empty repo or no refs — initialise locally and add origin.
		var initErr error
		repo, initErr = gogit.PlainInit(dir, false)
		if initErr != nil {
			return fmt.Errorf("init local clone: %w", initErr)
		}
		if _, remoteErr := repo.CreateRemote(&config.RemoteConfig{
			Name: "origin",
			URLs: []string{u.upstreamURL},
		}); remoteErr != nil {
			return fmt.Errorf("add origin: %w", remoteErr)
		}
	}
	u.repo = repo

	wt, err := repo.Worktree()
	if err != nil {
		return fmt.Errorf("worktree: %w", err)
	}
	u.wt = wt
	return nil
}

func (u *ConfigUpdater) AddKey(items map[string]*configuration.Item) error {
	return u.writeAndCommit(items, "add", false)
}

func (u *ConfigUpdater) UpdateKey(items map[string]*configuration.Item) error {
	return u.writeAndCommit(items, "update", false)
}

func (u *ConfigUpdater) DeleteKey(keys []string) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	for _, k := range keys {
		p := filepath.Join(u.workdir, k)
		if err := os.RemoveAll(p); err != nil {
			return fmt.Errorf("remove %q: %w", k, err)
		}
		if _, err := u.wt.Remove(k); err != nil {
			return fmt.Errorf("git rm %q: %w", k, err)
		}
	}
	return u.commitAndPush(fmt.Sprintf("delete %d", len(keys)))
}

func (u *ConfigUpdater) writeAndCommit(items map[string]*configuration.Item, op string, _ bool) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	for k, v := range items {
		p := filepath.Join(u.workdir, k)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			return fmt.Errorf("mkdir for %q: %w", k, err)
		}
		if err := os.WriteFile(p, []byte(v.Value), 0o600); err != nil {
			return fmt.Errorf("write %q: %w", k, err)
		}
		if _, err := u.wt.Add(k); err != nil {
			return fmt.Errorf("git add %q: %w", k, err)
		}
	}
	return u.commitAndPush(fmt.Sprintf("%s %d", op, len(items)))
}

func (u *ConfigUpdater) commitAndPush(message string) error {
	seq := u.seq.Add(1)
	sig := &object.Signature{
		Name:  "conformance",
		Email: "conformance@dapr.io",
		When:  time.Now(),
	}
	if _, err := u.wt.Commit(message+" (#"+strconv.FormatInt(seq, 10)+")", &gogit.CommitOptions{
		Author:    sig,
		Committer: sig,
	}); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	// Push both `master` and `main` so we don't fight the locally-default
	// branch. The destination is always `main` on the upstream.
	if err := u.repo.Push(&gogit.PushOptions{
		RemoteName: "origin",
		RefSpecs: []config.RefSpec{
			config.RefSpec("refs/heads/master:refs/heads/" + u.branch),
			config.RefSpec("refs/heads/" + u.branch + ":refs/heads/" + u.branch),
		},
	}); err != nil && !errors.Is(err, gogit.NoErrAlreadyUpToDate) {
		return fmt.Errorf("push: %w", err)
	}
	if u.settleWait > 0 {
		time.Sleep(u.settleWait)
	}
	return nil
}
