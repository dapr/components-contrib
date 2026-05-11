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
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/dapr/components-contrib/configuration"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// upstream is a lightweight fixture: a bare git repo on disk plus a private
// working clone. Tests commit through the working clone and the store under
// test reads from the bare repo over file://.
type upstream struct {
	t         *testing.T
	dir       string
	bareDir   string
	workDir   string
	url       string
	branch    string
	repo      *gogit.Repository
	mu        sync.Mutex
	commitSeq int
	signature *object.Signature
	worktree  *gogit.Worktree
}

func newUpstream(t *testing.T) *upstream {
	t.Helper()
	// t.TempDir registers cleanup before any panic-prone setup runs, so a
	// failed PlainInit/PlainClone never leaks the directory.
	root := t.TempDir()

	bareDir := filepath.Join(root, "upstream.git")
	workDir := filepath.Join(root, "work")

	_, err := gogit.PlainInit(bareDir, true)
	require.NoError(t, err)

	repo, err := gogit.PlainClone(workDir, false, &gogit.CloneOptions{
		URL: bareDir,
	})
	if err != nil {
		// Empty bare repo has no refs to clone — initialise via PlainInit
		// in the workdir, then add origin.
		repo, err = gogit.PlainInit(workDir, false)
		require.NoError(t, err)
		_, err = repo.CreateRemote(&config.RemoteConfig{
			Name: "origin",
			URLs: []string{bareDir},
		})
		require.NoError(t, err)
	}

	wt, err := repo.Worktree()
	require.NoError(t, err)

	u := &upstream{
		t:        t,
		dir:      root,
		bareDir:  bareDir,
		workDir:  workDir,
		url:      "file://" + bareDir,
		branch:   "main",
		repo:     repo,
		worktree: wt,
		signature: &object.Signature{
			Name:  "test",
			Email: "test@example.com",
			When:  time.Unix(1700000000, 0),
		},
	}
	u.commit(map[string]string{}, "initial")
	return u
}

// commit writes the given files (relative paths under the workdir, value is
// content) and pushes a commit with the given message. Files not present in
// `files` are left as-is unless already deleted via removeFiles.
func (u *upstream) commit(files map[string]string, message string) {
	u.mu.Lock()
	defer u.mu.Unlock()

	for rel, content := range files {
		p := filepath.Join(u.workDir, rel)
		require.NoError(u.t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(u.t, os.WriteFile(p, []byte(content), 0o600))
		_, err := u.worktree.Add(rel)
		require.NoError(u.t, err)
	}
	if u.commitSeq == 0 && len(files) == 0 {
		// allow-empty initial commit
		_, err := u.worktree.Commit(message, &gogit.CommitOptions{
			Author:            u.signature,
			Committer:         u.signature,
			AllowEmptyCommits: true,
		})
		require.NoError(u.t, err)
	} else {
		_, err := u.worktree.Commit(message, &gogit.CommitOptions{
			Author:    u.signature,
			Committer: u.signature,
		})
		require.NoError(u.t, err)
	}
	u.commitSeq++

	require.NoError(u.t, u.repo.Push(&gogit.PushOptions{
		RemoteName: "origin",
		RefSpecs:   []config.RefSpec{config.RefSpec("refs/heads/master:refs/heads/main"), config.RefSpec("refs/heads/main:refs/heads/main")},
	}))
}

// removeFiles deletes the given files in the working clone, stages, commits,
// and pushes.
func (u *upstream) removeFiles(rels []string, message string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	for _, rel := range rels {
		p := filepath.Join(u.workDir, rel)
		require.NoError(u.t, os.RemoveAll(p))
		_, err := u.worktree.Remove(rel)
		require.NoError(u.t, err)
	}
	_, err := u.worktree.Commit(message, &gogit.CommitOptions{
		Author:    u.signature,
		Committer: u.signature,
	})
	require.NoError(u.t, err)
	require.NoError(u.t, u.repo.Push(&gogit.PushOptions{
		RemoteName: "origin",
		RefSpecs:   []config.RefSpec{config.RefSpec("refs/heads/master:refs/heads/main"), config.RefSpec("refs/heads/main:refs/heads/main")},
	}))
}

func newTestStore(t *testing.T, u *upstream, props map[string]string) *ConfigurationStore {
	t.Helper()
	if props == nil {
		props = map[string]string{}
	}
	props["url"] = u.url
	if _, ok := props["pollInterval"]; !ok {
		props["pollInterval"] = "1s"
	}
	if props["mappingMode"] == "" {
		props["mappingMode"] = "file"
	}

	s := NewGitConfigurationStore(logger.NewLogger("test")).(*ConfigurationStore)
	require.NoError(t, s.Init(t.Context(), configuration.Metadata{
		Base: contribMetadata.Base{Properties: props},
	}))
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestGitStore_InitAndGet(t *testing.T) {
	u := newUpstream(t)
	u.commit(map[string]string{
		"agent_role.txt": "Weather expert",
		"agent_goal.txt": "Help users",
	}, "seed")

	s := newTestStore(t, u, nil)
	resp, err := s.Get(t.Context(), &configuration.GetRequest{})
	require.NoError(t, err)
	assert.Equal(t, "Weather expert", resp.Items["agent_role.txt"].Value)
	assert.Equal(t, "Help users", resp.Items["agent_goal.txt"].Value)
}

func TestGitStore_SubscribeReplaysInitialState(t *testing.T) {
	u := newUpstream(t)
	u.commit(map[string]string{
		"agent_role.txt": "Weather expert",
	}, "seed")

	s := newTestStore(t, u, nil)

	events := make(chan *configuration.UpdateEvent, 4)
	id, err := s.Subscribe(t.Context(),
		&configuration.SubscribeRequest{},
		makeChanHandler(events),
	)
	require.NoError(t, err)
	require.NotEmpty(t, id)

	e := drainOne(t, events)
	assert.Equal(t, "Weather expert", e.Items["agent_role.txt"].Value)
}

func TestGitStore_HotReloadOnCommit(t *testing.T) {
	u := newUpstream(t)
	u.commit(map[string]string{"agent_role.txt": "Weather expert"}, "seed")
	s := newTestStore(t, u, nil)

	events := make(chan *configuration.UpdateEvent, 16)
	_, err := s.Subscribe(t.Context(),
		&configuration.SubscribeRequest{},
		makeChanHandler(events),
	)
	require.NoError(t, err)
	drainOne(t, events) // initial state

	u.commit(map[string]string{"agent_role.txt": "Travel expert"}, "update role")

	require.Eventually(t, func() bool {
		select {
		case e := <-events:
			if v, ok := e.Items["agent_role.txt"]; ok && v.Value == "Travel expert" {
				return true
			}
			return false
		default:
			return false
		}
	}, 5*time.Second, 50*time.Millisecond, "expected role update event")
}

func TestGitStore_DeletionEmitsDeletedMetadata(t *testing.T) {
	u := newUpstream(t)
	u.commit(map[string]string{
		"a.txt": "alpha",
		"b.txt": "beta",
	}, "seed")
	s := newTestStore(t, u, nil)

	events := make(chan *configuration.UpdateEvent, 16)
	_, err := s.Subscribe(t.Context(),
		&configuration.SubscribeRequest{},
		makeChanHandler(events),
	)
	require.NoError(t, err)
	drainOne(t, events)

	u.removeFiles([]string{"b.txt"}, "delete b")

	require.Eventually(t, func() bool {
		select {
		case e := <-events:
			if v, ok := e.Items["b.txt"]; ok && v.Value == "" && v.Metadata["deleted"] == "true" {
				return true
			}
			return false
		default:
			return false
		}
	}, 5*time.Second, 50*time.Millisecond, "expected deletion event")
}

func TestGitStore_UnsubscribeStopsDelivery(t *testing.T) {
	u := newUpstream(t)
	u.commit(map[string]string{"a.txt": "1"}, "seed")
	s := newTestStore(t, u, nil)

	events := make(chan *configuration.UpdateEvent, 16)
	id, err := s.Subscribe(t.Context(),
		&configuration.SubscribeRequest{},
		makeChanHandler(events),
	)
	require.NoError(t, err)
	drainOne(t, events)

	require.NoError(t, s.Unsubscribe(t.Context(), &configuration.UnsubscribeRequest{ID: id}))

	u.commit(map[string]string{"a.txt": "2"}, "update")

	// Wait 3× the test pollInterval (1s = 3s total) so the poller has at
	// least two opportunities to fire after the commit. Any delivery to a
	// cancelled subscriber is a bug; silence here means unsubscribe took
	// effect. Inherently timing-dependent — if flaky on slow CI, raise the
	// store's pollInterval to slow the cadence in this test.
	select {
	case e := <-events:
		t.Fatalf("unexpected event after unsubscribe: %+v", e)
	case <-time.After(3 * time.Second):
	}
}

func TestGitStore_KeyFilter(t *testing.T) {
	u := newUpstream(t)
	u.commit(map[string]string{
		"role.txt": "Weather",
		"goal.txt": "Help",
	}, "seed")
	s := newTestStore(t, u, nil)

	events := make(chan *configuration.UpdateEvent, 16)
	_, err := s.Subscribe(t.Context(),
		&configuration.SubscribeRequest{Keys: []string{"role.txt"}},
		makeChanHandler(events),
	)
	require.NoError(t, err)

	e := drainOne(t, events)
	assert.Len(t, e.Items, 1)
	assert.Contains(t, e.Items, "role.txt")
	assert.NotContains(t, e.Items, "goal.txt")
}

func TestGitStore_CloseIdempotent(t *testing.T) {
	u := newUpstream(t)
	u.commit(map[string]string{"a.txt": "1"}, "seed")
	s := newTestStore(t, u, nil)
	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
}

func TestGitStore_UnsubscribeUnknownID(t *testing.T) {
	u := newUpstream(t)
	u.commit(map[string]string{"a.txt": "1"}, "seed")
	s := newTestStore(t, u, nil)
	err := s.Unsubscribe(t.Context(), &configuration.UnsubscribeRequest{ID: "does-not-exist"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestGitStore_GetAfterCloseFails(t *testing.T) {
	u := newUpstream(t)
	s := newTestStore(t, u, nil)
	require.NoError(t, s.Close())
	_, err := s.Get(t.Context(), &configuration.GetRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed", "Get should report the store is closed")
}

func TestGitStore_SubscribeAfterCloseFails(t *testing.T) {
	u := newUpstream(t)
	s := newTestStore(t, u, nil)
	require.NoError(t, s.Close())
	_, err := s.Subscribe(t.Context(), &configuration.SubscribeRequest{}, func(context.Context, *configuration.UpdateEvent) error { return nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed", "Subscribe should report the store is closed")
}

func TestGitStore_GetComponentMetadata(t *testing.T) {
	s := NewGitConfigurationStore(logger.NewLogger("test")).(*ConfigurationStore)
	info := s.GetComponentMetadata()
	assert.NotEmpty(t, info)
}

func TestGitStore_NoEmitInitialState(t *testing.T) {
	u := newUpstream(t)
	u.commit(map[string]string{"k.txt": "v"}, "seed")

	s := newTestStore(t, u, map[string]string{"emitInitialState": "false"})

	events := make(chan *configuration.UpdateEvent, 4)
	_, err := s.Subscribe(t.Context(), &configuration.SubscribeRequest{}, makeChanHandler(events))
	require.NoError(t, err)

	// Confirm the poller is alive by triggering a real change. The first event
	// the channel sees must be the hot-reload of "v2"; if an initial-state
	// event leaked through, it would carry "v" and this assertion would fail.
	u.commit(map[string]string{"k.txt": "v2"}, "second commit")
	select {
	case e := <-events:
		v := e.Items["k.txt"]
		require.NotNil(t, v, "expected k.txt in update event")
		assert.Equal(t, "v2", v.Value, "first delivered event must be the hot-reload, not the initial state")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for hot-reload confirmation")
	}
}

func TestGitStore_MultiSubscriberFanOut(t *testing.T) {
	u := newUpstream(t)
	u.commit(map[string]string{"a.txt": "1"}, "seed")
	s := newTestStore(t, u, nil)

	ch1 := make(chan *configuration.UpdateEvent, 8)
	ch2 := make(chan *configuration.UpdateEvent, 8)
	_, err := s.Subscribe(t.Context(), &configuration.SubscribeRequest{}, makeChanHandler(ch1))
	require.NoError(t, err)
	_, err = s.Subscribe(t.Context(), &configuration.SubscribeRequest{}, makeChanHandler(ch2))
	require.NoError(t, err)

	drainOne(t, ch1)
	drainOne(t, ch2)

	u.commit(map[string]string{"a.txt": "2"}, "update")

	require.Eventually(t, func() bool {
		select {
		case e := <-ch1:
			return e.Items["a.txt"] != nil && e.Items["a.txt"].Value == "2"
		default:
			return false
		}
	}, 5*time.Second, 50*time.Millisecond, "subscriber 1 missed the update")

	require.Eventually(t, func() bool {
		select {
		case e := <-ch2:
			return e.Items["a.txt"] != nil && e.Items["a.txt"].Value == "2"
		default:
			return false
		}
	}, 5*time.Second, 50*time.Millisecond, "subscriber 2 missed the update")
}

// TestGitStore_CloseStopsPoller asserts no events arrive after Close(),
// closing the gap left by TestGitStore_CloseIdempotent (which only checks
// Close() returns nil twice).
func TestGitStore_CloseStopsPoller(t *testing.T) {
	u := newUpstream(t)
	u.commit(map[string]string{"a.txt": "1"}, "seed")
	s := newTestStore(t, u, nil)

	events := make(chan *configuration.UpdateEvent, 16)
	_, err := s.Subscribe(t.Context(), &configuration.SubscribeRequest{}, makeChanHandler(events))
	require.NoError(t, err)
	drainOne(t, events) // initial state

	require.NoError(t, s.Close())

	// A commit after Close must not be delivered. Wait long enough for at
	// least 3 poll intervals to have elapsed if the poller were still alive.
	u.commit(map[string]string{"a.txt": "2"}, "post-close commit")

	select {
	case e := <-events:
		t.Fatalf("unexpected delivery after Close: %+v", e)
	case <-time.After(3 * time.Second):
	}
}

// makeChanHandler returns a handler that pushes events to the given channel.
// Errors are NEVER asserted with t.* from inside the handler — that's
// race-prone (Copilot review note in PR #4275). Tests inspect the channel
// from the main goroutine.
func makeChanHandler(ch chan<- *configuration.UpdateEvent) configuration.UpdateHandler {
	return func(_ context.Context, e *configuration.UpdateEvent) error {
		ch <- e
		return nil
	}
}

func drainOne(t *testing.T, ch <-chan *configuration.UpdateEvent) *configuration.UpdateEvent {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
		return nil
	}
}
