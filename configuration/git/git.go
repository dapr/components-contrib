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

// Package git implements a Dapr configuration store backed by a git
// repository. Configuration values live as files in a remote repository;
// the store clones the repo on Init, polls for new commits, and notifies
// subscribers when keys change. Three mapping modes are supported:
//
//   - file (default): each file maps to one config item.
//   - agentYaml: each YAML/JSON file is parsed as a flat map.
//   - prompty: each .prompty file's frontmatter and body become keys.
//
// See metadata.yaml for the full configuration surface.
package git

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/dapr/components-contrib/configuration"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// pollIntervalWarnThreshold is the soft minimum below which we log a warning
// at startup. The hard floor is enforced in metadata.parse.
const pollIntervalWarnThreshold = 5 * time.Second

var _ configuration.Store = (*ConfigurationStore)(nil)

// ConfigurationStore is a git-backed Dapr configuration store. A single
// polling goroutine, started in Init, fans out to per-subscriber handlers.
type ConfigurationStore struct {
	logger logger.Logger

	metadata metadata
	auth     authStrategy
	mapper   mappingStrategy

	repo    *git.Repository
	workdir string

	mu            sync.Mutex
	snapshot      map[string]*configuration.Item
	snapshotHead  plumbing.Hash
	subscriptions map[string]*subscription
	lru           *lru.Cache[plumbing.Hash, map[string]*configuration.Item]
	closed        atomic.Bool

	pollerCancel context.CancelFunc
	pollerWg     sync.WaitGroup
}

// NewGitConfigurationStore returns a new git-backed configuration store.
func NewGitConfigurationStore(log logger.Logger) configuration.Store {
	return &ConfigurationStore{
		logger:        log,
		subscriptions: make(map[string]*subscription),
		snapshot:      map[string]*configuration.Item{},
	}
}

// Init clones the upstream repository, builds an initial snapshot, and starts
// the polling goroutine. The poller is owned by the store and runs until
// Close is called; the Init ctx is used only for the initial clone and
// snapshot operations and does not bind the poller's lifetime.
//
// On any error after the workdir is created, this function cleans up the
// workdir and any partially-initialised auth strategy, so the Dapr runtime
// (which does not call Close on a failed Init) is not left with a leaked
// temp directory.
func (s *ConfigurationStore) Init(ctx context.Context, meta configuration.Metadata) error {
	if err := s.metadata.parse(meta); err != nil {
		return err
	}

	auth, err := selectAuth(&s.metadata, s.logger)
	if err != nil {
		return err
	}
	s.auth = auth

	mapper, err := selectMapper(s.metadata.mappingMode())
	if err != nil {
		return err
	}
	s.mapper = mapper

	dir, err := os.MkdirTemp("", "dapr-config-git-")
	if err != nil {
		return fmt.Errorf("create workdir: %w", err)
	}
	s.workdir = dir

	cleanupOnErr := true
	defer func() {
		if !cleanupOnErr {
			return
		}
		_ = os.RemoveAll(dir)
		s.workdir = ""
		if s.auth != nil {
			_ = s.auth.Close()
		}
	}()

	cloneCtx, cancel := context.WithTimeout(ctx, s.metadata.fetchTimeout())
	repo, err := cloneFresh(cloneCtx, dir, s.metadata.URL, s.metadata.branch(), s.metadata.depth(), s.auth)
	cancel()
	if err != nil {
		return err
	}
	s.repo = repo

	cache, err := lru.New[plumbing.Hash, map[string]*configuration.Item](s.metadata.snapshotCacheSize())
	if err != nil {
		return fmt.Errorf("init lru: %w", err)
	}
	s.lru = cache

	if err := s.bootstrapSnapshot(); err != nil {
		return fmt.Errorf("initial snapshot: %w", err)
	}

	if s.metadata.pollInterval() < pollIntervalWarnThreshold {
		s.logger.Warnf("git config: pollInterval %s is below recommended minimum %s — polling may overwhelm the git remote",
			s.metadata.pollInterval(), pollIntervalWarnThreshold)
	}

	pollerCtx, pollerCancel := context.WithCancel(context.Background())
	s.pollerCancel = pollerCancel
	s.pollerWg.Add(1)
	go s.pollLoop(pollerCtx)

	cleanupOnErr = false
	return nil
}

// bootstrapSnapshot builds the initial snapshot from the already-cloned
// worktree. No network I/O is performed here.
func (s *ConfigurationStore) bootstrapSnapshot() error {
	head, err := s.repo.Head()
	if err != nil {
		return fmt.Errorf("resolve HEAD: %w", err)
	}
	hash := head.Hash()
	entries, err := walkTree(s.workdir, s.metadata.path(), s.metadata.includeHidden(), s.metadata.maxFileSize(), s.logger)
	if err != nil {
		return err
	}
	snap, err := s.mapper.Map(entries, shortSHA(hash), s.logger)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.snapshot = snap
	s.snapshotHead = hash
	s.lru.Add(hash, snap)
	s.mu.Unlock()
	return nil
}

// Get returns the most-recently polled snapshot, filtered by req.Keys.
// It does not contact the upstream repository; the returned state may be up
// to pollInterval old. Use Subscribe to receive change notifications.
//
// The returned items reference the live snapshot and must be treated as
// immutable; mutating their fields will corrupt the next diff comparison.
func (s *ConfigurationStore) Get(_ context.Context, req *configuration.GetRequest) (*configuration.GetResponse, error) {
	if s.closed.Load() {
		return nil, errors.New("configuration store is closed")
	}
	s.mu.Lock()
	items := filterByKeys(s.snapshot, req.Keys)
	s.mu.Unlock()
	return &configuration.GetResponse{Items: items}, nil
}

// Subscribe registers a handler for change notifications. When
// emitInitialState is true (the default), the current snapshot is delivered
// synchronously to the handler before this method returns, so callers do
// not need a separate Get + Subscribe pair. If the initial-state delivery
// fails, the subscription is rolled back and the error is returned.
//
// The handler receives a child context derived from ctx; cancelling the
// subscription via Unsubscribe also cancels that context.
func (s *ConfigurationStore) Subscribe(ctx context.Context, req *configuration.SubscribeRequest, handler configuration.UpdateHandler) (string, error) {
	if s.closed.Load() {
		return "", errors.New("configuration store is closed")
	}

	id := uuid.NewString()
	childCtx, cancel := context.WithCancel(ctx)

	s.mu.Lock()
	sub := &subscription{
		id:            id,
		keys:          append([]string(nil), req.Keys...),
		handler:       handler,
		deliveredHEAD: s.snapshotHead,
		ctx:           childCtx,
		cancel:        cancel,
	}
	s.subscriptions[id] = sub
	var initial map[string]*configuration.Item
	if s.metadata.emitInitialState() {
		initial = filterByKeys(s.snapshot, req.Keys)
	}
	s.mu.Unlock()

	if len(initial) > 0 {
		if err := handler(childCtx, &configuration.UpdateEvent{ID: id, Items: initial}); err != nil {
			s.mu.Lock()
			delete(s.subscriptions, id)
			s.mu.Unlock()
			cancel()
			return "", fmt.Errorf("initial state delivery failed: %w", err)
		}
	}
	return id, nil
}

// Unsubscribe removes a subscription. Errors if the id is unknown.
func (s *ConfigurationStore) Unsubscribe(_ context.Context, req *configuration.UnsubscribeRequest) error {
	s.mu.Lock()
	sub, ok := s.subscriptions[req.ID]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("subscription with id %s does not exist", req.ID)
	}
	delete(s.subscriptions, req.ID)
	s.mu.Unlock()
	sub.cancel()
	return nil
}

// Close stops the poller, cancels all subscriptions, and removes the workdir.
// Idempotent — subsequent calls return nil without performing any work.
func (s *ConfigurationStore) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}

	s.mu.Lock()
	for id, sub := range s.subscriptions {
		sub.cancel()
		delete(s.subscriptions, id)
	}
	s.mu.Unlock()

	if s.pollerCancel != nil {
		s.pollerCancel()
		s.pollerWg.Wait()
	}
	if s.auth != nil {
		_ = s.auth.Close()
	}
	if s.workdir != "" {
		_ = os.RemoveAll(s.workdir)
	}
	return nil
}

// GetComponentMetadata returns the schema for daprd auto-discovery.
func (s *ConfigurationStore) GetComponentMetadata() (info contribMetadata.MetadataMap) {
	stub := metadata{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(stub), &info, contribMetadata.ConfigurationStoreType)
	return info
}
