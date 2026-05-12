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
	"errors"
	"time"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/dapr/components-contrib/configuration"
)

// pollLoop ticks every metadata.PollInterval until ctx is cancelled.
//
// When tick reports a rate-limit error (from go-git's HTTP transport or
// from the GitHub App installation-token fetcher), the loop pauses for
// `rateLimitRetryAfter` (or the server-suggested Retry-After when known)
// before its next attempt — protecting the provider's rate-limit budget
// for the rest of the deployment and avoiding hammering the API while it
// is asking us to back off.
func (s *ConfigurationStore) pollLoop(ctx context.Context) {
	defer s.pollerWg.Done()
	ticker := time.NewTicker(s.metadata.pollInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err := s.tick(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			backoff := s.rateLimitBackoff(err)
			if backoff > 0 {
				s.logger.Warnf("git config: rate-limited by upstream; backing off for %s before next tick: %v", backoff, err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				continue
			}
			s.logger.Warnf("git config: poll tick failed (will retry): %v", err)
		}
	}
}

// rateLimitBackoff returns the duration to wait before the next tick when
// err is a rate-limit response. Returns zero if err is not a rate-limit
// signal — the loop then proceeds on its normal cadence.
func (s *ConfigurationStore) rateLimitBackoff(err error) time.Duration {
	var rl *rateLimitError
	if errors.As(err, &rl) {
		if rl.RetryAfter > 0 {
			return rl.RetryAfter
		}
		return s.metadata.rateLimitRetryAfter()
	}
	if isTransportRateLimit(err) {
		return s.metadata.rateLimitRetryAfter()
	}
	return 0
}

// tick fetches the upstream branch and, if HEAD has moved, walks the tree,
// builds a new snapshot, and fans out diffs to subscribers. Uses the fetch
// itself (via gogit.NoErrAlreadyUpToDate) to detect change rather than a
// separate ls-remote probe — the probe downloads the entire ref advertisement
// and counts as a full request against the provider rate limit, so skipping
// it is a meaningful budget saving on large remotes.
func (s *ConfigurationStore) tick(ctx context.Context) error {
	fetchCtx, cancel := context.WithTimeout(ctx, s.metadata.fetchTimeout())
	defer cancel()
	head, changed, err := fetchIfChanged(fetchCtx, s.repo, s.metadata.branch(), s.auth)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	return s.refreshAndFanOut(head)
}

// refreshAndFanOut walks the worktree, builds a snapshot, and dispatches
// per-subscriber diffs. Network and disk I/O happen outside the lock; the
// lock is held only for the brief atomic snapshot/registry update plus the
// O(N) collection of subscriber state. Diff computation and handler calls
// happen outside the lock.
func (s *ConfigurationStore) refreshAndFanOut(head plumbing.Hash) error {
	entries, err := walkTree(s.workdir, s.metadata.path(), s.metadata.includeHidden(), s.metadata.maxFileSize(), s.logger)
	if err != nil {
		return err
	}

	version := shortSHA(head)
	newSnap, err := s.mapper.Map(entries, version, s.logger)
	if err != nil {
		return err
	}

	subs := s.installSnapshot(head, newSnap)
	for _, sub := range subs {
		base := sub.base
		diff := diffSnapshots(base, newSnap, sub.keys, version)
		if len(diff) == 0 {
			continue
		}
		event := &configuration.UpdateEvent{ID: sub.id, Items: diff}
		if err := sub.handler(sub.ctx, event); err != nil {
			s.logger.Errorf("git config: subscriber %s handler error: %v", sub.id, err)
		}
	}
	return nil
}

// subSnapshot is per-subscriber state captured under the lock for use in
// outside-the-lock diff computation and dispatch.
type subSnapshot struct {
	id      string
	keys    []string
	handler configuration.UpdateHandler
	ctx     context.Context
	base    map[string]*configuration.Item
}

// installSnapshot atomically replaces the shared snapshot, advances every
// subscriber's deliveredHEAD, captures each subscriber's prior diff base
// from the LRU, and returns a slice of subscriber snapshots for dispatch
// outside the lock.
func (s *ConfigurationStore) installSnapshot(head plumbing.Hash, newSnap map[string]*configuration.Item) []subSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.snapshot = newSnap
	s.snapshotHead = head
	if s.lru != nil {
		s.lru.Add(head, newSnap)
	}

	subs := make([]subSnapshot, 0, len(s.subscriptions))
	for _, sub := range s.subscriptions {
		subs = append(subs, subSnapshot{
			id:      sub.id,
			keys:    sub.keys,
			handler: sub.handler,
			ctx:     sub.ctx,
			base:    s.lookupSnapshot(sub.deliveredHEAD),
		})
		sub.deliveredHEAD = head
	}
	return subs
}

// lookupSnapshot returns the cached snapshot for hash, or nil if not found.
// Caller must hold s.mu.
func (s *ConfigurationStore) lookupSnapshot(hash plumbing.Hash) map[string]*configuration.Item {
	if s.lru == nil || hash.IsZero() {
		return nil
	}
	if snap, ok := s.lru.Get(hash); ok {
		return snap
	}
	return nil
}

// diffSnapshots returns items changed between prev and next, optionally
// filtered by keys. Deletions surface as Item{Value:"", Metadata:{deleted:true}}.
// version is applied to every emitted item.
func diffSnapshots(prev, next map[string]*configuration.Item, keys []string, version string) map[string]*configuration.Item {
	keyFilter := keysAsSet(keys)
	out := make(map[string]*configuration.Item)

	for k, nv := range next {
		if !keyFilter.match(k) {
			continue
		}
		pv, hadPrev := prev[k]
		if !hadPrev || pv == nil || pv.Value != nv.Value {
			out[k] = &configuration.Item{
				Value:    nv.Value,
				Version:  version,
				Metadata: cloneMetadata(nv.Metadata),
			}
		}
	}
	for k := range prev {
		if !keyFilter.match(k) {
			continue
		}
		if _, ok := next[k]; ok {
			continue
		}
		out[k] = &configuration.Item{
			Value:    "",
			Version:  version,
			Metadata: map[string]string{"deleted": "true"},
		}
	}
	return out
}

// cloneMetadata returns a freshly-allocated copy of the input metadata.
// Every emitted Item gets its own metadata map so handlers can safely
// mutate the map they receive without corrupting other events or the
// store's internal state.
func cloneMetadata(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

type keySet struct {
	all  bool
	want map[string]struct{}
}

func (k keySet) match(s string) bool {
	if k.all {
		return true
	}
	_, ok := k.want[s]
	return ok
}

func keysAsSet(keys []string) keySet {
	if len(keys) == 0 {
		return keySet{all: true}
	}
	want := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		want[k] = struct{}{}
	}
	return keySet{want: want}
}

func shortSHA(h plumbing.Hash) string {
	s := h.String()
	if len(s) > 7 {
		return s[:7]
	}
	return s
}
