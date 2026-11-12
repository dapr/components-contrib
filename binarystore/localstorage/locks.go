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

package localstorage

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"golang.org/x/sync/semaphore"
	"golang.org/x/text/unicode/norm"
)

var mutationLocks pathLockRegistry

type pathLockRegistry struct {
	mu      sync.Mutex
	entries []*pathLock
}

type pathLock struct {
	parent fs.FileInfo
	name   string
	refs   int
	gate   *semaphore.Weighted
}

func (s *LocalStorage) lockPath(ctx context.Context, path string) (func(), error) {
	parent, err := s.root.Stat(filepath.Dir(path))
	if err != nil {
		return nil, fmt.Errorf("failed to stat parent of %s: %w", path, err)
	}
	return mutationLocks.acquire(ctx, parent, filepath.Base(path))
}

func (r *pathLockRegistry) acquire(ctx context.Context, parent fs.FileInfo, name string) (func(), error) {
	// Directory identity handles overlapping roots and symlink aliases.
	// Conservatively fold names even on case-sensitive filesystems so casing
	// and Unicode normalization cannot bypass locks on insensitive filesystems.
	name = norm.NFC.String(name)
	r.mu.Lock()
	var entry *pathLock
	for _, candidate := range r.entries {
		if strings.EqualFold(candidate.name, name) && os.SameFile(candidate.parent, parent) {
			entry = candidate
			break
		}
	}
	if entry == nil {
		entry = &pathLock{parent: parent, name: name, gate: semaphore.NewWeighted(1)}
		r.entries = append(r.entries, entry)
	}
	entry.refs++
	r.mu.Unlock()

	if err := entry.gate.Acquire(ctx, 1); err != nil {
		r.releaseRef(entry)
		return nil, err
	}
	return func() {
		entry.gate.Release(1)
		r.releaseRef(entry)
	}, nil
}

func (r *pathLockRegistry) releaseRef(entry *pathLock) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry.refs--
	if entry.refs == 0 {
		index := slices.Index(r.entries, entry)
		r.entries = slices.Delete(r.entries, index, index+1)
	}
}
