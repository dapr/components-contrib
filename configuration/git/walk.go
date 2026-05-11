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
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/dapr/kit/logger"
)

// walkTree walks files under root/sub and returns POSIX-pathed entries
// relative to root/sub. Symlinks are skipped. Hidden files (basename
// starts with `.`) are skipped unless includeHidden is true. The `.git`
// directory is **always** excluded regardless of includeHidden so that
// the remote URL stored in `.git/config` (and any embedded credentials)
// cannot leak into the configuration store output. Files larger than
// maxFileSize are skipped with a warning to keep memory bounded.
func walkTree(root, sub string, includeHidden bool, maxFileSize int64, log logger.Logger) ([]fileEntry, error) {
	if root == "" {
		return nil, errors.New("walkTree: empty root")
	}
	scope := filepath.Join(root, filepath.FromSlash(sub))
	info, err := os.Stat(scope)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat %q: %w", scope, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("path %q is not a directory", scope)
	}

	var entries []fileEntry
	err = filepath.WalkDir(scope, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// `.git` is always excluded so credentials stored in .git/config
		// can never leak into the snapshot, regardless of includeHidden.
		if d.IsDir() && d.Name() == ".git" && p != scope {
			return fs.SkipDir
		}
		// Skip hidden files/dirs unless includeHidden is set.
		if !includeHidden && strings.HasPrefix(d.Name(), ".") && p != scope {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		// Skip symlinks — never follow.
		mode := d.Type()
		if mode&os.ModeSymlink != 0 {
			return nil
		}
		rel, err := filepath.Rel(scope, p)
		if err != nil {
			return fmt.Errorf("rel %q: %w", p, err)
		}
		if maxFileSize > 0 {
			fi, statErr := d.Info()
			if statErr != nil {
				return fmt.Errorf("stat %q: %w", p, statErr)
			}
			if fi.Size() > maxFileSize {
				if log != nil {
					log.Warnf("git config: skipping %q (size %d exceeds maxFileSize %d)",
						filepath.ToSlash(rel), fi.Size(), maxFileSize)
				}
				return nil
			}
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return fmt.Errorf("read %q: %w", p, err)
		}
		entries = append(entries, fileEntry{
			RelPath: filepath.ToSlash(rel),
			Bytes:   data,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}
