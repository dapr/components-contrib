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

// Package localstorage provides a BinaryStore implementation backed by the
// local filesystem. Files are stored beneath a configured root directory,
// with slashes in file names mapping to nested directories. Uploads are
// staged in a temporary file and atomically published into place. Reads do not
// take the mutation lock.
//
// All filesystem access goes through an os.Root opened on the root
// directory. Besides guaranteeing that no operation can escape the root,
// os.Root gives every platform the same semantics for renaming over and
// deleting files that another reader still has open, which plain os.Rename
// and os.Remove do not provide on Windows.
//
// Each root directory must be exclusively owned by one process. Mutations
// of the same path are serialized across component instances in that process;
// concurrent access by other sidecars or external writers is unsupported.
//
// Initialization does not remove existing files. Staging files left behind
// by a crashed process require operator cleanup; their names alone do not
// distinguish them from stored objects or another instance's active uploads.
//
// Cancellation is cooperative: contexts are checked between reads and before
// publishing or deleting files, but cannot interrupt an already-blocked read
// or filesystem operation. Completed writes and deletes are not rolled back.
// Callers must close download streams even after cancellation.
// Delete leaves parent directories intact.
package localstorage

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	dirPerm  = 0o755
	filePerm = 0o644
	// stagingPrefix names the staging file used while an upload is read. The
	// leading dot keeps staging files out of the way of stored objects.
	stagingPrefix = ".dapr-upload-"
)

// disallowedRootPaths are locations the store refuses to write to.
var disallowedRootPaths = []string{
	filepath.Clean("/proc"),
	filepath.Clean("/sys"),
	filepath.Clean("/boot"),
	// See: https://github.com/dapr/components-contrib/issues/2444
	filepath.Clean("/var/run/secrets"),
}

// localStorageMetadata contains the parsed metadata for the local storage store.
type localStorageMetadata struct {
	RootPath string `json:"rootPath" mapstructure:"rootPath"`
	Prefix   string `json:"prefix" mapstructure:"prefix"`
	// MaxSize is the maximum file size, as a byte size quantity (e.g. "4Mi").
	// Zero means unlimited.
	MaxSize kitmd.ByteSize `json:"maxSize" mapstructure:"maxSize"`
}

// LocalStorage is a BinaryStore implementation backed by the local filesystem.
type LocalStorage struct {
	mu        sync.Mutex
	active    sync.WaitGroup
	closeDone chan struct{}
	closeErr  error

	rootPath string
	root     *os.Root
	prefix   string
	maxSize  int64
}

// NewLocalStorage returns a new local filesystem BinaryStore.
func NewLocalStorage(_ logger.Logger) binarystore.BinaryStore {
	return &LocalStorage{}
}

func (s *LocalStorage) Init(ctx context.Context, md binarystore.Metadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closeDone != nil {
		return fs.ErrClosed
	}
	if s.root != nil {
		return errors.New("local storage is already initialized")
	}
	var m localStorageMetadata
	if err := kitmd.DecodeMetadata(md.Properties, &m); err != nil {
		return fmt.Errorf("failed to decode metadata: %w", err)
	}

	rootPath, err := validateRootPath(m.RootPath)
	if err != nil {
		return err
	}
	// Fail fast on a malformed prefix instead of on the first operation.
	if _, err = binarystore.ObjectPath(m.Prefix, "probe"); err != nil {
		return err
	}
	if err = validateRelativePath(m.Prefix); err != nil {
		return fmt.Errorf("%w: %w", binarystore.ErrInvalidPrefix, err)
	}
	maxSize, err := m.MaxSize.GetBytes()
	if err != nil {
		return fmt.Errorf("invalid value for maxSize: %w", err)
	}
	if maxSize < 0 {
		return errors.New("maxSize must not be negative")
	}

	if err = ctx.Err(); err != nil {
		return err
	}
	if err = os.MkdirAll(rootPath, dirPerm); err != nil {
		return fmt.Errorf("unable to create directory specified by 'rootPath' %s: %w", rootPath, err)
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return fmt.Errorf("unable to open 'rootPath' %s: %w", rootPath, err)
	}
	if err = ctx.Err(); err != nil {
		return errors.Join(err, root.Close())
	}

	s.rootPath = rootPath
	s.root = root
	s.prefix = m.Prefix
	s.maxSize = maxSize
	return nil
}

func (s *LocalStorage) Features() []binarystore.Feature {
	return []binarystore.Feature{}
}

func (s *LocalStorage) beginOperation(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.root == nil || s.closeDone != nil {
		return fs.ErrClosed
	}
	s.active.Add(1)
	return nil
}

func (s *LocalStorage) Set(ctx context.Context, req *binarystore.SetRequest) (err error) {
	if err = s.beginOperation(ctx); err != nil {
		return err
	}
	defer s.active.Done()

	if req.FileName == "" {
		return binarystore.ErrMissingFileName
	}
	// Validate the name before reading the body so that requests that will be
	// rejected do not pay for the read.
	relPath, err := s.resolve(req.FileName)
	if err != nil {
		return err
	}
	dir := filepath.Dir(relPath)
	if dir != "." {
		if err = s.root.MkdirAll(dir, dirPerm); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}
	unlock, err := s.lockPath(ctx, relPath)
	if err != nil {
		return err
	}
	defer unlock()

	if !req.Overwrite {
		// Cheap early rejection of duplicates; the authoritative check is the
		// no-replace publication below.
		if _, err = s.root.Lstat(relPath); err == nil {
			return binarystore.ErrFileAlreadyExists
		}
	}

	tmp, tmpPath, err := s.createStaging(dir)
	if err != nil {
		return err
	}
	// Failed uploads discard the staging file. Successful create-only uploads
	// remove the staging link after publishing the final link.
	defer func() {
		if removeErr := s.root.Remove(tmpPath); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("failed to remove staging file %s: %w", tmpPath, removeErr))
		}
	}()

	if err = s.writeStaging(ctx, tmp, req.Data); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}

	if req.Overwrite {
		if err = s.root.Rename(tmpPath, relPath); err != nil {
			return fmt.Errorf("failed to move file %s into place: %w", req.FileName, err)
		}
		return nil
	}

	// A hard link publishes the fully staged file atomically and fails if the
	// destination exists. If the process exits before cleanup, the final object
	// is complete and only the staging link remains abandoned.
	if err = s.root.Link(tmpPath, relPath); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return binarystore.ErrFileAlreadyExists
		}
		return fmt.Errorf("failed to move file %s into place: %w", req.FileName, err)
	}
	return nil
}

// createStaging creates a uniquely named staging file in an existing directory.
func (s *LocalStorage) createStaging(dir string) (*os.File, string, error) {
	path := filepath.Join(dir, stagingPrefix+rand.Text())
	f, err := s.root.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, filePerm)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create staging file in %s: %w", dir, err)
	}
	return f, path, nil
}

// writeStaging streams data into tmp, enforcing maxSize, and closes tmp.
func (s *LocalStorage) writeStaging(ctx context.Context, tmp *os.File, data io.Reader) (err error) {
	defer func() {
		if closeErr := tmp.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to close staging file: %w", closeErr))
		}
	}()

	if data == nil {
		data = strings.NewReader("")
	}
	if s.maxSize > 0 {
		data = binarystore.NewSizeLimitReader(data, s.maxSize)
	}
	if _, err := io.Copy(tmp, &contextReader{ctx: ctx, reader: data}); err != nil {
		if errors.Is(err, binarystore.ErrFileTooLarge) {
			return err
		}
		return fmt.Errorf("failed to write file data: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("failed to sync file data: %w", err)
	}
	return nil
}

func (s *LocalStorage) Get(ctx context.Context, req *binarystore.GetRequest) (*binarystore.GetResponse, error) {
	if err := s.beginOperation(ctx); err != nil {
		return nil, err
	}
	defer s.active.Done()

	if req.FileName == "" {
		return nil, binarystore.ErrMissingFileName
	}
	relPath, err := s.resolve(req.FileName)
	if err != nil {
		return nil, err
	}

	f, err := s.root.Open(relPath)
	if err != nil {
		if isFileNotFound(err) {
			return nil, binarystore.ErrFileNotFound
		}
		return nil, fmt.Errorf("failed to open file %s: %w", req.FileName, err)
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("failed to stat file %s: %w", req.FileName, err), f.Close())
	}
	// A directory created to hold nested files is not itself a stored file.
	if fi.IsDir() {
		return nil, errors.Join(binarystore.ErrFileNotFound, f.Close())
	}
	if err = ctx.Err(); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return &binarystore.GetResponse{
		Data: struct {
			io.Reader
			io.Closer
		}{
			Reader: &contextReader{ctx: ctx, reader: f},
			Closer: f,
		},
	}, nil
}

// contextReader intentionally exposes only Read so io.Copy cannot bypass
// cancellation checks through an underlying WriterTo implementation.
type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

func (s *LocalStorage) Delete(ctx context.Context, req *binarystore.DeleteRequest) error {
	if err := s.beginOperation(ctx); err != nil {
		return err
	}
	defer s.active.Done()

	if req.FileName == "" {
		return binarystore.ErrMissingFileName
	}
	relPath, err := s.resolve(req.FileName)
	if err != nil {
		return err
	}
	unlock, err := s.lockPath(ctx, relPath)
	if err != nil {
		if isFileNotFound(err) {
			return binarystore.ErrFileNotFound
		}
		return err
	}
	defer unlock()

	fi, err := s.root.Lstat(relPath)
	if err != nil {
		if isFileNotFound(err) {
			return binarystore.ErrFileNotFound
		}
		return fmt.Errorf("failed to stat file %s: %w", req.FileName, err)
	}
	if fi.IsDir() {
		return binarystore.ErrFileNotFound
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = s.root.Remove(relPath); err != nil {
		if isFileNotFound(err) {
			return binarystore.ErrFileNotFound
		}
		return fmt.Errorf("failed to delete file %s: %w", req.FileName, err)
	}
	return nil
}

// isFileNotFound includes paths whose ancestor is a regular file. Unix
// reports those as ENOTDIR rather than fs.ErrNotExist.
func isFileNotFound(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR)
}

// resolve maps a file name to a path relative to the root directory,
// rejecting names that would not map to the same file on every platform.
// os.Root independently guarantees the result cannot escape the root.
func (s *LocalStorage) resolve(fileName string) (string, error) {
	objectPath, err := binarystore.ObjectPath(s.prefix, fileName)
	if err != nil {
		return "", err
	}
	if err = validateRelativePath(fileName); err != nil {
		return "", fmt.Errorf("%w: %w", binarystore.ErrInvalidFileName, err)
	}
	return filepath.FromSlash(objectPath), nil
}

// validateRelativePath checks that a slash-separated object path is made of
// plain segments that map to the same file on every platform: no empty, "."
// or ".." segments, no backslashes, no NUL bytes, and no names the host
// filesystem reserves.
func validateRelativePath(p string) error {
	if p == "" {
		return nil
	}
	if strings.ContainsRune(p, 0) {
		return errors.New("must not contain NUL bytes")
	}
	if strings.ContainsRune(p, '\\') {
		return errors.New("must not contain backslashes")
	}
	for _, segment := range strings.Split(p, "/") {
		switch segment {
		case "":
			return errors.New("must not contain empty path segments")
		case ".", "..":
			return errors.New("must not contain '.' or '..' path segments")
		}
	}
	if !filepath.IsLocal(filepath.FromSlash(p)) {
		return errors.New("must be a local relative path")
	}
	return nil
}

// validateRootPath cleans and absolutises rootPath, resolves symlinks in the
// part of it that already exists, and rejects disallowed locations.
func validateRootPath(rootPath string) (string, error) {
	if rootPath == "" {
		return "", errors.New("property rootPath must not be empty")
	}

	requested, err := filepath.Abs(rootPath)
	if err != nil {
		return "", fmt.Errorf("failed to resolve rootPath: %w", err)
	}
	resolved, err := resolveExistingSymlinks(requested)
	if err != nil {
		return "", err
	}

	if isDisallowedRootPath(requested) || isDisallowedRootPath(resolved) {
		return "", errors.New("property rootPath points to a disallowed location")
	}
	return resolved, nil
}

// resolveExistingSymlinks evaluates symlinks in the deepest ancestor of path
// that exists and re-attaches the remainder, so paths that are yet to be
// created still resolve. An existing path that is not a directory is an
// error.
func resolveExistingSymlinks(path string) (string, error) {
	existing := path
	var remainder string
	for {
		fi, err := os.Stat(existing)
		if err == nil {
			if !fi.IsDir() {
				return "", errors.New("property rootPath represents a file and not a directory")
			}
			break
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf("error getting stat for path %s: %w", existing, err)
		}
		parent := filepath.Dir(existing)
		if parent == existing {
			break
		}
		remainder = filepath.Join(filepath.Base(existing), remainder)
		existing = parent
	}
	resolved, err := filepath.EvalSymlinks(existing)
	if err != nil {
		return "", fmt.Errorf("failed to evaluate symlinks in rootPath: %w", err)
	}
	return filepath.Join(resolved, remainder), nil
}

// isDisallowedRootPath reports whether p overlaps one of the disallowed
// locations: it may be the location itself, a descendant, or an ancestor that
// would grant access to it. Each disallowed location is also compared in its
// symlink-resolved form, since for example /var/run is a symlink to /run on
// most Linux distributions and a resolved rootPath would otherwise slip past.
func isDisallowedRootPath(p string) bool {
	for _, d := range disallowedRootPaths {
		candidates := []string{d}
		if resolved, err := resolveExistingSymlinks(d); err == nil && resolved != d {
			candidates = append(candidates, resolved)
		}
		for _, c := range candidates {
			if pathsOverlap(p, c) {
				return true
			}
		}
	}
	return false
}

func pathsOverlap(a, b string) bool {
	return pathContains(a, b) || pathContains(b, a)
}

func pathContains(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

func (s *LocalStorage) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := localStorageMetadata{}
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BinaryStoreType)
	return
}

// Close rejects new operations and waits for active operations, including
// their cleanup, before closing the root. An upload blocked in a caller-owned
// reader must finish or unblock before Close can return. Already-open download
// streams remain the caller's responsibility and do not keep Close waiting.
func (s *LocalStorage) Close() error {
	s.mu.Lock()
	if s.closeDone != nil {
		done := s.closeDone
		s.mu.Unlock()
		<-done
		return s.closeErr
	}
	s.closeDone = make(chan struct{})
	s.mu.Unlock()

	s.active.Wait()
	if s.root != nil {
		s.closeErr = s.root.Close()
	}
	close(s.closeDone)
	return s.closeErr
}
