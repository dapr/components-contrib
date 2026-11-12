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
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/binarystore"
	"github.com/dapr/components-contrib/binarystore/internal/storetest"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

func newTestStore(t *testing.T, prefix string) *LocalStorage {
	t.Helper()

	props := map[string]string{"rootPath": t.TempDir()}
	if prefix != "" {
		props["prefix"] = prefix
	}
	return newTestStoreWithProperties(t, props)
}

func newTestStoreWithProperties(t *testing.T, props map[string]string) *LocalStorage {
	t.Helper()

	store := NewLocalStorage(logger.NewLogger("test")).(*LocalStorage)
	require.NoError(t, store.Init(t.Context(), binarystore.Metadata{
		Base: contribMetadata.Base{Properties: props},
	}))
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

// --- shared behaviour suite ---

func TestStoreBehaviour(t *testing.T) {
	storetest.RunSuite(t, func(t *testing.T, prefix string) storetest.Harness {
		store := newTestStore(t, prefix)
		return storetest.Harness{
			Store:  store,
			Prefix: prefix,
			Names:  func() []string { return names(t, store.rootPath) },
		}
	})
}

// --- interface compliance and constructor ---

func TestImplementsBinaryStore(t *testing.T) {
	var _ binarystore.BinaryStore = (*LocalStorage)(nil)
}

func TestNewLocalStorage(t *testing.T) {
	require.NotNil(t, NewLocalStorage(logger.NewLogger("test")))
}

// --- Close ---

func TestClose(t *testing.T) {
	t.Run("uninitialised store", func(t *testing.T) {
		store := NewLocalStorage(logger.NewLogger("test"))
		require.NoError(t, store.Close())
	})

	t.Run("configured store", func(t *testing.T) {
		store := newTestStore(t, "")
		require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
			FileName: "file.bin",
			Data:     bytes.NewReader([]byte("payload")),
		}))
		require.NoError(t, store.Close())

		// Unlike the in-memory store, closing does not discard data.
		assert.Equal(t, []string{"file.bin"}, names(t, store.rootPath))
	})
}

// --- provider specific behaviour ---

func TestOperationsRejectCancelledContext(t *testing.T) {
	for _, tt := range []struct {
		name     string
		deadline bool
		wantErr  error
	}{
		{name: "cancelled", wantErr: context.Canceled},
		{name: "deadline exceeded", deadline: true, wantErr: context.DeadlineExceeded},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if tt.deadline {
				ctx, cancel = context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
			}
			defer cancel()

			t.Run("init", func(t *testing.T) {
				root := filepath.Join(t.TempDir(), "store")
				store := NewLocalStorage(logger.NewLogger("test")).(*LocalStorage)
				t.Cleanup(func() {
					_ = store.Close()
				})
				err := store.Init(ctx, binarystore.Metadata{
					Base: contribMetadata.Base{Properties: map[string]string{"rootPath": root}},
				})
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, store.root)
				assert.NoDirExists(t, root)
			})

			t.Run("file operations", func(t *testing.T) {
				store := newTestStore(t, "")
				require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
					FileName: "existing.bin",
					Data:     strings.NewReader("original"),
				}))

				err := store.Set(ctx, &binarystore.SetRequest{
					FileName: "new.bin",
					Data:     &unreadableReader{},
				})
				require.ErrorIs(t, err, tt.wantErr)
				err = store.Set(ctx, &binarystore.SetRequest{
					FileName:  "existing.bin",
					Data:      &unreadableReader{},
					Overwrite: true,
				})
				require.ErrorIs(t, err, tt.wantErr)

				resp, err := store.Get(ctx, &binarystore.GetRequest{FileName: "existing.bin"})
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, resp)
				err = store.Delete(ctx, &binarystore.DeleteRequest{FileName: "existing.bin"})
				require.ErrorIs(t, err, tt.wantErr)

				assert.Equal(t, "original", string(mustGet(t, store, "existing.bin")))
				assert.Equal(t, []string{"existing.bin"}, names(t, store.rootPath))
			})
		})
	}
}

func TestSetCancellationDiscardsStaging(t *testing.T) {
	for _, maxSize := range []string{"0", "1Ki"} {
		t.Run("maxSize="+maxSize, func(t *testing.T) {
			for _, mode := range []struct {
				name      string
				overwrite bool
			}{
				{name: "create-only"},
				{name: "overwrite", overwrite: true},
			} {
				t.Run(mode.name, func(t *testing.T) {
					for _, phase := range []struct {
						name    string
						payload string
						readErr error
					}{
						{name: "between reads", payload: "more"},
						{name: "final payload", payload: "last", readErr: io.EOF},
						{name: "EOF", readErr: io.EOF},
					} {
						t.Run(phase.name, func(t *testing.T) {
							store := newTestStoreWithProperties(t, map[string]string{
								"rootPath": t.TempDir(),
								"maxSize":  maxSize,
							})
							if mode.overwrite {
								require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
									FileName: "file.bin",
									Data:     strings.NewReader("original"),
								}))
							}

							ctx, cancel := context.WithCancel(t.Context())
							defer cancel()
							var reads int
							data := &trackedUploadReader{Reader: readerFunc(func(p []byte) (int, error) {
								reads++
								if reads == 1 {
									return copy(p, "first"), nil
								}
								if reads > 2 {
									return 0, errMustNotRead
								}
								cancel()
								return copy(p, phase.payload), phase.readErr
							})}
							err := store.Set(ctx, &binarystore.SetRequest{
								FileName:  "file.bin",
								Data:      data,
								Overwrite: mode.overwrite,
							})
							require.ErrorIs(t, err, context.Canceled)
							assert.Equal(t, 2, reads)
							assert.False(t, data.closed, "Set must not close the caller's reader")

							if mode.overwrite {
								assert.Equal(t, "original", string(mustGet(t, store, "file.bin")))
								assert.Equal(t, []string{"file.bin"}, names(t, store.rootPath))
							} else {
								_, err = store.Get(t.Context(), &binarystore.GetRequest{FileName: "file.bin"})
								require.ErrorIs(t, err, binarystore.ErrFileNotFound)
								assert.Empty(t, names(t, store.rootPath))
							}
						})
					}
				})
			}
		})
	}
}

func TestGetStreamHonorsCancellation(t *testing.T) {
	store := newTestStore(t, "")
	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName: "file.bin",
		Data:     strings.NewReader("payload"),
	}))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	resp, err := store.Get(ctx, &binarystore.GetRequest{FileName: "file.bin"})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = resp.Data.Close()
	})
	var first [1]byte
	_, err = io.ReadFull(resp.Data, first[:])
	require.NoError(t, err)
	assert.Equal(t, byte('p'), first[0])

	cancel()
	var remaining bytes.Buffer
	n, err := io.Copy(&remaining, resp.Data)
	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, n)
	assert.Empty(t, remaining.Bytes())
	require.NoError(t, resp.Data.Close())
	assert.Equal(t, "payload", string(mustGet(t, store, "file.bin")))
}

func TestNestedFileNames(t *testing.T) {
	store := newTestStore(t, "tenant")

	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName:  "a/b/c.bin",
		Data:      strings.NewReader("nested"),
		Overwrite: true,
	}))
	assert.FileExists(t, filepath.Join(store.rootPath, "tenant", "a", "b", "c.bin"))
	assert.Equal(t, []string{"tenant/a/b/c.bin"}, names(t, store.rootPath))

	assert.Equal(t, "nested", string(mustGet(t, store, "a/b/c.bin")))

	require.NoError(t, store.Delete(t.Context(), &binarystore.DeleteRequest{FileName: "a/b/c.bin"}))
	assert.NoFileExists(t, filepath.Join(store.rootPath, "tenant", "a", "b", "c.bin"))
	assert.DirExists(t, filepath.Join(store.rootPath, "tenant", "a", "b"))
	assert.DirExists(t, store.rootPath)
}

func TestDeleteLeavesSiblingsAndNonEmptyDirs(t *testing.T) {
	store := newTestStore(t, "")

	for _, name := range []string{"dir/one.bin", "dir/two.bin"} {
		require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
			FileName:  name,
			Data:      strings.NewReader(name),
			Overwrite: true,
		}))
	}
	require.NoError(t, store.Delete(t.Context(), &binarystore.DeleteRequest{FileName: "dir/one.bin"}))
	assert.Equal(t, []string{"dir/two.bin"}, names(t, store.rootPath))
}

func TestDirectoryIsNotAFile(t *testing.T) {
	store := newTestStore(t, "")
	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName:  "dir/file.bin",
		Data:      strings.NewReader("payload"),
		Overwrite: true,
	}))

	_, err := store.Get(t.Context(), &binarystore.GetRequest{FileName: "dir"})
	require.ErrorIs(t, err, binarystore.ErrFileNotFound)

	err = store.Delete(t.Context(), &binarystore.DeleteRequest{FileName: "dir"})
	require.ErrorIs(t, err, binarystore.ErrFileNotFound)
	assert.FileExists(t, filepath.Join(store.rootPath, "dir", "file.bin"))
}

func TestFileAncestorIsNotFound(t *testing.T) {
	store := newTestStore(t, "")
	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName: "a",
		Data:     strings.NewReader("payload"),
	}))

	for _, name := range []string{"a/b", "a/b/c"} {
		t.Run(name, func(t *testing.T) {
			_, err := store.Get(t.Context(), &binarystore.GetRequest{FileName: name})
			require.ErrorIs(t, err, binarystore.ErrFileNotFound)

			err = store.Delete(t.Context(), &binarystore.DeleteRequest{FileName: name})
			require.ErrorIs(t, err, binarystore.ErrFileNotFound)
		})
	}
}

func TestRejectsPathTraversal(t *testing.T) {
	store := newTestStore(t, "")

	// A sibling of the root that must never be touched.
	outside := filepath.Join(filepath.Dir(store.rootPath), "outside.bin")
	require.NoError(t, os.WriteFile(outside, []byte("untouched"), 0o600))

	invalid := []string{
		"../outside.bin",
		"a/../../outside.bin",
		"a/./b.bin",
		"a//b.bin",
		"back\\slash.bin",
		"nul\x00byte.bin",
		"..",
		".",
	}
	if runtime.GOOS == "windows" {
		invalid = append(invalid, "C:/outside.bin", "CON", "dir/COM1")
	}
	for _, name := range invalid {
		err := store.Set(t.Context(), &binarystore.SetRequest{
			FileName:  name,
			Data:      &unreadableReader{},
			Overwrite: true,
		})
		require.ErrorIs(t, err, binarystore.ErrInvalidFileName, name)

		_, err = store.Get(t.Context(), &binarystore.GetRequest{FileName: name})
		require.ErrorIs(t, err, binarystore.ErrInvalidFileName, name)

		err = store.Delete(t.Context(), &binarystore.DeleteRequest{FileName: name})
		require.ErrorIs(t, err, binarystore.ErrInvalidFileName, name)
	}

	got, err := os.ReadFile(outside) //nolint:gosec // G304: outside is a test-owned fixture path.
	require.NoError(t, err)
	assert.Equal(t, "untouched", string(got))
	assert.Empty(t, names(t, store.rootPath))
}

func TestSetEnforcesObjectSizeLimit(t *testing.T) {
	store := newTestStoreWithProperties(t, map[string]string{
		"rootPath": t.TempDir(),
		"maxSize":  "7",
	})

	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName: "allowed.bin",
		Data:     bytes.NewBufferString("payload"),
	}))

	err := store.Set(t.Context(), &binarystore.SetRequest{
		FileName: "large.bin",
		Data:     bytes.NewBufferString("too large"),
	})
	require.ErrorIs(t, err, binarystore.ErrFileTooLarge)

	_, err = store.Get(t.Context(), &binarystore.GetRequest{FileName: "large.bin"})
	require.ErrorIs(t, err, binarystore.ErrFileNotFound)

	// Neither the rejected file nor its staging file is left behind.
	assert.Equal(t, []string{"allowed.bin"}, names(t, store.rootPath))
}

func TestFailedOverwriteKeepsExistingFile(t *testing.T) {
	store := newTestStoreWithProperties(t, map[string]string{
		"rootPath": t.TempDir(),
		"maxSize":  "7",
	})
	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName: "file.bin",
		Data:     bytes.NewBufferString("first"),
	}))

	err := store.Set(t.Context(), &binarystore.SetRequest{
		FileName:  "file.bin",
		Data:      bytes.NewBufferString("far too large"),
		Overwrite: true,
	})
	require.ErrorIs(t, err, binarystore.ErrFileTooLarge)

	err = store.Set(t.Context(), &binarystore.SetRequest{
		FileName:  "file.bin",
		Data:      &failingReader{},
		Overwrite: true,
	})
	require.ErrorIs(t, err, errReadFailed)

	assert.Equal(t, "first", string(mustGet(t, store, "file.bin")))
	assert.Equal(t, []string{"file.bin"}, names(t, store.rootPath))
}

func TestSetPreservesStagingCleanupError(t *testing.T) {
	store := newTestStore(t, "")
	data := readerFunc(func([]byte) (int, error) {
		staging := names(t, store.rootPath)
		require.Len(t, staging, 1)
		path := filepath.FromSlash(staging[0])
		// Replace the open staging file with a nonempty directory to make
		// cleanup fail on all platforms, independently of file permissions.
		require.NoError(t, store.root.Remove(path))
		require.NoError(t, store.root.Mkdir(path, 0o700))
		require.NoError(t, store.root.WriteFile(filepath.Join(path, "child"), []byte("x"), 0o600))
		return 0, errReadFailed
	})

	err := store.Set(t.Context(), &binarystore.SetRequest{
		FileName: "file.bin",
		Data:     data,
	})
	require.ErrorIs(t, err, errReadFailed)
	require.ErrorContains(t, err, "failed to remove staging file")
	var pathErr *fs.PathError
	require.ErrorAs(t, err, &pathErr)
	assert.True(t, strings.HasPrefix(pathErr.Path, stagingPrefix))
	assert.NoFileExists(t, filepath.Join(store.rootPath, "file.bin"))
}

func TestWriteStagingPreservesCloseErrors(t *testing.T) {
	for _, tt := range []struct {
		name    string
		readErr error
		wantErr error
		message string
	}{
		{
			name:    "read failure",
			readErr: errReadFailed,
			wantErr: errReadFailed,
			message: "failed to write file data",
		},
		{
			name:    "sync failure",
			readErr: io.EOF,
			wantErr: fs.ErrClosed,
			message: "failed to sync file data",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := newTestStore(t, "")
			tmp, _, err := store.createStaging(".")
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = tmp.Close()
			})

			data := readerFunc(func([]byte) (int, error) {
				require.NoError(t, tmp.Close())
				return 0, tt.readErr
			})
			err = store.writeStaging(t.Context(), tmp, data)
			require.ErrorIs(t, err, tt.wantErr)
			require.ErrorIs(t, err, fs.ErrClosed)
			require.ErrorContains(t, err, tt.message)
			require.ErrorContains(t, err, "failed to close staging file")
		})
	}
}

func TestOpenReaderDoesNotBlockOverwriteOrDelete(t *testing.T) {
	store := newTestStore(t, "")
	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName:  "file.bin",
		Data:      strings.NewReader("first"),
		Overwrite: true,
	}))

	// Hold a Get stream open across an overwrite and a delete. On POSIX this
	// is naturally fine; on Windows it relies on FILE_SHARE_DELETE and POSIX
	// rename semantics, otherwise both operations fail with sharing errors.
	resp, err := store.Get(t.Context(), &binarystore.GetRequest{FileName: "file.bin"})
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, resp.Data.Close())
	}()

	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName:  "file.bin",
		Data:      strings.NewReader("second"),
		Overwrite: true,
	}))
	assert.Equal(t, "second", string(mustGet(t, store, "file.bin")))

	require.NoError(t, store.Delete(t.Context(), &binarystore.DeleteRequest{FileName: "file.bin"}))
	_, err = store.Get(t.Context(), &binarystore.GetRequest{FileName: "file.bin"})
	require.ErrorIs(t, err, binarystore.ErrFileNotFound)

	// The reader opened before the overwrite still sees the content it opened.
	old, err := io.ReadAll(resp.Data)
	require.NoError(t, err)
	assert.Equal(t, "first", string(old))
}

func TestCrossInstanceMutationLockSerializesOverwrite(t *testing.T) {
	root := t.TempDir()
	first := newTestStoreWithProperties(t, map[string]string{"rootPath": root})
	second := newTestStoreWithProperties(t, map[string]string{"rootPath": root})

	uploadStarted := make(chan struct{})
	releaseUpload := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseUpload:
		default:
			close(releaseUpload)
		}
	})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- first.Set(t.Context(), &binarystore.SetRequest{
			FileName:  "file.bin",
			Data:      blockingReader(uploadStarted, releaseUpload, "first"),
			Overwrite: true,
		})
	}()
	<-uploadStarted
	requireMutationLockState(t, 1, 1)

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- second.Set(t.Context(), &binarystore.SetRequest{
			FileName:  "file.bin",
			Data:      strings.NewReader("second"),
			Overwrite: true,
		})
	}()
	requireMutationLockState(t, 1, 2)

	close(releaseUpload)
	require.NoError(t, <-firstDone)
	require.NoError(t, <-secondDone)
	requireMutationLockState(t, 0, 0)
	assert.Equal(t, "second", string(mustGet(t, first, "file.bin")))
}

func TestCrossInstanceMutationLockCancellationCleansRegistry(t *testing.T) {
	root := t.TempDir()
	first := newTestStoreWithProperties(t, map[string]string{"rootPath": root})
	second := newTestStoreWithProperties(t, map[string]string{"rootPath": root})

	uploadStarted := make(chan struct{})
	releaseUpload := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseUpload:
		default:
			close(releaseUpload)
		}
	})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- first.Set(t.Context(), &binarystore.SetRequest{
			FileName:  "file.bin",
			Data:      blockingReader(uploadStarted, releaseUpload, "payload"),
			Overwrite: true,
		})
	}()
	<-uploadStarted
	requireMutationLockState(t, 1, 1)

	ctx, cancel := context.WithCancel(t.Context())
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- second.Delete(ctx, &binarystore.DeleteRequest{FileName: "file.bin"})
	}()
	requireMutationLockState(t, 1, 2)

	cancel()
	require.ErrorIs(t, <-deleteDone, context.Canceled)
	requireMutationLockState(t, 1, 1)

	close(releaseUpload)
	require.NoError(t, <-firstDone)
	requireMutationLockState(t, 0, 0)
	assert.Equal(t, "payload", string(mustGet(t, second, "file.bin")))
}

func TestSetRejectsWithoutReadingData(t *testing.T) {
	store := newTestStore(t, "")
	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{
		FileName: "exists.bin",
		Data:     bytes.NewBufferString("payload"),
	}))

	t.Run("invalid file name", func(t *testing.T) {
		err := store.Set(t.Context(), &binarystore.SetRequest{
			FileName: "/leading-slash",
			Data:     &unreadableReader{},
		})
		require.ErrorIs(t, err, binarystore.ErrInvalidFileName)
	})

	t.Run("existing file without overwrite", func(t *testing.T) {
		err := store.Set(t.Context(), &binarystore.SetRequest{
			FileName: "exists.bin",
			Data:     &unreadableReader{},
		})
		require.ErrorIs(t, err, binarystore.ErrFileAlreadyExists)
	})
}

func TestSetNilDataStoresEmptyFile(t *testing.T) {
	store := newTestStore(t, "")

	// A request without Data must not panic; it stores an empty file.
	require.NoError(t, store.Set(t.Context(), &binarystore.SetRequest{FileName: "empty.bin"}))

	assert.Empty(t, mustGet(t, store, "empty.bin"))
}

func TestDataPersistsAcrossInstances(t *testing.T) {
	root := t.TempDir()
	first := newTestStoreWithProperties(t, map[string]string{"rootPath": root})
	require.NoError(t, first.Set(t.Context(), &binarystore.SetRequest{
		FileName: "durable.bin",
		Data:     strings.NewReader("still here"),
	}))
	require.NoError(t, first.Close())

	second := newTestStoreWithProperties(t, map[string]string{"rootPath": root})
	assert.Equal(t, "still here", string(mustGet(t, second, "durable.bin")))
}

func TestInitPreservesStoredObjects(t *testing.T) {
	root := t.TempDir()
	first := newTestStoreWithProperties(t, map[string]string{"rootPath": root})
	fileNames := []string{
		".dapr-upload-invoice",
		"tenant/dir/.dapr-upload-123456",
		"tenant/dir/.dapr-upload-",
		"tenant/dir/file.bin",
	}
	old := time.Now().Add(-24 * time.Hour)
	for _, name := range fileNames {
		require.NoError(t, first.Set(t.Context(), &binarystore.SetRequest{
			FileName: name,
			Data:     strings.NewReader(name),
		}))
		require.NoError(t, os.Chtimes(filepath.Join(root, filepath.FromSlash(name)), old, old))
	}
	require.NoError(t, first.Close())

	second := newTestStoreWithProperties(t, map[string]string{"rootPath": root})
	for _, name := range fileNames {
		assert.Equal(t, name, string(mustGet(t, second, name)))
	}
	assert.ElementsMatch(t, fileNames, names(t, root))
}

func TestInitPreservesAbandonedUploads(t *testing.T) {
	root := t.TempDir()
	first := newTestStoreWithProperties(t, map[string]string{"rootPath": root})
	require.NoError(t, first.root.MkdirAll(filepath.Join("tenant", "dir"), dirPerm))
	tmp, tmpPath, err := first.createStaging(filepath.Join("tenant", "dir"))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = tmp.Close()
	})
	_, err = tmp.WriteString("partial upload")
	require.NoError(t, err)
	require.NoError(t, tmp.Close())
	old := time.Now().Add(-24 * time.Hour)
	require.NoError(t, os.Chtimes(filepath.Join(root, tmpPath), old, old))
	require.NoError(t, first.Close())

	second := newTestStoreWithProperties(t, map[string]string{"rootPath": root})
	data, err := second.root.ReadFile(tmpPath)
	require.NoError(t, err)
	assert.Equal(t, "partial upload", string(data))
	assert.Equal(t, []string{filepath.ToSlash(tmpPath)}, names(t, root))
}

// --- metadata ---

func TestParseMetadata(t *testing.T) {
	t.Run("missing rootPath", func(t *testing.T) {
		store := NewLocalStorage(logger.NewLogger("test"))
		err := store.Init(t.Context(), binarystore.Metadata{
			Base: contribMetadata.Base{Properties: map[string]string{}},
		})
		require.EqualError(t, err, "property rootPath must not be empty")
	})

	t.Run("rootPath is created", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "nested", "store")
		store := newTestStoreWithProperties(t, map[string]string{"rootPath": root})
		assert.DirExists(t, root)
		assert.Equal(t, root, store.rootPath)
	})

	t.Run("relative rootPath is resolved to an absolute path", func(t *testing.T) {
		t.Chdir(t.TempDir())
		store := newTestStoreWithProperties(t, map[string]string{"rootPath": "relative/store"})
		assert.True(t, filepath.IsAbs(store.rootPath))
		assert.DirExists(t, store.rootPath)
	})

	t.Run("rootPath must not be a file", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "file")
		require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))

		store := NewLocalStorage(logger.NewLogger("test"))
		err := store.Init(t.Context(), binarystore.Metadata{
			Base: contribMetadata.Base{Properties: map[string]string{"rootPath": file}},
		})
		require.EqualError(t, err, "property rootPath represents a file and not a directory")
	})

	t.Run("rootPath symlinks are resolved", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("symlink creation requires elevated privileges on Windows")
		}
		base := t.TempDir()
		target := filepath.Join(base, "target")
		require.NoError(t, os.Mkdir(target, 0o700))
		link := filepath.Join(base, "link")
		require.NoError(t, os.Symlink(target, link))

		store := newTestStoreWithProperties(t, map[string]string{"rootPath": filepath.Join(link, "store")})
		resolvedTarget, err := filepath.EvalSymlinks(target)
		require.NoError(t, err)
		assert.Equal(t, filepath.Join(resolvedTarget, "store"), store.rootPath)
	})

	t.Run("disallowed rootPath is rejected", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("disallowed locations are Unix paths")
		}
		paths := []string{
			"/",
			"/proc",
			"/sys/kernel",
			"/boot",
			"/var",
			"/var/run/secrets/kubernetes.io",
		}
		resolved, err := resolveExistingSymlinks("/var/run/secrets")
		require.NoError(t, err)
		paths = append(paths, filepath.Dir(resolved), filepath.Join(resolved, "kubernetes.io"))
		for _, p := range paths {
			store := NewLocalStorage(logger.NewLogger("test"))
			err := store.Init(t.Context(), binarystore.Metadata{
				Base: contribMetadata.Base{Properties: map[string]string{"rootPath": p}},
			})
			require.EqualError(t, err, "property rootPath points to a disallowed location", p)
		}
	})

	t.Run("metadata is matched case-insensitively", func(t *testing.T) {
		store := newTestStoreWithProperties(t, map[string]string{
			"ROOTPATH": t.TempDir(),
			"PREFIX":   "tenant-a",
			"MAXSIZE":  "1024",
		})
		assert.Equal(t, "tenant-a", store.prefix)
		assert.Equal(t, int64(1024), store.maxSize)
	})

	t.Run("maxSize defaults to unlimited", func(t *testing.T) {
		store := newTestStore(t, "")
		assert.Zero(t, store.maxSize)
	})

	t.Run("maxSize accepts byte size quantities", func(t *testing.T) {
		store := newTestStoreWithProperties(t, map[string]string{
			"rootPath": t.TempDir(),
			"maxSize":  "1Ki",
		})
		assert.Equal(t, int64(1024), store.maxSize)
	})

	t.Run("maxSize must not be negative", func(t *testing.T) {
		store := NewLocalStorage(logger.NewLogger("test"))
		err := store.Init(t.Context(), binarystore.Metadata{
			Base: contribMetadata.Base{Properties: map[string]string{
				"rootPath": t.TempDir(),
				"maxSize":  "-1",
			}},
		})
		require.EqualError(t, err, "maxSize must not be negative")
	})

	t.Run("invalid prefix is rejected", func(t *testing.T) {
		for _, prefix := range []string{"/leading", "trailing/", "a/../b", "a//b", "back\\slash"} {
			store := NewLocalStorage(logger.NewLogger("test"))
			err := store.Init(t.Context(), binarystore.Metadata{
				Base: contribMetadata.Base{Properties: map[string]string{
					"rootPath": t.TempDir(),
					"prefix":   prefix,
				}},
			})
			require.ErrorIs(t, err, binarystore.ErrInvalidPrefix, prefix)
		}
	})
}

func TestGetComponentMetadata(t *testing.T) {
	md := newTestStore(t, "").GetComponentMetadata()
	require.NotNil(t, md)
	assert.Contains(t, md, "rootPath")
	assert.Contains(t, md, "prefix")
	assert.Contains(t, md, "maxSize")
}

// --- helpers ---

var (
	errMustNotRead = errors.New("data must not be read")
	errReadFailed  = errors.New("read failed")
)

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) {
	return f(p)
}

func blockingReader(started chan<- struct{}, release <-chan struct{}, payload string) io.Reader {
	return readerFunc(func(p []byte) (int, error) {
		close(started)
		<-release
		return copy(p, payload), io.EOF
	})
}

func requireMutationLockState(t *testing.T, entries, refs int) {
	t.Helper()
	require.Eventually(t, func() bool {
		mutationLocks.mu.Lock()
		defer mutationLocks.mu.Unlock()
		if len(mutationLocks.entries) != entries {
			return false
		}
		if entries == 0 {
			return true
		}
		return mutationLocks.entries[0].refs == refs
	}, time.Second, time.Millisecond)
}

type trackedUploadReader struct {
	io.Reader
	closed bool
}

func (r *trackedUploadReader) Close() error {
	r.closed = true
	return nil
}

func (*trackedUploadReader) WriteTo(io.Writer) (int64, error) {
	return 0, errMustNotRead
}

// unreadableReader fails on any read, proving that requests rejected before
// the body is consumed never pay for the read.
type unreadableReader struct{}

func (*unreadableReader) Read([]byte) (int, error) {
	return 0, errMustNotRead
}

// failingReader delivers some data and then fails, simulating a broken
// upstream stream mid-upload.
type failingReader struct{ sent bool }

func (r *failingReader) Read(p []byte) (int, error) {
	if !r.sent {
		r.sent = true
		return copy(p, "partial"), nil
	}
	return 0, errReadFailed
}

func mustGet(t *testing.T, store binarystore.BinaryStore, fileName string) []byte {
	t.Helper()

	resp, err := store.Get(t.Context(), &binarystore.GetRequest{FileName: fileName})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Data)
	defer func() {
		assert.NoError(t, resp.Data.Close())
	}()

	data, err := io.ReadAll(resp.Data)
	require.NoError(t, err)
	return data
}

// names returns the slash-separated paths of all regular files beneath root,
// sorted, for asserting on the on-disk layout.
func names(t *testing.T, root string) []string {
	t.Helper()

	out := []string{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		out = append(out, filepath.ToSlash(rel))
		return nil
	})
	require.NoError(t, err)
	sort.Strings(out)
	return out
}
