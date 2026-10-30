/*
Copyright 2021 The Dapr Authors
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
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	path := "/files"
	if runtime.GOOS == "windows" {
		path = "C:\\files"
	}
	m.Properties = map[string]string{"rootPath": path}
	localStorage := NewLocalStorage(logger.NewLogger("test")).(*LocalStorage)
	meta, err := localStorage.parseMetadata(m)
	require.NoError(t, err)
	assert.Equal(t, path, meta.RootPath)
}

func TestValidateRootPath(t *testing.T) {
	// Get the current working directory
	cwd, err := os.Getwd()
	require.NoError(t, err)

	// Set up some things in the FS
	tmpDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "aaa/bbb"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "aaa/ccc"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "zzz/aaa"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "notgood"), 0o755))
	require.NoError(t, os.Symlink(filepath.Join(tmpDir, "aaa/bbb"), filepath.Join(tmpDir, "zzz/link")))
	require.NoError(t, os.Symlink(filepath.Join(tmpDir, "notgood"), filepath.Join(tmpDir, "aaa/notgood")))
	f, err := os.Create(filepath.Join(tmpDir, "aaa/file"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	rootDir := "/"
	if runtime.GOOS == "windows" {
		rootDir = "C:\\"
	}

	// Set the list of disallowed paths to some locations that don't exist
	// This is because the list contains folders that otherwise are resolved as symlinks in some OS's (like macOS)
	oldDisallowedRootPaths := disallowedRootPaths
	disallowedRootPaths = []string{
		// Explicitly set both the Linux and Windows formats
		"/notgood",
		"C:\\notgood",
		filepath.Join(joinWithMustEvalSymlinks(tmpDir), "notgood"),
	}
	defer func() {
		disallowedRootPaths = oldDisallowedRootPaths
	}()

	tests := []struct {
		name     string
		rootPath string
		wantRes  string
		// String that will be matched in the error
		// If no error is expected, set this to an empty string
		wantErr string
	}{
		{name: "empty", rootPath: "", wantErr: "must not be empty"},
		{name: "relative path 1", rootPath: "path", wantRes: filepath.Join(cwd, "path")},
		{name: "relative path 2", rootPath: filepath.Clean("../path"), wantRes: filepath.Join(cwd, "..", "path")},
		{name: "existing path 1", rootPath: filepath.Join(tmpDir, "aaa/bbb"), wantRes: joinWithMustEvalSymlinks(tmpDir, "aaa/bbb")},
		{name: "existing path 2", rootPath: filepath.Join(tmpDir, "zzz/aaa"), wantRes: joinWithMustEvalSymlinks(tmpDir, "zzz/aaa")},
		{name: "path does not exist 1", rootPath: filepath.Join(tmpDir, "zzz/foo"), wantRes: filepath.Join(joinWithMustEvalSymlinks(tmpDir, "zzz"), "foo")},
		{name: "path does not exist 1", rootPath: filepath.Join(tmpDir, "zzz/aaa/deep/deep"), wantRes: filepath.Join(joinWithMustEvalSymlinks(tmpDir, "zzz/aaa"), "deep/deep")},
		{name: "resolve symlinks", rootPath: filepath.Join(tmpDir, "zzz/link"), wantRes: joinWithMustEvalSymlinks(tmpDir, "aaa/bbb")},
		{name: "resolve symlinks subfolder", rootPath: filepath.Join(tmpDir, "zzz/link/sub"), wantRes: filepath.Join(joinWithMustEvalSymlinks(tmpDir, "aaa/bbb"), "sub")},
		{name: "file", rootPath: filepath.Join(tmpDir, "aaa/file"), wantErr: "not a directory"},
		{name: "file in higher level", rootPath: filepath.Join(tmpDir, "aaa/file/2"), wantErr: "not a directory"},
		{name: "disallowed path 1", rootPath: filepath.Join(rootDir, "notgood"), wantErr: "disallowed location"},
		{name: "disallowed path 1 subfolder", rootPath: filepath.Join(rootDir, "notgood", "foo"), wantErr: "disallowed location"},
		{name: "disallowed path 2", rootPath: filepath.Join(tmpDir, "notgood"), wantErr: "disallowed location"},
		{name: "disallowed path 2 subfolder", rootPath: filepath.Join(tmpDir, "notgood", "foo"), wantErr: "disallowed location"},
		{name: "symlink to disallowed path", rootPath: filepath.Join(tmpDir, "aaa/notgood"), wantErr: "disallowed location"},
		{name: "symlink to disallowed path subfolder", rootPath: filepath.Join(tmpDir, "aaa/notgood/foo"), wantErr: "disallowed location"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := validateRootPath(tt.rootPath)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantRes, res)
		})
	}
}

func TestOperations(t *testing.T) {
	ls := newTestLocalStorage(t)
	rootPath := ls.metadata.RootPath

	// Names that point above the root, or are absolute, map to a path inside the root.
	type file struct {
		name string
		rel  string
	}
	files := []file{
		{name: "a.txt", rel: "a.txt"},
		{name: "nested/dir/b.txt", rel: filepath.Join("nested", "dir", "b.txt")},
		{name: "../c.txt", rel: "c.txt"},
		{name: "/d.txt", rel: "d.txt"},
	}
	if runtime.GOOS == "windows" {
		files = append(files, file{name: `C:\e.txt`, rel: "e.txt"})
	}
	want := make([]string, 0, len(files))
	for _, f := range files {
		res, err := invoke(t, ls, bindings.CreateOperation, f.name, []byte("data "+f.name))
		require.NoError(t, err)
		var created createResponse
		require.NoError(t, json.Unmarshal(res.Data, &created))
		assert.Equal(t, f.rel, created.FileName)

		b, err := os.ReadFile(filepath.Join(rootPath, f.rel))
		require.NoError(t, err)
		assert.Equal(t, "data "+f.name, string(b))

		res, err = invoke(t, ls, bindings.GetOperation, f.name, nil)
		require.NoError(t, err)
		assert.Equal(t, "data "+f.name, string(res.Data))

		want = append(want, filepath.Join(rootPath, f.rel))
	}

	res, err := invoke(t, ls, bindings.ListOperation, "", nil)
	require.NoError(t, err)
	var listed []string
	require.NoError(t, json.Unmarshal(res.Data, &listed))
	assert.ElementsMatch(t, want, listed)

	res, err = invoke(t, ls, bindings.ListOperation, "nested", nil)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(res.Data, &listed))
	assert.Equal(t, []string{filepath.Join(rootPath, "nested", "dir", "b.txt")}, listed)

	for _, f := range files {
		_, err = invoke(t, ls, bindings.DeleteOperation, f.name, nil)
		require.NoError(t, err)
		assert.NoFileExists(t, filepath.Join(rootPath, f.rel))

		_, err = invoke(t, ls, bindings.GetOperation, f.name, nil)
		require.ErrorContains(t, err, "file not found")
	}

	// The root directory is created again if it is removed after Init.
	require.NoError(t, os.RemoveAll(rootPath))
	_, err = invoke(t, ls, bindings.CreateOperation, "a.txt", []byte("again"))
	require.NoError(t, err)
	assert.FileExists(t, filepath.Join(rootPath, "a.txt"))
}

func TestSymlinkOutsideRoot(t *testing.T) {
	outside := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outside, "file.txt"), []byte("outside"), 0o600))

	ls := newTestLocalStorage(t)
	require.NoError(t, os.Symlink(outside, filepath.Join(ls.metadata.RootPath, "link")))

	_, err := invoke(t, ls, bindings.CreateOperation, "link/new.txt", []byte("data"))
	require.Error(t, err)
	_, err = invoke(t, ls, bindings.GetOperation, "link/file.txt", nil)
	require.Error(t, err)
	_, err = invoke(t, ls, bindings.DeleteOperation, "link/file.txt", nil)
	require.Error(t, err)

	b, err := os.ReadFile(filepath.Join(outside, "file.txt"))
	require.NoError(t, err)
	assert.Equal(t, "outside", string(b))
	assert.NoFileExists(t, filepath.Join(outside, "new.txt"))
}

func newTestLocalStorage(t *testing.T) *LocalStorage {
	t.Helper()
	ls := NewLocalStorage(logger.NewLogger("test")).(*LocalStorage)
	m := bindings.Metadata{}
	m.Properties = map[string]string{"rootPath": t.TempDir()}
	require.NoError(t, ls.Init(t.Context(), m))
	t.Cleanup(func() { assert.NoError(t, ls.Close()) })
	return ls
}

func invoke(t *testing.T, ls *LocalStorage, op bindings.OperationKind, fileName string, data []byte) (*bindings.InvokeResponse, error) {
	t.Helper()
	return ls.Invoke(t.Context(), &bindings.InvokeRequest{
		Operation: op,
		Data:      data,
		Metadata:  map[string]string{fileNameMetadataKey: fileName},
	})
}

func joinWithMustEvalSymlinks(v ...string) string {
	r, err := filepath.EvalSymlinks(filepath.Join(v...))
	if err != nil {
		panic(err)
	}
	return r
}
