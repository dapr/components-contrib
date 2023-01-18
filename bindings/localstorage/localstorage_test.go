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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
)

func TestParseMetadata(t *testing.T) {
	m := bindings.Metadata{}
	m.Properties = map[string]string{"rootPath": filepath.Clean("/files")}
	localStorage := NewLocalStorage(logger.NewLogger("test")).(*LocalStorage)
	meta, err := localStorage.parseMetadata(m)
	require.NoError(t, err)
	assert.Equal(t, "/files", meta.RootPath)
}

func TestValidateRootPath(t *testing.T) {
	// Set up some things in the FS
	tmpDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "aaa/bbb"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "aaa/ccc"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "zzz/aaa"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "notgood"), 0o755))
	require.NoError(t, os.Symlink(filepath.Join(tmpDir, "aaa/bbb"), filepath.Join(tmpDir, "zzz/link")))
	require.NoError(t, os.Symlink(filepath.Join(tmpDir, "notgood"), filepath.Join(tmpDir, "aaa/notgood")))
	f, err := os.Create(filepath.Join(tmpDir, "aaa/file"))
	f.Close()
	require.NoError(t, err)

	// Set the list of disallowed paths to some locations that don't exist
	// This is because the list contains folders that otherwise are resolved as symlinks in some OS's (like macOS)
	oldDisallowedRootPaths := disallowedRootPaths
	disallowedRootPaths = []string{
		filepath.Clean("/notgood"),
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
		{name: "relative path 1", rootPath: "path", wantErr: "must be an absolute path"},
		{name: "relative path 2", rootPath: filepath.Clean("../path"), wantErr: "must be an absolute path"},
		{name: "existing path 1", rootPath: filepath.Join(tmpDir, "aaa/bbb"), wantRes: joinWithMustEvalSymlinks(tmpDir, "aaa/bbb")},
		{name: "existing path 2", rootPath: filepath.Join(tmpDir, "zzz/aaa"), wantRes: joinWithMustEvalSymlinks(tmpDir, "zzz/aaa")},
		{name: "path does not exist 1", rootPath: filepath.Join(tmpDir, "zzz/foo"), wantRes: filepath.Join(joinWithMustEvalSymlinks(tmpDir, "zzz"), "foo")},
		{name: "path does not exist 1", rootPath: filepath.Join(tmpDir, "zzz/aaa/deep/deep"), wantRes: filepath.Join(joinWithMustEvalSymlinks(tmpDir, "zzz/aaa"), "deep/deep")},
		{name: "resolve symlinks", rootPath: filepath.Join(tmpDir, "zzz/link"), wantRes: joinWithMustEvalSymlinks(tmpDir, "aaa/bbb")},
		{name: "resolve symlinks subfolder", rootPath: filepath.Join(tmpDir, "zzz/link/sub"), wantRes: filepath.Join(joinWithMustEvalSymlinks(tmpDir, "aaa/bbb"), "sub")},
		{name: "file", rootPath: filepath.Join(tmpDir, "aaa/file"), wantErr: "not a directory"},
		{name: "file in higher level", rootPath: filepath.Join(tmpDir, "aaa/file/2"), wantErr: "not a directory"},
		{name: "disallowed path 1", rootPath: filepath.Clean("/notgood"), wantErr: "disallowed location"},
		{name: "disallowed path 1 subfolder", rootPath: filepath.Clean("/notgood/foo"), wantErr: "disallowed location"},
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

func joinWithMustEvalSymlinks(v ...string) string {
	r, err := filepath.EvalSymlinks(filepath.Join(v...))
	if err != nil {
		panic(err)
	}
	return r
}
