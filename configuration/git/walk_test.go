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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWalkTree_NonexistentScopeReturnsEmpty(t *testing.T) {
	root := t.TempDir()
	entries, err := walkTree(root, "nope", false, 0, nil)
	require.NoError(t, err)
	assert.Nil(t, entries)
}

func TestWalkTree_FileScopeReturnsError(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "scope"), []byte("file not dir"), 0o600))
	_, err := walkTree(root, "scope", false, 0, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a directory")
}

func TestWalkTree_HiddenSkipDefault(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "visible.txt"), []byte("v"), 0o600))
	require.NoError(t, os.MkdirAll(filepath.Join(root, ".hidden"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, ".hidden", "ignored.txt"), []byte("x"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(root, ".dotfile"), []byte("x"), 0o600))

	entries, err := walkTree(root, ".", false, 0, nil)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "visible.txt", entries[0].RelPath)
}

func TestWalkTree_GitDirAlwaysExcluded(t *testing.T) {
	// .git/config typically contains the remote URL and may embed credentials.
	// It must be skipped even with includeHidden=true.
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, ".git"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, ".git", "config"),
		[]byte("[remote \"origin\"]\n  url = https://user:tok@example.com/repo\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "k.txt"), []byte("v"), 0o600))

	entries, err := walkTree(root, ".", true, 0, nil)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "k.txt", entries[0].RelPath)
}

func TestWalkTree_FileSizeCap(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "small.txt"), []byte("ok"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "big.txt"), make([]byte, 4096), 0o600))

	entries, err := walkTree(root, ".", false, 1024, nil)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "small.txt", entries[0].RelPath)
}

func TestWalkTree_IncludeHidden(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "v.txt"), []byte("v"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(root, ".dotfile"), []byte("x"), 0o600))

	entries, err := walkTree(root, ".", true, 0, nil)
	require.NoError(t, err)
	assert.Len(t, entries, 2)
}

func TestWalkTree_NestedAndPosixPaths(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "agents", "weather"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "agents", "weather", "role.txt"), []byte("r"), 0o600))

	entries, err := walkTree(root, "agents", false, 0, nil)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "weather/role.txt", entries[0].RelPath)
}
