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
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"github.com/google/uuid"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
)

const (
	fileNameMetadataKey = "fileName"
)

// List of root paths that are disallowed
var disallowedRootPaths = []string{
	filepath.Clean("/proc"),
	filepath.Clean("/sys"),
	filepath.Clean("/boot"),
	// See: https://github.com/dapr/components-contrib/issues/2444
	filepath.Clean("/var/run/secrets"),
}

// LocalStorage allows saving files to disk.
type LocalStorage struct {
	metadata *Metadata
	logger   logger.Logger
}

// Metadata defines the metadata.
type Metadata struct {
	RootPath string `json:"rootPath"`
}

type createResponse struct {
	FileName string `json:"fileName"`
}

// NewLocalStorage returns a new LocalStorage instance.
func NewLocalStorage(logger logger.Logger) bindings.OutputBinding {
	return &LocalStorage{logger: logger}
}

// Init performs metadata parsing.
func (ls *LocalStorage) Init(_ context.Context, metadata bindings.Metadata) error {
	m, err := ls.parseMetadata(metadata)
	if err != nil {
		return fmt.Errorf("failed to parse metadata: %w", err)
	}
	ls.metadata = m

	err = os.MkdirAll(ls.metadata.RootPath, 0o777)
	if err != nil {
		return fmt.Errorf("unable to create directory specified by 'rootPath' %s: %w", ls.metadata.RootPath, err)
	}

	return nil
}

func (ls *LocalStorage) parseMetadata(meta bindings.Metadata) (*Metadata, error) {
	var m Metadata
	err := kitmd.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return nil, err
	}

	m.RootPath, err = validateRootPath(m.RootPath)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

func validateRootPath(rootPath string) (string, error) {
	var err error

	if rootPath == "" {
		return "", errors.New("property rootPath must not be empty")
	}

	// If the root path is relative, resolve it as absolute
	rootPath = filepath.Clean(rootPath)
	if !filepath.IsAbs(rootPath) {
		var cwd string
		cwd, err = os.Getwd()
		if err != nil {
			return "", fmt.Errorf("failed to obtain current working directory: %w", err)
		}
		rootPath = filepath.Join(cwd, rootPath)
	}

	// Resolve symlinks
	// If rootPath doesn't (yet) exist, we need to check its parent folders
	checkPath := rootPath
	sep := string(os.PathSeparator)
	// Per docs of filepath.Dir and filepath.Clean (invoked earlier): "The returned path does not end in a separator unless it is the root directory."
	var fi os.FileInfo
	for !strings.HasSuffix(checkPath, sep) {
		fi, err = os.Stat(checkPath)
		if err == nil {
			if !fi.IsDir() {
				return "", errors.New("property rootPath represents a file and not a directory")
			}
			break
		} else if os.IsNotExist(err) {
			checkPath = filepath.Dir(checkPath)
			continue
		} else {
			return "", fmt.Errorf("error getting stat for path %s: %w", checkPath, err)
		}
	}
	resolvedCheckPath, err := filepath.EvalSymlinks(checkPath)
	if err != nil {
		return "", fmt.Errorf("failed to evaluate symlinks in rootPath: %w", err)
	}
	rootPath = filepath.Join(resolvedCheckPath, rootPath[len(checkPath):])

	// Check if the path is in a disallowed location
	for _, p := range disallowedRootPaths {
		if rootPath == p || strings.HasPrefix(rootPath, p+sep) {
			return "", errors.New("property rootPath points to a disallowed location")
		}
	}

	return rootPath, nil
}

// Operations enumerates supported binding operations.
func (ls *LocalStorage) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.CreateOperation,
		bindings.GetOperation,
		bindings.ListOperation,
		bindings.DeleteOperation,
	}
}

func (ls *LocalStorage) create(filename string, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	d, err := strconv.Unquote(string(req.Data))
	if err == nil {
		req.Data = []byte(d)
	}

	decoded, err := base64.StdEncoding.DecodeString(string(req.Data))
	if err == nil {
		req.Data = decoded
	}

	absPath, relPath := resolvePath(ls.metadata.RootPath, filename)

	// The root directory may have been removed since Init.
	err = os.MkdirAll(ls.metadata.RootPath, 0o777)
	if err != nil {
		return nil, fmt.Errorf("error creating directory %s: %w", ls.metadata.RootPath, err)
	}

	root, err := ls.openRoot()
	if err != nil {
		return nil, err
	}
	defer root.Close()

	dir := filepath.Dir(relPath)
	err = root.MkdirAll(dir, 0o777)
	if err != nil {
		return nil, fmt.Errorf("error creating directory %s: %w", filepath.Join(ls.metadata.RootPath, dir), err)
	}

	// O_NONBLOCK keeps the open from waiting for a reader if the name is a
	// FIFO; it then fails with ENXIO.
	f, err := root.OpenFile(relPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|syscall.O_NONBLOCK, 0o666)
	if err != nil {
		return nil, fmt.Errorf("error creating file %s: %w", absPath, err)
	}
	defer f.Close()

	err = checkRegularFile(f, absPath)
	if err != nil {
		return nil, err
	}

	numBytes, err := f.Write(req.Data)
	if err != nil {
		return nil, fmt.Errorf("error writing to file %s: %w", absPath, err)
	}

	ls.logger.Debugf("wrote file: %s. numBytes: %d", absPath, numBytes)

	resp := createResponse{
		FileName: relPath,
	}

	b, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("error encoding response as JSON: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: b,
	}, nil
}

func (ls *LocalStorage) get(filename string, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	absPath, relPath := resolvePath(ls.metadata.RootPath, filename)

	root, err := os.OpenRoot(ls.metadata.RootPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("file not found: %s", absPath)
		}
		return nil, fmt.Errorf("error opening directory %s: %w", ls.metadata.RootPath, err)
	}
	defer root.Close()

	// O_NONBLOCK keeps the open from waiting for a writer if the name is a FIFO.
	f, err := root.OpenFile(relPath, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("file not found: %s", absPath)
		}
		return nil, fmt.Errorf("error opening path %s: %w", absPath, err)
	}
	defer f.Close()

	err = checkRegularFile(f, absPath)
	if err != nil {
		return nil, err
	}

	b, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("error reading file %s: %w", absPath, err)
	}

	ls.logger.Debugf("read file: %s. size: %d bytes", absPath, len(b))

	return &bindings.InvokeResponse{
		Data: b,
	}, nil
}

func (ls *LocalStorage) delete(filename string, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	absPath, relPath := resolvePath(ls.metadata.RootPath, filename)

	root, err := ls.openRoot()
	if err != nil {
		return nil, err
	}
	defer root.Close()

	err = root.Remove(relPath)
	if err != nil {
		return nil, fmt.Errorf("error deleting file %s: %w", absPath, err)
	}

	ls.logger.Debugf("removed file: %s", absPath)

	return nil, nil
}

func (ls *LocalStorage) list(filename string, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	absPath, relPath := resolvePath(ls.metadata.RootPath, filename)

	root, err := ls.openRoot()
	if err != nil {
		return nil, err
	}
	defer root.Close()

	fi, err := root.Stat(relPath)
	if err != nil {
		return nil, fmt.Errorf("error getting stats for path %s: %w", absPath, err)
	}

	if !fi.IsDir() {
		return nil, fmt.Errorf("unable to list files as the file specified is not a directory: %s", absPath)
	}

	dir, err := root.OpenRoot(relPath)
	if err != nil {
		return nil, fmt.Errorf("error listing files in the directory %s: %w", absPath, err)
	}
	defer dir.Close()

	files, err := ls.walkPath(dir, absPath)
	if err != nil {
		return nil, fmt.Errorf("error listing files in the directory %s: %w", absPath, err)
	}

	b, err := json.Marshal(files)
	if err != nil {
		return nil, fmt.Errorf("error encoding response as JSON: %w", err)
	}

	return &bindings.InvokeResponse{
		Data: b,
	}, nil
}

// openRoot opens the root directory for a single operation.
//
// The root is opened again for every operation, so that create can recreate
// the directory if it is removed after Init. The trade-off is that the checks
// in validateRootPath (symlink resolution and the disallowed locations) only
// hold at Init: if rootPath itself is later replaced by a symlink, operations
// follow it. os.Root only keeps names below the opened directory from
// resolving outside it.
func (ls *LocalStorage) openRoot() (*os.Root, error) {
	root, err := os.OpenRoot(ls.metadata.RootPath)
	if err != nil {
		return nil, fmt.Errorf("error opening directory %s: %w", ls.metadata.RootPath, err)
	}
	return root, nil
}

// checkRegularFile returns an error if f is not a regular file, such as a
// directory, a FIFO or a device, which os.Root does not prevent opening.
func checkRegularFile(f *os.File, absPath string) error {
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("error getting stats for path %s: %w", absPath, err)
	}
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("not a regular file: %s", absPath)
	}
	return nil
}

// resolvePath maps a file name from a request to a path relative to the root
// directory, and to the matching absolute path. Volume names, leading
// separators and ".." elements cannot move the name above the root, so "../a",
// "/../a", "/a" and, on Windows, "C:\a" all refer to "a" inside the root. The
// name is cleaned with package path rather than path/filepath, because on
// Windows filepath.Clean reads a name such as `\..\a` as a UNC volume and keeps
// the "..". Files are then accessed through an os.Root, which also keeps
// symlinks from resolving to a location outside the root.
func resolvePath(rootPath string, filename string) (absPath string, relPath string) {
	filename = filename[len(filepath.VolumeName(filename)):]
	relPath = filepath.FromSlash(strings.TrimPrefix(path.Clean("/"+filepath.ToSlash(filename)), "/"))
	if relPath == "" {
		relPath = "."
	}

	return filepath.Join(rootPath, relPath), relPath
}

// walkPath lists the files in dir and its subdirectories as paths under
// prefix, in lexical order. Each subdirectory is opened as a child os.Root, so
// it is resolved once from the directory that listed it. A subdirectory that
// cannot be read because of its permissions, such as a root-owned lost+found,
// is skipped. It reads directories through os.Root directly because
// Root.FS() rejects names that are not valid UTF-8.
func (ls *LocalStorage) walkPath(dir *os.Root, prefix string) ([]string, error) {
	d, err := dir.Open(".")
	if err != nil {
		return nil, err
	}
	entries, err := d.ReadDir(-1)
	_ = d.Close()
	if err != nil {
		return nil, err
	}
	slices.SortFunc(entries, func(a, b os.DirEntry) int {
		return strings.Compare(a.Name(), b.Name())
	})

	var files []string
	for _, e := range entries {
		name := filepath.Join(prefix, e.Name())
		if !e.IsDir() {
			files = append(files, name)
			continue
		}
		var sub []string
		subdir, err := dir.OpenRoot(e.Name())
		if err == nil {
			sub, err = ls.walkPath(subdir, name)
			_ = subdir.Close()
		}
		if errors.Is(err, fs.ErrPermission) {
			ls.logger.Debugf("skipping unreadable directory %s: %v", name, err)
			continue
		}
		if err != nil {
			return nil, err
		}
		files = append(files, sub...)
	}

	return files, nil
}

// Invoke is called for output bindings.
func (ls *LocalStorage) Invoke(_ context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	filename := req.Metadata[fileNameMetadataKey]
	if filename == "" && req.Operation == bindings.CreateOperation {
		u, err := uuid.NewRandom()
		if err != nil {
			return nil, fmt.Errorf("failed to generate UUID: %w", err)
		}
		filename = u.String()
	}

	switch req.Operation {
	case bindings.CreateOperation:
		return ls.create(filename, req)
	case bindings.GetOperation:
		return ls.get(filename, req)
	case bindings.DeleteOperation:
		return ls.delete(filename, req)
	case bindings.ListOperation:
		return ls.list(filename, req)
	default:
		return nil, fmt.Errorf("unsupported operation %s", req.Operation)
	}
}

// GetComponentMetadata returns the metadata of the component.
func (ls *LocalStorage) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := Metadata{}
	_ = metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}

func (ls *LocalStorage) Close() error {
	return nil
}
