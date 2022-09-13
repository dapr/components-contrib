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
	"os"
	"path/filepath"
	"strconv"

	securejoin "github.com/cyphar/filepath-securejoin"
	"github.com/google/uuid"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const (
	fileNameMetadataKey = "fileName"
)

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
func (ls *LocalStorage) Init(metadata bindings.Metadata) error {
	m, err := ls.parseMetadata(metadata)
	if err != nil {
		return err
	}
	ls.metadata = m

	err = os.MkdirAll(ls.metadata.RootPath, 0o777)
	if err != nil {
		return fmt.Errorf("unable to create directory specified by 'rootPath': %s", ls.metadata.RootPath)
	}

	return nil
}

func (ls *LocalStorage) parseMetadata(meta bindings.Metadata) (*Metadata, error) {
	var m Metadata
	err := metadata.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return nil, err
	}

	return &m, nil
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

	absPath, relPath, err := getSecureAbsRelPath(ls.metadata.RootPath, filename)
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(filepath.Dir(absPath), 0o777)
	if err != nil {
		return nil, err
	}

	f, err := os.Create(absPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	numBytes, err := f.Write(req.Data)
	if err != nil {
		return nil, err
	}

	ls.logger.Debugf("wrote file: %s. numBytes: %d", absPath, numBytes)

	resp := createResponse{
		FileName: relPath,
	}

	b, err := json.Marshal(resp)
	if err != nil {
		return nil, err
	}

	return &bindings.InvokeResponse{
		Data: b,
	}, nil
}

func (ls *LocalStorage) get(filename string, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	absPath, _, err := getSecureAbsRelPath(ls.metadata.RootPath, filename)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(absPath)
	if err != nil {
		ls.logger.Debugf("%s", err)

		return nil, err
	}

	b, err := io.ReadAll(f)
	if err != nil {
		ls.logger.Debugf("%s", err)

		return nil, err
	}

	ls.logger.Debugf("read file: %s. size: %d bytes", absPath, len(b))

	return &bindings.InvokeResponse{
		Data: b,
	}, nil
}

func (ls *LocalStorage) delete(filename string, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	absPath, _, err := getSecureAbsRelPath(ls.metadata.RootPath, filename)
	if err != nil {
		return nil, err
	}

	err = os.Remove(absPath)
	if err != nil {
		return nil, err
	}

	ls.logger.Debugf("removed file: %s.", absPath)

	return nil, nil
}

func (ls *LocalStorage) list(filename string, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	absPath, _, err := getSecureAbsRelPath(ls.metadata.RootPath, filename)
	if err != nil {
		return nil, err
	}

	fi, err := os.Stat(absPath)
	if err != nil {
		return nil, err
	}

	if !fi.IsDir() {
		msg := fmt.Sprintf("unable to list files as the file specified is not a directory [%s]", absPath)

		return nil, errors.New(msg)
	}

	files, err := walkPath(absPath)
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(files)
	if err != nil {
		return nil, err
	}

	return &bindings.InvokeResponse{
		Data: b,
	}, nil
}

func getSecureAbsRelPath(rootPath string, filename string) (absPath string, relPath string, err error) {
	absPath, err = securejoin.SecureJoin(rootPath, filename)
	if err != nil {
		return
	}
	relPath, err = filepath.Rel(rootPath, absPath)
	if err != nil {
		return
	}

	return
}

func walkPath(root string) ([]string, error) {
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			files = append(files, path)
		}

		return nil
	})

	return files, err
}

// Invoke is called for output bindings.
func (ls *LocalStorage) Invoke(_ context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	filename := ""
	if val, ok := req.Metadata[fileNameMetadataKey]; ok && val != "" {
		filename = val
	} else if req.Operation == bindings.CreateOperation {
		filename = uuid.New().String()
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
