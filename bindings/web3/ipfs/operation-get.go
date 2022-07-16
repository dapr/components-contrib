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

package ipfs

import (
	"context"
	"errors"
	"fmt"
	"io"

	ipfs_files "github.com/ipfs/go-ipfs-files"
	ipfs_path "github.com/ipfs/interface-go-ipfs-core/path"

	"github.com/mitchellh/mapstructure"

	"github.com/dapr/components-contrib/bindings"
)

// Handler for the "get" operation, which retrieves a document
func (h *IPFSBinding) getOperation(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	reqMetadata := &getRequestMetadata{}
	err := reqMetadata.FromMap(req.Metadata)
	if err != nil {
		return nil, err
	}

	if reqMetadata.Path == "" {
		return nil, errors.New("metadata property 'path' is empty")
	}
	p := ipfs_path.New(reqMetadata.Path)
	err = p.IsValid()
	if err != nil {
		return nil, fmt.Errorf("invalid value for metadata property 'path': %v", err)
	}

	res, err := h.ipfsAPI.Unixfs().Get(ctx, p)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	f, ok := res.(ipfs_files.File)
	if !ok {
		return nil, errors.New("path does not represent a file")
	}

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	return &bindings.InvokeResponse{
		Data:     data,
		Metadata: nil,
	}, nil
}

type getRequestMetadata struct {
	Path string `mapstructure:"path"`
}

func (m *getRequestMetadata) FromMap(mp map[string]string) (err error) {
	if len(mp) > 0 {
		err = mapstructure.WeakDecode(mp, m)
		if err != nil {
			return err
		}
	}
	return nil
}
