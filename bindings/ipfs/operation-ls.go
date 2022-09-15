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
	"encoding/json"
	"errors"
	"fmt"

	ipfsPath "github.com/ipfs/interface-go-ipfs-core/path"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
)

// Handler for the "ls" operation, which retrieves a document
func (b *IPFSBinding) lsOperation(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	reqMetadata := &lsRequestMetadata{}
	err := reqMetadata.FromMap(req.Metadata)
	if err != nil {
		return nil, err
	}

	if reqMetadata.Path == "" {
		return nil, errors.New("metadata property 'path' is empty")
	}
	p := ipfsPath.New(reqMetadata.Path)
	err = p.IsValid()
	if err != nil {
		return nil, fmt.Errorf("invalid value for metadata property 'path': %v", err)
	}

	ls, err := b.ipfsAPI.Unixfs().Ls(ctx, p)
	if err != nil {
		return nil, err
	}

	res := lsOperationResponse{}
	for e := range ls {
		if e.Err != nil {
			return nil, e.Err
		}
		res = append(res, lsOperationResponseItem{
			Name: e.Name,
			Size: e.Size,
			Type: e.Type.String(),
			Cid:  e.Cid.String(),
		})
	}

	j, _ := json.Marshal(res)
	return &bindings.InvokeResponse{
		Data:     j,
		Metadata: nil,
	}, nil
}

type lsOperationResponseItem struct {
	Name string `json:"name,omitempty"`
	Size uint64 `json:"size,omitempty"`
	Cid  string `json:"cid,omitempty"`
	Type string `json:"type,omitempty"`
}

type lsOperationResponse []lsOperationResponseItem

type lsRequestMetadata struct {
	Path string `mapstructure:"path"`
}

func (m *lsRequestMetadata) FromMap(mp map[string]string) (err error) {
	if len(mp) > 0 {
		err = metadata.DecodeMetadata(mp, m)
		if err != nil {
			return err
		}
	}
	return nil
}
