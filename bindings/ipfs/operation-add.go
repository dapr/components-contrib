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

	ipfsFiles "github.com/ipfs/go-ipfs-files"
	ipfsOptions "github.com/ipfs/interface-go-ipfs-core/options"

	"github.com/multiformats/go-multihash"

	"github.com/mitchellh/mapstructure"

	"github.com/dapr/components-contrib/bindings"
)

// Handler for the "add" operation, which adds a new file
func (b *IPFSBinding) addOperation(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	if len(req.Data) == 0 {
		return nil, errors.New("data is empty")
	}

	reqMetadata := &addRequestMetadata{}
	err := reqMetadata.FromMap(req.Metadata)
	if err != nil {
		return nil, err
	}

	opts, err := reqMetadata.UnixfsAddOptions()
	if err != nil {
		return nil, err
	}
	f := ipfsFiles.NewBytesFile(req.Data)
	resolved, err := b.ipfsAPI.Unixfs().Add(ctx, f, opts...)
	if err != nil {
		return nil, err
	}

	res := addOperationResponse{
		Path: resolved.String(),
	}
	enc, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	return &bindings.InvokeResponse{
		Data:     enc,
		Metadata: nil,
	}, nil
}

type addOperationResponse struct {
	Path string `json:"path"`
}

type addRequestMetadata struct {
	CidVersion  *int    `mapstructure:"cidVersion"`
	Pin         *bool   `mapstructure:"pin"`
	Hash        *string `mapstructure:"hash"`
	Inline      *bool   `mapstructure:"inline"`
	InlineLimit *int    `mapstructure:"inlineLimit"`
}

func (m *addRequestMetadata) FromMap(mp map[string]string) (err error) {
	if len(mp) > 0 {
		err = mapstructure.WeakDecode(mp, m)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *addRequestMetadata) UnixfsAddOptions() ([]ipfsOptions.UnixfsAddOption, error) {
	opts := []ipfsOptions.UnixfsAddOption{}
	if m.CidVersion != nil {
		opts = append(opts, ipfsOptions.Unixfs.CidVersion(*m.CidVersion))
	}
	if m.Pin != nil {
		opts = append(opts, ipfsOptions.Unixfs.Pin(*m.Pin))
	} else {
		opts = append(opts, ipfsOptions.Unixfs.Pin(true))
	}
	if m.Hash != nil {
		hash, ok := multihash.Names[*m.Hash]
		if !ok {
			return nil, fmt.Errorf("invalid hash %s", *m.Hash)
		}
		opts = append(opts, ipfsOptions.Unixfs.Hash(hash))
	}
	if m.Inline != nil {
		opts = append(opts, ipfsOptions.Unixfs.Inline(*m.Inline))
	}
	if m.InlineLimit != nil {
		opts = append(opts, ipfsOptions.Unixfs.InlineLimit(*m.InlineLimit))
	}
	return opts, nil
}
