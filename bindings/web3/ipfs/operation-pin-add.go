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

	ipfs_options "github.com/ipfs/interface-go-ipfs-core/options"
	ipfs_path "github.com/ipfs/interface-go-ipfs-core/path"

	"github.com/mitchellh/mapstructure"

	"github.com/dapr/components-contrib/bindings"
)

// Handler for the "pin-add" operation, which adds a new pin
func (h *IPFSBinding) pinAddOperation(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	reqMetadata := &pinAddRequestMetadata{}
	err := reqMetadata.FromMap(req.Metadata)
	if err != nil {
		return nil, err
	}

	if reqMetadata.Cid == "" {
		return nil, errors.New("metadata property 'cid' is empty")
	}
	p := ipfs_path.New(reqMetadata.Cid)
	err = p.IsValid()
	if err != nil {
		return nil, fmt.Errorf("invalid value for metadata property 'cid': %v", err)
	}

	opts, err := reqMetadata.PinAddOptions()
	if err != nil {
		return nil, err
	}
	err = h.ipfsAPI.Pin().Add(ctx, p, opts...)
	if err != nil {
		return nil, err
	}

	return &bindings.InvokeResponse{
		Data:     nil,
		Metadata: nil,
	}, nil
}

type pinAddRequestMetadata struct {
	Cid       string `mapstructure:"cid"`
	Recursive *bool  `mapstructure:"recursive"`
}

func (m *pinAddRequestMetadata) FromMap(mp map[string]string) (err error) {
	if len(mp) > 0 {
		err = mapstructure.WeakDecode(mp, m)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *pinAddRequestMetadata) PinAddOptions() ([]ipfs_options.PinAddOption, error) {
	opts := []ipfs_options.PinAddOption{}
	if m.Recursive != nil {
		opts = append(opts, ipfs_options.Pin.Recursive(*m.Recursive))
	} else {
		opts = append(opts, ipfs_options.Pin.Recursive(true))
	}
	return opts, nil
}
