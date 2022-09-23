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

	ipfsOptions "github.com/ipfs/interface-go-ipfs-core/options"
	ipfsPath "github.com/ipfs/interface-go-ipfs-core/path"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
)

// Handler for the "pin-add" operation, which adds a new pin
func (b *IPFSBinding) pinAddOperation(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	reqMetadata := &pinAddRequestMetadata{}
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

	opts, err := reqMetadata.PinAddOptions()
	if err != nil {
		return nil, err
	}
	err = b.ipfsAPI.Pin().Add(ctx, p, opts...)
	if err != nil {
		return nil, err
	}

	return &bindings.InvokeResponse{
		Data:     nil,
		Metadata: nil,
	}, nil
}

type pinAddRequestMetadata struct {
	Path      string `mapstructure:"path"`
	Recursive *bool  `mapstructure:"recursive"`
}

func (m *pinAddRequestMetadata) FromMap(mp map[string]string) (err error) {
	if len(mp) > 0 {
		err = metadata.DecodeMetadata(mp, m)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *pinAddRequestMetadata) PinAddOptions() ([]ipfsOptions.PinAddOption, error) {
	opts := []ipfsOptions.PinAddOption{}
	if m.Recursive != nil {
		opts = append(opts, ipfsOptions.Pin.Recursive(*m.Recursive))
	} else {
		opts = append(opts, ipfsOptions.Pin.Recursive(true))
	}
	return opts, nil
}
