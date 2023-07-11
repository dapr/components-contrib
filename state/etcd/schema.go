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

package etcd

import (
	"encoding/json"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pbv2 "github.com/dapr/components-contrib/internal/proto/state/etcd/v2"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
)

// schemaMarshaller is an interface for encoding and decoding values which are
// written and read from ETCD. Different storage schema versions store values
// in different formats or envelopes.
type schemaMarshaller interface {
	// encode the value in the correct storage schema.
	encode(data any, ttlInSeconds *int64) (string, error)

	// decode the value from the correct storage schema, optionally returning
	// metadata extracted from the envelope.
	decode(data []byte) ([]byte, map[string]string, error)
}

type schemaV1 struct{}

func (schemaV1) encode(data any, _ *int64) (string, error) {
	reqVal, err := utils.Marshal(data, json.Marshal)
	if err != nil {
		return "", err
	}
	return string(reqVal), nil
}

func (schemaV1) decode(data []byte) ([]byte, map[string]string, error) {
	return data, nil, nil
}

type schemaV2 struct{}

func (schemaV2) encode(data any, ttlInSeconds *int64) (string, error) {
	dataB, err := utils.JSONStringify(data)
	if err != nil {
		return "", err
	}

	var duration durationpb.Duration
	if ttlInSeconds != nil {
		duration = durationpb.Duration{Seconds: *ttlInSeconds}
	}

	value, err := proto.Marshal(&pbv2.Value{
		Data: dataB,
		Ts:   timestamppb.New(time.Now().UTC()),
		Ttl:  &duration,
	})

	return string(value), err
}

func (schemaV2) decode(data []byte) ([]byte, map[string]string, error) {
	var value pbv2.Value
	if err := proto.Unmarshal(data, &value); err != nil {
		return nil, nil, err
	}

	var metadata map[string]string
	if value.Ttl != nil {
		metadata = map[string]string{
			state.GetRespMetaKeyTTLExpireTime: value.Ts.AsTime().Add(value.Ttl.AsDuration()).Format(time.RFC3339),
		}
	}

	return value.Data, metadata, nil
}
