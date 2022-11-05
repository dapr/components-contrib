/*
Copyright 2022 The Dapr Authors
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

package jetstream

import (
	"fmt"
	"reflect"
	"strings"

	jsoniter "github.com/json-iterator/go"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// StateStore is a nats jetstream KV state store.
type StateStore struct {
	state.DefaultBulkStore
	nc     *nats.Conn
	json   jsoniter.API
	bucket nats.KeyValue
	logger logger.Logger
}

type jetstreamMetadata struct {
	Name    string
	NatsURL string
	Jwt     string
	SeedKey string
	Bucket  string
}

// NewJetstreamStateStore returns a new nats jetstream KV state store.
func NewJetstreamStateStore(logger logger.Logger) state.Store {
	s := &StateStore{
		json:   jsoniter.ConfigFastest,
		logger: logger,
	}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

// Init does parse metadata and establishes connection to nats broker.
func (js *StateStore) Init(metadata state.Metadata) error {
	meta, err := js.getMetadata(metadata)
	if err != nil {
		return err
	}

	var opts []nats.Option
	opts = append(opts, nats.Name(meta.Name))

	// Set nats.UserJWT options when jwt and seed key is provided.
	if meta.Jwt != "" && meta.SeedKey != "" {
		opts = append(opts, nats.UserJWT(func() (string, error) {
			return meta.Jwt, nil
		}, func(nonce []byte) ([]byte, error) {
			return sigHandler(meta.SeedKey, nonce)
		}))
	}

	js.nc, err = nats.Connect(meta.NatsURL, opts...)
	if err != nil {
		return err
	}

	jsc, err := js.nc.JetStream()
	if err != nil {
		return err
	}

	js.bucket, err = jsc.KeyValue(meta.Bucket)
	if err != nil {
		return err
	}

	return nil
}

func (js *StateStore) Features() []state.Feature {
	return nil
}

// Get retrieves state with a key.
func (js *StateStore) Get(req *state.GetRequest) (*state.GetResponse, error) {
	entry, err := js.bucket.Get(escape(req.Key))
	if err != nil {
		return nil, err
	}

	return &state.GetResponse{
		Data: entry.Value(),
	}, nil
}

// Set stores value for a key.
func (js *StateStore) Set(req *state.SetRequest) error {
	bt, _ := utils.Marshal(req.Value, js.json.Marshal)
	_, err := js.bucket.Put(escape(req.Key), bt)
	return err
}

// Delete performs a delete operation.
func (js *StateStore) Delete(req *state.DeleteRequest) error {
	return js.bucket.Delete(escape(req.Key))
}

func (js *StateStore) getMetadata(meta state.Metadata) (jetstreamMetadata, error) {
	var m jetstreamMetadata
	err := metadata.DecodeMetadata(meta.Properties, &m)
	if err != nil {
		return jetstreamMetadata{}, err
	}

	if m.NatsURL == "" {
		return jetstreamMetadata{}, fmt.Errorf("missing nats URL")
	}

	if m.Jwt != "" && m.SeedKey == "" {
		return jetstreamMetadata{}, fmt.Errorf("missing seed key")
	}

	if m.Jwt == "" && m.SeedKey != "" {
		return jetstreamMetadata{}, fmt.Errorf("missing jwt")
	}

	if m.Name == "" {
		m.Name = "dapr.io - statestore.jetstream"
	}

	if m.Bucket == "" {
		return jetstreamMetadata{}, fmt.Errorf("missing bucket")
	}

	return m, nil
}

// Handle nats signature request for challenge response authentication.
func sigHandler(seedKey string, nonce []byte) ([]byte, error) {
	kp, err := nkeys.FromSeed([]byte(seedKey))
	if err != nil {
		return nil, err
	}
	// Wipe our key on exit.
	defer kp.Wipe()

	sig, _ := kp.Sign(nonce)
	return sig, nil
}

// Escape dapr keys, because || is forbidden.
func escape(key string) string {
	return strings.ReplaceAll(key, "||", ".")
}

func (js *StateStore) GetComponentMetadata() map[string]string {
	metadataStruct := jetstreamMetadata{}
	metadataInfo := map[string]string{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo)
	return metadataInfo
}
