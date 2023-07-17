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

package statechange

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"

	r "github.com/dancannon/gorethink"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

// Binding represents RethinkDB change state input binding which fires handler with
// both the previous and current state store content each time there is a change.
type Binding struct {
	logger  logger.Logger
	session *r.Session
	config  StateConfig
	closed  atomic.Bool
	closeCh chan struct{}
	wg      sync.WaitGroup
}

// StateConfig is the binding config.
type StateConfig struct {
	r.ConnectOpts `mapstructure:",squash"`
	Table         string `mapstructure:"table"`
}

// NewRethinkDBStateChangeBinding returns a new RethinkDB actor event input binding.
func NewRethinkDBStateChangeBinding(logger logger.Logger) bindings.InputBinding {
	return &Binding{
		logger:  logger,
		closeCh: make(chan struct{}),
	}
}

// Init initializes the RethinkDB binding.
func (b *Binding) Init(ctx context.Context, metadata bindings.Metadata) error {
	cfg, err := metadataToConfig(metadata.Properties, b.logger)
	if err != nil {
		return fmt.Errorf("unable to parse metadata properties: %w", err)
	}
	b.config = cfg

	ses, err := r.Connect(b.config.ConnectOpts)
	if err != nil {
		return fmt.Errorf("error connecting to the database: %w", err)
	}
	b.session = ses

	return nil
}

// Read triggers the RethinkDB scheduler.
func (b *Binding) Read(ctx context.Context, handler bindings.Handler) error {
	if b.closed.Load() {
		return errors.New("binding is closed")
	}

	b.logger.Infof("subscribing to state changes in %s.%s...", b.config.Database, b.config.Table)
	cursor, err := r.DB(b.config.Database).
		Table(b.config.Table).
		Changes(r.ChangesOpts{
			IncludeTypes: true,
		}).
		Run(b.session, r.RunOpts{
			Context: ctx,
		})
	if err != nil {
		return fmt.Errorf("error connecting to table '%s': %w", b.config.Table, err)
	}

	readCtx, cancel := context.WithCancel(ctx)
	b.wg.Add(2)

	go func() {
		defer b.wg.Done()
		defer cancel()
		select {
		case <-b.closeCh:
		case <-readCtx.Done():
		}
	}()

	go func() {
		defer b.wg.Done()
		for readCtx.Err() == nil {
			var change interface{}
			ok := cursor.Next(&change)
			if !ok {
				b.logger.Errorf("error detecting change: %v", cursor.Err())
				continue
			}

			data, err := json.Marshal(change)
			if err != nil {
				b.logger.Errorf("error marshalling change handler: %v", err)
			}
			b.logger.Debugf("event: %s", string(data))

			resp := &bindings.ReadResponse{
				Data: data,
				Metadata: map[string]string{
					"store-address":  b.config.Address,
					"store-database": b.config.Database,
					"store-table":    b.config.Table,
				},
			}

			if _, err := handler(readCtx, resp); err != nil {
				b.logger.Errorf("error invoking change handler: %v", err)
				continue
			}
		}

		cursor.Close()
	}()

	return nil
}

func (b *Binding) Close() error {
	if b.closed.CompareAndSwap(false, true) {
		close(b.closeCh)
	}
	defer b.wg.Wait()
	return b.session.Close()
}

func metadataToConfig(cfg map[string]string, logger logger.Logger) (StateConfig, error) {
	c := StateConfig{}

	// prepare metadata keys for decoding
	for k, v := range cfg {
		cfg[strings.ReplaceAll(k, "_", "")] = v
		delete(cfg, k)
	}

	err := metadata.DecodeMetadata(cfg, &c)
	if err != nil {
		return c, err
	}

	return c, nil
}

// GetComponentMetadata returns the metadata of the component.
func (b *Binding) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := StateConfig{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.BindingType)
	return
}
