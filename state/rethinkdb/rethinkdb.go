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

package rethinkdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"

	r "github.com/dancannon/gorethink"

	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

const (
	stateTableNameDefault   = "daprstate"
	stateTablePKName        = "id"
	stateArchiveTableName   = "daprstate_archive"
	stateArchiveTablePKName = "key"
)

// RethinkDB is a state store implementation for RethinkDB.
type RethinkDB struct {
	session  *r.Session
	config   *stateConfig
	features []state.Feature
	logger   logger.Logger
}

type stateConfig struct {
	r.ConnectOpts `mapstructure:",squash"`
	Archive       bool   `json:"archive"`
	Table         string `json:"table"`
}

type stateRecord struct {
	ID   string `json:"id" rethinkdb:"id"`
	TS   int64  `json:"timestamp" rethinkdb:"timestamp"`
	Hash string `json:"hash,omitempty" rethinkdb:"hash,omitempty"`
	Data any    `json:"data,omitempty" rethinkdb:"data,omitempty"`
}

// NewRethinkDBStateStore returns a new RethinkDB state store.
func NewRethinkDBStateStore(logger logger.Logger) state.Store {
	s := &RethinkDB{
		features: []state.Feature{},
		logger:   logger,
	}
	return s
}

// Init parses metadata, initializes the RethinkDB client, and ensures the state table exists.
func (s *RethinkDB) Init(ctx context.Context, metadata state.Metadata) error {
	r.Log.Out = io.Discard
	r.SetTags("rethinkdb", "json")
	cfg, err := metadataToConfig(metadata.Properties, s.logger)
	if err != nil {
		return fmt.Errorf("unable to parse metadata properties: %w", err)
	}

	// in case someone runs Init multiple times
	if s.session != nil && s.session.IsConnected() {
		s.session.Close()
	}
	ses, err := r.Connect(cfg.ConnectOpts)
	if err != nil {
		return fmt.Errorf("error connecting to the database: %w", err)
	}

	s.session = ses
	s.config = cfg

	// check if table already exists
	listContext, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	c, err := r.DB(s.config.Database).TableList().Run(s.session, r.RunOpts{Context: listContext})
	if err != nil {
		return fmt.Errorf("error checking for state table existence in DB: %w", err)
	}

	if c == nil {
		return fmt.Errorf("invalid database response, cursor required: %w", err)
	}
	defer c.Close()

	var list []string
	err = c.All(&list)
	if err != nil {
		return fmt.Errorf("invalid database responsewhile listing tables: %w", err)
	}

	if !tableExists(list, s.config.Table) {
		cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, err = r.DB(s.config.Database).TableCreate(s.config.Table, r.TableCreateOpts{
			PrimaryKey: stateTablePKName,
		}).RunWrite(s.session, r.RunOpts{Context: cctx})
		if err != nil {
			return fmt.Errorf("error creating state table in DB: %w", err)
		}
	}

	if s.config.Archive && !tableExists(list, stateArchiveTableName) {
		// create archive table with autokey to preserve state id
		ctblCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, err = r.DB(s.config.Database).TableCreate(stateArchiveTableName,
			r.TableCreateOpts{PrimaryKey: stateArchiveTablePKName}).RunWrite(s.session, r.RunOpts{Context: ctblCtx})
		if err != nil {
			return fmt.Errorf("error creating state archive table in DB: %w", err)
		}

		// index archive table for id and timestamp
		cindCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, err = r.DB(s.config.Database).Table(stateArchiveTableName).
			IndexCreateFunc("state_index", func(row r.Term) interface{} {
				return []interface{}{row.Field("id"), row.Field("timestamp")}
			}).RunWrite(s.session, r.RunOpts{Context: cindCtx})
		if err != nil {
			return fmt.Errorf("error creating state archive index in DB: %w", err)
		}
	}

	return nil
}

// Features returns the features available in this state store.
func (s *RethinkDB) Features() []state.Feature {
	return s.features
}

func tableExists(arr []string, table string) bool {
	for _, a := range arr {
		if a == table {
			return true
		}
	}

	return false
}

// Get retrieves a RethinkDB KV item.
func (s *RethinkDB) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	if req == nil || req.Key == "" {
		return nil, errors.New("invalid state request, missing key")
	}

	c, err := r.Table(s.config.Table).Get(req.Key).Run(s.session, r.RunOpts{Context: ctx})
	if err != nil {
		return nil, fmt.Errorf("error getting record from the database: %w", err)
	}

	if c == nil || c.IsNil() {
		return &state.GetResponse{}, nil
	}

	if c != nil {
		defer c.Close()
	}

	var doc stateRecord
	err = c.One(&doc)
	if err != nil {
		return nil, fmt.Errorf("error parsing database content: %w", err)
	}

	resp := &state.GetResponse{ETag: ptr.Of(doc.Hash)}
	b, ok := doc.Data.([]byte)
	if ok {
		resp.Data = b
	} else {
		data, err := json.Marshal(doc.Data)
		if err != nil {
			return nil, errors.New("error serializing data from database")
		}
		resp.Data = data
	}

	return resp, nil
}

func (s *RethinkDB) BulkGet(ctx context.Context, req []state.GetRequest, opts state.BulkGetOpts) ([]state.BulkGetResponse, error) {
	return state.DoBulkGet(ctx, req, opts, s.Get)
}

// Set saves a state KV item.
func (s *RethinkDB) Set(ctx context.Context, req *state.SetRequest) error {
	if req == nil || req.Key == "" || req.Value == nil {
		return errors.New("invalid state request, key and value required")
	}

	return s.BulkSet(ctx, []state.SetRequest{*req}, state.BulkStoreOpts{})
}

// BulkSet performs a bulk save operation.
func (s *RethinkDB) BulkSet(ctx context.Context, req []state.SetRequest, _ state.BulkStoreOpts) error {
	docs := make([]*stateRecord, len(req))
	now := time.Now().UnixNano()
	for i, v := range req {
		var etag string
		if v.ETag != nil {
			etag = *v.ETag
		}

		docs[i] = &stateRecord{
			ID:   v.Key,
			TS:   now,
			Data: v.Value,
			Hash: etag,
		}
	}

	resp, err := r.Table(s.config.Table).Insert(docs, r.InsertOpts{
		Conflict:      "replace",
		ReturnChanges: true,
	}).RunWrite(s.session, r.RunOpts{Context: ctx})
	if err != nil {
		return fmt.Errorf("error saving records to the database: %w", err)
	}

	if s.config.Archive && len(resp.Changes) > 0 {
		s.archive(ctx, resp.Changes)
	}

	return nil
}

func (s *RethinkDB) archive(ctx context.Context, changes []r.ChangeResponse) error {
	list := make([]map[string]interface{}, 0)
	for _, c := range changes {
		if c.NewValue != nil {
			record, ok := c.NewValue.(map[string]interface{})
			if !ok {
				s.logger.Infof("invalid state DB change type: %T", c.NewValue)

				continue
			}
			list = append(list, record)
		}
	}
	if len(list) > 0 {
		_, err := r.Table(stateArchiveTableName).Insert(list).RunWrite(s.session, r.RunOpts{Context: ctx})
		if err != nil {
			return fmt.Errorf("error archiving records to the database: %w", err)
		}
	}

	return nil
}

// Delete performes a RethinkDB KV delete operation.
func (s *RethinkDB) Delete(ctx context.Context, req *state.DeleteRequest) error {
	if req == nil || req.Key == "" {
		return errors.New("invalid request, missing key")
	}

	return s.BulkDelete(ctx, []state.DeleteRequest{*req}, state.BulkStoreOpts{})
}

// BulkDelete performs a bulk delete operation.
func (s *RethinkDB) BulkDelete(ctx context.Context, req []state.DeleteRequest, _ state.BulkStoreOpts) error {
	list := make([]string, len(req))
	for i, d := range req {
		list[i] = d.Key
	}

	c, err := r.Table(s.config.Table).GetAll(r.Args(list)).Delete().Run(s.session, r.RunOpts{Context: ctx})
	if err != nil {
		return fmt.Errorf("error deleting record from the database: %w", err)
	}
	defer c.Close()

	return nil
}

func metadataToConfig(cfg map[string]string, logger logger.Logger) (*stateConfig, error) {
	// defaults
	c := stateConfig{
		Table: stateTableNameDefault,
	}

	err := metadata.DecodeMetadata(cfg, &c)
	if err != nil {
		return nil, err
	}

	return &c, nil
}

func (s *RethinkDB) GetComponentMetadata() (metadataInfo metadata.MetadataMap) {
	metadataStruct := stateConfig{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, metadata.StateStoreType)
	return
}

func (s *RethinkDB) Close() error {
	if s.session == nil {
		return nil
	}
	return s.session.Close()
}
