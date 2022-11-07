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
	"encoding/json"
	"io"
	"reflect"
	"time"

	r "github.com/dancannon/gorethink"
	"github.com/pkg/errors"

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

// RethinkDB is a state store implementation with transactional support for RethinkDB.
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
	ID   string      `json:"id" rethinkdb:"id"`
	TS   int64       `json:"timestamp" rethinkdb:"timestamp"`
	Hash string      `json:"hash,omitempty" rethinkdb:"hash,omitempty"`
	Data interface{} `json:"data,omitempty" rethinkdb:"data,omitempty"`
}

// NewRethinkDBStateStore returns a new RethinkDB state store.
func NewRethinkDBStateStore(logger logger.Logger) state.Store {
	return &RethinkDB{
		features: []state.Feature{},
		logger:   logger,
	}
}

// Init parses metadata, initializes the RethinkDB client, and ensures the state table exists.
func (s *RethinkDB) Init(metadata state.Metadata) error {
	r.Log.Out = io.Discard
	r.SetTags("rethinkdb", "json")
	cfg, err := metadataToConfig(metadata.Properties, s.logger)
	if err != nil {
		return errors.Wrap(err, "unable to parse metadata properties")
	}

	// in case someone runs Init multiple times
	if s.session != nil && s.session.IsConnected() {
		s.session.Close()
	}
	ses, err := r.Connect(cfg.ConnectOpts)
	if err != nil {
		return errors.Wrap(err, "error connecting to the database")
	}

	s.session = ses
	s.config = cfg

	// check if table already exists
	c, err := r.DB(s.config.Database).TableList().Run(s.session)
	if err != nil {
		return errors.Wrap(err, "error checking for state table existence in DB")
	}

	if c == nil {
		return errors.Wrap(err, "invalid database response, cursor required")
	}
	defer c.Close()

	var list []string
	err = c.All(&list)
	if err != nil {
		return errors.Wrap(err, "invalid database responsewhile listing tables")
	}

	if !tableExists(list, s.config.Table) {
		_, err = r.DB(s.config.Database).TableCreate(s.config.Table, r.TableCreateOpts{
			PrimaryKey: stateTablePKName,
		}).RunWrite(s.session)
		if err != nil {
			return errors.Wrap(err, "error creating state table in DB")
		}
	}

	if s.config.Archive && !tableExists(list, stateArchiveTableName) {
		// create archive table with autokey to preserve state id
		_, err = r.DB(s.config.Database).TableCreate(stateArchiveTableName,
			r.TableCreateOpts{PrimaryKey: stateArchiveTablePKName}).RunWrite(s.session)
		if err != nil {
			return errors.Wrap(err, "error creating state archive table in DB")
		}
		// index archive table for id and timestamp
		_, err = r.DB(s.config.Database).Table(stateArchiveTableName).
			IndexCreateFunc("state_index", func(row r.Term) interface{} {
				return []interface{}{row.Field("id"), row.Field("timestamp")}
			}).RunWrite(s.session)
		if err != nil {
			return errors.Wrap(err, "error creating state archive index in DB")
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
func (s *RethinkDB) Get(req *state.GetRequest) (*state.GetResponse, error) {
	if req == nil || req.Key == "" {
		return nil, errors.New("invalid state request, missing key")
	}

	c, err := r.Table(s.config.Table).Get(req.Key).Run(s.session)
	if err != nil {
		return nil, errors.Wrap(err, "error getting record from the database")
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
		return nil, errors.Wrap(err, "error parsing database content")
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

// BulkGet performs a bulks get operations.
func (s *RethinkDB) BulkGet(req []state.GetRequest) (bool, []state.BulkGetResponse, error) {
	// TODO: replace with bulk get for performance
	return false, nil, nil
}

// Set saves a state KV item.
func (s *RethinkDB) Set(req *state.SetRequest) error {
	if req == nil || req.Key == "" || req.Value == nil {
		return errors.New("invalid state request, key and value required")
	}

	return s.BulkSet([]state.SetRequest{*req})
}

// BulkSet performs a bulk save operation.
func (s *RethinkDB) BulkSet(req []state.SetRequest) error {
	docs := make([]*stateRecord, len(req))
	for i, v := range req {
		var etag string
		if v.ETag != nil {
			etag = *v.ETag
		}

		docs[i] = &stateRecord{
			ID:   v.Key,
			TS:   time.Now().UTC().UnixNano(),
			Data: v.Value,
			Hash: etag,
		}
	}

	resp, err := r.Table(s.config.Table).Insert(docs, r.InsertOpts{
		Conflict:      "replace",
		ReturnChanges: true,
	}).RunWrite(s.session)
	if err != nil {
		return errors.Wrap(err, "error saving records to the database")
	}

	if s.config.Archive && len(resp.Changes) > 0 {
		s.archive(resp.Changes)
	}

	return nil
}

func (s *RethinkDB) archive(changes []r.ChangeResponse) error {
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
		_, err := r.Table(stateArchiveTableName).Insert(list).RunWrite(s.session)
		if err != nil {
			return errors.Wrap(err, "error archiving records to the database")
		}
	}

	return nil
}

// Delete performes a RethinkDB KV delete operation.
func (s *RethinkDB) Delete(req *state.DeleteRequest) error {
	if req == nil || req.Key == "" {
		return errors.New("invalid request, missing key")
	}

	return s.BulkDelete([]state.DeleteRequest{*req})
}

// BulkDelete performs a bulk delete operation.
func (s *RethinkDB) BulkDelete(req []state.DeleteRequest) error {
	list := make([]string, 0)
	for _, d := range req {
		list = append(list, d.Key)
	}

	c, err := r.Table(s.config.Table).GetAll(r.Args(list)).Delete().Run(s.session)
	if err != nil {
		return errors.Wrap(err, "error deleting record from the database")
	}
	defer c.Close()

	return nil
}

// Multi performs multiple operations.
func (s *RethinkDB) Multi(req *state.TransactionalStateRequest) error {
	upserts := make([]state.SetRequest, 0)
	deletes := make([]state.DeleteRequest, 0)

	for _, v := range req.Operations {
		switch v.Operation {
		case state.Upsert:
			r, ok := v.Request.(state.SetRequest)
			if !ok {
				return errors.Errorf("invalid request type (expected SetRequest, got %t)", v.Request)
			}
			upserts = append(upserts, r)
		case state.Delete:
			r, ok := v.Request.(state.DeleteRequest)
			if !ok {
				return errors.Errorf("invalid request type (expected DeleteRequest, got %t)", v.Request)
			}
			deletes = append(deletes, r)
		default:
			return errors.Errorf("invalid operation type: %s", v.Operation)
		}
	}

	// best effort, no transacts supported
	if err := s.BulkSet(upserts); err != nil {
		return errors.Wrap(err, "error saving records to the database")
	}

	if err := s.BulkDelete(deletes); err != nil {
		return errors.Wrap(err, "error deleting records to the database")
	}

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

func (s *RethinkDB) GetComponentMetadata() map[string]string {
	metadataStruct := stateConfig{}
	metadataInfo := map[string]string{}
	metadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo)
	return metadataInfo
}
