// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package rethinkdb

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	r "github.com/dancannon/gorethink"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/dapr/pkg/logger"
	"github.com/pkg/errors"
)

const (
	stateTableName   = "daprstate"
	stateTablePKName = "id"
)

// RethinkDB is a state store implementation for RethinkDB.
type RethinkDB struct {
	session *r.Session
	config  *r.ConnectOpts
	logger  logger.Logger
}

// StateRecord represents a single state record
type StateRecord struct {
	ID   string      `json:"id" rethinkdb:"id"`
	Ts   int64       `json:"timestamp" rethinkdb:"timestamp"`
	Hash string      `json:"hash,omitempty" rethinkdb:"hash,omitempty"`
	Data interface{} `json:"data,omitempty" rethinkdb:"data,omitempty"`
}

func init() {
	r.Log.Out = os.Stderr
	r.SetTags("rethinkdb", "json")
}

// NewRethinkDBStateStore returns a new RethinkDB state store.
func NewRethinkDBStateStore(logger logger.Logger) *RethinkDB {
	return &RethinkDB{logger: logger}
}

// Init parses metadata, initializes the RethinkDB client, and ensures the state table exists
func (s *RethinkDB) Init(metadata state.Metadata) error {
	cfg, err := metadataToConfig(metadata.Properties)
	if err != nil {
		return errors.Wrap(err, "unable to parse metadata properties")
	}

	ses, err := r.Connect(*cfg)
	if err != nil {
		return errors.Wrap(err, "error connecting to the database")
	}

	s.session = ses
	s.config = cfg

	// check if table already exists
	c, err := r.DB(s.config.Database).TableList().Contains(stateTableName).Run(s.session)
	if err != nil {
		return errors.Wrap(err, "error checking for state table existence in DB")
	}

	if c == nil {
		return errors.Wrap(err, "invalid database response, cursor required")
	}

	var exists bool
	err = c.One(&exists)
	if err != nil {
		return errors.Wrap(err, "invalid database response, data not bool")
	}

	if !exists {
		_, err = r.DB(s.config.Database).TableCreate(stateTableName, r.TableCreateOpts{
			PrimaryKey: stateTablePKName,
		}).RunWrite(s.session)
		if err != nil {
			return errors.Wrap(err, "error ensuring the state table exists in DB")
		}
	}

	return nil
}

// Get retrieves a RethinkDB KV item
func (s *RethinkDB) Get(req *state.GetRequest) (*state.GetResponse, error) {
	if req == nil || req.Key == "" {
		return nil, errors.New("invalid state request, missing key")
	}

	c, err := r.Table(stateTableName).Get(req.Key).Run(s.session)
	if err != nil {
		return nil, errors.Wrap(err, "error getting recrod from the database")
	}

	if c == nil || c.IsNil() {
		return &state.GetResponse{}, nil
	}

	var doc StateRecord
	err = c.One(&doc)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing database content")
	}

	resp := &state.GetResponse{ETag: doc.Hash}
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

// Set saves a state KV item
func (s *RethinkDB) Set(req *state.SetRequest) error {
	if req == nil || req.Key == "" || req.Value == nil {
		return errors.New("invalid state request, key and value required")
	}
	return s.BulkSet([]state.SetRequest{*req})
}

// BulkSet performs a bulk save operation
func (s *RethinkDB) BulkSet(req []state.SetRequest) error {
	docs := make([]*StateRecord, len(req))
	for i, v := range req {
		docs[i] = &StateRecord{
			ID:   v.Key,
			Ts:   time.Now().UTC().UnixNano(),
			Hash: v.ETag,
			Data: v.Value,
		}
	}

	_, err := r.Table(stateTableName).Insert(docs, r.InsertOpts{Conflict: "replace"}).RunWrite(s.session)
	if err != nil {
		return errors.Wrap(err, "error saving records to the database")
	}

	return nil
}

// Delete performes a RethinkDB KV delete operation
func (s *RethinkDB) Delete(req *state.DeleteRequest) error {
	if req == nil || req.Key == "" {
		return errors.New("invalid request, missing key")
	}
	return s.BulkDelete([]state.DeleteRequest{*req})
}

// BulkDelete performs a bulk delete operation
func (s *RethinkDB) BulkDelete(req []state.DeleteRequest) error {
	list := make([]string, 0)
	for _, d := range req {
		list = append(list, d.Key)
	}

	_, err := r.Table(stateTableName).GetAll(r.Args(list)).Delete().Run(s.session)
	if err != nil {
		return errors.Wrap(err, "error deleting record from the database")
	}

	return nil
}

// Multi performs multiple operations
func (s *RethinkDB) Multi(reqs []state.TransactionalRequest) error {
	upserts := make([]*StateRecord, 0)
	deletes := make([]string, 0)

	for _, v := range reqs {
		switch v.Operation {
		case state.Upsert:
			r, ok := v.Request.(state.SetRequest)
			if !ok {
				return errors.Errorf("invalid request type (expected SetRequest, got %t)", v.Request)
			}
			if r.Key == "" || r.Value == nil {
				return errors.Errorf("invalid request data: %v", r)
			}
			d := &StateRecord{ID: r.Key, Ts: time.Now().UTC().UnixNano(), Hash: r.ETag, Data: r.Value}
			upserts = append(upserts, d)
		case state.Delete:
			r, ok := v.Request.(state.DeleteRequest)
			if !ok {
				return errors.Errorf("invalid request type (expected DeleteRequest, got %t)", v.Request)
			}
			if r.Key == "" {
				return errors.Errorf("invalid request data: %v", r)
			}
			deletes = append(deletes, r.Key)
		default:
			return errors.Errorf("invalid operation type: %s", v.Operation)
		}
	}

	// note, no transactional support so this is best effort
	tbl := r.Table(stateTableName)
	_, err := tbl.Insert(upserts, r.InsertOpts{Conflict: "replace"}).RunWrite(s.session)
	if err != nil {
		return errors.Wrap(err, "error saving records to the database")
	}

	_, err = tbl.GetAll(r.Args(deletes)).Delete().Run(s.session)
	if err != nil {
		return errors.Wrap(err, "error saving records to the database")
	}

	return nil
}

func metadataToConfig(cfg map[string]string) (*r.ConnectOpts, error) {

	c := r.ConnectOpts{}
	for k, v := range cfg {
		switch k {
		case "address": //string
			c.Address = v
		case "addresses": // []string
			c.Addresses = strings.Split(v, ",")
		case "database": //string
			c.Database = v
		case "username": //string
			c.Username = v
		case "password": //string
			c.Password = v
		case "authkey": //string
			c.AuthKey = v
		case "timeout": //time.Duration
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid timeout format: %v", v)
			}
			c.Timeout = d
		case "write_timeout": //time.Duration
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid write timeout format: %v", v)
			}
			c.WriteTimeout = d
		case "read_timeout": //time.Duration
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid read timeout format: %v", v)
			}
			c.ReadTimeout = d
		case "keep_alive_timeout": //time.Duration
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid keep alive timeout format: %v", v)
			}
			c.KeepAlivePeriod = d
		case "initial_cap": //int
			i, err := strconv.Atoi(v)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid keep initial cap format: %v", v)
			}
			c.InitialCap = i
		case "max_open": //int
			i, err := strconv.Atoi(v)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid keep max open format: %v", v)
			}
			c.MaxOpen = i
		case "discover_hosts": //bool
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid discover hosts format: %v", v)
			}
			c.DiscoverHosts = b
		case "use-open-tracing": //bool
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid use open tracing format: %v", v)
			}
			c.UseOpentracing = b
		case "node_refresh_interval": //time.Duration
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid keep node refresh interval format: %v", v)
			}
			c.NodeRefreshInterval = d
		case "max_idle": //int
			i, err := strconv.Atoi(v)
			if err != nil {
				return nil, errors.Wrapf(err, "invalid keep max idle format: %v", v)
			}
			c.MaxIdle = i
		default:
			return nil, errors.Errorf("unrecognized metadata: %s", k)
		}
	}

	return &c, nil
}
