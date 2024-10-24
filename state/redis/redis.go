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

package redis

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"

	jsoniter "github.com/json-iterator/go"

	rediscomponent "github.com/dapr/components-contrib/common/component/redis"
	"github.com/dapr/components-contrib/contenttype"
	daprmetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/query"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
	"github.com/dapr/kit/ptr"
)

const (
	setDefaultQuery = `
	local etag = redis.pcall("HGET", KEYS[1], "version");
	if type(etag) == "table" then
	  redis.call("DEL", KEYS[1]);
	end;
	local fwr = redis.pcall("HGET", KEYS[1], "first-write");
	if not etag or type(etag)=="table" or etag == "" or etag == ARGV[1] or (not fwr and ARGV[1] == "0") then
	  redis.call("HSET", KEYS[1], "data", ARGV[2]);
	  if ARGV[3] == "0" then
	    redis.call("HSET", KEYS[1], "first-write", 0);
	  end;
	  return redis.call("HINCRBY", KEYS[1], "version", 1)
	else
	  return error("failed to set key " .. KEYS[1])
	end`
	delDefaultQuery = `
	local etag = redis.pcall("HGET", KEYS[1], "version");
	if not etag or type(etag)=="table" or etag == ARGV[1] or etag == "" or ARGV[1] == "0" then
	  return redis.call("DEL", KEYS[1])
	else
	  return error("failed to delete " .. KEYS[1])
	end`
	setJSONQuery = `
	local etag = redis.pcall("JSON.GET", KEYS[1], ".version");
	if type(etag) == "table" then
	  redis.call("JSON.DEL", KEYS[1]);
	end;
	if not etag or type(etag)=="table" or etag == "" then
	  etag = ARGV[1];
	end;
	local fwr = redis.pcall("JSON.GET", KEYS[1], ".first-write");
	if etag == ARGV[1] or ((not fwr or type(fwr) == "table") and ARGV[1] == "0") then
	  redis.call("JSON.SET", KEYS[1], "$", ARGV[2]);
	  if ARGV[3] == "0" then
	    redis.call("JSON.SET", KEYS[1], ".first-write", 0);
	  end;
	  return redis.call("JSON.SET", KEYS[1], ".version", (etag+1))
	else
	  return error("failed to set key " .. KEYS[1])
	end`
	delJSONQuery = `
	local etag = redis.pcall("JSON.GET", KEYS[1], ".version");
	if not etag or type(etag)=="table" or etag == ARGV[1] or etag == "" or ARGV[1] == "0" then
	  return redis.call("JSON.DEL", KEYS[1])
	else
	  return error("failed to delete " .. KEYS[1])
	end`
	connectedSlavesReplicas  = "connected_slaves:"
	infoReplicationDelimiter = "\r\n"
	ttlInSeconds             = "ttlInSeconds"
	defaultBase              = 10
	defaultBitSize           = 0
	defaultDB                = 0
)

// StateStore is a Redis state store.
type StateStore struct {
	state.BulkStore

	client                         rediscomponent.RedisClient
	clientSettings                 *rediscomponent.Settings
	clientHasJSON                  bool
	json                           jsoniter.API
	replicas                       int
	querySchemas                   querySchemas
	suppressActorStateStoreWarning atomic.Bool

	logger logger.Logger
}

// NewRedisStateStore returns a new redis state store.
func NewRedisStateStore(log logger.Logger) state.Store {
	s := newStateStore(log)
	s.BulkStore = state.NewDefaultBulkStore(s)
	return s
}

func newStateStore(log logger.Logger) *StateStore {
	return &StateStore{
		json:                           jsoniter.ConfigFastest,
		logger:                         log,
		suppressActorStateStoreWarning: atomic.Bool{},
	}
}

func (r *StateStore) Ping(ctx context.Context) error {
	if _, err := r.client.PingResult(ctx); err != nil {
		return fmt.Errorf("redis store: error connecting to redis at %s: %w", r.clientSettings.Host, err)
	}

	return nil
}

// Init does metadata and connection parsing.
func (r *StateStore) Init(ctx context.Context, metadata state.Metadata) error {
	var err error
	r.client, r.clientSettings, err = rediscomponent.ParseClientFromProperties(metadata.Properties, daprmetadata.StateStoreType, ctx, &r.logger)
	if err != nil {
		return err
	}

	// check for query schemas
	if r.querySchemas, err = parseQuerySchemas(r.clientSettings.QueryIndexes); err != nil {
		return fmt.Errorf("redis store: error parsing query index schema: %w", err)
	}

	if _, err = r.client.PingResult(ctx); err != nil {
		return fmt.Errorf("redis store: error connecting to redis at %s: %w", r.clientSettings.Host, err)
	}

	if r.replicas, err = r.getConnectedSlaves(ctx); err != nil {
		return err
	}

	if err = r.registerSchemas(ctx); err != nil {
		return fmt.Errorf("redis store: error registering query schemas: %w", err)
	}

	r.clientHasJSON = rediscomponent.ClientHasJSONSupport(r.client)

	return nil
}

// Features returns the features available in this state store.
func (r *StateStore) Features() []state.Feature {
	if r.clientHasJSON {
		return []state.Feature{state.FeatureETag, state.FeatureTransactional, state.FeatureTTL, state.FeatureQueryAPI}
	} else {
		return []state.Feature{state.FeatureETag, state.FeatureTransactional, state.FeatureTTL}
	}
}

func (r *StateStore) getConnectedSlaves(ctx context.Context) (int, error) {
	res, err := r.client.DoRead(ctx, "INFO", "replication")
	if err != nil {
		return 0, err
	}

	// Response example: https://redis.io/commands/info#return-value
	// # Replication\r\nrole:master\r\nconnected_slaves:1\r\n
	s, _ := strconv.Unquote(fmt.Sprintf("%q", res))
	if len(s) == 0 {
		return 0, nil
	}

	return r.parseConnectedSlaves(s), nil
}

func (r *StateStore) parseConnectedSlaves(res string) int {
	infos := strings.Split(res, infoReplicationDelimiter)
	for _, info := range infos {
		if strings.Contains(info, connectedSlavesReplicas) {
			parsedReplicas, _ := strconv.ParseUint(info[len(connectedSlavesReplicas):], 10, 32)

			//nolint:gosec
			return int(parsedReplicas)
		}
	}

	return 0
}

// Delete performs a delete operation.
func (r *StateStore) Delete(ctx context.Context, req *state.DeleteRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	if !req.HasETag() {
		req.ETag = ptr.Of("0")
	}

	if req.Metadata[daprmetadata.ContentType] == contenttype.JSONContentType && r.clientHasJSON {
		err = r.client.DoWrite(ctx, "EVAL", delJSONQuery, 1, req.Key, *req.ETag)
	} else {
		err = r.client.DoWrite(ctx, "EVAL", delDefaultQuery, 1, req.Key, *req.ETag)
	}
	if err != nil {
		return state.NewETagError(state.ETagMismatch, err)
	}

	return nil
}

func (r *StateStore) directGet(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	res, err := r.client.DoRead(ctx, "GET", req.Key)
	if err != nil {
		return nil, err
	}

	if res == nil {
		return &state.GetResponse{}, nil
	}

	s, _ := strconv.Unquote(fmt.Sprintf("%q", res))

	return &state.GetResponse{
		Data: []byte(s),
	}, nil
}

func (r *StateStore) getDefault(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	res, err := r.client.DoRead(ctx, "HGETALL", req.Key) // Prefer values with ETags
	if err != nil {
		return r.directGet(ctx, req) // Falls back to original get for backward compats.
	}
	if res == nil {
		return &state.GetResponse{}, nil
	}
	vals, ok := res.([]any)
	if !ok {
		// we retrieved a JSON value from a non-JSON store
		valMap := res.(map[any]any)
		// convert valMap to []any
		vals = make([]any, 0, len(valMap))
		for k, v := range valMap {
			vals = append(vals, k, v)
		}
	}
	if len(vals) == 0 {
		return &state.GetResponse{}, nil
	}

	data, version, err := r.getKeyVersion(vals)
	if err != nil {
		return nil, err
	}

	return &state.GetResponse{
		Data: []byte(data),
		ETag: version,
	}, nil
}

func (r *StateStore) getJSON(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	res, err := r.client.DoRead(ctx, "JSON.GET", req.Key)
	if err != nil {
		return nil, err
	}

	if res == nil {
		return &state.GetResponse{}, nil
	}

	str, ok := res.(string)
	if !ok {
		return nil, errors.New("invalid result")
	}

	var entry jsonEntry
	if err = r.json.UnmarshalFromString(str, &entry); err != nil {
		return nil, err
	}

	var version *string
	if entry.Version != nil {
		version = new(string)
		*version = strconv.Itoa(*entry.Version)
	}

	data, err := r.json.Marshal(entry.Data)
	if err != nil {
		return nil, err
	}

	return &state.GetResponse{
		Data: data,
		ETag: version,
	}, nil
}

// Get retrieves state from redis with a key.
func (r *StateStore) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	if req.Metadata[daprmetadata.ContentType] == contenttype.JSONContentType && r.clientHasJSON {
		return r.getJSON(ctx, req)
	}

	return r.getDefault(ctx, req)
}

type jsonEntry struct {
	Data    interface{} `json:"data"`
	Version *int        `json:"version,omitempty"`
}

// Set saves state into redis.
func (r *StateStore) Set(ctx context.Context, req *state.SetRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}
	ver, err := r.parseETag(req)
	if err != nil {
		return err
	}
	ttl, err := r.parseTTL(req)
	if err != nil {
		return fmt.Errorf("failed to parse ttl from metadata: %w", err)
	}
	// apply global TTL
	if ttl == nil {
		ttl = r.clientSettings.TTLInSeconds
	}

	firstWrite := 1
	if req.Options.Concurrency == state.FirstWrite {
		firstWrite = 0
	}

	if req.Metadata[daprmetadata.ContentType] == contenttype.JSONContentType && r.clientHasJSON {
		bt, _ := utils.Marshal(&jsonEntry{Data: req.Value}, r.json.Marshal)
		err = r.client.DoWrite(ctx, "EVAL", setJSONQuery, 1, req.Key, ver, bt, firstWrite)
	} else {
		bt, _ := utils.Marshal(req.Value, r.json.Marshal)
		err = r.client.DoWrite(ctx, "EVAL", setDefaultQuery, 1, req.Key, ver, bt, firstWrite)
	}

	if err != nil {
		if req.HasETag() {
			return state.NewETagError(state.ETagMismatch, err)
		}

		return fmt.Errorf("failed to set key %s: %w", req.Key, err)
	}

	if ttl != nil && *ttl > 0 {
		err = r.client.DoWrite(ctx, "EXPIRE", req.Key, *ttl)
		if err != nil {
			return fmt.Errorf("failed to set key %s ttl: %w", req.Key, err)
		}
	}

	if ttl != nil && *ttl <= 0 {
		err = r.client.DoWrite(ctx, "PERSIST", req.Key)
		if err != nil {
			return fmt.Errorf("failed to persist key %s: %w", req.Key, err)
		}
	}

	if req.Options.Consistency == state.Strong && r.replicas > 0 {
		err = r.client.DoWrite(ctx, "WAIT", r.replicas, 1000)
		if err != nil {
			return fmt.Errorf("redis waiting for %v replicas to acknowledge write, err: %w", r.replicas, err)
		}
	}

	return nil
}

// Multi performs a transactional operation. succeeds only if all operations succeed, and fails if one or more operations fail.
func (r *StateStore) Multi(ctx context.Context, request *state.TransactionalStateRequest) error {
	if r.suppressActorStateStoreWarning.CompareAndSwap(false, true) {
		r.logger.Warn("Redis does not support transaction rollbacks and should not be used in production as an actor state store.")
	}

	// Check if the entire transaction is using JSON based on the transactional request's metadata
	isJSON := request.Metadata[daprmetadata.ContentType] == contenttype.JSONContentType && r.clientHasJSON

	pipe := r.client.TxPipeline()
	for _, o := range request.Operations {
		switch req := o.(type) {
		case state.SetRequest:
			ver, err := r.parseETag(&req)
			if err != nil {
				return err
			}
			ttl, err := r.parseTTL(&req)
			if err != nil {
				return fmt.Errorf("failed to parse ttl from metadata: %w", err)
			}
			// apply global TTL
			if ttl == nil {
				ttl = r.clientSettings.TTLInSeconds
			}
			var bt []byte
			isReqJSON := isJSON ||
				(len(req.Metadata) > 0 && req.Metadata[daprmetadata.ContentType] == contenttype.JSONContentType)
			if isReqJSON {
				bt, _ = utils.Marshal(&jsonEntry{Data: req.Value}, r.json.Marshal)
				pipe.Do(ctx, "EVAL", setJSONQuery, 1, req.Key, ver, bt)
			} else {
				bt, _ = utils.Marshal(req.Value, r.json.Marshal)
				pipe.Do(ctx, "EVAL", setDefaultQuery, 1, req.Key, ver, bt)
			}
			if ttl != nil && *ttl > 0 {
				pipe.Do(ctx, "EXPIRE", req.Key, *ttl)
			}
			if ttl != nil && *ttl <= 0 {
				pipe.Do(ctx, "PERSIST", req.Key)
			}

		case state.DeleteRequest:
			if !req.HasETag() {
				req.ETag = ptr.Of("0")
			}
			isReqJSON := isJSON ||
				(len(req.Metadata) > 0 && req.Metadata[daprmetadata.ContentType] == contenttype.JSONContentType)
			if isReqJSON {
				pipe.Do(ctx, "EVAL", delJSONQuery, 1, req.Key, *req.ETag)
			} else {
				pipe.Do(ctx, "EVAL", delDefaultQuery, 1, req.Key, *req.ETag)
			}
		}
	}

	err := pipe.Exec(ctx)

	return err
}

func (r *StateStore) registerSchemas(ctx context.Context) error {
	for name, elem := range r.querySchemas {
		r.logger.Infof("create query index %s", name)
		if err := r.client.DoWrite(ctx, elem.schema...); err != nil {
			if err.Error() != "Index already exists" {
				return err
			}
			r.logger.Infof("drop stale query index %s", name)
			if err = r.client.DoWrite(ctx, "FT.DROPINDEX", name); err != nil {
				return err
			}
			if err = r.client.DoWrite(ctx, elem.schema...); err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *StateStore) getKeyVersion(vals []interface{}) (data string, version *string, err error) {
	seenData := false
	seenVersion := false
	for i := 0; i < len(vals); i += 2 {
		field, _ := strconv.Unquote(fmt.Sprintf("%q", vals[i]))
		switch field {
		case "data":
			data, _ = strconv.Unquote(fmt.Sprintf("%q", vals[i+1]))
			seenData = true
		case "version":
			versionVal, _ := strconv.Unquote(fmt.Sprintf("%q", vals[i+1]))
			version = ptr.Of(versionVal)
			seenVersion = true
		}
	}
	if !seenData || !seenVersion {
		return "", nil, errors.New("required hash field 'data' or 'version' was not found")
	}

	return data, version, nil
}

func (r *StateStore) parseETag(req *state.SetRequest) (int, error) {
	if req.Options.Concurrency == state.LastWrite || req.ETag == nil || *req.ETag == "" {
		return 0, nil
	}
	ver, err := strconv.Atoi(*req.ETag)
	if err != nil {
		return -1, state.NewETagError(state.ETagInvalid, err)
	}

	return ver, nil
}

func (r *StateStore) parseTTL(req *state.SetRequest) (*int, error) {
	if val, ok := req.Metadata[ttlInSeconds]; ok && val != "" {
		parsedVal, err := strconv.ParseInt(val, defaultBase, defaultBitSize)
		if err != nil {
			return nil, err
		}
		ttl := int(parsedVal)

		return &ttl, nil
	}

	return nil, nil
}

// Query executes a query against store.
func (r *StateStore) Query(ctx context.Context, req *state.QueryRequest) (*state.QueryResponse, error) {
	if !r.clientHasJSON {
		return nil, errors.New("redis-json server support is required for query capability")
	}
	indexName, ok := daprmetadata.TryGetQueryIndexName(req.Metadata)
	if !ok {
		return nil, errors.New("query index not found")
	}
	elem, ok := r.querySchemas[indexName]
	if !ok {
		return nil, fmt.Errorf("query index schema %q not found", indexName)
	}

	q := NewQuery(indexName, elem.keys)
	qbuilder := query.NewQueryBuilder(q)
	if err := qbuilder.BuildQuery(&req.Query); err != nil {
		return &state.QueryResponse{}, err
	}
	data, token, err := q.execute(ctx, r.client)
	if err != nil {
		return &state.QueryResponse{}, err
	}

	return &state.QueryResponse{
		Results: data,
		Token:   token,
	}, nil
}

func (r *StateStore) Close() error {
	return r.client.Close()
}

func (r *StateStore) GetComponentMetadata() (metadataInfo daprmetadata.MetadataMap) {
	settingsStruct := rediscomponent.Settings{}
	daprmetadata.GetMetadataInfoFromStructType(reflect.TypeOf(settingsStruct), &metadataInfo, daprmetadata.StateStoreType)
	return
}
