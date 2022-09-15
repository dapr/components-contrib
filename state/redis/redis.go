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
	"fmt"
	"strconv"
	"strings"

	"github.com/agrea/ptr"
	redisClientv8 "github.com/go-redis/redis/v8"
	redisClientv9 "github.com/go-redis/redis/v9"
	jsoniter "github.com/json-iterator/go"

	"github.com/dapr/components-contrib/contenttype"
	rediscomponent "github.com/dapr/components-contrib/internal/component/redis"
	daprmetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/query"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
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
	state.DefaultBulkStore
	clientv8       redisClientv8.UniversalClient
	clientv9       redisClientv9.UniversalClient
	legacyRedis    bool
	clientSettings *rediscomponent.Settings
	json           jsoniter.API
	metadata       rediscomponent.Metadata
	replicas       int
	querySchemas   querySchemas

	features []state.Feature
	logger   logger.Logger

	ctx    context.Context
	cancel context.CancelFunc
}

// NewRedisStateStore returns a new redis state store.
func NewRedisStateStore(logger logger.Logger) state.Store {
	s := &StateStore{
		json:     jsoniter.ConfigFastest,
		features: []state.Feature{state.FeatureETag, state.FeatureTransactional, state.FeatureQueryAPI},
		logger:   logger,
	}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

func (r *StateStore) Ping() error {
	// redis 6
	if r.legacyRedis {
		if _, err := r.clientv8.Ping(context.Background()).Result(); err != nil {
			return fmt.Errorf("redis store: error connecting to redis at %s: %s", r.clientSettings.Host, err)
		}
	} else {
		if _, err := r.clientv9.Ping(context.Background()).Result(); err != nil {
			return fmt.Errorf("redis store: error connecting to redis at %s: %s", r.clientSettings.Host, err)
		}
	}

	return nil
}

// Init does metadata and connection parsing.
func (r *StateStore) Init(metadata state.Metadata) error {
	m, err := rediscomponent.ParseRedisMetadata(metadata.Properties)
	if err != nil {
		return err
	}
	r.metadata = m
	if rediscomponent.IsLegacyRedisVersion(metadata.Properties) {
		r.legacyRedis = true
	}

	defaultSettings := rediscomponent.Settings{RedisMaxRetries: m.MaxRetries, RedisMaxRetryInterval: rediscomponent.Duration(m.MaxRetryBackoff)}
	// redis 6 and below
	if r.legacyRedis {
		r.clientv8, r.clientSettings, err = rediscomponent.ParseClientv8FromProperties(metadata.Properties, &defaultSettings)
	} else {
		r.clientv9, r.clientSettings, err = rediscomponent.ParseClientv9FromProperties(metadata.Properties, &defaultSettings)
	}
	if err != nil {
		return err
	}

	// check for query schemas
	if r.querySchemas, err = parseQuerySchemas(m.QueryIndexes); err != nil {
		return fmt.Errorf("redis store: error parsing query index schema: %v", err)
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())

	// redis 6 and below
	if r.legacyRedis {
		if _, err = r.clientv8.Ping(r.ctx).Result(); err != nil {
			return fmt.Errorf("redis store: error connecting to redis at %s: %v", r.clientSettings.Host, err)
		}
	} else {
		if _, err = r.clientv9.Ping(r.ctx).Result(); err != nil {
			return fmt.Errorf("redis store: error connecting to redis at %s: %v", r.clientSettings.Host, err)
		}
	}

	if r.replicas, err = r.getConnectedSlaves(); err != nil {
		return err
	}

	if err = r.registerSchemas(); err != nil {
		return fmt.Errorf("redis store: error registering query schemas: %v", err)
	}

	return nil
}

// Features returns the features available in this state store.
func (r *StateStore) Features() []state.Feature {
	return r.features
}

func (r *StateStore) getConnectedSlaves() (int, error) {
	var res interface{}
	var err error

	// redis 6 and below
	if r.legacyRedis {
		res, err = r.clientv8.Do(r.ctx, "INFO", "replication").Result()
	} else {
		res, err = r.clientv9.Do(r.ctx, "INFO", "replication").Result()
	}
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

			return int(parsedReplicas)
		}
	}

	return 0
}

func (r *StateStore) deleteValue(req *state.DeleteRequest) error {
	if req.ETag == nil {
		etag := "0"
		req.ETag = &etag
	}

	var delQuery string
	if contentType, ok := req.Metadata[daprmetadata.ContentType]; ok && contentType == contenttype.JSONContentType {
		delQuery = delJSONQuery
	} else {
		delQuery = delDefaultQuery
	}
	var err error
	// redis 6 and below
	if r.legacyRedis {
		_, err = r.clientv8.Do(r.ctx, "EVAL", delQuery, 1, req.Key, *req.ETag).Result()
	} else {
		_, err = r.clientv9.Do(r.ctx, "EVAL", delQuery, 1, req.Key, *req.ETag).Result()
	}
	if err != nil {
		return state.NewETagError(state.ETagMismatch, err)
	}

	return nil
}

// Delete performs a delete operation.
func (r *StateStore) Delete(req *state.DeleteRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	return state.DeleteWithOptions(r.deleteValue, req)
}

func (r *StateStore) directGet(req *state.GetRequest) (*state.GetResponse, error) {
	var res interface{}
	var err error

	// redis 6 and below
	if r.legacyRedis {
		res, err = r.clientv8.Do(r.ctx, "GET", req.Key).Result()
	} else {
		res, err = r.clientv9.Do(r.ctx, "GET", req.Key).Result()
	}
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

func (r *StateStore) getDefault(req *state.GetRequest) (*state.GetResponse, error) {
	var res interface{}
	var err error

	// redis 6 and below
	if r.legacyRedis {
		res, err = r.clientv8.Do(r.ctx, "HGETALL", req.Key).Result() // Prefer values with ETags
	} else {
		res, err = r.clientv9.Do(r.ctx, "HGETALL", req.Key).Result() // Prefer values with ETags
	}
	if err != nil {
		return r.directGet(req) // Falls back to original get for backward compats.
	}
	if res == nil {
		return &state.GetResponse{}, nil
	}
	vals := res.([]interface{})
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

func (r *StateStore) getJSON(req *state.GetRequest) (*state.GetResponse, error) {
	var res interface{}
	var err error

	// redis 6 and below
	if r.legacyRedis {
		res, err = r.clientv8.Do(r.ctx, "JSON.GET", req.Key).Result()
	} else {
		res, err = r.clientv9.Do(r.ctx, "JSON.GET", req.Key).Result()
	}
	if err != nil {
		return nil, err
	}

	if res == nil {
		return &state.GetResponse{}, nil
	}

	str, ok := res.(string)
	if !ok {
		return nil, fmt.Errorf("invalid result")
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
func (r *StateStore) Get(req *state.GetRequest) (*state.GetResponse, error) {
	if contentType, ok := req.Metadata[daprmetadata.ContentType]; ok && contentType == contenttype.JSONContentType {
		return r.getJSON(req)
	}

	return r.getDefault(req)
}

type jsonEntry struct {
	Data    interface{} `json:"data"`
	Version *int        `json:"version,omitempty"`
}

func (r *StateStore) setValue(req *state.SetRequest) error {
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
		return fmt.Errorf("failed to parse ttl from metadata: %s", err)
	}
	// apply global TTL
	if ttl == nil {
		ttl = r.metadata.TTLInSeconds
	}

	firstWrite := 1
	if req.Options.Concurrency == state.FirstWrite {
		firstWrite = 0
	}

	var bt []byte
	var setQuery string
	if contentType, ok := req.Metadata[daprmetadata.ContentType]; ok && contentType == contenttype.JSONContentType {
		setQuery = setJSONQuery
		bt, _ = utils.Marshal(&jsonEntry{Data: req.Value}, r.json.Marshal)
	} else {
		setQuery = setDefaultQuery
		bt, _ = utils.Marshal(req.Value, r.json.Marshal)
	}

	// redis 6 and below
	if r.legacyRedis {
		err = r.clientv8.Do(r.ctx, "EVAL", setQuery, 1, req.Key, ver, bt, firstWrite).Err()
	} else {
		err = r.clientv9.Do(r.ctx, "EVAL", setQuery, 1, req.Key, ver, bt, firstWrite).Err()
	}
	if err != nil {
		if req.ETag != nil {
			return state.NewETagError(state.ETagMismatch, err)
		}

		return fmt.Errorf("failed to set key %s: %s", req.Key, err)
	}

	if ttl != nil && *ttl > 0 {
		// redis 6 and below
		if r.legacyRedis {
			_, err = r.clientv8.Do(r.ctx, "EXPIRE", req.Key, *ttl).Result()
		} else {
			_, err = r.clientv9.Do(r.ctx, "EXPIRE", req.Key, *ttl).Result()
		}
		if err != nil {
			return fmt.Errorf("failed to set key %s ttl: %s", req.Key, err)
		}
	}

	if ttl != nil && *ttl <= 0 {
		// redis 6 and below
		if r.legacyRedis {
			_, err = r.clientv8.Do(r.ctx, "PERSIST", req.Key).Result()
		} else {
			_, err = r.clientv9.Do(r.ctx, "PERSIST", req.Key).Result()
		}
		if err != nil {
			return fmt.Errorf("failed to persist key %s: %s", req.Key, err)
		}
	}

	if req.Options.Consistency == state.Strong && r.replicas > 0 {
		// redis 6 and below
		if r.legacyRedis {
			_, err = r.clientv8.Do(r.ctx, "WAIT", r.replicas, 1000).Result()
		} else {
			_, err = r.clientv9.Do(r.ctx, "WAIT", r.replicas, 1000).Result()
		}
		if err != nil {
			return fmt.Errorf("redis waiting for %v replicas to acknowledge write, err: %s", r.replicas, err.Error())
		}
	}

	return nil
}

// Set saves state into redis.
func (r *StateStore) Set(req *state.SetRequest) error {
	return state.SetWithOptions(r.setValue, req)
}

// Multi performs a transactional operation. succeeds only if all operations succeed, and fails if one or more operations fail.
func (r *StateStore) Multi(request *state.TransactionalStateRequest) error {
	var setQuery, delQuery string
	var isJSON bool
	if contentType, ok := request.Metadata[daprmetadata.ContentType]; ok && contentType == contenttype.JSONContentType {
		isJSON = true
		setQuery = setJSONQuery
		delQuery = delJSONQuery
	} else {
		setQuery = setDefaultQuery
		delQuery = delDefaultQuery
	}

	var pipe any
	// redis 6 and below
	if r.legacyRedis {
		pipe = r.clientv8.TxPipeline()
	} else {
		pipe = r.clientv9.TxPipeline()
	}
	for _, o := range request.Operations {
		if o.Operation == state.Upsert {
			req := o.Request.(state.SetRequest)
			ver, err := r.parseETag(&req)
			if err != nil {
				return err
			}
			ttl, err := r.parseTTL(&req)
			if err != nil {
				return fmt.Errorf("failed to parse ttl from metadata: %s", err)
			}
			// apply global TTL
			if ttl == nil {
				ttl = r.metadata.TTLInSeconds
			}
			var bt []byte
			if isJSON {
				bt, _ = utils.Marshal(&jsonEntry{Data: req.Value}, r.json.Marshal)
			} else {
				bt, _ = utils.Marshal(req.Value, r.json.Marshal)
			}

			if r.legacyRedis {
				pipe.(redisClientv8.Pipeliner).Do(r.ctx, "EVAL", setQuery, 1, req.Key, ver, bt)
				if ttl != nil && *ttl > 0 {
					pipe.(redisClientv8.Pipeliner).Do(r.ctx, "EXPIRE", req.Key, *ttl)
				}
				if ttl != nil && *ttl <= 0 {
					pipe.(redisClientv8.Pipeliner).Do(r.ctx, "PERSIST", req.Key)
				}
			} else {
				pipe.(redisClientv9.Pipeliner).Do(r.ctx, "EVAL", setQuery, 1, req.Key, ver, bt)
				if ttl != nil && *ttl > 0 {
					pipe.(redisClientv9.Pipeliner).Do(r.ctx, "EXPIRE", req.Key, *ttl)
				}
				if ttl != nil && *ttl <= 0 {
					pipe.(redisClientv9.Pipeliner).Do(r.ctx, "PERSIST", req.Key)
				}
			}

		} else if o.Operation == state.Delete {
			req := o.Request.(state.DeleteRequest)
			if req.ETag == nil {
				etag := "0"
				req.ETag = &etag
			}
			if r.legacyRedis {
				pipe.(redisClientv8.Pipeliner).Do(r.ctx, "EVAL", delQuery, 1, req.Key, *req.ETag)
			} else {
				pipe.(redisClientv9.Pipeliner).Do(r.ctx, "EVAL", delQuery, 1, req.Key, *req.ETag)
			}
		}
	}

	var err error
	// redis 6 and below
	if r.legacyRedis {
		_, err = pipe.(redisClientv8.Pipeliner).Exec(r.ctx)
	} else {
		_, err = pipe.(redisClientv9.Pipeliner).Exec(r.ctx)
	}

	return err
}

func (r *StateStore) registerSchemas() error {
	for name, elem := range r.querySchemas {
		r.logger.Infof("redis: create query index %s", name)
		// redis 6 and below
		if r.legacyRedis {
			if err := r.clientv8.Do(r.ctx, elem.schema...).Err(); err != nil {
				if err.Error() != "Index already exists" {
					return err
				}
				r.logger.Infof("redis: drop stale query index %s", name)
				if err = r.clientv8.Do(r.ctx, "FT.DROPINDEX", name).Err(); err != nil {
					return err
				}
				if err = r.clientv8.Do(r.ctx, elem.schema...).Err(); err != nil {
					return err
				}
			}
		} else {
			if err := r.clientv9.Do(r.ctx, elem.schema...).Err(); err != nil {
				if err.Error() != "Index already exists" {
					return err
				}
				r.logger.Infof("redis: drop stale query index %s", name)
				if err = r.clientv9.Do(r.ctx, "FT.DROPINDEX", name).Err(); err != nil {
					return err
				}
				if err = r.clientv9.Do(r.ctx, elem.schema...).Err(); err != nil {
					return err
				}
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
			version = ptr.String(versionVal)
			seenVersion = true
		}
	}
	if !seenData || !seenVersion {
		return "", nil, fmt.Errorf("required hash field 'data' or 'version' was not found")
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
func (r *StateStore) Query(req *state.QueryRequest) (*state.QueryResponse, error) {
	indexName, ok := daprmetadata.TryGetQueryIndexName(req.Metadata)
	if !ok {
		return nil, fmt.Errorf("query index not found")
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
	var data []state.QueryItem
	var token string
	var err error
	// redis 6 and below
	if r.legacyRedis {
		data, token, err = q.executev8(r.ctx, r.clientv8)
	} else {
		data, token, err = q.executev9(r.ctx, r.clientv9)
	}
	if err != nil {
		return &state.QueryResponse{}, err
	}

	return &state.QueryResponse{
		Results: data,
		Token:   token,
	}, nil
}

func (r *StateStore) Close() error {
	r.cancel()

	return r.clientv8.Close()
}
