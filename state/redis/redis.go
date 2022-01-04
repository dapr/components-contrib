// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/agrea/ptr"
	"github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"

	rediscomponent "github.com/dapr/components-contrib/internal/component/redis"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/kit/logger"
)

const (
	setQuery                 = "local var1 = redis.pcall(\"HGET\", KEYS[1], \"version\"); if type(var1) == \"table\" then redis.call(\"DEL\", KEYS[1]); end; local var2 = redis.pcall(\"HGET\", KEYS[1], \"first-write\"); if not var1 or type(var1)==\"table\" or var1 == \"\" or var1 == ARGV[1] or (not var2 and ARGV[1] == \"0\") then redis.call(\"HSET\", KEYS[1], \"data\", ARGV[2]); if ARGV[3] == \"0\" then redis.call(\"HSET\", KEYS[1], \"first-write\", 0); end; return redis.call(\"HINCRBY\", KEYS[1], \"version\", 1) else return error(\"failed to set key \" .. KEYS[1]) end"
	delQuery                 = "local var1 = redis.pcall(\"HGET\", KEYS[1], \"version\"); if not var1 or type(var1)==\"table\" or var1 == ARGV[1] or var1 == \"\" or ARGV[1] == \"0\" then return redis.call(\"DEL\", KEYS[1]) else return error(\"failed to delete \" .. KEYS[1]) end"
	connectedSlavesReplicas  = "connected_slaves:"
	infoReplicationDelimiter = "\r\n"
	maxRetries               = "maxRetries"
	maxRetryBackoff          = "maxRetryBackoff"
	ttlInSeconds             = "ttlInSeconds"
	defaultBase              = 10
	defaultBitSize           = 0
	defaultDB                = 0
	defaultMaxRetries        = 3
	defaultMaxRetryBackoff   = time.Second * 2
)

// StateStore is a Redis state store.
type StateStore struct {
	state.DefaultBulkStore
	client         redis.UniversalClient
	clientSettings *rediscomponent.Settings
	json           jsoniter.API
	metadata       metadata
	replicas       int

	features []state.Feature
	logger   logger.Logger

	ctx    context.Context
	cancel context.CancelFunc
}

// NewRedisStateStore returns a new redis state store.
func NewRedisStateStore(logger logger.Logger) *StateStore {
	s := &StateStore{
		json:     jsoniter.ConfigFastest,
		features: []state.Feature{state.FeatureETag, state.FeatureTransactional},
		logger:   logger,
	}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

func parseRedisMetadata(meta state.Metadata) (metadata, error) {
	m := metadata{}

	m.maxRetries = defaultMaxRetries
	if val, ok := meta.Properties[maxRetries]; ok && val != "" {
		parsedVal, err := strconv.ParseInt(val, defaultBase, defaultBitSize)
		if err != nil {
			return m, fmt.Errorf("redis store error: can't parse maxRetries field: %s", err)
		}
		m.maxRetries = int(parsedVal)
	}

	m.maxRetryBackoff = defaultMaxRetryBackoff
	if val, ok := meta.Properties[maxRetryBackoff]; ok && val != "" {
		parsedVal, err := strconv.ParseInt(val, defaultBase, defaultBitSize)
		if err != nil {
			return m, fmt.Errorf("redis store error: can't parse maxRetryBackoff field: %s", err)
		}
		m.maxRetryBackoff = time.Duration(parsedVal)
	}

	if val, ok := meta.Properties[ttlInSeconds]; ok && val != "" {
		parsedVal, err := strconv.ParseInt(val, defaultBase, defaultBitSize)
		if err != nil {
			return m, fmt.Errorf("redis store error: can't parse ttlInSeconds field: %s", err)
		}
		intVal := int(parsedVal)
		m.ttlInSeconds = &intVal
	} else {
		m.ttlInSeconds = nil
	}

	return m, nil
}

func (r *StateStore) Ping() error {
	if _, err := r.client.Ping(context.Background()).Result(); err != nil {
		return fmt.Errorf("redis store: error connecting to redis at %s: %s", r.clientSettings.Host, err)
	}

	return nil
}

// Init does metadata and connection parsing.
func (r *StateStore) Init(metadata state.Metadata) error {
	m, err := parseRedisMetadata(metadata)
	if err != nil {
		return err
	}
	r.metadata = m

	defaultSettings := rediscomponent.Settings{RedisMaxRetries: m.maxRetries, RedisMaxRetryInterval: rediscomponent.Duration(m.maxRetryBackoff)}
	r.client, r.clientSettings, err = rediscomponent.ParseClientFromProperties(metadata.Properties, &defaultSettings)
	if err != nil {
		return err
	}

	r.ctx, r.cancel = context.WithCancel(context.Background())

	if _, err = r.client.Ping(r.ctx).Result(); err != nil {
		return fmt.Errorf("redis store: error connecting to redis at %s: %s", r.clientSettings.Host, err)
	}

	r.replicas, err = r.getConnectedSlaves()

	return err
}

// Features returns the features available in this state store.
func (r *StateStore) Features() []state.Feature {
	return r.features
}

func (r *StateStore) getConnectedSlaves() (int, error) {
	res, err := r.client.Do(r.ctx, "INFO", "replication").Result()
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
	_, err := r.client.Do(r.ctx, "EVAL", delQuery, 1, req.Key, *req.ETag).Result()
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
	res, err := r.client.Do(r.ctx, "GET", req.Key).Result()
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

// Get retrieves state from redis with a key.
func (r *StateStore) Get(req *state.GetRequest) (*state.GetResponse, error) {
	res, err := r.client.Do(r.ctx, "HGETALL", req.Key).Result() // Prefer values with ETags
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
		ttl = r.metadata.ttlInSeconds
	}

	bt, _ := utils.Marshal(req.Value, r.json.Marshal)

	firstWrite := 1
	if req.Options.Concurrency == state.FirstWrite {
		firstWrite = 0
	}
	_, err = r.client.Do(r.ctx, "EVAL", setQuery, 1, req.Key, ver, bt, firstWrite).Result()
	if err != nil {
		if req.ETag != nil {
			return state.NewETagError(state.ETagMismatch, err)
		}

		return fmt.Errorf("failed to set key %s: %s", req.Key, err)
	}

	if ttl != nil && *ttl > 0 {
		_, err = r.client.Do(r.ctx, "EXPIRE", req.Key, *ttl).Result()
		if err != nil {
			return fmt.Errorf("failed to set key %s ttl: %s", req.Key, err)
		}
	}

	if ttl != nil && *ttl <= 0 {
		_, err = r.client.Do(r.ctx, "PERSIST", req.Key).Result()
		if err != nil {
			return fmt.Errorf("failed to persist key %s: %s", req.Key, err)
		}
	}

	if req.Options.Consistency == state.Strong && r.replicas > 0 {
		_, err = r.client.Do(r.ctx, "WAIT", r.replicas, 1000).Result()
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
	pipe := r.client.TxPipeline()
	for _, o := range request.Operations {
		//nolint:golint,nestif
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
				ttl = r.metadata.ttlInSeconds
			}

			bt, _ := utils.Marshal(req.Value, r.json.Marshal)
			pipe.Do(r.ctx, "EVAL", setQuery, 1, req.Key, ver, bt)
			if ttl != nil && *ttl > 0 {
				pipe.Do(r.ctx, "EXPIRE", req.Key, *ttl)
			}
			if ttl != nil && *ttl <= 0 {
				pipe.Do(r.ctx, "PERSIST", req.Key)
			}
		} else if o.Operation == state.Delete {
			req := o.Request.(state.DeleteRequest)
			if req.ETag == nil {
				etag := "0"
				req.ETag = &etag
			}
			pipe.Do(r.ctx, "EVAL", delQuery, 1, req.Key, *req.ETag)
		}
	}

	_, err := pipe.Exec(r.ctx)

	return err
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

func (r *StateStore) Close() error {
	r.cancel()

	return r.client.Close()
}
