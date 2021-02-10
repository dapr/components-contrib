// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/state/utils"
	"github.com/dapr/dapr/pkg/logger"
	redis "github.com/go-redis/redis/v7"
	jsoniter "github.com/json-iterator/go"
)

const (
	setQuery                 = "local var1 = redis.pcall(\"HGET\", KEYS[1], \"version\"); if type(var1) == \"table\" then redis.call(\"DEL\", KEYS[1]); end; if not var1 or type(var1)==\"table\" or var1 == \"\" or var1 == ARGV[1] or ARGV[1] == \"0\" then redis.call(\"HSET\", KEYS[1], \"data\", ARGV[2]) return redis.call(\"HINCRBY\", KEYS[1], \"version\", 1) else return error(\"failed to set key \" .. KEYS[1]) end"
	delQuery                 = "local var1 = redis.pcall(\"HGET\", KEYS[1], \"version\"); if not var1 or type(var1)==\"table\" or var1 == ARGV[1] or var1 == \"\" or ARGV[1] == \"0\" then return redis.call(\"DEL\", KEYS[1]) else return error(\"failed to delete \" .. KEYS[1]) end"
	connectedSlavesReplicas  = "connected_slaves:"
	infoReplicationDelimiter = "\r\n"
	host                     = "redisHost"
	password                 = "redisPassword"
	enableTLS                = "enableTLS"
	maxRetries               = "maxRetries"
	maxRetryBackoff          = "maxRetryBackoff"
	failover                 = "failover"
	sentinelMasterName       = "sentinelMasterName"
	defaultBase              = 10
	defaultBitSize           = 0
	defaultDB                = 0
	defaultMaxRetries        = 3
	defaultMaxRetryBackoff   = time.Second * 2
	defaultEnableTLS         = false
)

// StateStore is a Redis state store
type StateStore struct {
	state.DefaultBulkStore
	client   *redis.Client
	json     jsoniter.API
	metadata metadata
	replicas int

	logger logger.Logger
}

// NewRedisStateStore returns a new redis state store
func NewRedisStateStore(logger logger.Logger) *StateStore {
	s := &StateStore{
		json:   jsoniter.ConfigFastest,
		logger: logger,
	}
	s.DefaultBulkStore = state.NewDefaultBulkStore(s)

	return s
}

func parseRedisMetadata(meta state.Metadata) (metadata, error) {
	m := metadata{}

	if val, ok := meta.Properties[host]; ok && val != "" {
		m.host = val
	} else {
		return m, errors.New("redis store error: missing host address")
	}

	if val, ok := meta.Properties[password]; ok && val != "" {
		m.password = val
	}

	m.enableTLS = defaultEnableTLS
	if val, ok := meta.Properties[enableTLS]; ok && val != "" {
		tls, err := strconv.ParseBool(val)
		if err != nil {
			return m, fmt.Errorf("redis store error: can't parse enableTLS field: %s", err)
		}
		m.enableTLS = tls
	}

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
			return m, fmt.Errorf("redis store error: can't parse maxRetries field: %s", err)
		}
		m.maxRetryBackoff = time.Duration(parsedVal)
	}

	if val, ok := meta.Properties[failover]; ok && val != "" {
		failover, err := strconv.ParseBool(val)
		if err != nil {
			return m, fmt.Errorf("redis store error: can't parse failover field: %s", err)
		}
		m.failover = failover
	}

	// set the sentinelMasterName only with failover == true.
	if m.failover {
		if val, ok := meta.Properties[sentinelMasterName]; ok && val != "" {
			m.sentinelMasterName = val
		} else {
			return m, errors.New("redis store error: missing sentinelMasterName")
		}
	}

	return m, nil
}

// Init does metadata and connection parsing
func (r *StateStore) Init(metadata state.Metadata) error {
	m, err := parseRedisMetadata(metadata)
	if err != nil {
		return err
	}
	r.metadata = m

	if r.metadata.failover {
		r.client = r.newFailoverClient(m)
	} else {
		r.client = r.newClient(m)
	}

	if _, err = r.client.Ping().Result(); err != nil {
		return fmt.Errorf("redis store: error connecting to redis at %s: %s", m.host, err)
	}

	r.replicas, err = r.getConnectedSlaves()

	return err
}

func (r *StateStore) newClient(m metadata) *redis.Client {
	opts := &redis.Options{
		Addr:            m.host,
		Password:        m.password,
		DB:              defaultDB,
		MaxRetries:      m.maxRetries,
		MaxRetryBackoff: m.maxRetryBackoff,
	}

	// tell the linter to skip a check here.
	/* #nosec */
	if m.enableTLS {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: m.enableTLS,
		}
	}

	return redis.NewClient(opts)
}

func (r *StateStore) newFailoverClient(m metadata) *redis.Client {
	opts := &redis.FailoverOptions{
		MasterName:      r.metadata.sentinelMasterName,
		SentinelAddrs:   []string{r.metadata.host},
		DB:              defaultDB,
		MaxRetries:      m.maxRetries,
		MaxRetryBackoff: m.maxRetryBackoff,
	}

	/* #nosec */
	if m.enableTLS {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: m.enableTLS,
		}
	}

	return redis.NewFailoverClient(opts)
}

func (r *StateStore) getConnectedSlaves() (int, error) {
	res, err := r.client.DoContext(context.Background(), "INFO", "replication").Result()
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
	_, err := r.client.DoContext(context.Background(), "EVAL", delQuery, 1, req.Key, *req.ETag).Result()
	if err != nil {
		return state.NewETagError(state.ETagMismatch, err)
	}

	return nil
}

// Delete performs a delete operation
func (r *StateStore) Delete(req *state.DeleteRequest) error {
	err := state.CheckRequestOptions(req.Options)
	if err != nil {
		return err
	}

	return state.DeleteWithOptions(r.deleteValue, req)
}

func (r *StateStore) directGet(req *state.GetRequest) (*state.GetResponse, error) {
	res, err := r.client.DoContext(context.Background(), "GET", req.Key).Result()
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

// Get retrieves state from redis with a key
func (r *StateStore) Get(req *state.GetRequest) (*state.GetResponse, error) {
	res, err := r.client.DoContext(context.Background(), "HGETALL", req.Key).Result() // Prefer values with ETags
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

	bt, _ := utils.Marshal(req.Value, r.json.Marshal)

	_, err = r.client.DoContext(context.Background(), "EVAL", setQuery, 1, req.Key, ver, bt).Result()
	if err != nil {
		if req.ETag != nil {
			return state.NewETagError(state.ETagMismatch, err)
		}

		return fmt.Errorf("failed to set key %s: %s", req.Key, err)
	}

	if req.Options.Consistency == state.Strong && r.replicas > 0 {
		_, err = r.client.DoContext(context.Background(), "WAIT", r.replicas, 1000).Result()
		if err != nil {
			return fmt.Errorf("timed out while waiting for %v replicas to acknowledge write", r.replicas)
		}
	}

	return nil
}

// Set saves state into redis
func (r *StateStore) Set(req *state.SetRequest) error {
	return state.SetWithOptions(r.setValue, req)
}

// Multi performs a transactional operation. succeeds only if all operations succeed, and fails if one or more operations fail
func (r *StateStore) Multi(request *state.TransactionalStateRequest) error {
	pipe := r.client.TxPipeline()
	for _, o := range request.Operations {
		if o.Operation == state.Upsert {
			req := o.Request.(state.SetRequest)
			ver, err := r.parseETag(&req)
			if err != nil {
				return err
			}
			bt, _ := utils.Marshal(req.Value, r.json.Marshal)
			pipe.Do("EVAL", setQuery, 1, req.Key, ver, bt)
		} else if o.Operation == state.Delete {
			req := o.Request.(state.DeleteRequest)
			if req.ETag == nil {
				etag := "0"
				req.ETag = &etag
			}
			pipe.Do("EVAL", delQuery, 1, req.Key, *req.ETag)
		}
	}

	_, err := pipe.Exec()

	return err
}

func (r *StateStore) getKeyVersion(vals []interface{}) (data string, version string, err error) {
	seenData := false
	seenVersion := false
	for i := 0; i < len(vals); i += 2 {
		field, _ := strconv.Unquote(fmt.Sprintf("%q", vals[i]))
		switch field {
		case "data":
			data, _ = strconv.Unquote(fmt.Sprintf("%q", vals[i+1]))
			seenData = true
		case "version":
			version, _ = strconv.Unquote(fmt.Sprintf("%q", vals[i+1]))
			seenVersion = true
		}
	}
	if !seenData || !seenVersion {
		return "", "", errors.New("required hash field 'data' or 'version' was not found")
	}

	return data, version, nil
}

func (r *StateStore) parseETag(req *state.SetRequest) (int, error) {
	if req.Options.Concurrency == state.LastWrite || req.ETag == nil || (req.ETag != nil && *req.ETag == "") {
		return 0, nil
	}
	ver, err := strconv.Atoi(*req.ETag)
	if err != nil {
		return -1, state.NewETagError(state.ETagInvalid, err)
	}

	return ver, nil
}
