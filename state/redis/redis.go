// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

package redis

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/dapr/pkg/logger"

	jsoniter "github.com/json-iterator/go"

	redis "github.com/go-redis/redis/v7"
)

const (
	setQuery                 = "local var1 = redis.pcall(\"HGET\", KEYS[1], \"version\"); if type(var1) == \"table\" then redis.call(\"DEL\", KEYS[1]); end; if not var1 or type(var1)==\"table\" or var1 == \"\" or var1 == ARGV[1] or ARGV[1] == \"0\" then redis.call(\"HSET\", KEYS[1], \"data\", ARGV[2]) return redis.call(\"HINCRBY\", KEYS[1], \"version\", 1) else return error(\"failed to set key \" .. KEYS[1]) end"
	delQuery                 = "local var1 = redis.pcall(\"HGET\", KEYS[1], \"version\"); if not var1 or type(var1)==\"table\" or var1 == ARGV[1] or var1 == \"\" or ARGV[1] == \"0\" then return redis.call(\"DEL\", KEYS[1]) else return error(\"failed to delete \" .. KEYS[1]) end"
	connectedSlavesReplicas  = "connected_slaves:"
	infoReplicationDelimiter = "\r\n"
)

// StateStore is a Redis state store
type StateStore struct {
	client   *redis.Client
	json     jsoniter.API
	replicas int

	logger logger.Logger
}

type credentials struct {
	Host     string `json:"redisHost"`
	Password string `json:"redisPassword"`
}

// NewRedisStateStore returns a new redis state store
func NewRedisStateStore(logger logger.Logger) *StateStore {
	return &StateStore{
		json:   jsoniter.ConfigFastest,
		logger: logger,
	}
}

// Init does metadata and connection parsing
func (r *StateStore) Init(metadata state.Metadata) error {
	rand.Seed(time.Now().Unix())

	connInfo := metadata.Properties
	b, err := json.Marshal(connInfo)
	if err != nil {
		return err
	}

	var redisCreds credentials
	err = json.Unmarshal(b, &redisCreds)
	if err != nil {
		return err
	}

	var tlsConfig *tls.Config
	err = json.Unmarshal(b, tlsConfig)
	if err != nil {
		return err
	}

	opts := redis.Options{
		DB:        0,
		Password:  redisCreds.Password,
		TLSConfig: tlsConfig,
	}

	r.client = redis.NewClient(&opts)

	r.replicas, err = r.getConnectedSlaves()

	return err
}

func (r *StateStore) getConnectedSlaves() (int, error) {
	res, err := r.client.DoContext(context.Background(), "INFO replication").Result()
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
	if req.ETag == "" {
		req.ETag = "0"
	}
	_, err := r.client.DoContext(context.Background(), "EVAL", delQuery, 1, req.Key, req.ETag).Result()

	if err != nil {
		return fmt.Errorf("failed to delete key '%s' due to ETag mismatch", req.Key)
	}

	return nil
}

// Delete performs a delete operation
func (r *StateStore) Delete(req *state.DeleteRequest) error {
	err := state.CheckDeleteRequestOptions(req)
	if err != nil {
		return err
	}
	return state.DeleteWithRetries(r.deleteValue, req)
}

// BulkDelete performs a bulk delete operation
func (r *StateStore) BulkDelete(req []state.DeleteRequest) error {
	for _, re := range req {
		err := r.Delete(&re)
		if err != nil {
			return err
		}
	}

	return nil
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
		return r.directGet(req) //Falls back to original get
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
	err := state.CheckSetRequestOptions(req)
	if err != nil {
		return err
	}
	ver, err := r.parseETag(req.ETag)
	if err != nil {
		return err
	}

	if req.Options.Concurrency == state.LastWrite {
		ver = 0
	}

	var bt []byte
	b, ok := req.Value.([]byte)
	if ok {
		bt = b
	} else {
		bt, _ = r.json.Marshal(req.Value)
	}

	_, err = r.client.DoContext(context.Background(), "EVAL", setQuery, 1, req.Key, ver, bt).Result()
	if err != nil {
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
	return state.SetWithRetries(r.setValue, req)
}

// BulkSet performs a bulks save operation
func (r *StateStore) BulkSet(req []state.SetRequest) error {
	for _, s := range req {
		err := r.Set(&s)
		if err != nil {
			return err
		}
	}

	return nil
}

// Multi performs a transactional operation. succeeds only if all operations succeed, and fails if one or more operations fail
func (r *StateStore) Multi(operations []state.TransactionalRequest) error {

	pipe := r.client.TxPipeline()
	for _, o := range operations {
		if o.Operation == state.Upsert {
			req := o.Request.(state.SetRequest)
			b, _ := r.json.Marshal(req.Value)
			pipe.Set(req.Key, b, 0)
		} else if o.Operation == state.Delete {
			req := o.Request.(state.DeleteRequest)
			pipe.Del(req.Key)
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

func (r *StateStore) parseETag(etag string) (int, error) {
	ver := 0
	var err error
	if etag != "" {
		ver, err = strconv.Atoi(etag)
		if err != nil {
			return -1, err
		}
	}
	return ver, nil
}
