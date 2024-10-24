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
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"

	rediscomponent "github.com/dapr/components-contrib/common/component/redis"
	"github.com/dapr/components-contrib/configuration"
	"github.com/dapr/components-contrib/configuration/redis/internal"
	contribMetadata "github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const (
	connectedSlavesReplicas   = "connected_slaves:"
	infoReplicationDelimiter  = "\r\n"
	defaultBase               = 10
	defaultBitSize            = 0
	redisWrongTypeIdentifyStr = "WRONGTYPE"
)

// ConfigurationStore is a Redis configuration store.
type ConfigurationStore struct {
	client         rediscomponent.RedisClient
	clientSettings *rediscomponent.Settings
	json           jsoniter.API
	replicas       int

	cancelMap sync.Map
	wg        sync.WaitGroup
	lock      sync.RWMutex

	logger logger.Logger
}

// NewRedisConfigurationStore returns a new redis state store.
func NewRedisConfigurationStore(logger logger.Logger) configuration.Store {
	s := &ConfigurationStore{
		json:   jsoniter.ConfigFastest,
		logger: logger,
	}

	return s
}

// Init does metadata and connection parsing.
func (r *ConfigurationStore) Init(ctx context.Context, metadata configuration.Metadata) error {
	var err error
	r.client, r.clientSettings, err = rediscomponent.ParseClientFromProperties(metadata.Properties, contribMetadata.ConfigurationStoreType, ctx, &r.logger)
	if err != nil {
		return err
	}

	if _, err = r.client.PingResult(ctx); err != nil {
		return fmt.Errorf("redis store: error connecting to redis at %s: %s", r.clientSettings.Host, err)
	}

	r.replicas, err = r.getConnectedSlaves(ctx)

	return err
}

func (r *ConfigurationStore) getConnectedSlaves(ctx context.Context) (int, error) {
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

func (r *ConfigurationStore) parseConnectedSlaves(res string) int {
	infos := strings.Split(res, infoReplicationDelimiter)
	for _, info := range infos {
		if strings.Contains(info, connectedSlavesReplicas) {
			parsedReplicas, _ := strconv.ParseUint(info[len(connectedSlavesReplicas):], 10, 32)
			return int(parsedReplicas) //nolint:gosec
		}
	}

	return 0
}

func (r *ConfigurationStore) Get(ctx context.Context, req *configuration.GetRequest) (*configuration.GetResponse, error) {
	keys := req.Keys
	var err error
	if len(keys) == 0 {
		var res interface{}
		if res, err = r.client.DoRead(ctx, "KEYS", "*"); err != nil {
			r.logger.Errorf("failed to all keys, error is %s", err)
			return nil, err
		}
		keyList := res.([]interface{})
		for _, key := range keyList {
			keys = append(keys, fmt.Sprint(key))
		}
	}

	items := make(map[string]*configuration.Item, len(keys))

	// query by keys
	for _, redisKey := range keys {
		item := &configuration.Item{
			Metadata: map[string]string{},
		}

		redisValue, err := r.client.Get(ctx, redisKey)
		if err != nil {
			if err.Error() == redis.Nil.Error() {
				r.logger.Warnf("redis key %s does not exist, ignore it\n", redisKey)
				continue
			}
			if strings.Contains(err.Error(), redisWrongTypeIdentifyStr) {
				r.logger.Warnf("redis key %s 's type is not supported, ignore it\n", redisKey)
				continue
			}
			return &configuration.GetResponse{}, fmt.Errorf("fail to get configuration for redis key=%s, error is %s", redisKey, err)
		}
		val, version := internal.GetRedisValueAndVersion(redisValue)
		item.Version = version
		item.Value = val

		if item.Value != "" {
			items[redisKey] = item
		}
	}

	return &configuration.GetResponse{
		Items: items,
	}, nil
}

func (r *ConfigurationStore) Subscribe(ctx context.Context, req *configuration.SubscribeRequest, handler configuration.UpdateHandler) (string, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	subscribeID := uuid.New().String()
	ctx, cancel := context.WithCancel(ctx)
	r.cancelMap.Store(subscribeID, cancel)

	if len(req.Keys) == 0 {
		// subscribe all keys
		allKeysChannel := internal.GetRedisChannelFromKey("*", r.clientSettings.DB)
		subscribeArgs := &rediscomponent.ConfigurationSubscribeArgs{
			HandleSubscribedChange: r.handleSubscribedChange,
			Req:                    req,
			Handler:                handler,
			RedisChannel:           allKeysChannel,
			IsAllKeysChannel:       true,
			ID:                     subscribeID,
		}

		r.wg.Add(1)
		go func() {
			r.client.ConfigurationSubscribe(ctx, subscribeArgs)
			cancel()
			r.cancelMap.Delete(subscribeID)
			r.wg.Done()
		}()
		return subscribeID, nil
	}

	for _, k := range req.Keys {
		// subscribe single key
		redisChannel := internal.GetRedisChannelFromKey(k, r.clientSettings.DB)
		subscribeArgs := &rediscomponent.ConfigurationSubscribeArgs{
			HandleSubscribedChange: r.handleSubscribedChange,
			Req:                    req,
			Handler:                handler,
			RedisChannel:           redisChannel,
			IsAllKeysChannel:       false,
			ID:                     subscribeID,
		}

		r.wg.Add(1)
		go func() {
			r.client.ConfigurationSubscribe(ctx, subscribeArgs)
			cancel()
			r.cancelMap.Delete(subscribeID)
			r.wg.Done()
		}()
	}

	return subscribeID, nil
}

func (r *ConfigurationStore) Unsubscribe(ctx context.Context, req *configuration.UnsubscribeRequest) error {
	r.lock.RLock()
	defer r.lock.RUnlock()

	if cancel, ok := r.cancelMap.LoadAndDelete(req.ID); ok {
		cancel.(context.CancelFunc)()
		return nil
	}

	return fmt.Errorf("subscription with id %s does not exist", req.ID)
}

func (r *ConfigurationStore) handleSubscribedChange(ctx context.Context, req *configuration.SubscribeRequest, handler configuration.UpdateHandler, redisChannel string, id string) {
	targetKey, err := internal.ParseRedisKeyFromChannel(redisChannel, r.clientSettings.DB)
	if err != nil {
		r.logger.Errorf("parse redis key failed: %s", err)
		return
	}

	var items map[string]*configuration.Item

	// get all keys if only one is changed
	getResponse, errGet := r.Get(ctx, &configuration.GetRequest{
		Metadata: req.Metadata,
		Keys:     []string{targetKey},
	})
	if errGet != nil {
		r.logger.Errorf("get response from redis failed: %s", err)
		return
	}
	items = getResponse.Items
	if len(items) == 0 {
		items = map[string]*configuration.Item{
			targetKey: {},
		}
	}

	e := &configuration.UpdateEvent{
		Items: items,
		ID:    id,
	}
	err = handler(ctx, e)
	if err != nil {
		r.logger.Errorf("fail to call handler to notify event for configuration update subscribe: %s", err)
	}
}

// GetComponentMetadata returns the metadata of the component.
func (r *ConfigurationStore) GetComponentMetadata() (metadataInfo contribMetadata.MetadataMap) {
	metadataStruct := rediscomponent.Settings{}
	contribMetadata.GetMetadataInfoFromStructType(reflect.TypeOf(metadataStruct), &metadataInfo, contribMetadata.ConfigurationStoreType)
	return
}

func (r *ConfigurationStore) Close() error {
	defer r.wg.Wait()
	r.lock.Lock()
	defer r.lock.Unlock()

	r.cancelMap.Range(func(key, value interface{}) bool {
		value.(context.CancelFunc)()
		return true
	})
	r.cancelMap.Clear()

	return r.client.Close()
}
