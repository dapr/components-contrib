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
	"strings"
	"time"

	"golang.org/x/mod/semver"
)

const (
	ClusterType = "cluster"
	NodeType    = "node"
)

type RedisXMessage struct {
	ID     string
	Values map[string]interface{}
}

type RedisXStream struct {
	Stream   string
	Messages []RedisXMessage
}

type RedisXPendingExt struct {
	ID         string
	Consumer   string
	Idle       time.Duration
	RetryCount int64
}

type RedisPipeliner interface {
	Exec(ctx context.Context) error
	Do(ctx context.Context, args ...interface{})
}

//nolint:interfacebloat
type RedisClient interface {
	GetNilValueError() RedisError
	Context() context.Context
	DoRead(ctx context.Context, args ...interface{}) (interface{}, error)
	DoWrite(ctx context.Context, args ...interface{}) error
	Del(ctx context.Context, keys ...string) error
	Get(ctx context.Context, key string) (string, error)
	GetDel(ctx context.Context, key string) (string, error)
	Close() error
	PingResult(ctx context.Context) (string, error)
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) (*bool, error)
	EvalInt(ctx context.Context, script string, keys []string, args ...interface{}) (*int, error, error)
	XAdd(ctx context.Context, stream string, maxLenApprox int64, values map[string]interface{}) (string, error)
	XGroupCreateMkStream(ctx context.Context, stream string, group string, start string) error
	XAck(ctx context.Context, stream string, group string, messageID string) error
	XReadGroupResult(ctx context.Context, group string, consumer string, streams []string, count int64, block time.Duration) ([]RedisXStream, error)
	XPendingExtResult(ctx context.Context, stream string, group string, start string, end string, count int64) ([]RedisXPendingExt, error)
	XClaimResult(ctx context.Context, stream string, group string, consumer string, minIdleTime time.Duration, messageIDs []string) ([]RedisXMessage, error)
	TxPipeline() RedisPipeliner
	TTLResult(ctx context.Context, key string) (time.Duration, error)
}

func ParseClientFromProperties(properties map[string]string, defaultSettings *Settings) (client RedisClient, settings *Settings, err error) {
	if defaultSettings == nil {
		settings = &Settings{}
	} else {
		settings = defaultSettings
	}
	err = settings.Decode(properties)
	if err != nil {
		return nil, nil, fmt.Errorf("redis client configuration error: %w", err)
	}

	var c RedisClient
	if settings.Failover {
		c = newV8FailoverClient(settings)
	} else {
		c = newV8Client(settings)
	}
	version, versionErr := GetServerVersion(c)
	c.Close() // close the client to avoid leaking connections

	useNewClient := false
	if versionErr != nil {
		// we couldn't query the server version, so we will assume the v8 client is not supported
		useNewClient = true
	} else if semver.Compare("v"+version, "v7.0.0") > -1 {
		// if the server version is >= 7, we will use the v9 client
		useNewClient = true
	}
	if useNewClient {
		if settings.Failover {
			return newV9FailoverClient(settings), settings, nil
		}
		return newV9Client(settings), settings, nil
	} else {
		if settings.Failover {
			return newV8FailoverClient(settings), settings, nil
		}
		return newV8Client(settings), settings, nil
	}
}

func ClientHasJSONSupport(c RedisClient) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := c.DoWrite(ctx, "JSON.GET")
	if err == nil {
		return true
	}

	if strings.HasPrefix(err.Error(), "ERR unknown command") {
		return false
	}
	return true
}

func GetServerVersion(c RedisClient) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := c.DoRead(ctx, "INFO", "server")
	if err != nil {
		return "", err
	}
	// get row in string res beginning with "redis_version"
	rows := strings.Split(res.(string), "\n")
	for _, row := range rows {
		if strings.HasPrefix(row, "redis_version:") {
			return strings.TrimSpace(strings.Split(row, ":")[1]), nil
		}
	}
	return "", fmt.Errorf("could not find redis_version in redis info response")
}

type RedisError string

func (e RedisError) Error() string { return string(e) }

func (RedisError) RedisError() {}
