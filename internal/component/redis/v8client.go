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
	"crypto/tls"
	"strings"
	"time"

	v8 "github.com/go-redis/redis/v8"
)

type v8Pipeliner struct {
	pipeliner v8.Pipeliner
}

func (p v8Pipeliner) Exec(ctx context.Context) error {
	_, err := p.pipeliner.Exec(ctx)
	return err
}

func (p v8Pipeliner) Do(ctx context.Context, args ...interface{}) {
	p.pipeliner.Do(ctx, args...)
}

// v8Client is an interface implementation of RedisClient

type v8Client struct {
	client v8.UniversalClient
}

func (c v8Client) DoErr(ctx context.Context, args ...interface{}) error {
	return c.client.Do(ctx, args...).Err()
}

func (c v8Client) DoResult(ctx context.Context, args ...interface{}) (interface{}, error) {
	return c.client.Do(ctx, args...).Result()
}

func (c v8Client) Context() context.Context {
	return c.client.Context()
}

func (c v8Client) Close() error {
	return c.client.Close()
}

func (c v8Client) PingResult(ctx context.Context) (string, error) {
	return c.client.Ping(ctx).Result()
}

func (c v8Client) EvalInt(ctx context.Context, script string, keys []string, args ...interface{}) (*int, error, error) {
	eval := c.client.Eval(ctx, script, keys, args...)
	if eval == nil {
		return nil, nil, nil
	}
	i, err := eval.Int()
	return &i, err, eval.Err()
}

func (c v8Client) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) (*bool, error) {
	nx := c.client.SetNX(ctx, key, value, expiration)
	if nx == nil {
		return nil, nil
	}
	val := nx.Val()
	return &val, nx.Err()
}

func (c v8Client) XAdd(ctx context.Context, stream string, maxLenApprox int64, values map[string]interface{}) (string, error) {
	return c.client.XAdd(ctx, &v8.XAddArgs{
		Stream:       stream,
		Values:       values,
		MaxLenApprox: maxLenApprox,
	}).Result()
}

func (c v8Client) XGroupCreateMkStream(ctx context.Context, stream string, group string, start string) error {
	return c.client.XGroupCreateMkStream(ctx, stream, group, start).Err()
}

func (c v8Client) XAck(ctx context.Context, stream string, group string, messageID string) error {
	ack := c.client.XAck(ctx, stream, group, messageID)
	return ack.Err()
}

func (c v8Client) XReadGroupResult(ctx context.Context, group string, consumer string, streams []string, count int64, block time.Duration) ([]RedisXStream, error) {
	res, err := c.client.XReadGroup(ctx,
		&v8.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  streams,
			Count:    count,
			Block:    block,
		},
	).Result()
	if err != nil {
		return nil, err
	}

	// convert []v8.XStream to []RedisXStream
	redisXStreams := make([]RedisXStream, len(res))
	for i, xStream := range res {
		redisXStreams[i].Stream = xStream.Stream
		redisXStreams[i].Messages = make([]RedisXMessage, len(xStream.Messages))
		for j, message := range xStream.Messages {
			redisXStreams[i].Messages[j].ID = message.ID
			redisXStreams[i].Messages[j].Values = message.Values
		}
	}

	return redisXStreams, nil
}

func (c v8Client) XPendingExtResult(ctx context.Context, stream string, group string, start string, end string, count int64) ([]RedisXPendingExt, error) {
	res, err := c.client.XPendingExt(ctx, &v8.XPendingExtArgs{
		Stream: stream,
		Group:  group,
		Start:  start,
		End:    end,
		Count:  count,
	}).Result()
	if err != nil {
		return nil, err
	}

	// convert []v8.XPendingExt to []RedisXPendingExt
	redisXPendingExts := make([]RedisXPendingExt, len(res))
	for i, xPendingExt := range res {
		redisXPendingExts[i] = RedisXPendingExt(xPendingExt)
	}
	return redisXPendingExts, nil
}

func (c v8Client) XClaimResult(ctx context.Context, stream string, group string, consumer string, minIdleTime time.Duration, messageIDs []string) ([]RedisXMessage, error) {
	res, err := c.client.XClaim(ctx, &v8.XClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  minIdleTime,
		Messages: messageIDs,
	}).Result()
	if err != nil {
		return nil, err
	}

	// convert res to []RedisXMessage
	redisXMessages := make([]RedisXMessage, len(res))
	for i, xMessage := range res {
		redisXMessages[i] = RedisXMessage(xMessage)
	}

	return redisXMessages, nil
}

func (c v8Client) TxPipeline() RedisPipeliner {
	return v8Pipeliner{pipeliner: c.client.TxPipeline()}
}

func (c v8Client) TTLResult(ctx context.Context, key string) (time.Duration, error) {
	return c.client.TTL(ctx, key).Result()
}

func newV8FailoverClient(s *Settings) RedisClient {
	if s == nil {
		return nil
	}
	opts := &v8.FailoverOptions{
		DB:                 s.DB,
		MasterName:         s.SentinelMasterName,
		SentinelAddrs:      []string{s.Host},
		Password:           s.Password,
		Username:           s.Username,
		MaxRetries:         s.RedisMaxRetries,
		MaxRetryBackoff:    time.Duration(s.RedisMaxRetryInterval),
		MinRetryBackoff:    time.Duration(s.RedisMinRetryInterval),
		DialTimeout:        time.Duration(s.DialTimeout),
		ReadTimeout:        time.Duration(s.ReadTimeout),
		WriteTimeout:       time.Duration(s.WriteTimeout),
		PoolSize:           s.PoolSize,
		MaxConnAge:         time.Duration(s.MaxConnAge),
		MinIdleConns:       s.MinIdleConns,
		PoolTimeout:        time.Duration(s.PoolTimeout),
		IdleCheckFrequency: time.Duration(s.IdleCheckFrequency),
		IdleTimeout:        time.Duration(s.IdleTimeout),
	}

	/* #nosec */
	if s.EnableTLS {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: s.EnableTLS,
		}
	}

	if s.RedisType == ClusterType {
		opts.SentinelAddrs = strings.Split(s.Host, ",")

		return v8Client{client: v8.NewFailoverClusterClient(opts)}
	}

	return v8Client{client: v8.NewFailoverClient(opts)}
}

func newV8Client(s *Settings) RedisClient {
	if s == nil {
		return nil
	}
	if s.RedisType == ClusterType {
		options := &v8.ClusterOptions{
			Addrs:              strings.Split(s.Host, ","),
			Password:           s.Password,
			Username:           s.Username,
			MaxRetries:         s.RedisMaxRetries,
			MaxRetryBackoff:    time.Duration(s.RedisMaxRetryInterval),
			MinRetryBackoff:    time.Duration(s.RedisMinRetryInterval),
			DialTimeout:        time.Duration(s.DialTimeout),
			ReadTimeout:        time.Duration(s.ReadTimeout),
			WriteTimeout:       time.Duration(s.WriteTimeout),
			PoolSize:           s.PoolSize,
			MaxConnAge:         time.Duration(s.MaxConnAge),
			MinIdleConns:       s.MinIdleConns,
			PoolTimeout:        time.Duration(s.PoolTimeout),
			IdleCheckFrequency: time.Duration(s.IdleCheckFrequency),
			IdleTimeout:        time.Duration(s.IdleTimeout),
		}
		/* #nosec */
		if s.EnableTLS {
			options.TLSConfig = &tls.Config{
				InsecureSkipVerify: s.EnableTLS,
			}
		}

		return v8Client{client: v8.NewClusterClient(options)}
	}

	options := &v8.Options{
		Addr:               s.Host,
		Password:           s.Password,
		Username:           s.Username,
		DB:                 s.DB,
		MaxRetries:         s.RedisMaxRetries,
		MaxRetryBackoff:    time.Duration(s.RedisMaxRetryInterval),
		MinRetryBackoff:    time.Duration(s.RedisMinRetryInterval),
		DialTimeout:        time.Duration(s.DialTimeout),
		ReadTimeout:        time.Duration(s.ReadTimeout),
		WriteTimeout:       time.Duration(s.WriteTimeout),
		PoolSize:           s.PoolSize,
		MaxConnAge:         time.Duration(s.MaxConnAge),
		MinIdleConns:       s.MinIdleConns,
		PoolTimeout:        time.Duration(s.PoolTimeout),
		IdleCheckFrequency: time.Duration(s.IdleCheckFrequency),
		IdleTimeout:        time.Duration(s.IdleTimeout),
	}

	/* #nosec */
	if s.EnableTLS {
		options.TLSConfig = &tls.Config{
			InsecureSkipVerify: s.EnableTLS,
		}
	}

	return v8Client{client: v8.NewClient(options)}
}

func ClientFromV8Client(client v8.UniversalClient) RedisClient {
	return v8Client{client: client}
}
