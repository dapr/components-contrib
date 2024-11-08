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

	v9 "github.com/redis/go-redis/v9"
)

type v9Pipeliner struct {
	pipeliner    v9.Pipeliner
	writeTimeout Duration
}

func (p v9Pipeliner) Exec(ctx context.Context) error {
	_, err := p.pipeliner.Exec(ctx)
	return err
}

func (p v9Pipeliner) Do(ctx context.Context, args ...interface{}) {
	if p.writeTimeout > 0 {
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(p.writeTimeout))
		defer cancel()
		p.pipeliner.Do(timeoutCtx, args...)
	}
	p.pipeliner.Do(ctx, args...)
}

// v9Client is an interface implementation of RedisClient

type v9Client struct {
	client       v9.UniversalClient
	readTimeout  Duration
	writeTimeout Duration
	dialTimeout  Duration
}

func (c v9Client) GetDel(ctx context.Context, key string) (string, error) {
	return c.client.GetDel(ctx, key).Result()
}

func (c v9Client) DoWrite(ctx context.Context, args ...interface{}) error {
	if c.writeTimeout > 0 {
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(c.writeTimeout))
		defer cancel()
		return c.client.Do(timeoutCtx, args...).Err()
	}
	return c.client.Do(ctx, args...).Err()
}

func (c v9Client) DoRead(ctx context.Context, args ...interface{}) (interface{}, error) {
	if c.readTimeout > 0 {
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(c.readTimeout))
		defer cancel()
		return c.client.Do(timeoutCtx, args...).Result()
	}
	return c.client.Do(ctx, args...).Result()
}

func (c v9Client) Del(ctx context.Context, keys ...string) error {
	err := c.client.Del(ctx, keys...).Err()
	if err != nil {
		return err
	}
	return nil
}

func (c v9Client) ConfigurationSubscribe(ctx context.Context, args *ConfigurationSubscribeArgs) {
	// enable notify-keyspace-events by redis Set command
	// only subscribe to generic and string keyspace events
	c.DoWrite(ctx, "CONFIG", "SET", "notify-keyspace-events", "Kg$xe")

	var p *v9.PubSub
	if args.IsAllKeysChannel {
		p = c.client.PSubscribe(ctx, args.RedisChannel)
	} else {
		p = c.client.Subscribe(ctx, args.RedisChannel)
	}
	defer p.Close()
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-p.Channel():
			args.HandleSubscribedChange(ctx, args.Req, args.Handler, msg.Channel, args.ID)
		}
	}
}

func (c v9Client) Get(ctx context.Context, key string) (string, error) {
	return c.client.Get(ctx, key).Result()
}

func (c v9Client) GetNilValueError() RedisError {
	return RedisError(v9.Nil.Error())
}

func (c v9Client) Context() context.Context {
	return context.Background()
}

func (c v9Client) Close() error {
	return c.client.Close()
}

func (c v9Client) PingResult(ctx context.Context) (string, error) {
	if c.dialTimeout > 0 {
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(c.dialTimeout))
		defer cancel()
		return c.client.Ping(timeoutCtx).Result()
	}
	return c.client.Ping(ctx).Result()
}

func (c v9Client) EvalInt(ctx context.Context, script string, keys []string, args ...interface{}) (*int, error, error) {
	var evalCtx context.Context
	if c.readTimeout > 0 {
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(c.readTimeout))
		defer cancel()
		evalCtx = timeoutCtx
	} else {
		evalCtx = ctx
	}
	eval := c.client.Eval(evalCtx, script, keys, args...)
	if eval == nil {
		return nil, nil, nil
	}
	i, err := eval.Int()
	return &i, err, eval.Err()
}

func (c v9Client) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) (*bool, error) {
	var writeCtx context.Context
	if c.writeTimeout > 0 {
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(c.writeTimeout))
		defer cancel()
		writeCtx = timeoutCtx
	} else {
		writeCtx = ctx
	}
	nx := c.client.SetNX(writeCtx, key, value, expiration)
	if nx == nil {
		return nil, nil
	}
	val := nx.Val()
	return &val, nx.Err()
}

func (c v9Client) XAdd(ctx context.Context, stream string, maxLenApprox int64, values map[string]interface{}) (string, error) {
	var writeCtx context.Context
	if c.writeTimeout > 0 {
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(c.writeTimeout))
		defer cancel()
		writeCtx = timeoutCtx
	} else {
		writeCtx = ctx
	}
	return c.client.XAdd(writeCtx, &v9.XAddArgs{
		Stream: stream,
		Values: values,
		MaxLen: maxLenApprox,
		Approx: true,
	}).Result()
}

func (c v9Client) XGroupCreateMkStream(ctx context.Context, stream string, group string, start string) error {
	var writeCtx context.Context
	if c.writeTimeout > 0 {
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(c.writeTimeout))
		defer cancel()
		writeCtx = timeoutCtx
	} else {
		writeCtx = ctx
	}
	return c.client.XGroupCreateMkStream(writeCtx, stream, group, start).Err()
}

func (c v9Client) XAck(ctx context.Context, stream string, group string, messageID string) error {
	var readCtx context.Context
	if c.readTimeout > 0 {
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(c.readTimeout))
		defer cancel()
		readCtx = timeoutCtx
	} else {
		readCtx = ctx
	}
	ack := c.client.XAck(readCtx, stream, group, messageID)
	return ack.Err()
}

func (c v9Client) XReadGroupResult(ctx context.Context, group string, consumer string, streams []string, count int64, block time.Duration) ([]RedisXStream, error) {
	var readCtx context.Context
	if c.readTimeout > 0 {
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(c.readTimeout))
		defer cancel()
		readCtx = timeoutCtx
	} else {
		readCtx = ctx
	}
	res, err := c.client.XReadGroup(readCtx,
		&v9.XReadGroupArgs{
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

	// convert []v9.XStream to []RedisXStream
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

func (c v9Client) XPendingExtResult(ctx context.Context, stream string, group string, start string, end string, count int64) ([]RedisXPendingExt, error) {
	var readCtx context.Context
	if c.readTimeout > 0 {
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(c.readTimeout))
		defer cancel()
		readCtx = timeoutCtx
	} else {
		readCtx = ctx
	}
	res, err := c.client.XPendingExt(readCtx, &v9.XPendingExtArgs{
		Stream: stream,
		Group:  group,
		Start:  start,
		End:    end,
		Count:  count,
	}).Result()
	if err != nil {
		return nil, err
	}

	// convert []v9.XPendingExt to []RedisXPendingExt
	redisXPendingExts := make([]RedisXPendingExt, len(res))
	for i, xPendingExt := range res {
		redisXPendingExts[i] = RedisXPendingExt(xPendingExt)
	}
	return redisXPendingExts, nil
}

func (c v9Client) XClaimResult(ctx context.Context, stream string, group string, consumer string, minIdleTime time.Duration, messageIDs []string) ([]RedisXMessage, error) {
	var readCtx context.Context
	if c.readTimeout > 0 {
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(c.readTimeout))
		defer cancel()
		readCtx = timeoutCtx
	} else {
		readCtx = ctx
	}
	res, err := c.client.XClaim(readCtx, &v9.XClaimArgs{
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

func (c v9Client) TxPipeline() RedisPipeliner {
	return v9Pipeliner{
		pipeliner:    c.client.TxPipeline(),
		writeTimeout: c.writeTimeout,
	}
}

func (c v9Client) TTLResult(ctx context.Context, key string) (time.Duration, error) {
	var writeCtx context.Context
	if c.writeTimeout > 0 {
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(c.writeTimeout))
		defer cancel()
		writeCtx = timeoutCtx
	} else {
		writeCtx = ctx
	}
	return c.client.TTL(writeCtx, key).Result()
}

func (c v9Client) AuthACL(ctx context.Context, username, password string) error {
	pipeline := c.client.Pipeline()
	statusCmd := pipeline.AuthACL(ctx, username, password)
	return statusCmd.Err()
}

func newV9FailoverClient(s *Settings) (RedisClient, error) {
	if s == nil {
		return nil, nil
	}
	opts := &v9.FailoverOptions{
		DB:                    s.DB,
		MasterName:            s.SentinelMasterName,
		SentinelAddrs:         []string{s.Host},
		Password:              s.Password,
		Username:              s.Username,
		MaxRetries:            s.RedisMaxRetries,
		MaxRetryBackoff:       time.Duration(s.RedisMaxRetryInterval),
		MinRetryBackoff:       time.Duration(s.RedisMinRetryInterval),
		DialTimeout:           time.Duration(s.DialTimeout),
		ReadTimeout:           time.Duration(s.ReadTimeout),
		WriteTimeout:          time.Duration(s.WriteTimeout),
		PoolSize:              s.PoolSize,
		ConnMaxLifetime:       time.Duration(s.MaxConnAge),
		MinIdleConns:          s.MinIdleConns,
		PoolTimeout:           time.Duration(s.PoolTimeout),
		ConnMaxIdleTime:       time.Duration(s.IdleTimeout),
		ContextTimeoutEnabled: true,
	}

	/* #nosec */
	if s.EnableTLS {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: s.EnableTLS,
		}
		err := s.SetCertificate(func(cert *tls.Certificate) {
			opts.TLSConfig.Certificates = []tls.Certificate{*cert}
		})
		if err != nil {
			return nil, err
		}
	}

	if s.RedisType == ClusterType {
		opts.SentinelAddrs = strings.Split(s.Host, ",")

		return v9Client{
			client:       v9.NewFailoverClusterClient(opts),
			readTimeout:  s.ReadTimeout,
			writeTimeout: s.WriteTimeout,
			dialTimeout:  s.DialTimeout,
		}, nil
	}

	return v9Client{
		client:       v9.NewFailoverClient(opts),
		readTimeout:  s.ReadTimeout,
		writeTimeout: s.WriteTimeout,
		dialTimeout:  s.DialTimeout,
	}, nil
}

func newV9Client(s *Settings) (RedisClient, error) {
	if s == nil {
		return nil, nil
	}

	if s.RedisType == ClusterType {
		options := &v9.ClusterOptions{
			Addrs:                 strings.Split(s.Host, ","),
			Password:              s.Password,
			Username:              s.Username,
			MaxRetries:            s.RedisMaxRetries,
			MaxRetryBackoff:       time.Duration(s.RedisMaxRetryInterval),
			MinRetryBackoff:       time.Duration(s.RedisMinRetryInterval),
			DialTimeout:           time.Duration(s.DialTimeout),
			ReadTimeout:           time.Duration(s.ReadTimeout),
			WriteTimeout:          time.Duration(s.WriteTimeout),
			PoolSize:              s.PoolSize,
			ConnMaxLifetime:       time.Duration(s.MaxConnAge),
			MinIdleConns:          s.MinIdleConns,
			PoolTimeout:           time.Duration(s.PoolTimeout),
			ConnMaxIdleTime:       time.Duration(s.IdleTimeout),
			ContextTimeoutEnabled: true,
		}
		/* #nosec */
		if s.EnableTLS {
			/* #nosec */
			options.TLSConfig = &tls.Config{
				InsecureSkipVerify: s.EnableTLS,
			}
			err := s.SetCertificate(func(cert *tls.Certificate) {
				options.TLSConfig.Certificates = []tls.Certificate{*cert}
			})
			if err != nil {
				return nil, err
			}
		}

		return v9Client{
			client:       v9.NewClusterClient(options),
			readTimeout:  s.ReadTimeout,
			writeTimeout: s.WriteTimeout,
			dialTimeout:  s.DialTimeout,
		}, nil
	}

	options := &v9.Options{
		Addr:                  s.Host,
		Password:              s.Password,
		Username:              s.Username,
		DB:                    s.DB,
		MaxRetries:            s.RedisMaxRetries,
		MaxRetryBackoff:       time.Duration(s.RedisMaxRetryInterval),
		MinRetryBackoff:       time.Duration(s.RedisMinRetryInterval),
		DialTimeout:           time.Duration(s.DialTimeout),
		ReadTimeout:           time.Duration(s.ReadTimeout),
		WriteTimeout:          time.Duration(s.WriteTimeout),
		PoolSize:              s.PoolSize,
		ConnMaxLifetime:       time.Duration(s.MaxConnAge),
		MinIdleConns:          s.MinIdleConns,
		PoolTimeout:           time.Duration(s.PoolTimeout),
		ConnMaxIdleTime:       time.Duration(s.IdleTimeout),
		ContextTimeoutEnabled: true,
	}

	if s.EnableTLS {
		/* #nosec */
		options.TLSConfig = &tls.Config{
			InsecureSkipVerify: s.EnableTLS,
		}
		err := s.SetCertificate(func(cert *tls.Certificate) {
			options.TLSConfig.Certificates = []tls.Certificate{*cert}
		})
		if err != nil {
			return nil, err
		}
	}

	return v9Client{
		client:       v9.NewClient(options),
		readTimeout:  s.ReadTimeout,
		writeTimeout: s.WriteTimeout,
		dialTimeout:  s.DialTimeout,
	}, nil
}
