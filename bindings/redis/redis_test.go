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
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/components-contrib/bindings"
	rediscomponent "github.com/dapr/components-contrib/common/component/redis"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
)

const (
	testData = `{"data":"data"}`
	testKey  = "test"
)

func TestInvokeCreate(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	// miniRedis is compatible with the existing v8 client
	bind := &Redis{
		client: c,
		logger: logger.NewLogger("test"),
	}

	_, err := c.DoRead(context.Background(), "GET", testKey)
	assert.Equal(t, redis.Nil, err)

	bindingRes, err := bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Data:      []byte(testData),
		Metadata:  map[string]string{"key": testKey},
		Operation: bindings.CreateOperation,
	})
	require.NoError(t, err)
	assert.Nil(t, bindingRes)

	getRes, err := c.DoRead(context.Background(), "GET", testKey)
	require.NoError(t, err)
	assert.Equal(t, testData, getRes)
}

func TestInvokeGetWithoutDeleteFlag(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	bind := &Redis{
		client: c,
		logger: logger.NewLogger("test"),
	}

	err := c.DoWrite(context.Background(), "SET", testKey, testData)
	require.NoError(t, err)

	bindingRes, err := bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": testKey},
		Operation: bindings.GetOperation,
	})
	require.NoError(t, err)
	assert.Equal(t, testData, string(bindingRes.Data))

	bindingResGet, err := bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": testKey},
		Operation: bindings.GetOperation,
	})

	require.NoError(t, err)

	assert.Equal(t, testData, string(bindingResGet.Data))
}

func TestInvokeGetWithDeleteFlag(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	bind := &Redis{
		client: c,
		logger: logger.NewLogger("test"),
	}

	err := c.DoWrite(context.Background(), "SET", testKey, testData)
	require.NoError(t, err)

	bindingRes, err := bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": testKey, "delete": "true"},
		Operation: bindings.GetOperation,
	})
	require.NoError(t, err)
	assert.Equal(t, testData, string(bindingRes.Data))

	bindingResGet, err := bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": testKey},
		Operation: bindings.GetOperation,
	})

	require.NoError(t, err)

	assert.Equal(t, []byte(nil), bindingResGet.Data)
}

func TestInvokeDelete(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	bind := &Redis{
		client: c,
		logger: logger.NewLogger("test"),
	}

	err := c.DoWrite(context.Background(), "SET", testKey, testData)
	require.NoError(t, err)

	getRes, err := c.DoRead(context.Background(), "GET", testKey)
	require.NoError(t, err)
	assert.Equal(t, testData, getRes)

	_, err = bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": testKey},
		Operation: bindings.DeleteOperation,
	})

	require.NoError(t, err)

	rgetRep, err := c.DoRead(context.Background(), "GET", testKey)
	assert.Equal(t, redis.Nil, err)
	assert.Nil(t, rgetRep)
}

func TestCreateExpire(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	bind := &Redis{
		client: c,
		logger: logger.NewLogger("test"),
	}
	_, err := bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": testKey, metadata.TTLMetadataKey: "1"},
		Operation: bindings.CreateOperation,
		Data:      []byte(testData),
	})
	require.NoError(t, err)

	rgetRep, err := c.DoRead(context.Background(), "TTL", testKey)
	require.NoError(t, err)
	assert.Equal(t, int64(1), rgetRep)

	res, err2 := bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": testKey},
		Operation: bindings.GetOperation,
	})
	require.NoError(t, err2)
	assert.Equal(t, res.Data, []byte(testData))

	// wait for ttl to expire
	s.FastForward(2 * time.Second)

	res, err2 = bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": testKey},
		Operation: bindings.GetOperation,
	})
	require.NoError(t, err2)
	assert.Equal(t, []byte(nil), res.Data)

	_, err = bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": testKey},
		Operation: bindings.DeleteOperation,
	})
	require.NoError(t, err)
}

func TestIncrement(t *testing.T) {
	s, c := setupMiniredis()
	defer s.Close()

	bind := &Redis{
		client: c,
		logger: logger.NewLogger("test"),
	}
	_, err := bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": "incKey"},
		Operation: IncrementOperation,
	})
	require.NoError(t, err)

	res, err2 := bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": "incKey"},
		Operation: bindings.GetOperation,
	})
	assert.Nil(t, nil, err2)
	assert.Equal(t, res.Data, []byte("1"))

	_, err = bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": "incKey", metadata.TTLMetadataKey: "5"},
		Operation: IncrementOperation,
	})
	require.NoError(t, err)

	rgetRep, err := c.DoRead(context.Background(), "TTL", "incKey")
	require.NoError(t, err)
	assert.Equal(t, int64(5), rgetRep)

	res, err2 = bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": "incKey"},
		Operation: bindings.GetOperation,
	})
	require.NoError(t, err2)
	assert.Equal(t, []byte("2"), res.Data)

	// wait for ttl to expire
	s.FastForward(10 * time.Second)

	res, err2 = bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": "incKey"},
		Operation: bindings.GetOperation,
	})
	require.NoError(t, err2)
	assert.Equal(t, []byte(nil), res.Data)

	_, err = bind.Invoke(context.TODO(), &bindings.InvokeRequest{
		Metadata:  map[string]string{"key": "incKey"},
		Operation: bindings.DeleteOperation,
	})
	require.NoError(t, err)
}

func setupMiniredis() (*miniredis.Miniredis, rediscomponent.RedisClient) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	opts := &redis.Options{
		Addr: s.Addr(),
		DB:   0,
	}

	return s, rediscomponent.ClientFromV8Client(redis.NewClient(opts))
}
